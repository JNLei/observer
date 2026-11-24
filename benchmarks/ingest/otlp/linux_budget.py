"""Run one fixture in its own delegated cgroup, mount and network namespaces."""
import contextlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys

GIB = 1024 ** 3
BANDWIDTH = 50 * 1024 ** 2
# CPU quota (cores), hard memory limit (GiB). Source and store also get private ext4.
BUDGETS = {"driver": (1, 1), "receiver": (4, 1), "collector": (2, 2), "loki": (2, 2)}


def output(*cmd):
    return subprocess.check_output(cmd, text=True).strip()


def pairs(path):
    return {k: int(v) for k, v in (line.split() for line in path.read_text().splitlines())}


def resource_sample(groups, disks):
    result = {}
    for role, group in groups.items():
        memory = pairs(group / "memory.stat")
        pids = (group / "cgroup.procs").read_text().split()
        processes = []
        for pid in pids:
            try:
                status = Path(f"/proc/{pid}/status").read_text().splitlines()
            except FileNotFoundError:
                continue
            processes.append({k: v.strip() for k, v in (s.split(":", 1) for s in status)
                              if k in ("Name", "Threads", "VmRSS", "VmHWM")})
        item = result[role] = {
            "cpu": pairs(group / "cpu.stat"), "memory_current": int((group / "memory.current").read_text()),
            "memory_peak": int((group / "memory.peak").read_text()), "events": pairs(group / "memory.events"),
            "memory": {k: memory.get(k) for k in ("anon", "file", "kernel", "file_dirty", "file_writeback")},
            "pressure": {k: (group / f"{k}.pressure").read_text().strip() for k in ("cpu", "memory", "io")},
            "io": (group / "io.stat").read_text().strip(), "processes": processes}
        if role in disks:
            stat = os.statvfs(disks[role])
            item["fs_used_bytes"] = (stat.f_blocks - stat.f_bfree) * stat.f_frsize
    result["loopback_tx_bytes"] = int(Path("/sys/class/net/lo/statistics/tx_bytes").read_text())
    return result


def run(directory, controls, uid, gid, command):
    # systemd delegates this new, uniquely named scope. Never edit a host/root cgroup or qdisc.
    relative = Path(Path("/proc/self/cgroup").read_text().strip().split("::", 1)[1])
    group = Path("/sys/fs/cgroup") / relative.relative_to("/")
    if not (group.name.startswith("m1-logs-") and group.suffix == ".scope"):
        raise RuntimeError("expected a fresh m1-logs systemd scope")
    if any(os.stat(f"/proc/self/ns/{ns}").st_ino == os.stat(f"/proc/1/ns/{ns}").st_ino for ns in ("mnt", "net")):
        raise RuntimeError("private network and mount namespaces required")
    if uid == 0 or directory.stat().st_uid != uid:
        raise RuntimeError("run the outer benchmark as its non-root owner")

    groups = {role: group / role for role in BUDGETS}
    for path in groups.values():
        path.mkdir()
    (groups["driver"] / "cgroup.procs").write_text(str(os.getpid()))
    (group / "cgroup.subtree_control").write_text("+cpu +memory +io")
    os.chown(group / "cgroup.procs", uid, gid)
    subprocess.run(["ip", "link", "set", "lo", "up"], check=True)
    subprocess.run(["tc", "qdisc", "add", "dev", "lo", "root", "tbf", "rate", "100mbit",
                    "burst", "128kb", "latency", "100ms"], check=True)
    qdisc = json.loads(output("tc", "-j", "qdisc", "show", "dev", "lo"))
    if not any(q["kind"] == "tbf" and q["options"]["rate"] == 12_500_000 for q in qdisc):
        raise RuntimeError("loopback rate readback mismatch")
    evidence = {"kernel": platform.release(), "machine": platform.machine(), "cpus": os.cpu_count(),
                "cpu_model": output("lscpu"), "qdisc": qdisc, "roles": {}, "filesystem": "ext4",
                "affinity": sorted(os.sched_getaffinity(0)), "meminfo": Path("/proc/meminfo").read_text().splitlines()[0],
                "backing_mount": output("findmnt", "-n", "-o", "SOURCE,FSTYPE,MAJ:MIN", "-T", directory)}
    evidence["ancestors"] = {str(p): {k: (p/k).read_text().strip() for k in ("cpu.max", "memory.max") if (p/k).exists()}
                             for p in (group, *group.parents) if p.is_relative_to("/sys/fs/cgroup")}
    if len(evidence["affinity"]) < 4:
        raise RuntimeError("four available logical CPUs required for this profile")
    with contextlib.ExitStack() as cleanup:
        for role, (cores, memory) in BUDGETS.items():
            path = groups[role]
            os.chown(path / "cgroup.procs", uid, gid)
            settings = {"cpu.max": f"{cores * 100000} 100000", "memory.max": str(memory * GIB), "memory.swap.max": "0"}
            for key, value in settings.items():
                (path / key).write_text(value)
                if (path / key).read_text().strip() != value:
                    raise RuntimeError(f"{role} {key} limit readback mismatch")
            info = evidence["roles"][role] = {**settings, "cgroup": str(path)}
            if role in ("collector", "loki"):
                disk = directory / role
                disk.mkdir()
                image = directory / (role + ".ext4")
                with image.open("xb") as file:
                    file.truncate(GIB)
                subprocess.run(["mkfs.ext4", "-q", "-F", "-E", "lazy_itable_init=0,lazy_journal_init=0", image], check=True)
                subprocess.run(["mount", "-o", "loop,nodev,nosuid,noexec", image, disk], check=True)
                cleanup.callback(subprocess.run, ["umount", disk], check=True)
                device = output("findmnt", "-n", "-o", "MAJ:MIN", "-T", disk)
                (path / "io.max").write_text(f"{device} rbps={BANDWIDTH} wbps={BANDWIDTH}")
                readback = (path / "io.max").read_text().split()
                if not {device, f"rbps={BANDWIDTH}", f"wbps={BANDWIDTH}"} <= set(readback):
                    raise RuntimeError(f"{role} disk rate readback mismatch")
                os.chown(disk, uid, gid)
                stat = os.statvfs(disk)
                info.update(disk_bytes=image.stat().st_size, device=device, io_max=" ".join(readback),
                            empty_fs_used_bytes=(stat.f_blocks-stat.f_bfree)*stat.f_frsize)
        controls.write_text(json.dumps(evidence, indent=2) + "\n")
        os.chown(controls, uid, gid)
        return subprocess.run(command, user=uid, group=gid, extra_groups=[]).returncode


if __name__ == "__main__":
    raise SystemExit(run(Path(sys.argv[1]), Path(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5:]))
