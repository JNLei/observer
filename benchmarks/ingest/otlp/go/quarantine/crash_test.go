package quarantine

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

type crashPlan struct {
	Root, Point string
	Cmd         command
	Clock       int64
}

// Only the parent delivers the expected IDs/digests and fault selection. The
// helper cannot decide its expected result by looking at the tested database.
func TestCustodyProcess(t *testing.T) {
	raw := os.Getenv("QUARANTINE_CHILD")
	if raw == "" {
		t.Skip("subprocess entry point")
	}
	var p crashPlan
	must(t, json.Unmarshal([]byte(raw), &p))
	f := setup(t, p.Root, os.Getenv("QUARANTINE_CHILD_SECRET"), false, fixtureLimits(), time.Unix(0, p.Clock))
	kill := func() { must(t, syscall.Kill(os.Getpid(), syscall.SIGKILL)); select {} }
	f.fault = func(point string) error {
		if point == p.Point {
			kill()
		}
		if point == "before-commit" {
			must(t, os.Setenv("QUARANTINE_BOLT_POINT", p.Point))
		}
		return nil
	}
	must(t, f.reconcile(p.Point == "restore-staged"))
	if p.Point == "replay-handed-off" {
		must(t, f.drive(f.token("a.operator"), &p.Cmd, func(delivery) outcome { kill(); return outcome{} }))
	}
	if p.Point == "offline-dispatch" {
		var statuses []int
		must(t, f.drive(f.token("a.source"), nil, observedHTTP(t, liveReceiver().send, 503, &statuses)))
		kill()
	}
	f.act(t, p.Cmd)
	if p.Point == "before-receipt-delivery" {
		kill()
	}
	t.Fatal("requested crash point was not reached")
}
func crash(t *testing.T, f *lab, c command, point string) {
	t.Helper()
	p, err := json.Marshal(crashPlan{f.root, point, c, f.clock.Load()})
	must(t, err)
	cmd := exec.Command(os.Args[0], "-test.run=^TestCustodyProcess$", "-test.timeout=15s")
	cmd.Env = append(os.Environ(), "QUARANTINE_CHILD="+string(p), "QUARANTINE_CHILD_SECRET="+f.secret)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("fault %s did not SIGKILL the owner: %v %s", point, err, out)
	}
}
func TestScenario1RestartCustody(t *testing.T) {
	points := []string{"before-transfer", "transfer-staged", "before-commit", "after-commit", "before-receipt-delivery"}
	if os.Getenv("QUARANTINE_INSTRUMENTED") == "1" {
		points = append(points, "beforeSyncDataPages", "beforeSyncMetaPage")
	}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			f := fresh(t, fixtureLimits())
			x := f.offer(t, "a", f.row(1))
			other := f.offer(t, "a", f.row(1))
			c := instruction("park", x.Ref)
			must(t, f.close())
			f.owner = nil
			crash(t, f, c, point)
			crash(t, f, c, "during-recovery")
			f.reopen(t)
			f.assertFenced(t, x.Ref)
			must(t, f.reconcile(false))
			state := "active"
			// At the metadata sync boundary the page write has already returned.
			// An intact host after a process kill retains that write in its cache.
			if point == "after-commit" || point == "before-receipt-delivery" || point == "beforeSyncMetaPage" {
				state = "parked"
			}
			recovered := f.audit(t, map[ref]string{x.Ref: state, other.Ref: "active"})
			prior := f.state(t, x.Ref).Receipt
			d := f.act(t, c)
			saved := d.Receipt
			check(t, saved == *f.state(t, x.Ref).Receipt, "delivered receipt differs from durable custody")
			if prior != nil {
				check(t, *prior == saved, "lost receipt was replaced")
			}
			f.restart(t)
			check(t, f.act(t, c) == d && *f.state(t, x.Ref).Receipt == saved, "idempotent result changed across restart")
			retried := f.audit(t, map[ref]string{x.Ref: "parked", other.Ref: "active"})
			if dir := os.Getenv("QUARANTINE_CUT_EVIDENCE"); dir != "" {
				b, err := json.Marshal(map[string]any{"fault": point, "signals": []string{"SIGKILL", "SIGKILL during recovery"}, "reopened": recovered, "after_repeated_command": retried})
				must(t, err)
				must(t, os.WriteFile(filepath.Join(dir, point+".json"), b, 0600))
			}
			t.Logf("SIGKILL at %s and during recovery; two external identities reconciled; recovered=%s; one transfer charged", point, state)
		})
	}
}

func TestScenario2IdentityAndContention(t *testing.T) {
	f := fresh(t, fixtureLimits())
	x := f.offer(t, "a", f.row(1))
	y := f.offer(t, "a", f.row(1))
	check(t, x.Ref.ID != y.Ref.ID && x.Ref.Digest == y.Ref.Digest, "identical input fixture failed")
	_, err := f.enqueue(f.token("a.source"), x.Ref.ID, []sample{f.row(2)})
	wantErr(t, err, conflict)
	c := instruction("park", x.Ref)
	w, err := f.claim(f.token("a.source"), nil)
	must(t, err)
	_, err = f.decide(f.token("a.operator"), c)
	wantErr(t, err, blocked)
	f.restart(t)
	again, err := f.claim(f.token("a.source"), nil)
	must(t, err)
	wantErr(t, f.finish(w, outcome{Kind: "ack"}), conflict)
	must(t, f.finish(again, outcome{Kind: "unknown"}))
	start := make(chan struct{})
	results := make(chan error, 8)
	for range 8 {
		go func() { <-start; _, err := f.decide(f.token("a.operator"), c); results <- err }()
	}
	close(start)
	for range 8 {
		must(t, <-results)
	}
	saved := *f.state(t, x.Ref).Receipt
	check(t, saved.UnknownRecordAttempts == 2, "stale or unresolved sends lost historical uncertainty")
	wantErr(t, f.finish(w, outcome{Kind: "ack"}), conflict)
	for _, alter := range []func(*command){func(c *command) { c.Ref.Digest = y.Ref.ID }, func(c *command) { c.Ref.Generation = newID() }, func(c *command) { c.Ref = y.Ref }, func(c *command) { c.Action = "dispose"; c.Reason = reasons["dispose"] }} {
		bad := c
		alter(&bad)
		_, err := f.decide(f.token("a.operator"), bad)
		wantErr(t, err, conflict)
	}
	f.restart(t)
	f.act(t, c)
	check(t, *f.state(t, x.Ref).Receipt == saved, "receipt was replaced")
	f.act(t, instruction("park", y.Ref))
	f.audit(t, map[ref]string{x.Ref: "parked", y.Ref: "parked"})
}
func TestScenario1UnverifiableReceipt(t *testing.T) {
	for _, bad := range []string{"foreign-entry", "foreign-tenant", "unreadable"} {
		t.Run(bad, func(t *testing.T) {
			f := fresh(t, fixtureLimits())
			x := f.offer(t, "a", f.row(4))
			f.act(t, instruction("park", x.Ref))
			must(t, f.db.Update(func(tx *bolt.Tx) error {
				c, _, err := readCatalog(tx)
				if err != nil {
					return err
				}
				r := c.Entries[x.Ref.key()].Receipt
				if bad == "foreign-entry" {
					r.Ref.ID = newID()
				} else {
					r.Ref.Tenant = "b"
				}
				b, err := json.Marshal(c)
				if err != nil {
					return err
				}
				if bad == "unreadable" {
					b = []byte("{")
				}
				return tx.Bucket(meta).Put(dataKey, b)
			}))
			f.reopen(t)
			wantErr(t, f.reconcile(false), fenced)
			f.assertFenced(t, x.Ref)
		})
	}
}
