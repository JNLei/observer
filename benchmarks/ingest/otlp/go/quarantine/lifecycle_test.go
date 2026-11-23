package quarantine

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func neverSend(t *testing.T) sender {
	return func(delivery) outcome { t.Fatal("forbidden network dispatch"); return outcome{} }
}

func TestScenario6BoundReplay(t *testing.T) {
	f := fresh(t, fixtureLimits())
	x := f.offer(t, "a", f.row(7))
	y := f.offer(t, "a", f.row(8))
	f.act(t, instruction("park", x.Ref))
	f.act(t, instruction("park", y.Ref))
	c := instruction("replay", x.Ref)
	f.act(t, c)
	a, b := f.token("a.operator"), f.token("b.operator")
	for _, action := range []string{"park", "replay", "dispose"} {
		_, err := f.decide(b, instruction(action, x.Ref))
		wantErr(t, err, denied)
	}
	for _, who := range []string{"b.custodian", "a.auditor", "a.operator"} {
		for _, right := range []string{"payload", "export"} {
			_, p, err := f.read(f.token(who), right, x.Ref)
			wantErr(t, err, denied)
			check(t, p == nil, "protected payload disclosed")
		}
	}
	_, _, err := f.read(f.token("b.auditor"), "inspect", x.Ref)
	wantErr(t, err, denied)
	tampered := c
	tampered.Ref = y.Ref
	wantErr(t, f.drive(a, &tampered, neverSend(t)), denied)
	wantErr(t, f.drive(b, &c, neverSend(t)), denied)
	old := f.auth[a]
	f.setGrant(a, grant{})
	wantErr(t, f.drive(a, &c, neverSend(t)), denied)
	_, err = f.decide(a, c)
	wantErr(t, err, denied)
	f.setGrant(a, old)
	start, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- f.drive(a, &c, func(w delivery) outcome {
			close(start)
			<-release
			if w.Ref != x.Ref || digest(w.Payload) != x.Ref.Digest {
				return outcome{Kind: "wrong-fixture"}
			}
			return outcome{Kind: "unknown"}
		})
	}()
	<-start
	check(t, f.act(t, c).Outcome == "sending", "repeat did not find original in-flight intent")
	wantErr(t, f.drive(a, &c, neverSend(t)), blocked)
	_, err = f.decide(a, instruction("replay", x.Ref))
	wantErr(t, err, blocked)
	_, err = f.decide(a, instruction("dispose", x.Ref))
	wantErr(t, err, blocked)
	close(release)
	must(t, <-done)
	f.restart(t)
	check(t, f.act(t, c).Outcome == "unknown", "uncertain replay was not retained")
	wantErr(t, f.drive(a, &c, neverSend(t)), blocked)
	retry := instruction("replay", x.Ref)
	_, err = f.decide(a, retry)
	wantErr(t, err, blocked)
	retry.Previous = c.ID
	f.act(t, retry)
	calls := 0
	must(t, f.drive(a, &retry, func(w delivery) outcome {
		calls++
		check(t, w.Ref == x.Ref && w.Replay == retry.ID && bytes.Equal(w.Payload, expectedWire(t, []sample{f.row(7)})), "replay lost lineage or original bytes")
		return outcome{Kind: "ack"}
	}))
	wantErr(t, f.drive(a, &retry, neverSend(t)), blocked)
	s, err := f.summary(f.token("a.auditor"))
	must(t, err)
	check(t, s.ReplayOutcomes["unknown"] == 1 && s.ReplayOutcomes["acknowledged"] == 1, "tenant replay summary differs from intents")
	check(t, calls == 1 && f.act(t, retry).Outcome == "acknowledged" && f.state(t, x.Ref).UnknownRecordAttempts == 1, "incorrect replay accounting")
	f.audit(t, map[ref]string{x.Ref: "parked", y.Ref: "parked"})
}
func TestScenario6CaptureAndEgress(t *testing.T) {
	f := fresh(t, fixtureLimits())
	r := f.row(3)
	r.Body = "PRIVATE-BODY-CANARY"
	r.ClaimedService = "FORGED-SERVICE-CANARY"
	r.ClaimedTenant = "b"
	x := f.offer(t, "a", r)
	check(t, x.Ref.Tenant == "a" && f.state(t, x.Ref).Producer == "a.source", "trusted tenant or producer came from input")
	fake := x.Ref
	fake.Tenant = "b"
	_, p, err := f.read(f.token("b.custodian"), "export", fake)
	wantErr(t, err, conflict)
	check(t, p == nil, "forged claim granted egress")
	_, err = f.enqueue("unverified", newID(), []sample{r})
	wantErr(t, err, denied)
	expired := r
	expired.Event = f.nowTime().Add(-captureWindow).UnixNano()
	_, err = f.enqueue(f.token("a.source"), newID(), []sample{expired})
	wantErr(t, err, invalid)
	f.act(t, instruction("park", x.Ref))
	c := instruction("replay", x.Ref)
	f.act(t, c)
	f.clock.Add(int64(replayWindow + time.Second))
	wantErr(t, f.drive(f.token("a.operator"), &c, neverSend(t)), blocked)
	f.clock.Add(int64(captureWindow))
	_, p, err = f.read(f.token("a.custodian"), "payload", x.Ref)
	wantErr(t, err, blocked)
	check(t, p == nil, "expired protected bytes returned")
	files, err := filepath.Glob(filepath.Join(f.root, "external", "*"))
	must(t, err)
	scanPrivate(t, append(files, f.dbPath), r.Body, r.ClaimedService, f.token("a.source"), f.token("a.operator"))
}
func TestScenario7HoldDisposalAndRestore(t *testing.T) {
	f := fresh(t, fixtureLimits())
	x := f.offer(t, "a", f.row(1))
	y := f.offer(t, "a", f.row(2))
	z := f.offer(t, "b", f.row(3))
	// The backup has all three still active and predates even the first park.
	backup := filepath.Join(f.root, "old.db")
	must(t, f.db.View(func(tx *bolt.Tx) error { return tx.CopyFile(backup, 0600) }))
	staleEvidence, err := os.ReadFile(f.sealPath + ".catalog")
	must(t, err)
	worker := f.token("a.source")
	must(t, f.drive(worker, nil, func(delivery) outcome { return outcome{Kind: "unknown"} }))
	f.act(t, instruction("park", x.Ref))
	f.act(t, instruction("park", y.Ref))
	f.act(t, instruction("hold", x.Ref))
	f.act(t, instruction("hold", y.Ref))
	f.clock.Add(int64(reviewWindow + time.Second))
	s, err := f.summary(f.token("a.auditor"))
	must(t, err)
	check(t, s.ReviewDue == 2 && s.Held == 2, "review deadline or legal holds missing")
	dispose := instruction("dispose", x.Ref)
	_, err = f.decide(f.token("a.operator"), dispose)
	wantErr(t, err, blocked)
	f.act(t, instruction("release", x.Ref))
	replay := instruction("replay", x.Ref)
	f.act(t, replay)
	disposed := f.act(t, dispose)
	again := f.act(t, dispose)
	check(t, disposed == again && f.act(t, replay).Outcome == "revoked", "disposal or pending-intent revocation was not idempotent")
	old, err := os.ReadFile(backup)
	must(t, err)
	must(t, f.close())
	f.owner = nil
	must(t, os.WriteFile(f.dbPath, old, 0600))
	f.reopen(t)
	f.assertFenced(t, x.Ref)
	wantErr(t, f.reconcile(false), fenced)
	saved, err := os.ReadFile(f.sealPath)
	must(t, err)
	must(t, os.Remove(f.sealPath))
	for range 2 {
		f.reopen(t)
		wantErr(t, f.reconcile(true), fenced)
		f.assertFenced(t, y.Ref)
	}
	must(t, os.WriteFile(f.sealPath, []byte("{"), 0600))
	wantErr(t, f.reconcile(true), fenced)
	f.assertFenced(t, x.Ref)
	must(t, os.WriteFile(f.sealPath, saved, 0600))
	current, err := os.ReadFile(f.sealPath + ".catalog")
	must(t, err)
	must(t, os.WriteFile(f.sealPath+".catalog", staleEvidence, 0600))
	wantErr(t, f.reconcile(true), fenced)
	f.assertFenced(t, x.Ref)
	must(t, os.Remove(f.sealPath+".catalog"))
	wantErr(t, f.reconcile(true), fenced)
	f.assertFenced(t, x.Ref)
	must(t, os.WriteFile(f.sealPath+".catalog", current, 0600))
	must(t, f.close())
	f.owner = nil
	crash(t, f, dispose, "restore-staged")
	f.reopen(t)
	f.assertFenced(t, x.Ref)
	wantErr(t, f.reconcile(false), fenced)
	must(t, f.reconcile(true))
	f.audit(t, map[ref]string{x.Ref: "disposed", y.Ref: "parked", z.Ref: "active"})
	e := f.state(t, x.Ref)
	check(t, e.UnknownRecordAttempts == 1 && e.Disposal == dispose.ID && f.act(t, dispose) == disposed, "restored disposition lost audit or uncertainty")
	_, p, err := f.read(f.token("a.custodian"), "export", x.Ref)
	wantErr(t, err, blocked)
	check(t, p == nil, "restored deletion was exported")
	wantErr(t, f.drive(f.token("a.operator"), &replay, neverSend(t)), blocked)
	_, err = f.decide(f.token("a.operator"), instruction("dispose", y.Ref))
	wantErr(t, err, blocked)
	_, p, err = f.read(f.token("a.custodian"), "payload", y.Ref)
	must(t, err)
	check(t, digest(p) == y.Ref.Digest, "retained payload missing after reconciliation")
	f.restart(t)
	f.audit(t, map[ref]string{x.Ref: "disposed", y.Ref: "parked", z.Ref: "active"})
}
func TestScenario7IncompleteBackupRemainsFenced(t *testing.T) {
	f := fresh(t, fixtureLimits())
	x := f.offer(t, "a", f.row(1))
	backup := filepath.Join(f.root, "old.db")
	must(t, f.db.View(func(tx *bolt.Tx) error { return tx.CopyFile(backup, 0600) }))
	f.offer(t, "b", f.row(2))
	must(t, f.close())
	f.owner = nil
	b, err := os.ReadFile(backup)
	must(t, err)
	must(t, os.WriteFile(f.dbPath, b, 0600))
	f.reopen(t)
	wantErr(t, f.reconcile(true), fenced)
	f.assertFenced(t, x.Ref)
}

func TestScenario2RestoredCopyCannotRaceOwner(t *testing.T) {
	f := fresh(t, fixtureLimits())
	x := f.offer(t, "a", f.row(1))
	copy := filepath.Join(f.root, "copy.db")
	must(t, f.db.View(func(tx *bolt.Tx) error { return tx.CopyFile(copy, 0600) }))
	for _, path := range []string{f.dbPath, copy} {
		o, err := openOwner(path, f.sealPath, false, f.auth, f.cfg, f.nowTime, f.transport)
		if o != nil {
			o.close()
			t.Fatal("second owner opened the same restore lineage")
		}
		wantErr(t, err, blocked)
	}
	f.act(t, instruction("park", x.Ref))
	f.audit(t, map[ref]string{x.Ref: "parked"})
}
func TestScenario6ReplayLeaseSurvivesCrash(t *testing.T) {
	f := fresh(t, fixtureLimits())
	x := f.offer(t, "a", f.row(1))
	f.act(t, instruction("park", x.Ref))
	c := instruction("replay", x.Ref)
	f.act(t, c)
	must(t, f.close())
	f.owner = nil
	crash(t, f, c, "replay-handed-off")
	crash(t, f, c, "lease-recovery-staged")
	f.reopen(t)
	must(t, f.reconcile(false))
	check(t, f.act(t, c).Outcome == "unknown" && f.state(t, x.Ref).UnknownRecordAttempts == 1, "crashed replay lease was rearmed")
	wantErr(t, f.drive(f.token("a.operator"), &c, neverSend(t)), blocked)
	next := instruction("replay", x.Ref)
	_, err := f.decide(f.token("a.operator"), next)
	wantErr(t, err, blocked)
	next.Previous = c.ID
	f.act(t, next)
	must(t, f.drive(f.token("a.operator"), &next, func(delivery) outcome { return outcome{Kind: "ack"} }))
	f.audit(t, map[ref]string{x.Ref: "parked"})
}

func TestScenario6DisposalRequiresBoundAuthority(t *testing.T) {
	f := fresh(t, fixtureLimits())
	x := f.offer(t, "a", f.row(1))
	other := f.offer(t, "a", f.row(1))
	for _, r := range []ref{x.Ref, other.Ref} {
		f.act(t, instruction("park", r))
	}
	dispose := instruction("dispose", x.Ref)
	a := f.token("a.operator")
	bad := dispose
	bad.Ref.Digest = newID()
	_, err := f.decide(a, bad)
	wantErr(t, err, conflict)
	g := f.auth[a]
	f.setGrant(a, grant{})
	_, err = f.decide(a, dispose)
	wantErr(t, err, denied)
	f.setGrant(a, g)
	saved := f.act(t, dispose)
	different := dispose
	different.Ref = other.Ref
	_, err = f.decide(a, different)
	wantErr(t, err, conflict)
	different = dispose
	different.Action = "replay"
	different.Reason = reasons["replay"]
	_, err = f.decide(a, different)
	wantErr(t, err, conflict)
	f.restart(t)
	check(t, f.act(t, dispose) == saved, "disposal replay changed the audit record")
	f.audit(t, map[ref]string{x.Ref: "disposed", other.Ref: "parked"})
}

func TestScenario3KnownReplayCredentialFailure(t *testing.T) {
	f := fresh(t, fixtureLimits())
	x := f.offer(t, "a", f.row(1))
	f.act(t, instruction("park", x.Ref))
	cmd := instruction("replay", x.Ref)
	f.act(t, cmd)
	a := f.token("a.operator")
	edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) }))
	defer edge.Close()
	r := newReceiver(edge.URL, map[string]string{"a": newID()})
	must(t, f.drive(a, &cmd, r.send))
	e := f.state(t, x.Ref)
	check(t, e.CredentialHold && e.UnknownRecordAttempts == 0 && f.act(t, cmd).Outcome == "credentials", "known 403 was treated as ambiguous")
	next := instruction("replay", x.Ref)
	next.Previous = cmd.ID
	_, err := f.decide(a, next)
	wantErr(t, err, blocked)
	f.act(t, instruction("resume", x.Ref))
	f.act(t, next)
	must(t, f.drive(a, &next, func(delivery) outcome { return outcome{Kind: "ack"} }))
	f.audit(t, map[ref]string{x.Ref: "parked"})
}
