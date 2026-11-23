package quarantine

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	berrors "go.etcd.io/bbolt/errors"
)

func TestScenario5TenantAndActiveLimits(t *testing.T) {
	l := fixtureLimits()
	l.Parked.Tenant.Entries = 1
	l.Active.Tenant.Entries = 1
	f := fresh(t, l)
	x := f.offer(t, "a", f.row(1))
	first := f.act(t, instruction("park", x.Ref))
	y := f.offer(t, "a", f.row(2))
	cmd := instruction("park", y.Ref)
	_, err := f.decide(f.token("a.operator"), cmd)
	wantErr(t, err, full)
	s, err := f.summary(f.token("a.auditor"))
	must(t, err)
	check(t, s.FailedTransfersThisProcess == 1, "failed transfer signal missing")
	repeated := f.act(t, instruction("park", x.Ref))
	check(t, first.Receipt == repeated.Receipt, "full quota changed the original receipt")
	r, err := f.enqueue(f.token("a.source"), newID(), []sample{f.row(3)})
	wantErr(t, err, full)
	check(t, r == (ref{}), "acknowledgement on active exhaustion")
	z := f.offer(t, "b", f.row(3))
	f.act(t, instruction("park", z.Ref))
	f.audit(t, map[ref]string{x.Ref: "parked", y.Ref: "active", z.Ref: "parked"})
	f.act(t, instruction("dispose", x.Ref))
	d := f.act(t, cmd)
	check(t, f.act(t, cmd) == d, "retry consumed quota twice")
	f.audit(t, map[ref]string{x.Ref: "disposed", y.Ref: "parked", z.Ref: "parked"})
}
func TestScenario5ReservationsAndLastSlot(t *testing.T) {
	for _, dimension := range []string{"entries", "bytes"} {
		t.Run(dimension, func(t *testing.T) {
			f := fresh(t, fixtureLimits())
			x := f.offer(t, "a", f.row(1))
			y := f.offer(t, "a", f.row(1))
			z := f.offer(t, "b", f.row(1))
			a := f.offer(t, "a", f.row(1))
			b := f.offer(t, "b", f.row(1))
			// Two shared slots and one guaranteed slot for B. Start with the
			// global limit at two so B's unused reservation already binds A.
			p := &f.limits.Parked
			p.Reserved = amount{}
			if dimension == "entries" {
				p.Global.Entries = 2
				p.Reserved.Entries = 1
			} else {
				p.Global.Bytes = 2 * x.Size
				p.Reserved.Bytes = x.Size
			}
			f.act(t, instruction("park", x.Ref))
			_, err := f.decide(f.token("a.operator"), instruction("park", y.Ref))
			wantErr(t, err, full)
			f.act(t, instruction("park", z.Ref))
			_, err = f.decide(f.token("b.operator"), instruction("park", b.Ref))
			wantErr(t, err, full)
			if dimension == "entries" {
				p.Global.Entries++
			} else {
				p.Global.Bytes += x.Size
			}
			ready := make(chan struct{})
			type result struct {
				r   ref
				err error
			}
			done := make(chan result, 2)
			for _, who := range []ref{a.Ref, b.Ref} {
				go func() {
					<-ready
					_, err := f.decide(f.token(who.Tenant+".operator"), instruction("park", who))
					done <- result{who, err}
				}()
			}
			close(ready)
			states := map[ref]string{x.Ref: "parked", z.Ref: "parked", a.Ref: "active", b.Ref: "active", y.Ref: "active"}
			winners := 0
			for range 2 {
				r := <-done
				if r.err == nil {
					winners++
					states[r.r] = "parked"
				} else {
					wantErr(t, r.err, full)
				}
			}
			check(t, winners == 1, "last slot was not awarded once")
			f.audit(t, states)
		})
	}
}
func TestScenario5StorageFailures(t *testing.T) {
	points := []string{"transfer-staged", "before-commit", "after-commit"}
	if os.Getenv("QUARANTINE_INSTRUMENTED") == "1" {
		points = append(points, "writeMetaEIO", "syncDataEIO", "syncMetaEIO")
	}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			f := fresh(t, fixtureLimits())
			x := f.offer(t, "a", f.row(1))
			cmd := instruction("park", x.Ref)
			f.fault = func(at string) error {
				if at == point {
					return syscall.EIO
				}
				if at == "before-commit" {
					return os.Setenv("QUARANTINE_BOLT_POINT", point)
				}
				return nil
			}
			t.Cleanup(func() { os.Unsetenv("QUARANTINE_BOLT_POINT") })
			_, err := f.decide(f.token("a.operator"), cmd)
			wantErr(t, err, syscall.EIO)
			f.fault = nil
			must(t, os.Unsetenv("QUARANTINE_BOLT_POINT"))
			state := "active"
			if point == "transfer-staged" {
				check(t, f.state(t, x.Ref).State == "active", "confirmed abort lost active entry")
			} else {
				wantErr(t, err, fenced)
				f.assertFenced(t, x.Ref)
				f.reopen(t)
				f.assertFenced(t, x.Ref)
				must(t, f.reconcile(false))
				if point == "after-commit" || point == "syncMetaEIO" {
					state = "parked"
				}
			}
			f.audit(t, map[ref]string{x.Ref: state})
			f.act(t, cmd)
			f.audit(t, map[ref]string{x.Ref: "parked"})
			t.Logf("%s: commit reconciled to %s, retry charged once", point, state)
		})
	}
}
func TestScenario5PhysicalFileLimit(t *testing.T) {
	l := fixtureLimits()
	l.Active = pool{Global: amount{200, 1 << 22}, Tenant: amount{200, 1 << 22}}
	f := fresh(t, l)
	f.db.MaxSize = 512 * 1024
	rows := make([]sample, 256)
	for i := range rows {
		rows[i] = f.row(int64(i + 1))
	}
	states := map[ref]string{}
	var refused error
	var refusal ref
	for range 100 {
		x := f.predict(t, "a", newID(), rows)
		f.saveExpected(t, x, "offered-")
		r, err := f.enqueue(f.token("a.source"), x.Ref.ID, rows)
		if err != nil {
			refused = err
			refusal = x.Ref
			check(t, r == (ref{}), "file growth failure acknowledged")
			break
		}
		check(t, r == x.Ref, "incorrect acknowledged payload")
		dir := filepath.Join(f.root, "external")
		must(t, os.Rename(filepath.Join(dir, "offered-"+r.ID), filepath.Join(dir, "expected-"+r.ID)))
		states[x.Ref] = "active"
	}
	if len(states) == 0 || !errors.Is(refused, berrors.ErrMaxSizeReached) {
		t.Fatalf("file cap: acknowledgements=%d error=%v", len(states), refused)
	}
	f.reopen(t)
	must(t, f.reconcile(false))
	f.audit(t, states)
	_, _, err := f.read(f.token("a.custodian"), "payload", refusal)
	wantErr(t, err, conflict)
	s, err := f.summary(f.token("a.auditor"))
	must(t, err)
	check(t, s.Usage.Active.Entries == len(states), "partial admission on file limit")
	t.Logf("512 KiB file limit: %d acknowledged entries recovered, refused entry received no identity", len(states))
}

func TestScenario5TenantByteAndActiveReservations(t *testing.T) {
	f := fresh(t, fixtureLimits())
	rows := []sample{f.row(5)}
	n := len(expectedWire(t, rows))
	f.limits.Active = pool{Global: amount{10, 3 * n}, Tenant: amount{10, 2 * n}, Reserved: amount{0, n}}
	x := f.offer(t, "a", rows...)
	y := f.offer(t, "a", rows...)
	r, err := f.enqueue(f.token("a.source"), newID(), rows)
	wantErr(t, err, full)
	check(t, r == (ref{}), "tenant byte exhaustion acknowledged")
	z := f.offer(t, "b", rows...)
	_, err = f.enqueue(f.token("b.source"), newID(), rows)
	wantErr(t, err, full)
	f.limits.Parked.Tenant.Bytes = n
	f.act(t, instruction("park", x.Ref))
	_, err = f.decide(f.token("a.operator"), instruction("park", y.Ref))
	wantErr(t, err, full)
	f.audit(t, map[ref]string{x.Ref: "parked", y.Ref: "active", z.Ref: "active"})
}
