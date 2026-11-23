package quarantine

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"
)

var meta = []byte("catalog")
var dataKey = []byte("v1")

// A private, independently retained metadata witness models the restore authority.
// It never owns payload custody. Pending identifies the two possible catalogues
// around a bbolt commit. An ordinary snapshot of the queue excludes this file.
type seal struct {
	Stable        json.RawMessage `json:"-"`
	Hash, Pending string
}

func publish(path string, s seal) error {
	// The authority remembers the current digest separately from the evidence
	// offered for restore. A valid but obsolete catalogue cannot unseal an image.
	if s.Pending == "" {
		if err := atomicFile(path+".catalog", s.Stable); err != nil {
			return err
		}
	}
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return atomicFile(path, b)
}

func atomicFile(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".seal-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

type owner struct {
	failedTransfers map[string]int // per-process refusal evidence for the verified tenant
	guard           *os.File       // stable inode at the independent witness, outside queue backups
	send            sender         // fixed by the trusted fixture, outside the command interface
	network         sync.RWMutex   // recovery must not abandon a live network handoff
	mu              sync.Mutex     // serializes authorization, mutations and publication of their witness
	db              *bolt.DB
	witness         string
	stable          seal
	ready           bool
	grants          map[string]grant
	limits          limits
	now             func() time.Time
	fault           func(string) error // deterministic experiment seams, disabled by default
}

func (o *owner) cut(point string) error {
	if o.fault != nil {
		return o.fault(point)
	}
	return nil
}

// Creation is distinct from opening. A missing database or witness on restart
// can never initialize an empty replacement queue.
func openOwner(path, witness string, create bool, grants map[string]grant, l limits, now func() time.Time, send sender) (*owner, error) {
	// A copied database has a different bbolt file lock. Fence both copies at
	// the authority as well, so an old owner cannot publish over a restore.
	guard, err := os.OpenFile(witness+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(guard.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		guard.Close()
		return nil, errors.Join(blocked, err)
	}
	opened := false
	defer func() {
		if !opened {
			guard.Close()
		}
	}()

	if create {
		for _, p := range []string{witness, witness + ".catalog"} {
			if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
				return nil, conflict
			}
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		if err = f.Close(); err != nil {
			return nil, err
		}
	} else if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 100 * time.Millisecond, MaxSize: 16 << 20})
	if err != nil {
		return nil, err
	}
	o := &owner{db: db, guard: guard, failedTransfers: map[string]int{}, witness: witness, grants: grants, limits: l, now: now, send: send}
	if !create {
		opened = true
		return o, nil
	}
	c := catalog{Generation: newID(), Revision: 1, Entries: map[string]*entry{}, Commands: map[string]*decision{}, Usage: map[string]usage{"a": {}, "b": {}}}
	raw, err := json.Marshal(c)
	if err == nil {
		err = db.Update(func(tx *bolt.Tx) error {
			for _, name := range []string{"catalog", "active", "parked"} {
				if _, e := tx.CreateBucket([]byte(name)); e != nil {
					return e
				}
			}
			return tx.Bucket(meta).Put(dataKey, raw)
		})
	}
	o.stable = seal{Stable: raw, Hash: digest(raw)}
	if err == nil {
		err = publish(witness, o.stable)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	o.ready = true
	opened = true
	return o, nil
}

func (o *owner) close() error {
	o.network.Lock()
	defer o.network.Unlock()
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ready = false
	return errors.Join(o.db.Close(), o.guard.Close())
}

func readCatalog(tx *bolt.Tx) (catalog, []byte, error) {
	var c catalog
	b := tx.Bucket(meta)
	if b == nil {
		return c, nil, fenced
	}
	raw := b.Get(dataKey)
	if json.Unmarshal(raw, &c) != nil || !validID(c.Generation) || c.Entries == nil || c.Commands == nil || c.Usage == nil {
		return c, nil, fenced
	}
	return c, raw, nil
}

func verify(tx *bolt.Tx, c *catalog) error {
	var total usage
	byTenant := map[string]usage{"a": {}, "b": {}}
	for _, state := range []string{"active", "parked"} {
		b := tx.Bucket([]byte(state))
		if b == nil {
			return fenced
		}
		if err := b.ForEach(func(k, v []byte) error {
			e := c.Entries[string(k)]
			if e == nil || e.State != state || e.Size != len(v) || digest(v) != e.Ref.Digest {
				return fenced
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for k, e := range c.Entries {
		if e == nil || e.Ref.key() != k || e.Ref.Generation != c.Generation || !validID(e.Ref.ID) || e.Policy != policyVersion || e.Destination != "fixture-loki" || e.Count < 1 || e.Size < 1 {
			return fenced
		}
		if _, ok := byTenant[e.Ref.Tenant]; !ok {
			return fenced
		}
		switch e.State {
		case "active", "parked":
			if tx.Bucket([]byte(e.State)).Get([]byte(k)) == nil {
				return fenced
			}
			u := byTenant[e.Ref.Tenant]
			d := amount{1, e.Size}
			u.charge(e.State, d)
			byTenant[e.Ref.Tenant] = u
			total.charge(e.State, d)
		case "acknowledged", "disposed":
		default:
			return fenced
		}
		if e.State == "parked" || e.State == "disposed" {
			r := e.Receipt
			if r == nil || r.Ref != e.Ref || r.Revision == 0 || r.Revision > c.Revision || (r.UnknownRecordAttempts == 0 && r.UnknownMembers != 0) || (r.UnknownRecordAttempts > 0 && r.UnknownMembers != e.Count) {
				return fenced
			}
			d := c.Commands[e.Ref.Tenant+"/"+r.CommandID]
			if d == nil || d.Command.Ref != e.Ref || d.Command.Action != "park" || d.Actor != r.Actor || d.Outcome != "parked" || d.At != r.At || d.Command.Reason != r.Reason || r.Policy != policyVersion || r.ReviewAt != r.At+int64(reviewWindow) || r.UnknownRecordAttempts > e.UnknownRecordAttempts {
				return fenced
			}
		} else if e.Receipt != nil {
			return fenced
		}
		if e.State == "disposed" {
			d := c.Commands[e.Ref.Tenant+"/"+e.Disposal]
			if d == nil || d.Command.Action != "dispose" || d.Command.Ref != e.Ref || d.Outcome != "disposed" {
				return fenced
			}
		}
	}
	if c.Global != total || len(c.Usage) != 2 || c.Usage["a"] != byTenant["a"] || c.Usage["b"] != byTenant["b"] {
		return fenced
	}
	return nil
}

// updateLocked changes both memberships, receipts and charges in one DB.Update.
// A failed witness publication or a storage error after preparing the commit
// closes every dispatch/read gate before another worker can enter.
func (o *owner) updateLocked(fn func(*bolt.Tx, *catalog) error) error {
	if !o.ready {
		return fenced
	}
	var next seal
	prepared := false
	err := o.db.Update(func(tx *bolt.Tx) error {
		c, raw, err := readCatalog(tx)
		if err != nil || digest(raw) != o.stable.Hash {
			o.ready = false
			return fenced
		}
		c.Revision++
		if err = fn(tx, &c); err != nil {
			if errors.Is(err, unchanged) {
				c.Revision--
				if err := verify(tx, &c); err != nil {
					return err
				}
			}
			return err
		}
		if err = verify(tx, &c); err != nil {
			o.ready = false
			return err
		}
		raw, err = json.Marshal(c)
		if err != nil {
			return err
		}
		if err = tx.Bucket(meta).Put(dataKey, raw); err != nil {
			return err
		}
		next = seal{Stable: raw, Hash: digest(raw)}
		pending := o.stable
		pending.Pending = next.Hash
		prepared = true
		if err = publish(o.witness, pending); err != nil {
			return err
		}
		return o.cut("before-commit")
	})
	if errors.Is(err, unchanged) {
		return nil
	}
	if err == nil {
		err = o.cut("after-commit")
	}
	if err == nil {
		err = publish(o.witness, next)
	}
	if err != nil {
		if errors.Is(err, fenced) {
			o.ready = false
		}
		if prepared {
			o.ready = false
			return errors.Join(fenced, err)
		}
		return err
	}
	o.stable = next
	return nil
}

// A restore can project a complete, current disposition catalogue onto an old
// image only if every still-owned payload is available with its original hash.
func restoreCatalog(tx *bolt.Tx, old, latest *catalog, raw []byte) error {
	if old.Generation != latest.Generation || old.Revision >= latest.Revision {
		return fenced
	}
	for k, v := range old.Entries {
		n := latest.Entries[k]
		if n == nil || n.Ref != v.Ref {
			return fenced
		}
	}
	for k, e := range latest.Entries {
		prior := old.Entries[k]
		var payload []byte
		if prior != nil && (prior.State == "active" || prior.State == "parked") {
			b := tx.Bucket([]byte(prior.State))
			payload = bytes.Clone(b.Get([]byte(k)))
			if err := b.Delete([]byte(k)); err != nil {
				return err
			}
		}
		if e.State == "active" || e.State == "parked" {
			if digest(payload) != e.Ref.Digest || len(payload) != e.Size {
				return fenced
			}
			if err := tx.Bucket([]byte(e.State)).Put([]byte(k), payload); err != nil {
				return err
			}
		}
	}
	if err := verify(tx, latest); err != nil {
		return err
	}
	return tx.Bucket(meta).Put(dataKey, raw)
}

func (o *owner) reconcile(restored bool) error {
	o.network.Lock()
	defer o.network.Unlock()
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.ready {
		return blocked
	}
	raw, err := os.ReadFile(o.witness)
	var w seal
	if err != nil || json.Unmarshal(raw, &w) != nil || len(w.Hash) != 64 || restored && w.Pending != "" {
		return fenced
	}
	w.Stable, err = os.ReadFile(o.witness + ".catalog")
	if err != nil {
		return fenced
	}
	evidenceHash := digest(w.Stable)
	if evidenceHash != w.Hash && (restored || evidenceHash != w.Pending) {
		return fenced
	}
	if err = o.db.Sync(); err != nil {
		return err
	}
	err = o.db.Update(func(tx *bolt.Tx) error {
		c, b, err := readCatalog(tx)
		if err != nil {
			return err
		}
		if err = verify(tx, &c); err != nil {
			return err
		}
		if err = o.cut("during-recovery"); err != nil {
			return err
		}
		switch actual := digest(b); {
		case actual == w.Hash:
			if evidenceHash != actual {
				return fenced
			}
			w.Pending = ""
		case !restored && actual == w.Pending:
			w = seal{Stable: bytes.Clone(b), Hash: actual}
		case restored:
			var latest catalog
			if json.Unmarshal(w.Stable, &latest) != nil {
				return fenced
			}
			if err := restoreCatalog(tx, &c, &latest, w.Stable); err != nil {
				return err
			}
			return o.cut("restore-staged")
		default:
			return fenced
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err = publish(o.witness, w); err != nil {
		return err
	}
	o.stable = w
	o.ready = true
	// A send leased by a previous process may have reached Loki. Its completion
	// token is retired; a quarantined replay needs a fresh, explicit command.
	err = o.updateLocked(func(_ *bolt.Tx, c *catalog) error {
		changed := false
		for _, e := range c.Entries {
			if e.Lease != "" {
				e.Lease = ""
				e.UnknownRecordAttempts += e.Count
				changed = true
				if e.State == "parked" {
					c.Commands[e.Ref.Tenant+"/"+e.LastReplay].Outcome = "unknown"
				}
			}
		}
		if !changed {
			return unchanged
		}
		return o.cut("lease-recovery-staged")
	})
	if err != nil {
		o.ready = false
	}
	return err
}
