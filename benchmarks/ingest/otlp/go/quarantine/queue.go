package quarantine

import (
	"bytes"
	"errors"
	"time"

	bolt "go.etcd.io/bbolt"
)

func (o *owner) authorize(token, right, tenant string) (grant, error) {
	if !o.ready {
		return grant{}, fenced
	}
	g, ok := o.grants[token]
	if !ok || !g.Rights[right] || tenant != "" && g.Tenant != tenant {
		return grant{}, denied
	}
	return g, nil
}

// A fixture administrator uses the same lock as the authorization check and
// lease commit. Revocation after handoff cannot recall a network write.
func (o *owner) setGrant(token string, g grant) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if g.Rights == nil {
		delete(o.grants, token)
		return
	}
	o.grants[token] = g
}

type commandResult struct {
	decision
	Receipt receipt // populated only for the authorized park command
}

func (o *owner) enqueue(token, id string, rows []sample) (ref, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	g, err := o.authorize(token, "enqueue", "")
	if err != nil {
		return ref{}, err
	}
	if !validID(id) {
		return ref{}, invalid
	}
	now := o.now()
	wire, oldest, err := capture(rows, now)
	if err != nil {
		return ref{}, err
	}
	var r ref
	err = o.updateLocked(func(tx *bolt.Tx, c *catalog) error {
		r = ref{c.Generation, g.Tenant, id, digest(wire)}
		if prior := c.Entries[r.key()]; prior != nil {
			if prior.Ref == r && prior.Producer == g.Actor {
				return unchanged
			}
			return conflict
		}
		if len(c.Entries) >= 256 || !c.room(g.Tenant, "active", len(wire), o.limits) {
			return full
		}
		e := &entry{Ref: r, Producer: g.Actor, Policy: policyVersion, Destination: "fixture-loki", State: "active", Order: c.Revision,
			Count: len(rows), Size: len(wire), AcceptedAt: now.UnixNano(), Oldest: oldest, AccessUntil: oldest + int64(captureWindow)}
		c.Entries[r.key()] = e
		c.charge(g.Tenant, "active", amount{1, len(wire)})
		return tx.Bucket([]byte("active")).Put([]byte(r.key()), wire)
	})
	if err != nil {
		return ref{}, err
	}
	return r, nil
}

var reasons = map[string]string{
	"park": "operator-unknown", "replay": "cause-repaired", "dispose": "retention-decision",
	"hold": "legal-hold", "release": "hold-released", "resume": "credentials-repaired",
}

// A command ID names one immutable instruction per verified tenant. The full
// binding is checked even for an idempotent read of a previous outcome.
func (o *owner) decide(token string, cmd command) (commandResult, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	g, err := o.authorize(token, cmd.Action, cmd.Ref.Tenant)
	if err != nil {
		return commandResult{}, err
	}
	reason, ok := reasons[cmd.Action]
	if !ok || cmd.Reason != reason || !validID(cmd.ID) || cmd.Previous != "" && (cmd.Action != "replay" || !validID(cmd.Previous)) {
		return commandResult{}, invalid
	}
	var result decision
	var confirmed receipt
	err = o.updateLocked(func(tx *bolt.Tx, c *catalog) error {
		e, err := c.target(cmd.Ref)
		if err != nil {
			return err
		}
		if cmd.Action == "park" && e.Receipt != nil {
			confirmed = *e.Receipt
		}
		key := g.Tenant + "/" + cmd.ID
		if prior := c.Commands[key]; prior != nil {
			if prior.Command != cmd || prior.Actor != g.Actor {
				return conflict
			}
			result = *prior
			return unchanged
		}
		if len(c.Commands) >= 1024 {
			return full
		}
		if e.Lease != "" {
			return blocked
		}
		now := o.now().UnixNano()
		result = decision{Command: cmd, Actor: g.Actor, At: now, Policy: policyVersion}
		switch cmd.Action {
		case "park":
			r, err := o.park(tx, c, e, &result)
			if err != nil {
				return err
			}
			confirmed = *r
			result.Outcome = "parked"
		case "hold", "release":
			if e.State != "active" && e.State != "parked" {
				return blocked
			}
			e.Hold = cmd.Action == "hold"
			result.Outcome = cmd.Reason
		case "resume":
			if !e.CredentialHold {
				return blocked
			}
			e.CredentialHold = false
			result.Outcome = "resumed"
		case "dispose":
			if e.Hold || (e.State != "parked" && e.State != "disposed") {
				return blocked
			}
			if e.State == "parked" {
				if err = tx.Bucket([]byte("parked")).Delete([]byte(e.Ref.key())); err != nil {
					return err
				}
				e.State = "disposed"
				e.Disposal = cmd.ID
				c.charge(g.Tenant, "parked", amount{-1, -e.Size})
				if d := c.Commands[g.Tenant+"/"+e.LastReplay]; d != nil && d.Outcome == "pending" {
					d.Outcome = "revoked"
				}
			}
			result.Outcome = "disposed"
		case "replay":
			if !replayAllowed(e, now) || cmd.Previous != e.LastReplay {
				return blocked
			}
			if e.LastReplay != "" {
				prior := c.Commands[g.Tenant+"/"+e.LastReplay].Outcome
				if prior != "unknown" && prior != "credentials" {
					return blocked
				}
			}
			e.LastReplay = cmd.ID
			result.Outcome = "pending"
		}
		c.Commands[key] = &result
		return nil
	})
	if err != nil {
		if cmd.Action == "park" && !errors.Is(err, denied) && !errors.Is(err, conflict) {
			o.failedTransfers[g.Tenant]++
		}
		return commandResult{}, err
	}
	return commandResult{result, confirmed}, nil
}

// Park changes custody in the caller's synchronous bbolt transaction. The
// original receipt wins before all quota checks, including on a full queue.
func (o *owner) park(tx *bolt.Tx, c *catalog, e *entry, d *decision) (*receipt, error) {
	if e.State == "parked" {
		return e.Receipt, nil
	}
	if e.State != "active" || e.CredentialHold {
		return nil, blocked
	}
	tenant := e.Ref.Tenant
	if !c.room(tenant, "parked", e.Size, o.limits) {
		return nil, full
	}
	if err := o.cut("before-transfer"); err != nil {
		return nil, err
	}
	key := []byte(e.Ref.key())
	active := tx.Bucket([]byte("active"))
	payload := active.Get(key)
	if len(payload) != e.Size || digest(payload) != e.Ref.Digest {
		return nil, fenced
	}
	if err := tx.Bucket([]byte("parked")).Put(key, payload); err != nil {
		return nil, err
	}
	e.Receipt = &receipt{
		Ref: e.Ref, CommandID: d.Command.ID, Actor: d.Actor, Reason: d.Command.Reason,
		Policy: policyVersion, At: d.At, ReviewAt: d.At + int64(reviewWindow),
		Revision: c.Revision, UnknownRecordAttempts: e.UnknownRecordAttempts,
	}
	if e.UnknownRecordAttempts > 0 {
		e.Receipt.UnknownMembers = e.Count
	}
	c.charge(tenant, "parked", amount{1, e.Size})
	if err := o.cut("transfer-staged"); err != nil {
		return nil, err
	}
	if err := active.Delete(key); err != nil {
		return nil, err
	}
	c.charge(tenant, "active", amount{-1, -e.Size})
	e.State = "parked"
	return e.Receipt, nil
}

func replayAllowed(e *entry, now int64) bool {
	return e.State == "parked" && !e.Hold && !e.CredentialHold && now < e.AccessUntil && e.Oldest >= now-int64(replayWindow)
}

// Only the fixed read operations below can return protected bytes.
func (o *owner) read(token, right string, r ref) (entry, []byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if right != "inspect" && right != "payload" && right != "export" {
		return entry{}, nil, denied
	}
	if _, err := o.authorize(token, right, r.Tenant); err != nil {
		return entry{}, nil, err
	}
	var result entry
	var b []byte
	err := o.db.View(func(tx *bolt.Tx) error {
		c, raw, err := readCatalog(tx)
		if err != nil || digest(raw) != o.stable.Hash {
			return fenced
		}
		e, err := c.target(r)
		if err != nil {
			return err
		}
		result = *e
		if right == "inspect" {
			return nil
		}
		if (e.State != "active" && e.State != "parked") || o.now().UnixNano() >= e.AccessUntil {
			return blocked
		}
		b = bytes.Clone(tx.Bucket([]byte(e.State)).Get([]byte(r.key())))
		if len(b) != e.Size || digest(b) != r.Digest {
			return fenced
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, fenced) {
			o.ready = false
		}
		return entry{}, nil, err
	}
	return result, b, nil
}

type delivery struct {
	Ref           ref
	Lease, Replay string
	Payload       []byte
}
type outcome struct {
	HTTPStatus int
	Kind       string
	RetryAfter time.Duration
}
type sender func(delivery) outcome

// The oldest active member owns this tenant's only lane until an ACK or park.
func (o *owner) claim(token string, replay *command) (delivery, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	right, tenant := "dispatch", ""
	if replay != nil {
		right, tenant = "replay", replay.Ref.Tenant
	}
	g, err := o.authorize(token, right, tenant)
	if err != nil {
		return delivery{}, err
	}
	var work delivery
	err = o.updateLocked(func(tx *bolt.Tx, c *catalog) error {
		var e *entry
		now := o.now().UnixNano()
		if replay == nil {
			for _, v := range c.Entries {
				if v.Ref.Tenant == g.Tenant && v.State == "active" && (e == nil || v.Order < e.Order) {
					e = v
				}
			}
			if e == nil || now < e.RetryAt || now >= e.AccessUntil {
				return blocked
			}
		} else {
			d := c.Commands[g.Tenant+"/"+replay.ID]
			if d == nil || d.Actor != g.Actor || d.Command != *replay || replay.Action != "replay" {
				return denied
			}
			e = c.Entries[replay.Ref.key()]
			if d.Outcome != "pending" || e.LastReplay != replay.ID || !replayAllowed(e, now) {
				return blocked
			}
			work.Replay = replay.ID
			d.Outcome = "sending"
		}
		if e.Lease != "" || e.Hold || e.CredentialHold || e.Policy != policyVersion || e.Destination != "fixture-loki" {
			return blocked
		}
		work.Ref = e.Ref
		work.Lease = newID()
		work.Payload = bytes.Clone(tx.Bucket([]byte(e.State)).Get([]byte(e.Ref.key())))
		if len(work.Payload) != e.Size || digest(work.Payload) != e.Ref.Digest {
			return fenced
		}
		e.Lease = work.Lease
		e.Attempts++
		return nil
	})
	if err != nil {
		return delivery{}, err
	}
	return work, nil
}

func (o *owner) finish(w delivery, result outcome) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.updateLocked(func(tx *bolt.Tx, c *catalog) error {
		e, err := c.target(w.Ref)
		if err != nil {
			return err
		}
		if w.Lease == "" || e.Lease != w.Lease || (e.State == "parked" && e.LastReplay != w.Replay) || (e.State == "active" && w.Replay != "") {
			return conflict
		}
		e.Lease = ""
		if result.Kind == "credentials" {
			e.CredentialHold = true
		}
		if result.Kind != "ack" && result.Kind != "credentials" {
			e.UnknownRecordAttempts += e.Count
		}
		if w.Replay != "" {
			d := c.Commands[e.Ref.Tenant+"/"+w.Replay]
			switch result.Kind {
			case "ack":
				d.Outcome = "acknowledged"
			case "credentials":
				d.Outcome = "credentials"
			default:
				d.Outcome = "unknown"
			}
			return nil
		}
		if result.Kind == "ack" {
			if err = tx.Bucket([]byte("active")).Delete([]byte(e.Ref.key())); err != nil {
				return err
			}
			c.charge(e.Ref.Tenant, "active", amount{-1, -e.Size})
			e.State = "acknowledged"
		} else {
			delay := time.Second << min(e.Attempts-1, 5)
			jitter := time.Duration(e.Ref.ID[0]) * time.Millisecond
			e.RetryAt = o.now().Add(max(delay+jitter, result.RetryAfter)).UnixNano()
		}
		return nil
	})
}

func (o *owner) dispatch(token string, cmd *command) error {
	o.network.RLock()
	defer o.network.RUnlock()
	w, err := o.claim(token, cmd)
	if err != nil {
		return err
	}
	return o.finish(w, o.send(w))
}

// summary is tenant-scoped metadata. The review deadline raises an operator
// signal and never silently evicts or disposes a payload.
type summary struct {
	FailedTransfersThisProcess, Disposals int
	ReplayOutcomes                        map[string]int
	Usage                                 usage
	ReviewDue, Held                       int
	OldestParkedAt                        int64
}

func (o *owner) summary(token string) (summary, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	g, err := o.authorize(token, "inspect", "")
	if err != nil {
		return summary{}, err
	}
	var s summary
	err = o.db.View(func(tx *bolt.Tx) error {
		c, raw, err := readCatalog(tx)
		if err != nil || digest(raw) != o.stable.Hash {
			return fenced
		}
		s.Usage = c.Usage[g.Tenant]
		s.FailedTransfersThisProcess = o.failedTransfers[g.Tenant]
		s.ReplayOutcomes = map[string]int{}
		for _, d := range c.Commands {
			if d.Command.Ref.Tenant == g.Tenant && d.Command.Action == "replay" {
				s.ReplayOutcomes[d.Outcome]++
			}
		}
		for _, e := range c.Entries {
			if e.Ref.Tenant == g.Tenant && e.State == "disposed" {
				s.Disposals++
			}
			if e.Ref.Tenant != g.Tenant || e.State != "parked" {
				continue
			}
			if e.Hold {
				s.Held++
			}
			if e.Receipt.ReviewAt <= o.now().UnixNano() {
				s.ReviewDue++
			}
			if s.OldestParkedAt == 0 || e.Receipt.At < s.OldestParkedAt {
				s.OldestParkedAt = e.Receipt.At
			}
		}
		return nil
	})
	if err != nil {
		o.ready = false
		return summary{}, err
	}
	return s, nil
}
