// Package quarantine implements a private, synthetic logs queue for the M1 experiment.
package quarantine

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	collector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

var (
	denied    = errors.New("access denied")
	fenced    = errors.New("custody requires reconciliation")
	full      = errors.New("queue capacity exhausted")
	blocked   = errors.New("entry held or unavailable")
	conflict  = errors.New("identity or command conflict")
	invalid   = errors.New("outside synthetic capture policy")
	unchanged = errors.New("already committed")
)

const policyVersion = "synthetic-logs-v1"
const captureWindow = 96 * time.Hour
const replayWindow = 48 * time.Hour
const reviewWindow = time.Hour

// Only synthetic sequence and event time may enter the durable encoding. The
// other fields represent untrusted input and are deliberately never serialized.
type sample struct {
	Seq, Event                          int64
	Body, ClaimedTenant, ClaimedService string
}

func capture(rows []sample, now time.Time) ([]byte, int64, error) {
	if len(rows) == 0 || len(rows) > 256 {
		return nil, 0, invalid
	}
	oldest := int64(math.MaxInt64)
	scope := &logs.ScopeLogs{}
	for _, r := range rows {
		if r.Seq <= 0 || r.Event <= 0 || r.Event <= now.Add(-captureWindow).UnixNano() || r.Event > now.Add(time.Minute).UnixNano() {
			return nil, 0, invalid
		}
		oldest = min(oldest, r.Event)
		trace, span := make([]byte, 16), make([]byte, 8)
		binary.BigEndian.PutUint64(trace[8:], uint64(r.Seq))
		binary.BigEndian.PutUint64(span, uint64(r.Seq))
		scope.LogRecords = append(scope.LogRecords, &logs.LogRecord{
			TimeUnixNano: uint64(r.Event), ObservedTimeUnixNano: uint64(r.Event + 11), SeverityNumber: 9,
			TraceId: trace, SpanId: span,
			Attributes: []*common.KeyValue{{Key: "bench.seq", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: r.Seq}}}},
		})
	}
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&collector.ExportLogsServiceRequest{ResourceLogs: []*logs.ResourceLogs{{ScopeLogs: []*logs.ScopeLogs{scope}}}})
	if len(b) > 65536 {
		return nil, 0, invalid
	}
	return b, oldest, err
}

func newID() string {
	var id [16]byte
	rand.Read(id[:])
	return hex.EncodeToString(id[:])
}
func validID(id string) bool {
	b, err := hex.DecodeString(id)
	return err == nil && len(b) == 16 && hex.EncodeToString(b) == id
}
func digest(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }

// Only the private fixture authority installs grants. Requests present an opaque
// credential. Tenant, actor, destination and permissions never come from OTLP.
type grant struct {
	Tenant, Actor string
	Rights        map[string]bool
}

type ref struct{ Generation, Tenant, ID, Digest string }

func (r ref) key() string { return r.Tenant + "/" + r.ID }

type command struct {
	Ref                          ref
	ID, Action, Reason, Previous string
}
type decision struct {
	Command                command
	Actor, Outcome, Policy string
	At                     int64
}
type receipt struct {
	Ref                                   ref
	CommandID, Actor, Reason, Policy      string
	At, ReviewAt                          int64
	Revision                              uint64
	UnknownMembers, UnknownRecordAttempts int
}
type entry struct {
	Ref                                          ref
	Producer, Policy, Destination, State         string
	Order                                        uint64
	Count, Size, Attempts, UnknownRecordAttempts int
	AcceptedAt, Oldest, AccessUntil, RetryAt     int64
	Hold, CredentialHold                         bool
	Lease, LastReplay, Disposal                  string
	Receipt                                      *receipt
}
type amount struct{ Entries, Bytes int }

func (a amount) add(b amount) amount  { return amount{a.Entries + b.Entries, a.Bytes + b.Bytes} }
func (a amount) fits(cap amount) bool { return a.Entries <= cap.Entries && a.Bytes <= cap.Bytes }

type usage struct{ Active, Parked amount }
type pool struct{ Global, Tenant, Reserved amount }
type limits struct{ Active, Parked pool }

func fixtureLimits() limits {
	p := pool{Global: amount{12, 256 * 1024}, Tenant: amount{10, 224 * 1024}, Reserved: amount{2, 32 * 1024}}
	return limits{p, p}
}

type catalog struct {
	Generation string
	Revision   uint64
	Entries    map[string]*entry
	Commands   map[string]*decision
	Usage      map[string]usage
	Global     usage
}

func (c *catalog) target(r ref) (*entry, error) {
	e := c.Entries[r.key()]
	if e == nil || e.Ref != r {
		return nil, conflict
	}
	return e, nil
}
func (u *usage) charge(state string, delta amount) {
	if state == "active" {
		u.Active = u.Active.add(delta)
	} else {
		u.Parked = u.Parked.add(delta)
	}
}
func (c *catalog) charge(tenant, state string, delta amount) {
	u := c.Usage[tenant]
	u.charge(state, delta)
	c.Usage[tenant] = u
	c.Global.charge(state, delta)
}
func (c *catalog) room(tenant, state string, n int, l limits) bool {
	other := "a"
	if tenant == "a" {
		other = "b"
	}
	me, peer, total, p := c.Usage[tenant].Active, c.Usage[other].Active, c.Global.Active, l.Active
	if state == "parked" {
		me, peer, total, p = c.Usage[tenant].Parked, c.Usage[other].Parked, c.Global.Parked, l.Parked
	}
	d := amount{1, n}
	reserved := amount{max(0, p.Reserved.Entries-peer.Entries), max(0, p.Reserved.Bytes-peer.Bytes)}
	return me.add(d).fits(p.Tenant) && total.add(d).add(reserved).fits(p.Global)
}
