package quarantine

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	collector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func check(t *testing.T, ok bool, msg string) {
	t.Helper()
	if !ok {
		t.Fatal(msg)
	}
}
func wantErr(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("got %v, expected %v", err, want)
	}
}

type lab struct {
	routeMu sync.RWMutex
	route   sender
	*owner
	root, dbPath, sealPath, secret, generation string
	clock                                      atomic.Int64
	auth                                       map[string]grant
	cfg                                        limits
}

func (f *lab) token(principal string) string { return digest([]byte(f.secret + principal)) }
func (f *lab) nowTime() time.Time            { return time.Unix(0, f.clock.Load()) }

func setup(t *testing.T, root, secret string, create bool, l limits, now time.Time) *lab {
	t.Helper()
	f := &lab{root: root, secret: secret, dbPath: filepath.Join(root, "private", "owner.db"), sealPath: filepath.Join(root, "external", "latest.json"), cfg: l, auth: map[string]grant{}}
	f.clock.Store(now.UnixNano())
	for _, tenant := range []string{"a", "b"} {
		for role, rights := range map[string]string{"source": "enqueue dispatch", "operator": "inspect park replay dispose resume hold release", "auditor": "inspect", "custodian": "inspect payload export"} {
			p := tenant + "." + role
			g := grant{tenant, p, map[string]bool{}}
			for _, r := range strings.Fields(rights) {
				g.Rights[r] = true
			}
			f.auth[f.token(p)] = g
		}
	}
	if create {
		must(t, os.Mkdir(filepath.Dir(f.dbPath), 0700))
		must(t, os.Mkdir(filepath.Dir(f.sealPath), 0700))
	}
	var err error
	f.owner, err = openOwner(f.dbPath, f.sealPath, create, f.auth, l, f.nowTime, f.transport)
	must(t, err)
	if create {
		var c catalog
		must(t, json.Unmarshal(f.stable.Stable, &c))
		f.generation = c.Generation
	}
	t.Cleanup(func() {
		if f.owner != nil {
			must(t, f.close())
		}
	})
	return f
}
func fresh(t *testing.T, l limits) *lab {
	t.Helper()
	return setup(t, t.TempDir(), newID(), true, l, time.Now().Truncate(time.Second))
}
func (f *lab) reopen(t *testing.T) {
	t.Helper()
	if f.owner != nil {
		must(t, f.close())
		f.owner = nil
	}
	o, err := openOwner(f.dbPath, f.sealPath, false, f.auth, f.cfg, f.nowTime, f.transport)
	must(t, err)
	f.owner = o
}
func (f *lab) restart(t *testing.T) { t.Helper(); f.reopen(t); must(t, f.reconcile(false)) }

// The oracle builds the expected protobuf independently from the admission
// implementation using the public OTLP JSON mapping. Only its hash, count,
// byte length and source-minted identity leave the generator.
type expectation struct {
	Ref         ref
	Count, Size int
}

func expectedWire(t *testing.T, rows []sample) []byte {
	t.Helper()
	var logs []string
	for _, r := range rows {
		trace, err := hex.DecodeString(fmt.Sprintf("%032x", r.Seq))
		must(t, err)
		span, err := hex.DecodeString(fmt.Sprintf("%016x", r.Seq))
		must(t, err)
		logs = append(logs, fmt.Sprintf(`{"timeUnixNano":"%d","observedTimeUnixNano":"%d","severityNumber":9,"traceId":"%s","spanId":"%s","attributes":[{"key":"bench.seq","value":{"intValue":"%d"}}]}`, r.Event, r.Event+11, base64.StdEncoding.EncodeToString(trace), base64.StdEncoding.EncodeToString(span), r.Seq))
	}
	var req collector.ExportLogsServiceRequest
	must(t, protojson.Unmarshal([]byte(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[`+strings.Join(logs, ",")+`]}]}]}`), &req))
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&req)
	must(t, err)
	return b
}
func (f *lab) predict(t *testing.T, tenant, id string, rows []sample) expectation {
	t.Helper()
	b := expectedWire(t, rows)
	return expectation{ref{f.generation, tenant, id, fmt.Sprintf("%x", sha256.Sum256(b))}, len(rows), len(b)}
}
func (f *lab) saveExpected(t *testing.T, x expectation, prefix string) {
	t.Helper()
	b, err := json.Marshal(x)
	must(t, err)
	file, err := os.OpenFile(filepath.Join(f.root, "external", prefix+x.Ref.ID), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	must(t, err)
	_, err = file.Write(b)
	must(t, err)
	must(t, file.Sync())
	must(t, file.Close())
}
func (f *lab) offer(t *testing.T, tenant string, rows ...sample) expectation {
	t.Helper()
	x := f.predict(t, tenant, newID(), rows)
	f.saveExpected(t, x, "expected-")
	r, err := f.enqueue(f.token(tenant+".source"), x.Ref.ID, rows)
	must(t, err)
	check(t, r == x.Ref, "admission differs from the independent identity and digest")
	return x
}
func (f *lab) row(seq int64) sample { return sample{Seq: seq, Event: f.clock.Load() + seq} }
func instruction(action string, r ref) command {
	return command{Ref: r, ID: newID(), Action: action, Reason: reasons[action]}
}
func (f *lab) act(t *testing.T, c command) commandResult {
	t.Helper()
	d, err := f.decide(f.token(c.Ref.Tenant+".operator"), c)
	must(t, err)
	return d
}
func (f *lab) state(t *testing.T, r ref) entry {
	t.Helper()
	e, _, err := f.read(f.token(r.Tenant+".auditor"), "inspect", r)
	must(t, err)
	return e
}
func (f *lab) evidence(t *testing.T) []expectation {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(f.root, "external", "expected-*"))
	must(t, err)
	var xs []expectation
	for _, p := range files {
		var x expectation
		b, err := os.ReadFile(p)
		must(t, err)
		must(t, json.Unmarshal(b, &x))
		xs = append(xs, x)
	}
	return xs
}

type custodyObservation struct {
	Oracle                            expectation
	State, ActiveSHA256, ParkedSHA256 string
	Receipt                           *receipt
}
type inventory struct {
	Entries       []custodyObservation
	TenantCharges map[string]usage
	GlobalCharge  usage
}

// Inspect the actual bbolt buckets with the external oracle. Do not derive the
// expected occupants or charges from stored entries or the restore witness.
func (f *lab) audit(t *testing.T, states map[ref]string) inventory {
	var observed inventory
	t.Helper()
	xs := f.evidence(t)
	check(t, len(xs) == len(states), "incomplete expected inventory")
	want := map[string]usage{"a": {}, "b": {}}
	must(t, f.db.View(func(tx *bolt.Tx) error {
		c, _, err := readCatalog(tx)
		if err != nil {
			return err
		}
		check(t, len(c.Entries) == len(xs), "unexpected catalog members")
		nActive, nParked := 0, 0
		for _, x := range xs {
			state, ok := states[x.Ref]
			check(t, ok, "unexpected oracle identity")
			e := c.Entries[x.Ref.key()]
			check(t, e != nil && e.Ref == x.Ref && e.Count == x.Count && e.Size == x.Size && e.State == state, "wrong entry or disposition")
			item := custodyObservation{Oracle: x, State: e.State, Receipt: e.Receipt}
			for _, name := range []string{"active", "parked"} {
				raw := tx.Bucket([]byte(name)).Get([]byte(x.Ref.key()))
				if name == state {
					check(t, len(raw) == x.Size && fmt.Sprintf("%x", sha256.Sum256(raw)) == x.Ref.Digest, "owned bytes differ from oracle")
					u := want[x.Ref.Tenant]
					if name == "active" {
						item.ActiveSHA256 = digest(raw)
						u.Active.Entries++
						u.Active.Bytes += x.Size
						nActive++
					} else {
						item.ParkedSHA256 = digest(raw)
						u.Parked.Entries++
						u.Parked.Bytes += x.Size
						nParked++
					}
					want[x.Ref.Tenant] = u
				} else {
					check(t, raw == nil, "entry has an extra custodian")
				}
			}
			observed.Entries = append(observed.Entries, item)
			if state == "parked" || state == "disposed" {
				check(t, e.Receipt != nil && e.Receipt.Ref == x.Ref, "receipt does not match source oracle")
			} else {
				check(t, e.Receipt == nil, "unexpected transfer receipt")
			}
		}
		check(t, tx.Bucket([]byte("active")).Stats().KeyN == nActive && tx.Bucket([]byte("parked")).Stats().KeyN == nParked, "unaccounted payload")
		global := usage{want["a"].Active.add(want["b"].Active), want["a"].Parked.add(want["b"].Parked)}
		check(t, c.Usage["a"] == want["a"] && c.Usage["b"] == want["b"] && c.Global == global, "charge differs from external inventory")
		observed.TenantCharges = c.Usage
		observed.GlobalCharge = c.Global
		return nil
	}))
	return observed
}
func (f *lab) assertFenced(t *testing.T, r ref) {
	t.Helper()
	for _, right := range []string{"inspect", "payload", "export"} {
		_, b, err := f.read(f.token(r.Tenant+".custodian"), right, r)
		wantErr(t, err, fenced)
		check(t, b == nil, "fenced read returned bytes")
	}
	c := instruction("replay", r)
	noSend := func(delivery) outcome { t.Fatal("fenced owner dispatched"); return outcome{} }
	wantErr(t, f.drive(f.token(r.Tenant+".source"), nil, noSend), fenced)
	wantErr(t, f.drive(f.token(r.Tenant+".operator"), &c, noSend), fenced)
	_, err := f.decide(f.token(r.Tenant+".operator"), instruction("dispose", r))
	wantErr(t, err, fenced)
}
func scanPrivate(t *testing.T, paths []string, canaries ...string) {
	t.Helper()
	for _, p := range paths {
		b, err := os.ReadFile(p)
		must(t, err)
		for _, s := range canaries {
			check(t, !bytes.Contains(b, []byte(s)), "forbidden fixture content entered durable evidence")
		}
	}
}

// Only the harness configures the receiver or cuts its reply. Operator commands
// have no transport argument and cannot supply an alternate destination.
func (f *lab) transport(w delivery) outcome {
	f.routeMu.RLock()
	route := f.route
	f.routeMu.RUnlock()
	return route(w)
}
func (f *lab) drive(token string, cmd *command, route sender) error {
	f.routeMu.Lock()
	f.route = route
	f.routeMu.Unlock()
	return f.owner.dispatch(token, cmd)
}
