package quarantine

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

type counters struct {
	Ack      int `json:"store_ack_records"`
	Unknown  int `json:"store_unknown_records"`
	Bad      int `json:"store_http_400_attempts"`
	Attempts int `json:"store_attempts"`
	Invalid  int `json:"invalid_records"`
}

func (a counters) minus(b counters) counters {
	return counters{a.Ack - b.Ack, a.Unknown - b.Unknown, a.Bad - b.Bad, a.Attempts - b.Attempts, a.Invalid - b.Invalid}
}
func liveReceiver() *receiver {
	return newReceiver(os.Getenv("QUARANTINE_RECEIVER"), map[string]string{"a": os.Getenv("OBS_BENCH_TOKEN_A"), "b": os.Getenv("OBS_BENCH_TOKEN_B")})
}
func (r *receiver) get(t *testing.T, tenant, path string, data any) {
	t.Helper()
	req, err := http.NewRequest("GET", r.base+path, nil)
	must(t, err)
	req.Header.Set("Authorization", "Bearer "+r.tokens[tenant])
	resp, err := r.client.Do(req)
	must(t, err)
	defer resp.Body.Close()
	check(t, resp.StatusCode == 200, "authorized receiver query failed")
	must(t, json.NewDecoder(io.LimitReader(resp.Body, 512*1024)).Decode(data))
}
func (r *receiver) counts(t *testing.T) counters {
	t.Helper()
	var c counters
	r.get(t, "a", "/bench/v1/counts", &c)
	return c
}

// Count multiplicity only after checking each returned line, original timestamp,
// observed timestamp, severity, trace/span and fixed service against source data.
func (r *receiver) visible(t *testing.T, tenant string, lo, hi int64, expected ...sample) map[int64]int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var doc struct {
			Status string
			Data   struct {
				ResultType string
				Result     []struct {
					Stream map[string]string
					Values [][]json.RawMessage
				}
			}
		}
		r.get(t, tenant, fmt.Sprintf("/bench/v1/logs?start=%d&end=%d", lo, hi), &doc)
		check(t, doc.Status == "success" && doc.Data.ResultType == "streams", "incomplete Loki query")
		found := map[int64]int{}
		for _, s := range doc.Data.Result {
			for _, v := range s.Values {
				check(t, len(v) >= 2, "missing Loki value")
				var stamp, line string
				must(t, json.Unmarshal(v[0], &stamp))
				must(t, json.Unmarshal(v[1], &line))
				matched := false
				for _, x := range expected {
					if line != fmt.Sprintf("bench-%d", x.Seq) {
						continue
					}
					matched = stamp == strconv.FormatInt(x.Event, 10) && s.Stream["observed_timestamp"] == strconv.FormatInt(x.Event+11, 10) && s.Stream["severity_number"] == "9" && s.Stream["service_name"] == "m1-bench" && s.Stream["trace_id"] == fmt.Sprintf("%032x", x.Seq) && s.Stream["span_id"] == fmt.Sprintf("%016x", x.Seq)
					found[x.Seq]++
					break
				}
				check(t, matched, "unexpected event or metadata in Loki")
			}
		}
		if len(found) == len(expected) {
			return found
		}
		check(t, time.Now().Before(deadline), "expected event never became queryable")
		time.Sleep(50 * time.Millisecond)
	}
}
func checkDelta(t *testing.T, got, want counters) {
	t.Helper()
	if got != want {
		t.Fatalf("receiver counter delta %+v, expected %+v", got, want)
	}
}
func (f *lab) advance() { f.clock.Add(int64(2 * time.Minute)) }
func (f *lab) sendHead(t *testing.T, x expectation, s sender) {
	t.Helper()
	must(t, f.drive(f.token(x.Ref.Tenant+".source"), nil, func(w delivery) outcome {
		check(t, w.Ref == x.Ref && digest(w.Payload) == x.Ref.Digest, "wrong FIFO entry or digest sent")
		return s(w)
	}))
}
func observedHTTP(t *testing.T, send sender, want int, observed *[]int) sender {
	t.Helper()
	return func(w delivery) outcome {
		v := send(w)
		if v.HTTPStatus != want {
			t.Fatalf("receiver returned HTTP %d; expected %d", v.HTTPStatus, want)
		}
		*observed = append(*observed, v.HTTPStatus)
		return v
	}
}
func TestLiveReceiver(t *testing.T) {
	if os.Getenv("QUARANTINE_RECEIVER") == "" {
		t.Skip("paired real-Loki driver supplies endpoints")
	}
	ns, err := strconv.ParseInt(os.Getenv("QUARANTINE_EPOCH"), 10, 64)
	must(t, err)
	f := setup(t, t.TempDir(), newID(), true, fixtureLimits(), time.Unix(0, ns))
	r := liveReceiver()
	base := r.counts(t)
	report := map[string]any{}
	row := func(seq int64) sample { return sample{Seq: seq, Event: ns + seq} }
	sock := os.Getenv("QUARANTINE_LOKI_SOCKET")
	x := f.offer(t, "a", row(1))
	// Four observed attempts beyond a diagnostic threshold of three. The
	// receiver still authenticates and admits while its real Loki socket is absent.
	must(t, os.Rename(sock, sock+".offline"))
	t.Cleanup(func() {
		if _, err := os.Stat(sock + ".offline"); err == nil {
			os.Rename(sock+".offline", sock)
		}
	})
	var offlineHTTP, edgeHTTP, authHTTP []int
	for range 3 {
		f.sendHead(t, x, observedHTTP(t, r.send, 503, &offlineHTTP))
		f.advance()
	}
	must(t, f.close())
	f.owner = nil
	crash(t, f, command{Ref: x.Ref}, "offline-dispatch")
	f.reopen(t)
	must(t, f.reconcile(false))
	f.advance()
	checkDelta(t, r.counts(t).minus(base), counters{Unknown: 4, Attempts: 4})
	f.audit(t, map[ref]string{x.Ref: "active"})
	must(t, os.Rename(sock+".offline", sock))
	outage := r.counts(t)
	for _, code := range []int{429, 503} {
		relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Header().Set("Retry-After", "45"); w.WriteHeader(code) }))
		edge := newReceiver(relay.URL, r.tokens)
		f.sendHead(t, x, observedHTTP(t, edge.send, code, &edgeHTTP))
		relay.Close()
		check(t, f.state(t, x.Ref).RetryAt >= f.nowTime().Add(45*time.Second).UnixNano(), "Retry-After was ignored")
		wantErr(t, f.drive(f.token("a.source"), nil, neverSend(t)), blocked)
		f.advance()
	}
	bad := newReceiver(r.base, map[string]string{"a": newID()})
	f.sendHead(t, x, observedHTTP(t, bad.send, 401, &authHTTP))
	check(t, f.state(t, x.Ref).CredentialHold, "known receiver credential failure was not held")
	f.restart(t)
	f.advance()
	wantErr(t, f.drive(f.token("a.source"), nil, neverSend(t)), blocked)
	_, err = f.decide(f.token("a.operator"), instruction("park", x.Ref))
	wantErr(t, err, blocked)
	checkDelta(t, r.counts(t).minus(outage), counters{})
	f.act(t, instruction("resume", x.Ref))
	f.sendHead(t, x, r.send)
	check(t, f.state(t, x.Ref).State == "acknowledged" && f.state(t, x.Ref).UnknownRecordAttempts == 6, "outage recovery lost uncertainty")
	recovered := r.visible(t, "a", ns, ns+2, row(1))
	report["outage"] = map[string]any{"offline_attempts": 4, "injected_statuses": edgeHTTP, "offline_receiver_http_before_restart": offlineHTTP, "credential_http": authHTTP, "credential_hold_survived_reopen": true, "unknown_record_attempts": 6, "recovered_visible": recovered[1], "receiver_delta": r.counts(t).minus(base)}
	// Cut the reply after the real receiver completed a write. The source
	// sees an uncertain send even though the observer can query the member.
	amb := f.offer(t, "a", row(2))
	lost := func(w delivery) outcome {
		check(t, r.send(w).Kind == "ack", "lost-reply cut did not follow actual receiver success")
		return outcome{Kind: "unknown"}
	}
	before := r.counts(t)
	f.sendHead(t, amb, lost)
	f.act(t, instruction("park", amb.Ref))
	first := instruction("replay", amb.Ref)
	f.act(t, first)
	operator := f.token("a.operator")
	must(t, f.drive(operator, &first, lost))
	f.restart(t)
	check(t, f.act(t, first).Outcome == "unknown", "lost reply was upgraded to success")
	wantErr(t, f.drive(operator, &first, neverSend(t)), blocked)
	retry := instruction("replay", amb.Ref)
	_, err = f.decide(operator, retry)
	wantErr(t, err, blocked)
	retry.Previous = first.ID
	f.act(t, retry)
	must(t, f.drive(operator, &retry, r.send))
	checkDelta(t, r.counts(t).minus(before), counters{Ack: 3, Attempts: 3})
	copies := r.visible(t, "a", ns+2, ns+3, row(2))[2]
	check(t, f.state(t, amb.Ref).UnknownRecordAttempts == 2 && f.act(t, retry).Outcome == "acknowledged", "uncertain and acknowledged attempts conflated")
	report["lost_replies"] = map[string]any{"store_ack_records": 3, "source_unknown_record_attempts": 2, "explicit_replay_retries": 1, "visible_rows": copies}
	// Real Loki 400 on a wholly expired request. Empty event-time reads
	// provide no member-level confirmation of rejection.
	old := func(seq int64) sample { v := row(seq); v.Event -= int64(72 * time.Hour); return v }
	all := f.offer(t, "a", old(3), old(4))
	before = r.counts(t)
	var allHTTP, mixedHTTP []int
	f.sendHead(t, all, observedHTTP(t, r.send, 503, &allHTTP))
	allDelta := r.counts(t).minus(before)
	checkDelta(t, allDelta, counters{Unknown: 2, Bad: 1, Attempts: 1})
	check(t, f.state(t, all.Ref).State == "active" && f.state(t, all.Ref).UnknownRecordAttempts == 2, "all-expired 400 released or classified members")
	empty := r.visible(t, "a", old(3).Event-1, old(4).Event+1)
	f.act(t, instruction("park", all.Ref))
	report["all_expired"] = map[string]any{"receiver_delta": allDelta, "receiver_http": allHTTP, "query_rows": len(empty), "parked_records": 2}
	mixed := f.offer(t, "a", old(5), row(6))
	follow := f.offer(t, "a", row(7))
	before = r.counts(t)
	for range 2 {
		f.sendHead(t, mixed, observedHTTP(t, r.send, 503, &mixedHTTP))
		f.advance()
	}
	mixedDelta := r.counts(t).minus(before)
	checkDelta(t, mixedDelta, counters{Unknown: 4, Bad: 2, Attempts: 2})
	// This exact time window includes the follower, so early visibility fails.
	pre := r.visible(t, "a", ns+5, ns+8, row(6))
	check(t, f.state(t, mixed.Ref).State == "active" && f.state(t, follow.Ref).Attempts == 0, "same-lane precondition failed")
	f.act(t, instruction("park", mixed.Ref))
	checkDelta(t, r.counts(t).minus(before), mixedDelta)
	f.restart(t)
	receipt := f.state(t, mixed.Ref).Receipt
	check(t, receipt.Ref == mixed.Ref && receipt.UnknownRecordAttempts == 4 && receipt.UnknownMembers == 2, "mixed receipt discarded uncertainty")
	f.sendHead(t, follow, r.send)
	post := r.visible(t, "a", ns+5, ns+8, row(6), row(7))
	check(t, post[7] == 1, "unexpected follower multiplicity")
	r.visible(t, "b", ns, ns+8)
	checkDelta(t, r.counts(t).minus(before), counters{Ack: 1, Unknown: 4, Bad: 2, Attempts: 3})
	f.audit(t, map[ref]string{x.Ref: "acknowledged", amb.Ref: "parked", all.Ref: "parked", mixed.Ref: "parked", follow.Ref: "acknowledged"})
	report["mixed"] = map[string]any{"receiver_delta_before_park": mixedDelta, "receiver_http": mixedHTTP, "eligible_rows_before_park": pre[6], "same_lane_follower_rows_before_park": pre[7], "eligible_rows_after_park": post[6], "same_lane_follower_rows_after_park": post[7], "parking_store_ack_delta": 0, "unknown_members_at_park": receipt.UnknownMembers, "unknown_record_attempts_at_park": receipt.UnknownRecordAttempts, "other_tenant_rows": 0}
	b, err := json.MarshalIndent(report, "", "  ")
	must(t, err)
	must(t, os.WriteFile(filepath.Clean(os.Getenv("QUARANTINE_REPORT")), b, 0600))
}
