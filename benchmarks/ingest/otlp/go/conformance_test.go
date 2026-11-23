package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	collector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

type fixture struct {
	addr, a, b string
	client     http.Client
}
type observation struct {
	code int
	body []byte
	err  error
}

func secret() string {
	var b [32]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func startFixture(t *testing.T, binary string, store http.HandlerFunc) *fixture {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "otlp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "sink")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	mock := &http.Server{Handler: store, ReadHeaderTimeout: time.Second}
	go mock.Serve(l)
	t.Cleanup(func() { mock.Close() })
	f := &fixture{a: secret(), b: secret(), client: http.Client{Timeout: 5 * time.Second}}
	cmd := exec.Command(binary)
	cmd.Env = append(os.Environ(), "OBS_BENCH_LISTEN=127.0.0.1:0", "OBS_BENCH_LOKI_SOCKET="+path,
		"OBS_BENCH_TOKEN_A="+f.a, "OBS_BENCH_TOKEN_B="+f.b)
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	ready := make(chan string, 1)
	go func() {
		var tag, addr string
		fmt.Fscanln(out, &tag, &addr)
		if tag != "LISTENING" {
			addr = ""
		}
		ready <- addr
	}()
	select {
	case addr := <-ready:
		if addr == "" {
			t.Fatal("receiver failed before readiness")
		}
		f.addr = "http://" + addr
	case <-time.After(5 * time.Second):
		t.Fatal("receiver readiness timeout")
	}
	return f
}

func (f *fixture) request(method, path, token string, data []byte, headers map[string]string) observation {
	req, err := http.NewRequest(method, f.addr+path, bytes.NewReader(data))
	if err != nil {
		return observation{err: err}
	}
	req.Header.Set("Content-Type", media)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := f.client.Do(req)
	if err != nil {
		return observation{err: err}
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	return observation{res.StatusCode, body, err}
}

// Flush the complete chunked request in one write. A receiver can reply as
// soon as byteLimit+1 arrives. net/http's concurrent body writer can otherwise
// lose that response to a broken pipe while flushing the terminating chunk.
func (f *fixture) checkStreamedLimit(t *testing.T, payload []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, f.addr+"/v1/logs", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	req.Close = true
	req.Header.Set("Content-Type", media)
	req.Header.Set("Authorization", "Bearer "+f.a)
	var wire bytes.Buffer
	if err = req.Write(&wire); err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", req.URL.Host, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err = conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = wire.WriteTo(conn); err != nil {
		t.Fatal(err)
	}
	res, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("streamed request returned HTTP %d, want 413", res.StatusCode)
	}
}
func (o observation) want(t *testing.T, code int) []byte {
	t.Helper()
	if o.err != nil || o.code != code {
		t.Fatalf("HTTP got %d (%v), want %d", o.code, o.err, code)
	}
	return o.body
}
func (f *fixture) snapshot(t *testing.T) counts {
	t.Helper()
	var c counts
	if err := json.Unmarshal(f.request("GET", "/bench/v1/counts", f.a, nil, nil).want(t, 200), &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func fixtureWire(t *testing.T, ids ...int64) []byte {
	t.Helper()
	var records []*logs.LogRecord
	for _, n := range ids {
		records = append(records, &logs.LogRecord{TimeUnixNano: 123456, ObservedTimeUnixNano: 123999, SeverityNumber: 9,
			TraceId: bytes.Repeat([]byte{1}, 16), SpanId: bytes.Repeat([]byte{2}, 8), Flags: 0x101,
			Body: stringValue("private fixture body"), SeverityText: "private fixture severity", EventName: "private event",
			Attributes: []*common.KeyValue{{Key: "bench.seq", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: n}}},
				{Key: "tenant.id", Value: stringValue("b")}}})
	}
	req := &collector.ExportLogsServiceRequest{ResourceLogs: []*logs.ResourceLogs{{
		Resource:  &resource.Resource{Attributes: []*common.KeyValue{{Key: "service.name", Value: stringValue("forged")}}},
		SchemaUrl: "private resource", ScopeLogs: []*logs.ScopeLogs{{Scope: &common.InstrumentationScope{Name: "private scope"}, LogRecords: records}},
	}}}
	out, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func gz(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
func candidates(t *testing.T, test func(*testing.T, string)) {
	for _, lang := range []string{"rust", "go"} {
		t.Run(lang, func(t *testing.T) {
			path := os.Getenv("OBS_OTLP_" + strings.ToUpper(lang))
			if path == "" {
				if lang == "rust" {
					path = "../../../../target/debug/otlp-bench-rust"
				} else {
					path = "../../bin/otlp-bench-go"
				}
			}
			binary, err := filepath.Abs(path)
			if err != nil {
				t.Fatal(err)
			}
			test(t, binary)
		})
	}
}

func TestOTLPValidationAndDisclosure(t *testing.T) {
	candidates(t, func(t *testing.T, binary string) {
		type receipt struct {
			org  string
			data []byte
		}
		received := make(chan receipt, 4)
		f := startFixture(t, binary, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/otlp/v1/logs" || r.Header.Get("Content-Type") != media {
				w.WriteHeader(400)
				return
			}
			data, _ := io.ReadAll(r.Body)
			received <- receipt{r.Header.Get("X-Scope-OrgID"), data}
			w.WriteHeader(204)
		})
		send := func(data []byte, h map[string]string) observation { return f.request("POST", "/v1/logs", f.a, data, h) }
		raw := fixtureWire(t, 17, 0, 18)
		resp := send(gz(t, raw), map[string]string{"Content-Encoding": "gzip", "X-Scope-OrgID": "b"}).want(t, 200)
		var mixed collector.ExportLogsServiceResponse
		if proto.Unmarshal(resp, &mixed) != nil || mixed.GetPartialSuccess().GetRejectedLogRecords() != 1 {
			t.Fatal("missing OTLP partial success")
		}
		got := <-received
		var clean collector.ExportLogsServiceRequest
		if got.org != "a" || bytes.Contains(got.data, []byte("private")) || bytes.Contains(got.data, []byte("forged")) || proto.Unmarshal(got.data, &clean) != nil {
			t.Fatal("store received disallowed source data")
		}
		res := clean.GetResourceLogs()[0]
		if len(res.Resource.Attributes) != 1 || res.Resource.Attributes[0].GetKey() != "service.name" || res.Resource.Attributes[0].GetValue().GetStringValue() != "m1-bench" || res.ScopeLogs[0].Scope != nil {
			t.Fatal("untrusted resource or scope")
		}
		records := res.ScopeLogs[0].LogRecords
		if len(records) != 2 {
			t.Fatal("bad mixed batch")
		}
		for i, r := range records {
			if r.Body.GetStringValue() != "bench-"+strconv.Itoa(17+i) || r.TimeUnixNano != 123456 || r.ObservedTimeUnixNano != 123999 || r.Flags != 1 || r.SeverityNumber != 9 ||
				!bytes.Equal(r.TraceId, bytes.Repeat([]byte{1}, 16)) || !bytes.Equal(r.SpanId, bytes.Repeat([]byte{2}, 8)) || len(r.Attributes) != 0 {
				t.Fatal("metadata integrity mismatch")
			}
		}
		send(nil, nil).want(t, 200)
		send(fixtureWire(t, 0), nil).want(t, 400)
		send([]byte{0xff}, nil).want(t, 400)
		send(raw, map[string]string{"Content-Type": "text/plain"}).want(t, 415)
		send(raw, map[string]string{"Content-Encoding": "br"}).want(t, 415)
		send([]byte("invalid"), map[string]string{"Content-Encoding": "gzip"}).want(t, 400)
		oversized := make([]byte, byteLimit+1)
		// Observe the early Content-Length rejection before writing the payload.
		send(oversized, map[string]string{"Expect": "100-continue"}).want(t, 413)
		f.checkStreamedLimit(t, oversized)
		send(gz(t, make([]byte, byteLimit+1)), map[string]string{"Content-Encoding": "gzip"}).want(t, 413)
		send(fixtureWire(t, make([]int64, 257)...), nil).want(t, 413)
		f.request("POST", "/v1/logs", "", raw, nil).want(t, 401)
		f.request("POST", "/v1/metrics", f.a, raw, nil).want(t, 404)
		c := f.snapshot(t)
		if c.Admitted != 2 || c.Invalid != 2 || c.Acknowledged != 2 || c.Attempts != 1 || len(received) != 0 {
			t.Fatalf("unexpected counters %+v", c)
		}
		f.request("POST", "/v1/logs", f.b, fixtureWire(t, 21), nil).want(t, 200)
		second := <-received
		if second.org != "b" || proto.Unmarshal(second.data, &clean) != nil ||
			!proto.Equal(res.Resource, clean.ResourceLogs[0].Resource) {
			t.Fatal("both tenants must use the same fixed producer fixture")
		}
	})
}

func TestAckBoundaryAndRetry(t *testing.T) {
	candidates(t, func(t *testing.T, binary string) {
		var visits atomic.Int32
		f := startFixture(t, binary, func(w http.ResponseWriter, r *http.Request) {
			switch visits.Add(1) {
			case 1:
				w.Header().Set("Location", "/unexpected")
				w.WriteHeader(307)
			case 2:
				conn, _, _ := w.(http.Hijacker).Hijack()
				conn.Close()
			case 3:
				w.WriteHeader(200)
			case 4:
				w.WriteHeader(429)
			case 5:
				w.WriteHeader(503)
			default:
				w.WriteHeader(204)
			}
		})
		raw := fixtureWire(t, 5)
		for _, want := range []int{503, 503, 503, 503, 503, 200} {
			f.request("POST", "/v1/logs", f.a, raw, nil).want(t, want)
		}
		c := f.snapshot(t)
		if c.Admitted != 6 || c.Acknowledged != 1 || c.Unknown != 5 || c.BadRequests != 0 || c.Outstanding != 0 || c.Attempts != 6 || visits.Load() != 6 {
			t.Fatalf("ambiguous outcomes %+v", c)
		}
	})
}

func TestStoreValidationCanCommitBefore400(t *testing.T) {
	candidates(t, func(t *testing.T, binary string) {
		const oldest = 100000
		var writes atomic.Int32
		f := startFixture(t, binary, func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			var req collector.ExportLogsServiceRequest
			if err != nil || proto.Unmarshal(body, &req) != nil {
				w.WriteHeader(500)
				return
			}
			code := http.StatusNoContent
			for _, record := range req.ResourceLogs[0].ScopeLogs[0].LogRecords {
				if record.TimeUnixNano < oldest {
					code = http.StatusBadRequest
				} else {
					writes.Add(1)
				}
			}
			w.WriteHeader(code)
		})
		for _, ids := range [][]int64{{1}, {1, 2}, {1, 2}} {
			var req collector.ExportLogsServiceRequest
			if err := proto.Unmarshal(fixtureWire(t, ids...), &req); err != nil {
				t.Fatal(err)
			}
			req.ResourceLogs[0].ScopeLogs[0].LogRecords[0].TimeUnixNano = oldest - 1
			body, err := proto.Marshal(&req)
			if err != nil {
				t.Fatal(err)
			}
			f.request("POST", "/v1/logs", f.a, body, nil).want(t, 503)
		}
		c := f.snapshot(t)
		if c.BadRequests != 3 || c.Unknown != 5 || c.Acknowledged != 0 || c.Invalid != 0 || c.Outstanding != 0 || writes.Load() != 2 {
			t.Fatalf("store validation outcomes %+v, eligible writes %d", c, writes.Load())
		}
	})
}

func TestTenantBoundsAndClientTimeout(t *testing.T) {
	candidates(t, func(t *testing.T, binary string) {
		block := make(chan struct{})
		entered := make(chan struct{}, 4)
		f := startFixture(t, binary, func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Scope-OrgID") == "a" {
				entered <- struct{}{}
				<-block
			}
			w.WriteHeader(204)
		})
		released := false
		defer func() {
			if !released {
				close(block)
			}
		}()
		raw := fixtureWire(t, 9)
		fast := *f
		fast.client = http.Client{Timeout: 120 * time.Millisecond}
		done := make(chan observation, 1)
		go func() { done <- fast.request("POST", "/v1/logs", f.a, raw, nil) }()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("downstream did not start")
		}
		if o := <-done; o.err == nil {
			t.Fatal("expected lost caller outcome")
		}
		finished := make(chan observation, 2)
		for i := 0; i < 2; i++ {
			go func() { finished <- f.request("POST", "/v1/logs", f.a, raw, nil) }()
		}
		until := time.Now().Add(time.Second)
		for f.snapshot(t).Outstanding != 3 {
			if time.Now().After(until) {
				t.Fatal("timed-out write lost its reservation")
			}
			time.Sleep(5 * time.Millisecond)
		}
		f.request("POST", "/v1/logs", f.a, raw, nil).want(t, 429)
		f.request("POST", "/v1/logs", f.b, raw, nil).want(t, 200)
		close(block)
		released = true
		for i := 0; i < 2; i++ {
			(<-finished).want(t, 200)
		}
		c := f.snapshot(t)
		if c.Outstanding != 0 || c.Overloads != 1 || c.Acknowledged != 3 {
			t.Fatalf("residual work %+v", c)
		}
	})
}

func TestBoundedTenantQuery(t *testing.T) {
	candidates(t, func(t *testing.T, binary string) {
		reads := make(chan string, 2)
		f := startFixture(t, binary, func(w http.ResponseWriter, r *http.Request) {
			q := r.URL.Query()
			if r.Method != "GET" || r.URL.Path != "/loki/api/v1/query_range" || q.Get("query") != "{service_name=\"m1-bench\"}" || q.Get("limit") != "512" {
				w.WriteHeader(400)
				return
			}
			reads <- r.Header.Get("X-Scope-OrgID")
			fmt.Fprint(w, "{}")
		})
		path := "/bench/v1/logs?start=100&end=200"
		f.request("GET", path, "", nil, nil).want(t, 401)
		for _, q := range []string{"start=1&end=0", "start=1&end=999999999999999999", "start=1&end=2&query=other"} {
			f.request("GET", "/bench/v1/logs?"+q, f.a, nil, nil).want(t, 400)
		}
		for _, row := range []struct{ token, org string }{{f.a, "a"}, {f.b, "b"}} {
			f.request("GET", path, row.token, nil, map[string]string{"X-Scope-OrgID": "other"}).want(t, 200)
			if <-reads != row.org {
				t.Fatal("query tenant mismatch")
			}
		}
	})
}

func TestFullSizeGzipAndRecordLimit(t *testing.T) {
	candidates(t, func(t *testing.T, binary string) {
		var total atomic.Int32
		f := startFixture(t, binary, func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			var batch collector.ExportLogsServiceRequest
			if err != nil || proto.Unmarshal(body, &batch) != nil {
				w.WriteHeader(400)
				return
			}
			for _, resource := range batch.ResourceLogs {
				for _, scope := range resource.ScopeLogs {
					total.Add(int32(len(scope.LogRecords)))
				}
			}
			w.WriteHeader(204)
		})
		var large collector.ExportLogsServiceRequest
		if err := proto.Unmarshal(fixtureWire(t, 1), &large); err != nil {
			t.Fatal(err)
		}
		entry := large.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		entry.Body = stringValue(strings.Repeat("x", 65000))
		entry.Body = stringValue(strings.Repeat("x", 65000+byteLimit-proto.Size(&large)))
		raw, err := proto.Marshal(&large)
		if err != nil || len(raw) != byteLimit {
			t.Fatal("invalid boundary fixture")
		}
		f.request("POST", "/v1/logs", f.a, gz(t, raw), map[string]string{"Content-Encoding": "gzip"}).want(t, 200)
		ids := make([]int64, recordLimit)
		for i := range ids {
			ids[i] = int64(i + 1)
		}
		f.request("POST", "/v1/logs", f.a, fixtureWire(t, ids...), nil).want(t, 200)
		if got := f.snapshot(t); got.Acknowledged != 257 || got.Attempts != 2 || total.Load() != 257 {
			t.Fatalf("boundary counts %+v", got)
		}
	})
}

func TestQueryTimeoutRetainsSlotWhileWritesContinue(t *testing.T) {
	candidates(t, func(t *testing.T, binary string) {
		entered, release := make(chan struct{}, 1), make(chan struct{})
		f := startFixture(t, binary, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" && r.Header.Get("X-Scope-OrgID") == "a" {
				entered <- struct{}{}
				<-release
			}
			if r.Method == "POST" {
				w.WriteHeader(204)
			} else {
				fmt.Fprint(w, "{}")
			}
		})
		defer close(release)
		path := "/bench/v1/logs?start=100&end=200"
		fast := &fixture{addr: f.addr, client: http.Client{Timeout: 150 * time.Millisecond}}
		done := make(chan observation, 1)
		go func() { done <- fast.request("GET", path, f.a, nil, nil) }()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("downstream query did not start")
		}
		if o := <-done; o.err == nil {
			t.Fatal("expected caller deadline")
		}
		f.request("GET", path, f.a, nil, nil).want(t, 429)
		f.request("GET", path, f.b, nil, nil).want(t, 200)
		f.request("POST", "/v1/logs", f.a, fixtureWire(t, 12), nil).want(t, 200)
	})
}
