// Local OTLP/HTTP logs receiver for the paired Loki trial.
package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/netutil"
	rpc "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

const media = "application/x-protobuf"
const ioTime = 2 * time.Second
const queryLimit = 512 * 1024

type counts struct {
	InputBytes   int64 `json:"input_wire_bytes"`
	AckBytes     int64 `json:"store_ack_bytes"`
	Admitted     int64 `json:"admitted_records"`
	Invalid      int64 `json:"invalid_records"`
	Attempts     int64 `json:"store_attempts"`
	Acknowledged int64 `json:"store_ack_records"`
	Unknown      int64 `json:"store_unknown_records"`
	BadRequests  int64 `json:"store_http_400_attempts"`
	Outstanding  int64 `json:"outstanding_records"`
	Overloads    int64 `json:"overloaded_requests"`
}
type tenant struct {
	name, credential       string
	writes, writer, reader chan struct{}
	mu                     sync.Mutex
	counts                 counts
}
type trial struct {
	client   *http.Client
	tenants  [2]*tenant
	decoders chan struct{}
}

func reply(w http.ResponseWriter, code int, kind string, data []byte) {
	w.Header().Set("Content-Type", kind)
	if code == 429 || code == 503 {
		w.Header().Set("Retry-After", "1")
	}
	w.WriteHeader(code)
	_, _ = w.Write(data)
}
func failure(w http.ResponseWriter, code int) {
	data, _ := proto.Marshal(&rpc.Status{Message: http.StatusText(code)})
	reply(w, code, media, data)
}

func (t *trial) store(user, method, path string, body []byte) (int, []byte) {
	// A store transaction has its own deadline even after a caller disconnects.
	req, err := http.NewRequest(method, "http://localhost"+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil
	}
	req.Header.Set("X-Scope-OrgID", user)
	req.Header.Set("Content-Type", media)
	resp, err := t.client.Do(req)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return resp.StatusCode, nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, queryLimit+1))
	if err != nil || len(data) > queryLimit {
		return 0, nil
	}
	return resp.StatusCode, data
}

func queryPath(raw string) (string, bool) {
	startText, endText, ok := strings.Cut(strings.TrimPrefix(raw, "start="), "&end=")
	if !strings.HasPrefix(raw, "start=") || !ok {
		return "", false
	}
	for _, s := range []string{startText, endText} {
		if s == "" || strings.Trim(s, "0123456789") != "" {
			return "", false
		}
	}
	start, e1 := strconv.ParseInt(startText, 10, 64)
	end, e2 := strconv.ParseInt(endText, 10, 64)
	if e1 != nil || e2 != nil || start <= 0 || start >= end || end-start > int64(26*time.Hour) {
		return "", false
	}
	return fmt.Sprintf("/loki/api/v1/query_range?query=%%7Bservice_name%%3D%%22m1-bench%%22%%7D&start=%d&end=%d&limit=512&direction=forward", start, end), true
}

func (t *trial) ingest(w http.ResponseWriter, r *http.Request, u *tenant) {
	if types := r.Header.Values("Content-Type"); len(types) != 1 || !strings.EqualFold(types[0], media) {
		failure(w, 415)
		return
	}
	enc := r.Header.Values("Content-Encoding")
	compressed := len(enc) == 1 && strings.EqualFold(enc[0], "gzip")
	if len(enc) > 0 && !(compressed || (len(enc) == 1 && strings.EqualFold(enc[0], "identity"))) {
		failure(w, 415)
		return
	}
	if r.ContentLength > byteLimit {
		failure(w, 413)
		return
	}
	select {
	case u.writes <- struct{}{}:
		defer func() { <-u.writes }()
	default:
		u.mu.Lock()
		u.counts.Overloads++
		u.mu.Unlock()
		failure(w, 429)
		return
	}
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(ioTime))
	raw, err := io.ReadAll(io.LimitReader(r.Body, byteLimit+1))
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			failure(w, 408)
		} else {
			failure(w, 400)
		}
		return
	}
	if len(raw) > byteLimit {
		failure(w, 413)
		return
	}
	inputBytes := len(raw)
	t.decoders <- struct{}{}
	b, code := prepare(raw, compressed)
	<-t.decoders
	if code != 0 {
		failure(w, code)
		return
	}
	u.mu.Lock()
	u.counts.Invalid += int64(b.rejected)
	u.counts.Admitted += int64(b.accepted)
	u.counts.InputBytes += int64(inputBytes)
	u.mu.Unlock()
	if b.accepted == 0 {
		if b.rejected != 0 {
			failure(w, 400)
		} else {
			reply(w, 200, media, nil)
		}
		return
	}
	u.writer <- struct{}{}
	u.mu.Lock()
	u.counts.Attempts++
	u.mu.Unlock()
	code, _ = t.store(u.name, http.MethodPost, "/otlp/v1/logs", b.wire)
	u.mu.Lock()
	if code == 204 {
		u.counts.Acknowledged += int64(b.accepted)
		u.counts.AckBytes += int64(len(b.wire))
	} else {
		u.counts.Unknown += int64(b.accepted)
		if code == http.StatusBadRequest {
			// Loki can commit the valid subset before returning 400.
			u.counts.BadRequests++
		}
	}
	u.mu.Unlock()
	<-u.writer
	if code == 204 {
		reply(w, 200, media, success(b.rejected))
	} else {
		failure(w, 503)
	}
}

func (t *trial) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Values("Authorization")
	if len(auth) != 1 {
		failure(w, 401)
		return
	}
	var u *tenant
	for _, candidate := range t.tenants {
		if subtle.ConstantTimeCompare([]byte(auth[0]), []byte(candidate.credential)) == 1 {
			u = candidate
		}
	}
	if u == nil {
		failure(w, 401)
		return
	}
	switch {
	case r.Method == "POST" && r.RequestURI == "/v1/logs":
		t.ingest(w, r, u)
	case r.Method == "GET" && r.RequestURI == "/bench/v1/counts":
		u.mu.Lock()
		c := u.counts
		u.mu.Unlock()
		c.Outstanding = c.Admitted - c.Acknowledged - c.Unknown
		data, _ := json.Marshal(c)
		reply(w, 200, "application/json", data)
	case r.Method == "GET" && r.URL.EscapedPath() == "/bench/v1/logs":
		path, ok := queryPath(r.URL.RawQuery)
		if !ok {
			failure(w, 400)
			return
		}
		select {
		case u.reader <- struct{}{}:
			defer func() { <-u.reader }()
		default:
			failure(w, 429)
			return
		}
		code, data := t.store(u.name, http.MethodGet, path, nil)
		if code != 200 {
			failure(w, 503)
		} else {
			reply(w, 200, "application/json", data)
		}
	default:
		failure(w, 404)
	}
}

func run() error {
	addr := os.Getenv("OBS_BENCH_LISTEN")
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ep, err := netip.ParseAddrPort(addr)
	socket := os.Getenv("OBS_BENCH_LOKI_SOCKET")
	if err != nil || !ep.Addr().IsLoopback() || !filepath.IsAbs(socket) {
		return errors.New("invalid trial configuration")
	}
	t := trial{decoders: make(chan struct{}, 2)}
	for i, name := range []string{"a", "b"} {
		token := os.Getenv("OBS_BENCH_TOKEN_" + strings.ToUpper(name))
		if len(token) != 64 || strings.Trim(token, "0123456789abcdef") != "" {
			return errors.New("invalid trial credential")
		}
		t.tenants[i] = &tenant{name: name, credential: "Bearer " + token, writes: make(chan struct{}, 3), writer: make(chan struct{}, 1), reader: make(chan struct{}, 1)}
	}
	if t.tenants[0].credential == t.tenants[1].credential {
		return errors.New("invalid trial configuration")
	}
	t.client = &http.Client{Timeout: ioTime, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: &http.Transport{
		DisableCompression: true, DisableKeepAlives: true, MaxResponseHeaderBytes: 8192,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
	}}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	server := http.Server{Handler: &t, ReadHeaderTimeout: ioTime, ReadTimeout: 2 * ioTime, WriteTimeout: 9 * time.Second, MaxHeaderBytes: 8192}
	server.SetKeepAlivesEnabled(false)
	fmt.Println("LISTENING", listener.Addr())
	return server.Serve(netutil.LimitListener(listener, 12))
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
