package quarantine

import (
	"bytes"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"time"

	collector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

// The harness supplies a private loopback receiver and fresh tenant credentials.
// Only its complete OTLP response can acknowledge a sanitized, nonempty batch.
type receiver struct {
	base   string
	tokens map[string]string
	client *http.Client
}

func newReceiver(base string, tokens map[string]string) *receiver {
	u, err := url.Parse(base)
	if err != nil {
		panic("invalid fixture receiver")
	}
	ep, err := netip.ParseAddrPort(u.Host)
	if err != nil || !ep.Addr().IsLoopback() || u.Scheme != "http" || u.User != nil || u.RequestURI() != "/" || u.Fragment != "" {
		panic("fixture receiver requires a numeric loopback endpoint")
	}
	return &receiver{base, tokens, &http.Client{Timeout: 4 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func (r *receiver) send(w delivery) outcome {
	req, err := http.NewRequest(http.MethodPost, r.base+"/v1/logs", bytes.NewReader(w.Payload))
	if err != nil {
		return outcome{Kind: "unknown"}
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Authorization", "Bearer "+r.tokens[w.Ref.Tenant])
	resp, err := r.client.Do(req)
	if err != nil {
		return outcome{Kind: "unknown"}
	}
	defer resp.Body.Close()
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return outcome{Kind: "credentials", HTTPStatus: resp.StatusCode}
	}
	if resp.StatusCode != 200 {
		var delay time.Duration
		v := resp.Header.Get("Retry-After")
		if seconds, err := strconv.ParseUint(v, 10, 31); err == nil {
			delay = time.Duration(seconds) * time.Second
		} else if until, err := http.ParseTime(v); err == nil {
			delay = max(0, time.Until(until))
		}
		return outcome{Kind: "unknown", RetryAfter: delay, HTTPStatus: resp.StatusCode}
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	var reply collector.ExportLogsServiceResponse
	if err != nil || len(b) > 65536 || resp.Header.Get("Content-Type") != "application/x-protobuf" || proto.Unmarshal(b, &reply) != nil || reply.PartialSuccess != nil {
		return outcome{Kind: "unknown", HTTPStatus: resp.StatusCode}
	}
	return outcome{Kind: "ack", HTTPStatus: resp.StatusCode}
}
