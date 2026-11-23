// Synthetic benchmark-only forwarding protocol; not an OTLP or production receiver.
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	statusOK byte = iota
	statusBusy
	statusDownstreamUnknown
	statusInvalid
	statusReady
)

type counts struct {
	Accepted                uint64 `json:"accepted"`
	Admitted                uint64 `json:"admitted"`
	Overloaded              uint64 `json:"overloaded"`
	OverloadWriting         uint64 `json:"overload_writing"`
	OverloadWriteOK         uint64 `json:"overload_write_ok"`
	OverloadWriteFailed     uint64 `json:"overload_write_failed"`
	Queued                  uint64 `json:"queued"`
	InFlight                uint64 `json:"in_flight"`
	Completed               uint64 `json:"completed"`
	ReadyWriteFailed        uint64 `json:"ready_write_failed"`
	StatusOK                uint64 `json:"status_ok"`
	StatusDownstreamUnknown uint64 `json:"status_downstream_unknown"`
	StatusInvalid           uint64 `json:"status_invalid"`
	ResponseWriteOK         uint64 `json:"response_write_ok"`
	ResponseWriteFailed     uint64 `json:"response_write_failed"`
}

type counterState struct {
	mu sync.Mutex
	c  counts
}

func (state *counterState) report(kind string) {
	state.mu.Lock()
	snapshot := state.c
	state.mu.Unlock()
	encoded, _ := json.Marshal(snapshot)
	fmt.Printf("%s %s\n", kind, encoded)
}

type config struct {
	listen     *net.TCPAddr
	downstream *net.TCPAddr
	workers    int
	queue      int
	timeout    time.Duration
	maxPayload int
}

func socketAddress(value string, allowZeroPort bool) (*net.TCPAddr, error) {
	host, portText, err := net.SplitHostPort(value)
	if err != nil {
		return nil, errors.New("invalid numeric socket address")
	}
	ip := net.ParseIP(host)
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 || ip == nil || !ip.IsLoopback() || (!allowZeroPort && port == 0) {
		return nil, errors.New("addresses must be loopback and the downstream port must be nonzero")
	}
	return &net.TCPAddr{IP: ip, Port: port}, nil
}

func parseConfig() (config, error) {
	listen := flag.String("listen", "127.0.0.1:19091", "numeric loopback listener")
	downstream := flag.String("downstream", "127.0.0.1:19090", "numeric loopback sink")
	workers := flag.Int("workers", 8, "worker count")
	queue := flag.Int("queue", 16, "bounded connection queue")
	timeoutMS := flag.Int("timeout-ms", 500, "absolute request deadline")
	maxPayload := flag.Int("max-payload", 65536, "maximum payload bytes")
	flag.Parse()
	if flag.NArg() != 0 {
		return config{}, errors.New("unexpected arguments")
	}
	l, err := socketAddress(*listen, true)
	if err != nil {
		return config{}, err
	}
	d, err := socketAddress(*downstream, false)
	if err != nil {
		return config{}, err
	}
	if *workers < 1 || *workers > 256 || *queue < 1 || *queue > 4096 || *timeoutMS < 1 || *timeoutMS > 60000 || *maxPayload < 1 || *maxPayload > 1048576 {
		return config{}, errors.New("worker, queue, timeout or payload limit out of range")
	}
	return config{l, d, *workers, *queue, time.Duration(*timeoutMS) * time.Millisecond, *maxPayload}, nil
}

func writeAll(conn net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func forward(upstream *net.TCPConn, cfg config, deadline time.Time) byte {
	if err := upstream.SetDeadline(deadline); err != nil {
		return statusInvalid
	}
	header := make([]byte, 12)
	if _, err := io.ReadFull(upstream, header); err != nil {
		return statusInvalid
	}
	size := binary.BigEndian.Uint32(header[8:])
	if uint64(size) > uint64(cfg.maxPayload) {
		return statusInvalid
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(upstream, payload); err != nil {
		return statusInvalid
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return statusDownstreamUnknown
	}
	conn, err := (&net.Dialer{Timeout: remaining}).Dial("tcp", cfg.downstream.String())
	if err != nil {
		return statusDownstreamUnknown
	}
	defer conn.Close()
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return statusDownstreamUnknown
	}
	if writeAll(conn, header) != nil || writeAll(conn, payload) != nil {
		return statusDownstreamUnknown
	}
	ack := make([]byte, 8)
	if _, err := io.ReadFull(conn, ack); err != nil {
		return statusDownstreamUnknown
	}
	if string(ack) != string(header[:8]) {
		return statusDownstreamUnknown
	}
	return statusOK
}

func run() error {
	cfg, err := parseConfig()
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp", cfg.listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	queue := make(chan *net.TCPConn, cfg.queue)
	state := &counterState{}
	var draining atomic.Bool
	var workers sync.WaitGroup
	for i := 0; i < cfg.workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for conn := range queue {
				state.mu.Lock()
				state.c.Queued--
				state.c.InFlight++
				state.mu.Unlock()
				_ = conn.SetNoDelay(true)
				deadline := time.Now().Add(cfg.timeout)
				readyErr := conn.SetWriteDeadline(deadline)
				if readyErr == nil {
					readyErr = writeAll(conn, []byte{statusReady})
				}
				if readyErr != nil {
					_ = conn.Close()
					state.mu.Lock()
					state.c.InFlight--
					state.c.Completed++
					state.c.ReadyWriteFailed++
					state.mu.Unlock()
					continue
				}
				status := forward(conn, cfg, deadline)
				// A client timeout does not cancel downstream work; the fixed worker
				// remains occupied until forward returns or its absolute deadline expires.
				writeErr := conn.SetWriteDeadline(time.Now().Add(cfg.timeout))
				if writeErr == nil {
					writeErr = writeAll(conn, []byte{status})
				}
				_ = conn.Close()
				state.mu.Lock()
				state.c.InFlight--
				state.c.Completed++
				switch status {
				case statusOK:
					state.c.StatusOK++
				case statusDownstreamUnknown:
					state.c.StatusDownstreamUnknown++
				case statusInvalid:
					state.c.StatusInvalid++
				default:
					panic("forward returned an unexpected status")
				}
				if writeErr == nil {
					state.c.ResponseWriteOK++
				} else {
					state.c.ResponseWriteFailed++
				}
				state.mu.Unlock()
			}
		}()
	}
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			switch scanner.Text() {
			case "SNAPSHOT":
				state.report("SNAPSHOT")
			case "DRAIN":
				draining.Store(true)
			default:
				fmt.Fprintln(os.Stderr, "unknown control command")
			}
		}
	}()
	fmt.Printf("LISTENING %s\n", listener.Addr())
	for {
		if err := listener.SetDeadline(time.Now().Add(2 * time.Millisecond)); err != nil {
			return err
		}
		conn, err := listener.AcceptTCP()
		if err != nil {
			if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				if draining.Load() {
					break
				}
				continue
			}
			return err
		}
		state.mu.Lock()
		state.c.Accepted++
		select {
		case queue <- conn:
			state.c.Admitted++
			state.c.Queued++
			state.mu.Unlock()
		default:
			state.c.Overloaded++
			state.c.OverloadWriting++
			state.mu.Unlock()
			err := conn.SetWriteDeadline(time.Now().Add(cfg.timeout))
			if err == nil {
				err = writeAll(conn, []byte{statusBusy})
			}
			_ = conn.Close()
			state.mu.Lock()
			state.c.OverloadWriting--
			if err == nil {
				state.c.OverloadWriteOK++
			} else {
				state.c.OverloadWriteFailed++
			}
			state.mu.Unlock()
		}
	}
	_ = listener.Close()
	close(queue)
	workers.Wait()
	state.report("FINAL")
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "benchmark forwarder:", err)
		os.Exit(1)
	}
}
