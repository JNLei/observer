package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"strconv"

	collector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

const byteLimit = 65536
const recordLimit = 256

type batch struct {
	wire               []byte
	accepted, rejected int
}

func validID(id []byte, size int) bool {
	if len(id) == 0 {
		return true
	}
	if len(id) != size {
		return false
	}
	for _, b := range id {
		if b != 0 {
			return true
		}
	}
	return false
}

func stringValue(s string) *common.AnyValue {
	return &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: s}}
}

func sanitize(r *logs.LogRecord) *logs.LogRecord {
	var id int64
	for _, a := range r.Attributes {
		if a.GetKey() == "bench.seq" {
			n := a.GetValue().GetIntValue()
			if id != 0 || n <= 0 {
				return nil
			}
			id = n
		}
	}
	if id == 0 || r.TimeUnixNano == 0 || r.TimeUnixNano > 1<<63-1 ||
		r.SeverityNumber < 0 || r.SeverityNumber > 24 ||
		!validID(r.TraceId, 16) || !validID(r.SpanId, 8) || (len(r.SpanId) > 0 && len(r.TraceId) == 0) {
		return nil
	}
	return &logs.LogRecord{
		TimeUnixNano: r.TimeUnixNano, ObservedTimeUnixNano: r.ObservedTimeUnixNano, SeverityNumber: r.SeverityNumber,
		TraceId: r.TraceId, SpanId: r.SpanId, Flags: r.Flags & 0xff, Body: stringValue("bench-" + strconv.FormatInt(id, 10)),
	}
}

func prepare(raw []byte, compressed bool) (batch, int) {
	if compressed {
		decoder, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return batch{}, 400
		}
		defer decoder.Close()
		raw, err = io.ReadAll(io.LimitReader(decoder, byteLimit+1))
		if err != nil {
			return batch{}, 400
		}
		if len(raw) > byteLimit {
			return batch{}, 413
		}
	}
	var req collector.ExportLogsServiceRequest
	opts := proto.UnmarshalOptions{DiscardUnknown: true, RecursionLimit: 100}
	if err := opts.Unmarshal(raw, &req); err != nil {
		return batch{}, 400
	}
	result := batch{}
	var records []*logs.LogRecord
	for _, group := range req.ResourceLogs {
		for _, scope := range group.ScopeLogs {
			for _, r := range scope.LogRecords {
				if result.accepted+result.rejected == recordLimit {
					return batch{}, 413
				}
				if clean := sanitize(r); clean != nil {
					result.accepted++
					records = append(records, clean)
				} else {
					result.rejected++
				}
			}
		}
	}
	out := &collector.ExportLogsServiceRequest{ResourceLogs: []*logs.ResourceLogs{{
		Resource:  &resource.Resource{Attributes: []*common.KeyValue{{Key: "service.name", Value: stringValue("m1-bench")}}},
		ScopeLogs: []*logs.ScopeLogs{{LogRecords: records}},
	}}}
	wire, err := proto.Marshal(out)
	if err != nil {
		return batch{}, 500
	}
	result.wire = wire
	return result, 0
}

func success(rejected int) []byte {
	out := &collector.ExportLogsServiceResponse{}
	if rejected > 0 {
		out.PartialSuccess = &collector.ExportLogsPartialSuccess{RejectedLogRecords: int64(rejected), ErrorMessage: "record failed fixture validation"}
	}
	data, _ := proto.Marshal(out)
	return data
}
