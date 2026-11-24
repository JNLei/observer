//! OTLP admission, capture policy and the storage-independent record.
use crate::authority::{random_hex, Capture, Grant};
use crate::{Error, Result};
use flate2::read::MultiGzDecoder;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest, common::v1::any_value::Value,
    logs::v1::LogRecord,
};
use prost::{
    encoding::{decode_key, decode_varint, skip_field, DecodeContext, WireType},
    Message,
};
use serde::{Deserialize, Serialize};
use std::{
    io::Read,
    time::{SystemTime, UNIX_EPOCH},
};

pub const BYTE_LIMIT: usize = 65_536;
const RECORD_LIMIT: usize = 256;
const STRUCTURE_LIMIT: usize = 4096;

pub fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

// Public nanoseconds use decimal strings so JavaScript clients retain precision.
mod decimal_ns {
    use serde::{de::Error, Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(ns: &u64, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(ns)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        String::deserialize(d)?.parse().map_err(D::Error::custom)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub id: String,
    pub tenant: String,
    pub producer_id: String,
    pub service: String,
    #[serde(with = "decimal_ns")]
    pub timestamp_ns: u64,
    pub event_ns: Option<String>,
    pub observed_ns: Option<String>,
    pub received_ns: String,
    pub severity_number: i32,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub trace_flags: u8,
    pub body_capture: String,
    pub body: Option<String>,
}

pub struct Batch {
    pub records: Vec<Record>,
    pub rejected: usize,
}

fn valid_id(id: &[u8], n: usize) -> bool {
    id.is_empty() || (id.len() == n && id.iter().any(|b| *b != 0))
}

fn normalize(r: LogRecord, grant: &Grant, received: u64) -> Result<Option<Record>> {
    if r.time_unix_nano >= i64::MAX as u64
        || r.observed_time_unix_nano >= i64::MAX as u64
        || !(0..=24).contains(&r.severity_number)
        || !valid_id(&r.trace_id, 16)
        || !valid_id(&r.span_id, 8)
        || (!r.span_id.is_empty() && r.trace_id.is_empty())
    {
        return Ok(None);
    }
    let (body_capture, body) = match (grant.capture, r.body.and_then(|b| b.value)) {
        (_, None) => ("absent", None),
        (Capture::MetadataOnly, Some(_)) => ("withheld_by_policy", None),
        (Capture::TextBody, Some(Value::StringValue(s))) if s.len() <= 4096 => ("text", Some(s)),
        (Capture::TextBody, Some(Value::StringValue(_))) => ("withheld_size", None),
        (Capture::TextBody, Some(_)) => ("withheld_type", None),
    };
    let timestamp = [r.time_unix_nano, r.observed_time_unix_nano, received]
        .into_iter()
        .find(|&t| t != 0)
        .ok_or(Error::Internal)?;
    Ok(Some(Record {
        id: random_hex::<16>()?,
        tenant: grant.tenant().to_owned(),
        producer_id: grant.subject.clone(),
        service: grant.service.clone(),
        timestamp_ns: timestamp,
        event_ns: (r.time_unix_nano != 0).then(|| r.time_unix_nano.to_string()),
        observed_ns: (r.observed_time_unix_nano != 0)
            .then(|| r.observed_time_unix_nano.to_string()),
        received_ns: received.to_string(),
        severity_number: r.severity_number,
        trace_flags: r.flags as u8,
        trace_id: (!r.trace_id.is_empty()).then(|| hex::encode(r.trace_id)),
        span_id: (!r.span_id.is_empty()).then(|| hex::encode(r.span_id)),
        body_capture: body_capture.into(),
        body,
    }))
}

// Inspect only the protobuf envelope and known nested lengths. Generated OTLP
// types remain the decoder. Count repeated empty messages before allocating them.
#[derive(Clone, Copy)]
enum Node {
    Export,
    ResourceLogs,
    ScopeLogs,
    Resource,
    Scope,
    Record,
    Pair,
    Any,
    Array,
    Pairs,
    Entity,
}
impl Node {
    fn child(self, tag: u32) -> Option<Self> {
        use Node::*;
        Some(match (self, tag) {
            (Export, 1) => ResourceLogs,
            (ResourceLogs, 1) => Resource,
            (ResourceLogs, 2) => ScopeLogs,
            (ScopeLogs, 1) => Scope,
            (ScopeLogs, 2) => Record,
            (Record, 5) | (Pair, 2) | (Array, 1) => Any,
            (Resource, 1) | (Scope, 3) | (Record, 6) | (Pairs, 1) => Pair,
            (Resource, 3) => Entity,
            (Any, 5) => Array,
            (Any, 6) => Pairs,
            _ => return None,
        })
    }
}
fn protobuf_budget(
    mut data: &[u8],
    node: Node,
    depth: u8,
    fields: &mut usize,
    records: &mut usize,
) -> Result<()> {
    if depth > 32 {
        return Err(Error::TooLarge);
    }
    while !data.is_empty() {
        *fields += 1;
        if *fields > STRUCTURE_LIMIT {
            return Err(Error::TooLarge);
        }
        let (tag, wire) = decode_key(&mut data).map_err(|_| Error::Invalid)?;
        if let (Some(child), WireType::LengthDelimited) = (node.child(tag), wire) {
            let len = decode_varint(&mut data).map_err(|_| Error::Invalid)?;
            if len > data.len() as u64 {
                return Err(Error::Invalid);
            }
            if matches!(child, Node::Record) {
                *records += 1;
                if *records > RECORD_LIMIT {
                    return Err(Error::TooLarge);
                }
            }
            let (nested, rest) = data.split_at(len as usize);
            protobuf_budget(nested, child, depth + 1, fields, records)?;
            data = rest;
        } else {
            skip_field(wire, tag, &mut data, DecodeContext::default())
                .map_err(|_| Error::Invalid)?;
        }
    }
    Ok(())
}
fn json_budget(data: &[u8]) -> Result<()> {
    let (mut quoted, mut escaped, mut structural, mut depth) = (false, false, 0, 0i32);
    for &b in data {
        if quoted {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                quoted = false;
            }
        } else {
            match b {
                b'"' => quoted = true,
                b'{' | b'[' => {
                    depth += 1;
                    structural += 1;
                }
                b'}' | b']' => depth -= 1,
                b',' => structural += 1,
                _ => (),
            }
            if depth > 32 || structural > STRUCTURE_LIMIT {
                return Err(Error::TooLarge);
            }
        }
    }
    Ok(())
}

pub fn prepare(wire: &[u8], gzip: bool, json: bool, grant: &Grant) -> Result<Batch> {
    let mut inflated = Vec::new();
    let raw = if gzip {
        MultiGzDecoder::new(wire)
            .take(BYTE_LIMIT as u64 + 1)
            .read_to_end(&mut inflated)
            .map_err(|_| Error::Invalid)?;
        inflated.as_slice()
    } else {
        wire
    };
    if raw.len() > BYTE_LIMIT {
        return Err(Error::TooLarge);
    }
    let req: ExportLogsServiceRequest = if json {
        json_budget(raw)?;
        serde_json::from_slice(raw).map_err(|_| Error::Invalid)?
    } else {
        protobuf_budget(raw, Node::Export, 0, &mut 0, &mut 0)?;
        ExportLogsServiceRequest::decode(raw).map_err(|_| Error::Invalid)?
    };
    let mut batch = Batch {
        records: vec![],
        rejected: 0,
    };
    let received = now_ns();
    for record in req
        .resource_logs
        .into_iter()
        .flat_map(|r| r.scope_logs)
        .flat_map(|s| s.log_records)
    {
        if batch.records.len() + batch.rejected == RECORD_LIMIT {
            return Err(Error::TooLarge);
        }
        match normalize(record, grant, received)? {
            Some(record) => batch.records.push(record),
            None => batch.rejected += 1,
        }
    }
    Ok(batch)
}
