//! Bounded OTLP wire decoding shared by the independent signal paths.
use crate::{Error, Result};
use flate2::read::MultiGzDecoder;
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, trace::v1::ExportTraceServiceRequest,
};
use prost::{
    encoding::{decode_key, decode_varint, skip_field, DecodeContext, WireType},
    Message,
};
use std::io::Read;

pub const BYTE_LIMIT: usize = 65_536;
pub const RECORD_LIMIT: usize = 256;
pub const STRUCTURE_LIMIT: usize = 4096;
// Inspect only the protobuf envelope and known nested lengths. Generated OTLP
// types remain the decoder. Count repeated empty messages before allocating them.
#[derive(Clone, Copy)]
enum Node {
    Logs,
    Traces,
    ResourceSpans,
    ScopeSpans,
    Span,
    Event,
    Link,
    Status,
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
            (Logs, 1) => ResourceLogs,
            (ResourceLogs, 1) => Resource,
            (ResourceLogs, 2) => ScopeLogs,
            (ScopeLogs, 1) => Scope,
            (ScopeLogs, 2) => Record,
            (Record, 5) | (Pair, 2) | (Array, 1) => Any,
            (Resource, 1) | (Scope, 3) | (Record, 6) | (Pairs, 1) => Pair,
            (Traces, 1) => ResourceSpans,
            (ResourceSpans, 1) => Resource,
            (ResourceSpans, 2) => ScopeSpans,
            (ScopeSpans, 1) => Scope,
            (ScopeSpans, 2) => Span,
            (Span, 9) | (Event, 3) | (Link, 4) => Pair,
            (Span, 11) => Event,
            (Span, 13) => Link,
            (Span, 15) => Status,
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
            if matches!(child, Node::Record | Node::Span) {
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
fn decode<M: Message + Default>(
    wire: &[u8],
    gzip: bool,
    json: bool,
    node: Node,
    descriptor: &str,
) -> Result<M> {
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
    if json {
        crate::otlp_json::decode(raw, descriptor)
    } else {
        protobuf_budget(raw, node, 0, &mut 0, &mut 0)?;
        M::decode(raw).map_err(|_| Error::Invalid)
    }
}
pub fn logs(wire: &[u8], gzip: bool, json: bool) -> Result<ExportLogsServiceRequest> {
    decode(wire, gzip, json, Node::Logs, crate::otlp_json::LOGS_REQUEST)
}
pub fn traces(wire: &[u8], gzip: bool, json: bool) -> Result<ExportTraceServiceRequest> {
    decode(
        wire,
        gzip,
        json,
        Node::Traces,
        crate::otlp_json::TRACES_REQUEST,
    )
}

pub fn stored_unit(data: &[u8]) -> Result<opentelemetry_proto::tonic::trace::v1::ResourceSpans> {
    protobuf_budget(data, Node::ResourceSpans, 0, &mut 0, &mut 0)?;
    Message::decode(data).map_err(|_| Error::Invalid)
}
