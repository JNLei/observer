//! Capture is applied before handing spans to any store. The stored OTLP unit includes its resource and scope.
use crate::{
    authority::{random_hex, TraceCapture, TraceGrant},
    identity::now_ns,
    identity::valid_id,
    otlp, Error, Result,
};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use prost_reflect::{DynamicMessage, SerializeOptions};
use serde_json::{json, Value};

pub const UNIT_BYTES: usize = 12 * 1024;
const BATCH_BYTES: usize = 256 * 1024;
pub struct Captured {
    pub id: String,
    pub unit: ResourceSpans,
}
impl Captured {
    pub fn span(&self) -> &Span {
        &self.unit.scope_spans[0].spans[0]
    }
}

pub struct Batch {
    pub records: Vec<Captured>,
    pub rejected: usize,
}

pub fn valid_span(s: &Span) -> bool {
    valid_id(&s.trace_id, 16)
        && valid_id(&s.span_id, 8)
        && (s.parent_span_id.is_empty() || valid_id(&s.parent_span_id, 8))
        && s.start_time_unix_nano > 0
        && s.end_time_unix_nano >= s.start_time_unix_nano
        && s.end_time_unix_nano < i64::MAX as u64
        && (0..=5).contains(&s.kind)
        && s.status
            .as_ref()
            .is_none_or(|status| (0..=2).contains(&status.code))
        && s.events
            .iter()
            .all(|e| e.time_unix_nano > 0 && e.time_unix_nano < i64::MAX as u64)
        && s.links
            .iter()
            .all(|link| valid_id(&link.trace_id, 16) && valid_id(&link.span_id, 8))
}

fn withhold_text(s: &mut Span) {
    s.name.clear();
    s.trace_state.clear();
    s.attributes.clear();
    if let Some(status) = &mut s.status {
        status.message.clear();
    }
    for event in &mut s.events {
        event.name.clear();
        event.attributes.clear();
    }
    for link in &mut s.links {
        link.trace_state.clear();
        link.attributes.clear();
    }
}

pub fn metadata_only(unit: &ResourceSpans) -> bool {
    let mut sanitized = unit.clone();
    sanitized.resource = None;
    sanitized.schema_url.clear();
    for scope in &mut sanitized.scope_spans {
        scope.scope = None;
        scope.schema_url.clear();
        for span in &mut scope.spans {
            withhold_text(span);
        }
    }
    sanitized == *unit
}

pub fn prepare(raw: &[u8], gzip: bool, json: bool, grant: &TraceGrant) -> Result<Batch> {
    let request = otlp::traces(raw, gzip, json)?;
    let mut result = Batch {
        records: Vec::new(),
        rejected: 0,
    };
    let mut bytes = 0;
    let full = grant.capture == TraceCapture::Full;
    let now = now_ns();
    let window = now.saturating_sub(48 * 3_600_000_000_000)..=now.saturating_add(600_000_000_000);
    for mut resource in request.resource_spans {
        let scopes = std::mem::take(&mut resource.scope_spans);
        if !full {
            resource.resource = None;
            resource.schema_url.clear();
        }
        let resource_bytes = resource.encoded_len();
        for mut scope in scopes {
            let spans = std::mem::take(&mut scope.spans);
            if !full {
                scope.scope = None;
                scope.schema_url.clear();
            }
            // Check shared metadata once before replicating it across a batch.
            let metadata_bytes = resource_bytes + scope.encoded_len();
            if metadata_bytes > UNIT_BYTES {
                result.rejected += spans.len();
                continue;
            }
            for mut span in spans {
                if !valid_span(&span) || !window.contains(&span.start_time_unix_nano) {
                    result.rejected += 1;
                    continue;
                }
                if !full {
                    withhold_text(&mut span);
                }
                let unit = ResourceSpans {
                    scope_spans: vec![ScopeSpans {
                        spans: vec![span],
                        ..scope.clone()
                    }],
                    ..resource.clone()
                };
                let size = unit.encoded_len();
                if size > UNIT_BYTES {
                    result.rejected += 1;
                    continue;
                }
                bytes += size;
                if bytes > BATCH_BYTES {
                    return Err(Error::TooLarge);
                }
                result.records.push(Captured {
                    id: random_hex::<16>()?,
                    unit,
                });
            }
        }
    }
    Ok(result)
}

// Render the original OTLP fields with decimal nanoseconds, including special AnyValue doubles.
// ProtoJSON differs from OTLP/JSON on span context bytes.
pub fn render(unit: &ResourceSpans) -> Result<Value> {
    let descriptor = crate::otlp_json::POOL
        .get_message_by_name("opentelemetry.proto.trace.v1.ResourceSpans")
        .unwrap();
    let mut dynamic = DynamicMessage::new(descriptor);
    dynamic.transcode_from(unit).map_err(|_| Error::Internal)?;
    let mut json = dynamic
        .serialize_with_options(
            serde_json::value::Serializer,
            &SerializeOptions::new().use_enum_numbers(true),
        )
        .map_err(|_| Error::Internal)?;
    let span = &unit.scope_spans[0].spans[0];
    let output = &mut json["scopeSpans"][0]["spans"][0];
    output["traceId"] = json!(hex::encode(&span.trace_id));
    output["spanId"] = json!(hex::encode(&span.span_id));
    if !span.parent_span_id.is_empty() {
        output["parentSpanId"] = json!(hex::encode(&span.parent_span_id));
    }
    for (i, link) in span.links.iter().enumerate() {
        output["links"][i]["traceId"] = json!(hex::encode(&link.trace_id));
        output["links"][i]["spanId"] = json!(hex::encode(&link.span_id));
    }
    Ok(json)
}
