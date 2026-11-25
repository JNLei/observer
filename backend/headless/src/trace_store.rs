//! A private VictoriaTraces OTLP writer and fixed, tenant-scoped lookup. Physical span IDs stay native.
use crate::{
    authority::{name_ok, TraceCapture, TraceGrant},
    identity::lower_hex,
    otlp,
    query::{Query, Truncation},
    storage::{Exchange, StoreError, StoreResult},
    traces::{self, Captured, UNIT_BYTES},
};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use http_body_util::Full;
use hyper::{header, Method, Request};
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value, AnyValue, KeyValue},
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use prost::Message;
use serde::Deserialize;
use std::path::PathBuf;
use tokio::time::Instant;

pub struct Retrieved {
    pub captured: Captured,
    pub producer: String,
    pub service: String,
}
pub struct SpanPage {
    pub records: Vec<Retrieved>,
    pub truncation: Option<Truncation>,
}

#[async_trait]
pub trait TraceStore: Send + Sync {
    fn source(&self) -> &'static str;
    async fn write(&self, grant: &TraceGrant, records: &[Captured]) -> StoreResult<()>;
    async fn scan(&self, grant: &TraceGrant, q: &Query, deadline: Instant)
        -> StoreResult<SpanPage>;
}

pub struct VictoriaTraces(pub PathBuf);
fn field(key: &str, value: String) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value)),
        }),
        ..Default::default()
    }
}

fn storage_span(grant: &TraceGrant, record: &Captured) -> Span {
    let span = record.span();
    // The native store flattens attribute types. A bounded, sanitized OTLP unit
    // preserves typed resource/scope/event/link data alongside the native IDs.
    Span {
        trace_id: span.trace_id.clone(),
        span_id: span.span_id.clone(),
        parent_span_id: span.parent_span_id.clone(),
        start_time_unix_nano: span.start_time_unix_nano,
        end_time_unix_nano: span.end_time_unix_nano,
        name: span.name.clone(),
        kind: span.kind,
        attributes: vec![
            field("observe.id", record.id.clone()),
            field("observe.tenant", grant.principal.tenant().into()),
            field("observe.producer", grant.principal.subject.clone()),
            field("observe.service", grant.principal.service.clone()),
            field("observe.unit", STANDARD.encode(record.unit.encode_to_vec())),
        ],
        ..Default::default()
    }
}

#[derive(Deserialize)]
struct Row {
    #[serde(rename = "span_attr:observe.id")]
    id: String,
    #[serde(rename = "span_attr:observe.tenant")]
    tenant: String,
    #[serde(rename = "span_attr:observe.producer")]
    producer: String,
    #[serde(rename = "span_attr:observe.service")]
    service: String,
    #[serde(rename = "span_attr:observe.unit")]
    payload: String,
    trace_id: String,
    span_id: String,
    start_time_unix_nano: String,
    end_time_unix_nano: String,
}

impl Row {
    fn verify(self, grant: &TraceGrant, q: &Query, start: u64, end: u64) -> StoreResult<Retrieved> {
        if self.tenant != grant.principal.tenant()
            || !lower_hex(&self.id, 32)
            || !lower_hex(&self.producer, 32)
            || !name_ok(&self.service)
            || q.trace_id.as_ref() != Some(&self.trace_id)
            || q.service.as_ref().is_some_and(|s| s != &self.service)
            || q.record_id.as_ref().is_some_and(|id| id != &self.id)
            || self.payload.len() > UNIT_BYTES.div_ceil(3) * 4
        {
            return Err(StoreError::Response);
        }
        let bytes = STANDARD
            .decode(self.payload)
            .map_err(|_| StoreError::Response)?;
        let unit = otlp::stored_unit(&bytes).map_err(|_| StoreError::Response)?;
        if unit.scope_spans.len() != 1
            || unit.scope_spans[0].spans.len() != 1
            || (grant.capture == TraceCapture::MetadataOnly && !traces::metadata_only(&unit))
        {
            return Err(StoreError::Response);
        }
        let captured = Captured { id: self.id, unit };
        let span = captured.span();
        if !traces::valid_span(span)
            || hex::encode(&span.trace_id) != self.trace_id
            || hex::encode(&span.span_id) != self.span_id
            || !(start..end).contains(&span.start_time_unix_nano)
            || self.start_time_unix_nano != span.start_time_unix_nano.to_string()
            || self.end_time_unix_nano != span.end_time_unix_nano.to_string()
        {
            return Err(StoreError::Response);
        }
        Ok(Retrieved {
            captured,
            producer: self.producer,
            service: self.service,
        })
    }
}

fn request(
    grant: &TraceGrant,
    method: Method,
    path: &str,
    data: Vec<u8>,
) -> StoreResult<Request<Full<Bytes>>> {
    Request::builder()
        .method(method)
        .uri(path)
        .header(header::HOST, "localhost")
        .header("AccountID", grant.account)
        .header("ProjectID", "0")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Full::new(Bytes::from(data)))
        .map_err(|_| StoreError::Offline)
}

#[async_trait]
impl TraceStore for VictoriaTraces {
    fn source(&self) -> &'static str {
        "local-victoriatraces-v1"
    }
    async fn write(&self, grant: &TraceGrant, records: &[Captured]) -> StoreResult<()> {
        let spans = records.iter().map(|r| storage_span(grant, r)).collect();
        let export = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let req = request(
            grant,
            Method::POST,
            "/insert/opentelemetry/v1/traces",
            export.encode_to_vec(),
        )?;
        let (code, bytes) = Exchange::connect(&self.0).await?.perform(req).await?;
        // Pinned VT returns an empty protobuf ExportTraceServiceResponse on admission.
        if code != 200 || !bytes.is_empty() {
            return Err(StoreError::Response);
        }
        Ok(())
    }

    async fn scan(
        &self,
        grant: &TraceGrant,
        q: &Query,
        deadline: Instant,
    ) -> StoreResult<SpanPage> {
        let (start, end) = q.bounds().map_err(|_| StoreError::Response)?;
        let trace = q.trace_id.as_ref().ok_or(StoreError::Response)?;
        let mut expr = format!(r#"trace_id:="{trace}""#);
        for (field, value) in [
            ("observe.id", &q.record_id),
            ("observe.service", &q.service),
        ] {
            if let Some(value) = value {
                expr.push_str(&format!(r#" "span_attr:{field}":="{value}""#));
            }
        }
        expr.push_str(r#" | fields trace_id,span_id,start_time_unix_nano,end_time_unix_nano,"span_attr:observe.id","span_attr:observe.tenant","span_attr:observe.producer","span_attr:observe.service","span_attr:observe.unit""#);
        let conn = Exchange::connect(&self.0).await?;
        let ms = deadline
            .saturating_duration_since(Instant::now())
            .as_millis();
        if ms == 0 {
            return Err(StoreError::Deadline);
        }
        let seconds = |ns: u64| format!("{}.{:09}", ns / 1_000_000_000, ns % 1_000_000_000);
        let path = format!(
            "/select/logsql/query?disable_latency_offset=1&query={}&start={}&end={}&limit={}&timeout={ms}ms",
            utf8_percent_encode(&expr, NON_ALPHANUMERIC),seconds(start),seconds(end),
            q.limit + 1
        );
        let (code, reply) = conn
            .perform(request(grant, Method::GET, &path, vec![])?)
            .await?;
        if code != 200 {
            return Err(StoreError::Response);
        }
        let grant = grant.clone();
        let q = q.clone();
        tokio::task::spawn_blocking(move || decode_page(&reply, &grant, &q, start, end))
            .await
            .map_err(|_| StoreError::Response)?
    }
}

fn decode_page(
    reply: &[u8],
    grant: &TraceGrant,
    q: &Query,
    start: u64,
    end: u64,
) -> StoreResult<SpanPage> {
    // Bound the page's cumulative protobuf materialization separately from its HTTP and row limits.
    const PAGE_PAYLOAD_BYTES: usize = 128 * 1024;
    let mut records = Vec::new();
    let mut payload_bytes = 0;
    let mut truncation = None;
    for (index, row) in serde_json::Deserializer::from_slice(reply)
        .into_iter::<Row>()
        .enumerate()
    {
        let row = row.map_err(|_| StoreError::Response)?;
        if index > q.limit {
            return Err(StoreError::Response);
        }
        let size = row.payload.len();
        // Even a discarded row can disclose existence through a cutoff.
        // Verify one bounded unit at a time, retaining only the returned prefix.
        let record = row.verify(grant, q, start, end)?;
        if truncation.is_some() {
            continue;
        }
        payload_bytes += size;
        if index == q.limit {
            truncation = Some(Truncation::RowLimit);
        } else if payload_bytes > PAGE_PAYLOAD_BYTES {
            truncation = Some(Truncation::ResponseByteLimit);
        } else {
            records.push(record);
        }
    }
    records.sort_by(|a, b| {
        (a.captured.span().start_time_unix_nano, &a.captured.id)
            .cmp(&(b.captured.span().start_time_unix_nano, &b.captured.id))
    });
    Ok(SpanPage {
        records,
        truncation,
    })
}
