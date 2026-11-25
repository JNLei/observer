//! OTLP admission, capture policy and the storage-independent record.
use crate::authority::{name_ok, random_hex, Capture, Grant};
use crate::identity::{lower_hex, now_ns, Identity, TimestampNs};
use crate::otlp::RECORD_LIMIT;
use crate::{Error, Result};
use opentelemetry_proto::tonic::{common::v1::any_value::Value, logs::v1::LogRecord};
use serde::{Deserialize, Serialize};

pub const TEXT_BODY_LIMIT: usize = 4096;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub id: String,
    pub tenant: String,
    pub producer_id: String,
    pub service: String,
    pub timestamp_ns: TimestampNs,
    pub event_ns: Option<TimestampNs>,
    pub observed_ns: Option<TimestampNs>,
    pub received_ns: TimestampNs,
    pub severity_number: i32,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub trace_flags: u8,
    pub body_capture: String,
    pub body: Option<String>,
}

impl Record {
    pub fn identity<'a>(&'a self, source: &'a str) -> Identity<'a> {
        Identity {
            tenant: &self.tenant,
            producer_id: &self.producer_id,
            service: &self.service,
            trace_id: self.trace_id.as_deref(),
            span_id: self.span_id.as_deref(),
            parent_span_id: None,
            event_ns: self.event_ns,
            source,
        }
    }

    pub fn matches_writer(&self, grant: &Grant) -> bool {
        self.producer_id == grant.subject
            && self.service == grant.service
            && self.is_canonical(grant)
    }

    pub fn is_canonical(&self, grant: &Grant) -> bool {
        let correlation_id = |id: &str, len| lower_hex(id, len) && id.bytes().any(|b| b != b'0');
        let body_valid = match (self.body_capture.as_str(), self.body.as_deref()) {
            ("text", Some(s)) => grant.capture == Capture::TextBody && s.len() <= TEXT_BODY_LIMIT,
            ("absent", None) => true,
            ("withheld_by_policy", None) => grant.capture == Capture::MetadataOnly,
            ("withheld_size" | "withheld_type", None) => grant.capture == Capture::TextBody,
            _ => false,
        };
        self.tenant == grant.tenant()
            && lower_hex(&self.id, 32)
            && lower_hex(&self.producer_id, 32)
            && name_ok(&self.service)
            && self.timestamp_ns
                == self
                    .event_ns
                    .or(self.observed_ns)
                    .unwrap_or(self.received_ns)
            && (0..=24).contains(&self.severity_number)
            && self
                .trace_id
                .as_deref()
                .is_none_or(|id| correlation_id(id, 32))
            && self
                .span_id
                .as_deref()
                .is_none_or(|id| self.trace_id.is_some() && correlation_id(id, 16))
            && body_valid
    }
}

pub struct Batch {
    pub records: Vec<Record>,
    pub rejected: usize,
}

fn valid_id(id: &[u8], n: usize) -> bool {
    id.is_empty() || crate::identity::valid_id(id, n)
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
        (Capture::TextBody, Some(Value::StringValue(s))) if s.len() <= TEXT_BODY_LIMIT => {
            ("text", Some(s))
        }
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
        timestamp_ns: TimestampNs(timestamp),
        event_ns: (r.time_unix_nano != 0).then_some(TimestampNs(r.time_unix_nano)),
        observed_ns: (r.observed_time_unix_nano != 0)
            .then_some(TimestampNs(r.observed_time_unix_nano)),
        received_ns: TimestampNs(received),
        severity_number: r.severity_number,
        trace_flags: r.flags as u8,
        trace_id: (!r.trace_id.is_empty()).then(|| hex::encode(r.trace_id)),
        span_id: (!r.span_id.is_empty()).then(|| hex::encode(r.span_id)),
        body_capture: body_capture.into(),
        body,
    }))
}

pub fn prepare(wire: &[u8], gzip: bool, json: bool, grant: &Grant) -> Result<Batch> {
    let req = crate::otlp::logs(wire, gzip, json)?;
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
