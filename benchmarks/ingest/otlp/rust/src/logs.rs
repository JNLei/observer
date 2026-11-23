use flate2::read::MultiGzDecoder;
use opentelemetry_proto::tonic::{
    collector::logs::v1::{
        ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
    },
    common::v1::{any_value::Value, AnyValue, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use prost::Message;
use std::io::Read;

pub const BYTE_LIMIT: usize = 65536;
const RECORD_LIMIT: usize = 256;

pub struct Batch {
    pub wire: Vec<u8>,
    pub accepted: usize,
    pub rejected: usize,
}

fn valid_id(id: &[u8], length: usize) -> bool {
    id.is_empty() || (id.len() == length && id.iter().any(|&byte| byte != 0))
}

fn string_value(value: String) -> AnyValue {
    AnyValue {
        value: Some(Value::StringValue(value)),
    }
}

fn sanitize(record: LogRecord) -> Option<LogRecord> {
    let mut ids = record.attributes.iter().filter(|a| a.key == "bench.seq");
    let value = ids.next()?.value.as_ref()?.value.as_ref()?;
    let Value::IntValue(seq) = value else {
        return None;
    };
    if ids.next().is_some()
        || *seq <= 0
        || record.time_unix_nano == 0
        || record.time_unix_nano > i64::MAX as u64
        || !(0..=24).contains(&record.severity_number)
        || !valid_id(&record.trace_id, 16)
        || !valid_id(&record.span_id, 8)
        || (!record.span_id.is_empty() && record.trace_id.is_empty())
    {
        return None;
    }
    Some(LogRecord {
        time_unix_nano: record.time_unix_nano,
        observed_time_unix_nano: record.observed_time_unix_nano,
        severity_number: record.severity_number,
        trace_id: record.trace_id,
        span_id: record.span_id,
        flags: record.flags & 0xff,
        body: Some(string_value(format!("bench-{seq}"))),
        ..Default::default()
    })
}

pub fn prepare(wire: &[u8], gzip: bool) -> Result<Batch, u16> {
    let mut inflated = Vec::new();
    let raw = if gzip {
        MultiGzDecoder::new(wire)
            .take(BYTE_LIMIT as u64 + 1)
            .read_to_end(&mut inflated)
            .map_err(|_| 400u16)?;
        if inflated.len() > BYTE_LIMIT {
            return Err(413);
        }
        &inflated[..]
    } else {
        wire
    };
    let request = ExportLogsServiceRequest::decode(raw).map_err(|_| 400u16)?;
    let mut records = Vec::new();
    let mut rejected = 0;
    for group in request.resource_logs {
        for scope in group.scope_logs {
            for record in scope.log_records {
                if records.len() + rejected == RECORD_LIMIT {
                    return Err(413);
                }
                match sanitize(record) {
                    Some(record) => records.push(record),
                    None => rejected += 1,
                }
            }
        }
    }
    let accepted = records.len();
    let resource = Resource {
        attributes: vec![KeyValue {
            key: "service.name".into(),
            value: Some(string_value("m1-bench".into())),
            ..Default::default()
        }],
        ..Default::default()
    };
    let wire = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource),
            scope_logs: vec![ScopeLogs {
                log_records: records,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec();
    Ok(Batch {
        wire,
        accepted,
        rejected,
    })
}

pub fn success(rejected: usize) -> Vec<u8> {
    ExportLogsServiceResponse {
        partial_success: (rejected > 0).then(|| ExportLogsPartialSuccess {
            rejected_log_records: rejected as i64,
            error_message: "record failed fixture validation".into(),
        }),
    }
    .encode_to_vec()
}
