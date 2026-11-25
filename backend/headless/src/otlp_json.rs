//! Decode bounded ProtoJSON using the same OTLP descriptors as the binary receiver.
use crate::{
    logs::{BYTE_LIMIT, RECORD_LIMIT, STRUCTURE_LIMIT},
    Error, Result,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost_reflect::{DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor};
use serde_json::Value;
use std::sync::LazyLock;

const LOG_RECORD: &str = "opentelemetry.proto.logs.v1.LogRecord";
static LOGS_REQUEST: LazyLock<MessageDescriptor> = LazyLock::new(|| {
    DescriptorPool::decode(opentelemetry_proto::DESCRIPTORS)
        .expect("pinned OTLP descriptor set")
        .get_message_by_name("opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest")
        .expect("pinned logs request descriptor")
});

pub fn decode(raw: &[u8]) -> Result<ExportLogsServiceRequest> {
    json_budget(raw)?;
    let mut json: Value = serde_json::from_slice(raw).map_err(|_| Error::Invalid)?;
    let mut remaining = RECORD_LIMIT;
    adapt_message(&mut json, &LOGS_REQUEST, &mut remaining)?;
    DynamicMessage::deserialize(LOGS_REQUEST.clone(), json)
        .map_err(|_| Error::Invalid)?
        .transcode_to()
        .map_err(|_| Error::Invalid)
}

fn adapt_message(json: &mut Value, desc: &MessageDescriptor, remaining: &mut usize) -> Result<()> {
    if desc.full_name() == LOG_RECORD {
        *remaining = remaining.checked_sub(1).ok_or(Error::TooLarge)?;
    }
    let fields = json.as_object_mut().ok_or(Error::Invalid)?;
    for (name, mut value) in std::mem::take(fields) {
        // OTLP recognizes lowerCamelCase field names and ignores unknown fields.
        let Some(field) = desc.get_field_by_json_name(&name) else {
            continue;
        };
        // ProtoJSON null leaves the field unset.
        if value.is_null() {
            continue;
        }
        if field.is_list() {
            for item in value.as_array_mut().ok_or(Error::Invalid)? {
                if item.is_null() {
                    return Err(Error::Invalid);
                }
                adapt_value(item, &field, remaining)?;
            }
        } else {
            adapt_value(&mut value, &field, remaining)?;
        }
        fields.insert(name, value);
    }
    Ok(())
}

fn adapt_value(value: &mut Value, field: &FieldDescriptor, remaining: &mut usize) -> Result<()> {
    match field.kind() {
        Kind::Message(message) => adapt_message(value, &message, remaining)?,
        Kind::Enum(_) if !value.is_i64() && !value.is_u64() => return Err(Error::Invalid),
        Kind::Bytes
            if field.parent_message().full_name() == LOG_RECORD
                && matches!(field.name(), "trace_id" | "span_id") =>
        {
            let bytes =
                hex::decode(value.as_str().ok_or(Error::Invalid)?).map_err(|_| Error::Invalid)?;
            *value = Value::String(STANDARD.encode(bytes));
        }
        Kind::Double => {
            // Keep ProtoJSON's three special strings. A finite JSON number may
            // have a longer decimal spelling than its nearest IEEE double.
            let finite = match value {
                Value::String(s) if matches!(s.as_str(), "NaN" | "Infinity" | "-Infinity") => {
                    return Ok(())
                }
                Value::String(s) => s
                    .parse::<serde_json::Number>()
                    .ok()
                    .and_then(|n| n.as_f64()),
                Value::Number(n) => n.as_f64(),
                _ => None,
            };
            *value = Value::from(finite.filter(|n| n.is_finite()).ok_or(Error::Invalid)?);
        }
        Kind::Fixed64 | Kind::Int64 | Kind::Uint64 | Kind::Fixed32 | Kind::Int32 | Kind::Uint32 => {
            // prost-reflect accepts decimal integer strings. Preserve the exact
            // value of integral decimal/exponent notation without a float round trip.
            let text = match value {
                Value::String(s) if s.contains(['.', 'e', 'E']) => s.clone(),
                Value::Number(n) if !(n.is_i64() || n.is_u64()) => n.to_string(),
                _ => return Ok(()),
            };
            *value = Value::String(integer_notation(&text).ok_or(Error::Invalid)?);
        }
        _ => (),
    }
    Ok(())
}

fn integer_notation(text: &str) -> Option<String> {
    // Validate the JSON number grammar, including for quoted numbers.
    text.parse::<serde_json::Number>().ok()?;
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (mantissa, exponent) = unsigned.split_once(['e', 'E']).unwrap_or((unsigned, "0"));
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{whole}{fraction}");
    let significant = digits.trim_start_matches('0');
    if significant.is_empty() {
        return Some("0".into());
    }
    let point = (significant.len() as i64 - fraction.len() as i64)
        .checked_add(exponent.parse::<i64>().ok()?)?;
    if !(1..=20).contains(&point) {
        return None;
    }
    let point = point as usize;
    if significant
        .as_bytes()
        .get(point..)
        .is_some_and(|tail| tail.iter().any(|&b| b != b'0'))
    {
        return None;
    }
    let mut result = if text.starts_with('-') {
        "-".to_owned()
    } else {
        String::new()
    };
    result.push_str(&significant[..point.min(significant.len())]);
    result.extend(std::iter::repeat_n(
        '0',
        point.saturating_sub(significant.len()),
    ));
    Some(result)
}

fn json_budget(data: &[u8]) -> Result<()> {
    if data.len() > BYTE_LIMIT {
        return Err(Error::TooLarge);
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::any_value;

    #[test]
    fn preserves_exact_times_and_ieee_special_values() {
        let raw = br#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[
            {"timeUnixNano":18000000000000000010e-1,"body":{"doubleValue":"NaN"}},
            {"timeUnixNano":null,"observedTimeUnixNano":"1.5e1","body":{"doubleValue":"Infinity"}},
            {"body":{"doubleValue":"-Infinity"}},{"body":{"doubleValue":1.00e2}}
        ]}]}]}"#;
        let request = decode(raw).unwrap();
        let records = &request.resource_logs[0].scope_logs[0].log_records;
        assert_eq!(records[0].time_unix_nano, 1_800_000_000_000_000_001);
        assert_eq!(records[1].time_unix_nano, 0);
        assert_eq!(records[1].observed_time_unix_nano, 15);
        let doubles: Vec<_> = records
            .iter()
            .map(|r| match r.body.as_ref().and_then(|b| b.value.as_ref()) {
                Some(any_value::Value::DoubleValue(n)) => *n,
                _ => panic!("expected an OTLP double"),
            })
            .collect();
        assert!(doubles[0].is_nan());
        assert_eq!(&doubles[1..], &[f64::INFINITY, f64::NEG_INFINITY, 100.0]);
    }
}
