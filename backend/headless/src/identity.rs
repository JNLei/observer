//! Common evidence identity. Source-claimed resource metadata stays in the typed signal payload.
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize)]
pub struct Identity<'a> {
    pub tenant: &'a str,
    pub producer_id: &'a str,
    pub service: &'a str,
    pub trace_id: Option<&'a str>,
    pub span_id: Option<&'a str>,
    pub parent_span_id: Option<&'a str>,
    pub event_ns: Option<TimestampNs>,
    pub source: &'a str,
}

pub fn valid_id(bytes: &[u8], size: usize) -> bool {
    bytes.len() == size && bytes.iter().any(|&b| b != 0)
}
pub fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

// Stored records use positive canonical decimal strings. Reserve one nanosecond
// at the top of the range for the exclusive end of a record reference.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TimestampNs(pub u64);

impl TryFrom<String> for TimestampNs {
    type Error = &'static str;

    fn try_from(s: String) -> std::result::Result<Self, Self::Error> {
        let n = s.parse::<u64>().map_err(|_| "invalid timestamp")?;
        if n == 0 || n >= i64::MAX as u64 || n.to_string() != s {
            return Err("invalid timestamp");
        }
        Ok(Self(n))
    }
}

impl From<TimestampNs> for String {
    fn from(time: TimestampNs) -> Self {
        time.0.to_string()
    }
}

pub fn lower_hex(id: &str, len: usize) -> bool {
    id.len() == len
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
