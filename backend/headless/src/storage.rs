//! Candidate Loki adapter. Only authority-issued grants can address a store.
use crate::{
    authority::{Capture, Grant},
    logs::{LogTime, Record, TEXT_BODY_LIMIT},
    query::{Page, Query, Truncation},
};
use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::{body::Body, client::conn::http1, header, Method, Request};
use hyper_util::rt::TokioIo;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use serde::Deserialize;
use serde_json::json;
use std::{ops::RangeInclusive, path::PathBuf, time::Duration};
use tokio::{
    net::UnixStream,
    task::JoinSet,
    time::{timeout, Instant},
};

#[derive(Debug, PartialEq)]
pub enum StoreError {
    Offline,        // no request submitted
    Response,       // completed reply, unsuccessful or uninterpretable
    Throttled(u64), // requested minimum retry delay, in seconds
    WriteDenied,
    ResponseBudget, // a completed reply exceeded the materialization cap
    Indeterminate,  // lost transport after dispatch, work may still be running
    Deadline,       // expired before dispatch
}
type StoreResult<T> = std::result::Result<T, StoreError>;

#[async_trait]
pub trait LogStore: Send + Sync {
    fn source(&self) -> &'static str;
    fn write_time_range(&self, now_ns: u64) -> RangeInclusive<u64>;
    async fn writer(&self) -> StoreResult<Box<dyn LogWriter>>;
    async fn scan(&self, grant: &Grant, query: &Query, deadline: Instant) -> StoreResult<Page>;
}

#[async_trait]
pub trait LogWriter: Send {
    fn peer_pid(&self) -> i32;
    async fn write(self: Box<Self>, grant: &Grant, records: &[Record]) -> StoreResult<()>;
}

pub struct Loki(pub PathBuf);

const REPLY_BYTES: usize = 2 * 1024 * 1024;
const ENVELOPE_BYTES: usize = 64 * 1024; // Loki stats, warnings and envelope
const ROW_METADATA_BYTES: usize = 4096; // canonical fields, framing and extracted stream labels

fn page_limit(requested: usize, capture: Capture) -> usize {
    // A control byte can take six bytes in the stored JSON line and seven
    // when that line is embedded in Loki's JSON. Hold back one lookahead row.
    let text = if capture == Capture::TextBody {
        7 * TEXT_BODY_LIMIT
    } else {
        0
    };
    let wire_rows = (REPLY_BYTES - ENVELOPE_BYTES) / (ROW_METADATA_BYTES + text);
    requested.min(wire_rows - 1)
}

async fn finish_body<B>(mut body: B, capture: bool, cap: usize) -> StoreResult<Bytes>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let result = if capture {
        match Limited::new(&mut body, cap).collect().await {
            Ok(data) => return Ok(data.to_bytes()),
            Err(e) if e.is::<LengthLimitError>() => Err(StoreError::ResponseBudget),
            Err(_) => return Err(StoreError::Indeterminate),
        }
    } else {
        Ok(Bytes::new())
    };
    // Reaching the byte cap is a local decision. Drain without buffering
    // until HTTP completion; the exchange timeout still bounds this work.
    while let Some(frame) = body.frame().await {
        frame.map_err(|_| StoreError::Indeterminate)?;
    }
    result
}

impl Loki {
    async fn connect(&self) -> StoreResult<Exchange> {
        timeout(Duration::from_secs(1), async {
            let stream = UnixStream::connect(&self.0)
                .await
                .map_err(|_| StoreError::Offline)?;
            let pid = stream
                .peer_cred()
                .ok()
                .and_then(|c| c.pid())
                .filter(|p| *p > 0)
                .ok_or(StoreError::Offline)?;
            let (sender, conn) = http1::handshake(TokioIo::new(stream))
                .await
                .map_err(|_| StoreError::Offline)?;
            let mut transport = JoinSet::new();
            transport.spawn(conn);
            Ok(Exchange {
                sender,
                pid,
                _transport: transport,
            })
        })
        .await
        .map_err(|_| StoreError::Offline)?
    }
}

struct Exchange {
    sender: http1::SendRequest<Full<Bytes>>,
    pid: i32,
    _transport: JoinSet<std::result::Result<(), hyper::Error>>,
}

impl Exchange {
    async fn request(
        mut self,
        grant: &Grant,
        method: Method,
        path: String,
        body: Vec<u8>,
        deadline: Option<Instant>,
    ) -> StoreResult<(u16, Bytes)> {
        let write = method == Method::POST;
        let mut builder = Request::builder()
            .method(method)
            .uri(path)
            .header(header::HOST, "localhost")
            .header(header::CONTENT_TYPE, "application/json")
            .header("X-Scope-OrgID", grant.tenant());
        if let Some(deadline) = deadline {
            let ms = deadline
                .saturating_duration_since(Instant::now())
                .as_millis();
            if ms == 0 {
                return Err(StoreError::Deadline);
            }
            builder = builder.header(
                "X-Loki-Query-Limits",
                format!(r#"{{"maxQueryTime":"{ms}ms"}}"#),
            );
        }
        let req = builder
            .body(Full::new(Bytes::from(body)))
            .map_err(|_| StoreError::Offline)?;
        let exchange = async {
            let response = self
                .sender
                .send_request(req)
                .await
                .map_err(|_| StoreError::Indeterminate)?;
            let status = response.status().as_u16();
            let retry_after = response
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| {
                    v.parse::<u64>().ok().or_else(|| {
                        httpdate::parse_http_date(v).ok().map(|t| {
                            t.duration_since(std::time::SystemTime::now())
                                .unwrap_or_default()
                                .as_secs()
                                + 1
                        })
                    })
                });
            let data = finish_body(response.into_body(), status == 200, REPLY_BYTES).await?;
            if write && matches!(status, 401 | 403) {
                return Err(StoreError::WriteDenied);
            }
            if write && matches!(status, 429 | 503) {
                return Err(StoreError::Throttled(retry_after.unwrap_or(0)));
            }
            Ok((status, data))
        };
        timeout(Duration::from_secs(5), exchange)
            .await
            .map_err(|_| StoreError::Indeterminate)?
    }
}

#[async_trait]
impl LogStore for Loki {
    fn source(&self) -> &'static str {
        "local-loki-v1"
    }
    fn write_time_range(&self, now_ns: u64) -> RangeInclusive<u64> {
        const MINUTE: u64 = 60 * 1_000_000_000;
        now_ns.saturating_sub(48 * 60 * MINUTE)..=now_ns.saturating_add(10 * MINUTE)
    }
    async fn writer(&self) -> StoreResult<Box<dyn LogWriter>> {
        Ok(Box::new(self.connect().await?))
    }
    async fn scan(&self, grant: &Grant, q: &Query, deadline: Instant) -> StoreResult<Page> {
        let limit = page_limit(q.limit, grant.capture);
        let mut expr = r#"{observe_format="logs_v1"}"#.to_owned();
        for (field, filter) in [
            ("service", &q.service),
            ("trace_id", &q.trace_id),
            ("id", &q.record_id),
        ] {
            if let Some(value) = filter {
                // Field names are fixed here. Query validation excludes LogQL syntax in values.
                expr.push_str(&format!(
                    r#" | json filter_{field}="{field}" | filter_{field}="{value}""#
                ));
            }
        }
        let path = format!(
            "/loki/api/v1/query_range?query={}&start={}&end={}&limit={}&direction=forward",
            utf8_percent_encode(&expr, NON_ALPHANUMERIC),
            q.start_ns,
            q.end_ns,
            limit + 1
        );
        let (code, bytes) = self
            .connect()
            .await?
            .request(grant, Method::GET, path, vec![], Some(deadline))
            .await?;
        if code != 200 {
            return Err(StoreError::Response);
        }
        decode_page(&bytes, grant, q, limit)
    }
}

#[async_trait]
impl LogWriter for Exchange {
    fn peer_pid(&self) -> i32 {
        self.pid
    }
    async fn write(self: Box<Self>, grant: &Grant, records: &[Record]) -> StoreResult<()> {
        let values: Vec<_> = records
            .iter()
            .map(|r| {
                (
                    r.timestamp_ns,
                    serde_json::to_string(r).expect("serializable log"),
                )
            })
            .collect();
        let body = json!({"streams":[{"stream":{"observe_format":"logs_v1"},"values":values}]});
        let (status, _) = self
            .request(
                grant,
                Method::POST,
                "/loki/api/v1/push".into(),
                body.to_string().into_bytes(),
                None,
            )
            .await?;
        if status != 204 {
            return Err(StoreError::Response);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct LokiReply {
    status: String,
    data: LokiData,
    #[serde(default)]
    warnings: Vec<String>,
}
#[derive(Deserialize)]
struct LokiData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<Stream>,
}
#[derive(Deserialize)]
struct Stream {
    values: Vec<(LogTime, String)>,
}

fn decode_page(bytes: &[u8], grant: &Grant, query: &Query, limit: usize) -> StoreResult<Page> {
    let reply: LokiReply = serde_json::from_slice(bytes).map_err(|_| StoreError::Response)?;
    if reply.status != "success" || reply.data.result_type != "streams" {
        return Err(StoreError::Response);
    }
    let (start, end) = query.bounds().map_err(|_| StoreError::Response)?;
    let mut rows = vec![];
    for (index, (timestamp, line)) in reply
        .data
        .result
        .into_iter()
        .flat_map(|stream| stream.values)
        .enumerate()
    {
        if index > limit {
            return Err(StoreError::Response);
        }
        let row: Record = serde_json::from_str(&line).map_err(|_| StoreError::Response)?;
        if !row.is_canonical(grant)
            || timestamp != row.timestamp_ns
            || timestamp.0 < start
            || timestamp.0 > end
            || query.service.as_ref().is_some_and(|s| s != &row.service)
            || query
                .trace_id
                .as_ref()
                .is_some_and(|s| Some(s) != row.trace_id.as_ref())
            || query.record_id.as_ref().is_some_and(|s| s != &row.id)
        {
            return Err(StoreError::Response);
        }
        if timestamp.0 < end {
            rows.push(row);
        }
    }
    rows.sort_by(|a, b| {
        a.timestamp_ns
            .cmp(&b.timestamp_ns)
            .then_with(|| a.id.cmp(&b.id))
    });
    let truncation = (rows.len() > limit).then_some(if limit < query.limit {
        Truncation::ResponseByteLimit
    } else {
        Truncation::RowLimit
    });
    rows.truncate(limit);
    Ok(Page {
        records: rows,
        truncation,
        warning: !reply.warnings.is_empty(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::channel::Channel;

    #[tokio::test]
    async fn byte_limit_waits_for_the_terminal_frame() {
        for disconnect in [false, true] {
            let (mut tx, body) = Channel::<Bytes, std::io::Error>::new(1);
            let task = tokio::spawn(finish_body(body, true, 4));
            tx.send_data(Bytes::from_static(b"first")).await.unwrap();
            tx.send_data(Bytes::from_static(b"last")).await.unwrap();
            assert!(!task.is_finished());
            let expected = if disconnect {
                tx.abort(std::io::Error::other("transport ended early"));
                StoreError::Indeterminate
            } else {
                drop(tx);
                StoreError::ResponseBudget
            };
            assert_eq!(task.await.unwrap(), Err(expected));
        }
    }

    #[test]
    fn write_window_includes_both_endpoints() {
        let now = 1_800_000_000_000_000_000;
        let window = Loki(PathBuf::new()).write_time_range(now);
        let first = now - 48 * 3600 * 1_000_000_000;
        let last = now + 600 * 1_000_000_000;
        assert!(window.contains(&first) && window.contains(&last));
        assert!(!window.contains(&(first - 1)) && !window.contains(&(last + 1)));
    }

    #[test]
    fn page_budget_covers_escaped_text_and_lookahead() {
        // Measure one source row offline, including both JSON encodings.
        let mut record: Record = serde_json::from_value(json!({
            "id": "a".repeat(32), "producer_id": "a".repeat(32), "service": "a".repeat(48),
            "tenant": "a".repeat(48), "timestamp_ns": "9223372036854775806",
            "event_ns": "9223372036854775806", "observed_ns": "9223372036854775806",
            "received_ns": "9223372036854775806", "severity_number": 24, "trace_flags": 255,
            "trace_id": "a".repeat(32), "span_id": "a".repeat(16), "body_capture": "text", "body": ""
        })).unwrap();
        let row_bytes = |r: &Record| {
            let line = serde_json::to_string(r).unwrap();
            serde_json::to_vec(&(r.timestamp_ns, line)).unwrap().len()
        };
        let metadata = row_bytes(&record);
        assert!(metadata < ROW_METADATA_BYTES);
        let max_increment = (0..=127u8)
            .map(|byte| {
                record.body = Some(char::from(byte).to_string());
                row_bytes(&record) - metadata
            })
            .max()
            .unwrap();
        assert_eq!(max_increment, 7);
        let limit = page_limit(100, Capture::TextBody);
        assert!(
            (limit + 1) * (TEXT_BODY_LIMIT * max_increment + ROW_METADATA_BYTES) + ENVELOPE_BYTES
                <= REPLY_BYTES
        );
        assert_eq!(page_limit(100, Capture::MetadataOnly), 100);
    }
}
