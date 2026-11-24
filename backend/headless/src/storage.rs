//! Candidate Loki adapter. Only authority-issued grants can address a store.
use crate::{
    authority::Grant,
    logs::Record,
    query::{lower_hex, Page, Query},
};
use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{client::conn::http1, header, Method, Request};
use hyper_util::rt::TokioIo;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use serde::Deserialize;
use serde_json::json;
use std::{path::PathBuf, time::Duration};
use tokio::{
    net::UnixStream,
    task::JoinSet,
    time::{timeout, Instant},
};

#[derive(Debug, PartialEq)]
pub enum StoreError {
    Offline,       // no request submitted
    Response,      // completed reply, unsuccessful or uninterpretable
    Indeterminate, // lost transport after dispatch, work may still be running
    Deadline,      // expired before dispatch
}
type StoreResult<T> = std::result::Result<T, StoreError>;

#[async_trait]
pub trait LogStore: Send + Sync {
    fn source(&self) -> &'static str;
    async fn write(&self, grant: &Grant, records: &[Record]) -> StoreResult<()>;
    async fn scan(&self, grant: &Grant, query: &Query, deadline: Instant) -> StoreResult<Page>;
}

pub struct Loki(pub PathBuf);

impl Loki {
    async fn request(
        &self,
        grant: &Grant,
        method: Method,
        path: String,
        body: Vec<u8>,
        deadline: Option<Instant>,
    ) -> StoreResult<(u16, Bytes)> {
        let connect = async {
            let stream = UnixStream::connect(&self.0)
                .await
                .map_err(|_| StoreError::Offline)?;
            http1::handshake(TokioIo::new(stream))
                .await
                .map_err(|_| StoreError::Offline)
        };
        let (mut sender, conn) = timeout(Duration::from_secs(1), connect)
            .await
            .map_err(|_| StoreError::Offline)??;
        let mut transport = JoinSet::new();
        transport.spawn(conn);
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
            let response = sender
                .send_request(req)
                .await
                .map_err(|_| StoreError::Indeterminate)?;
            let status = response.status().as_u16();
            let data = if status == 200 {
                Limited::new(response.into_body(), 2 * 1024 * 1024)
                    .collect()
                    .await
                    .map_err(|_| StoreError::Indeterminate)?
                    .to_bytes()
            } else {
                Bytes::new()
            };
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
    async fn write(&self, grant: &Grant, records: &[Record]) -> StoreResult<()> {
        let values: Vec<_> = records
            .iter()
            .map(|r| {
                (
                    r.timestamp_ns.to_string(),
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
    async fn scan(&self, grant: &Grant, q: &Query, deadline: Instant) -> StoreResult<Page> {
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
            q.limit + 1
        );
        let (code, bytes) = self
            .request(grant, Method::GET, path, vec![], Some(deadline))
            .await?;
        if code != 200 {
            return Err(StoreError::Response);
        }
        decode_page(&bytes, grant.tenant(), q)
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
    values: Vec<(String, String)>,
}

fn decode_page(bytes: &[u8], tenant: &str, query: &Query) -> StoreResult<Page> {
    let reply: LokiReply = serde_json::from_slice(bytes).map_err(|_| StoreError::Response)?;
    if reply.status != "success" || reply.data.result_type != "streams" {
        return Err(StoreError::Response);
    }
    let (start, end) = query.bounds().map_err(|_| StoreError::Response)?;
    let mut rows = vec![];
    for (timestamp, line) in reply
        .data
        .result
        .into_iter()
        .flat_map(|stream| stream.values)
    {
        let row: Record = serde_json::from_str(&line).map_err(|_| StoreError::Response)?;
        let t = timestamp.parse::<u64>().map_err(|_| StoreError::Response)?;
        if row.tenant != tenant
            || t != row.timestamp_ns
            || timestamp != t.to_string()
            || t < start
            || t > end
            || !lower_hex(&row.id, 32)
            || !lower_hex(&row.producer_id, 32)
            || query.service.as_ref().is_some_and(|s| s != &row.service)
            || query
                .trace_id
                .as_ref()
                .is_some_and(|s| Some(s) != row.trace_id.as_ref())
            || query.record_id.as_ref().is_some_and(|s| s != &row.id)
        {
            return Err(StoreError::Response);
        }
        if t < end {
            rows.push(row);
        }
        if rows.len() > query.limit + 1 {
            return Err(StoreError::Response);
        }
    }
    rows.sort_by(|a, b| {
        a.timestamp_ns
            .cmp(&b.timestamp_ns)
            .then_with(|| a.id.cmp(&b.id))
    });
    let truncated = rows.len() > query.limit;
    rows.truncate(query.limit);
    Ok(Page {
        records: rows,
        truncated,
        warning: !reply.warnings.is_empty(),
    })
}
