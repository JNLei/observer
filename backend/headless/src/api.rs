use crate::{
    admission::Lane,
    authority::{Action, Authority, Key},
    delivery,
    logs::{now_ns, prepare, BYTE_LIMIT},
    query::{self, Query},
    spool::{blocking, Outcome, Spool},
    storage::{LogStore, StoreError},
    Error, Result,
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    body::Incoming, header, server::conn::http1, service::service_fn, HeaderMap, Method, Request,
    Response,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceResponse,
};
use prost::Message;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::Semaphore,
    time::{timeout, timeout_at, Instant},
};

const PROTO: &str = "application/x-protobuf";
const JSON: &str = "application/json";
type Reply = Response<Full<Bytes>>;

pub struct App {
    pub(crate) authority: Arc<Authority>,
    pub(crate) spool: Arc<Spool>,
    pub(crate) store: Arc<dyn LogStore>,
    writes: Arc<Lane>,
    reads: Arc<Lane>,
    decoders: Arc<Semaphore>,
}
impl App {
    pub fn new(authority: Authority, spool: Arc<Spool>, store: Arc<dyn LogStore>) -> Self {
        Self {
            authority: Arc::new(authority),
            spool,
            store,
            writes: Lane::new(8, 2),
            reads: Lane::new(4, 1),
            decoders: Arc::new(Semaphore::new(2)),
        }
    }
}
impl Error {
    fn response(self) -> (u16, &'static str) {
        match self {
            Self::Invalid | Self::Expired => (400, "invalid_request"),
            Self::Unauthenticated => (401, "denied"),
            Self::Denied => (403, "denied"),
            Self::Missing => (404, "not_found"),
            Self::Conflict => (409, "conflict"),
            Self::TooLarge => (413, "request_limit"),
            Self::Media => (415, "unsupported_media"),
            Self::Busy => (429, "capacity"),
            Self::ReadTimeout => (408, "body_timeout"),
            Self::Deadline => (504, "deadline"),
            Self::State | Self::Unavailable => (503, "unavailable"),
            Self::SourceBudget => (503, "source_response_byte_limit"),
            Self::Internal => (500, "internal_error"),
        }
    }
}
fn reply(code: u16, media: &str, data: impl Into<Bytes>) -> Reply {
    let mut response = Response::builder()
        .status(code)
        .header(header::CONTENT_TYPE, media)
        .header(header::CACHE_CONTROL, "no-store");
    if matches!(code, 429 | 503) {
        response = response.header(header::RETRY_AFTER, "1");
    }
    response.body(Full::new(data.into())).unwrap()
}
fn json_reply(code: u16, v: Value) -> Reply {
    reply(code, JSON, v.to_string())
}
fn api_error(err: Error) -> Reply {
    let (code, outcome) = err.response();
    json_reply(code, json!({"outcome":outcome}))
}
#[derive(Message)]
struct RpcStatus {
    #[prost(string, tag = "2")]
    message: String,
}
fn otlp_error(err: Error, is_json: bool) -> Reply {
    let (code, message) = err.response();
    otlp_status(code, message, is_json)
}
fn otlp_status(code: u16, message: &str, is_json: bool) -> Reply {
    if is_json {
        json_reply(code, json!({"message":message}))
    } else {
        reply(
            code,
            PROTO,
            RpcStatus {
                message: message.into(),
            }
            .encode_to_vec(),
        )
    }
}
const RECORD_REJECTION: &str =
    "record outside the write time window or invalid timestamp, severity or correlation ID";
fn otlp_ok(rejected: usize, is_json: bool) -> Reply {
    if is_json {
        return json_reply(
            200,
            if rejected == 0 {
                json!({})
            } else {
                json!({"partialSuccess":{
            "rejectedLogRecords":rejected.to_string(),"errorMessage":RECORD_REJECTION}})
            },
        );
    }
    reply(
        200,
        PROTO,
        ExportLogsServiceResponse {
            partial_success: (rejected != 0).then(|| ExportLogsPartialSuccess {
                rejected_log_records: rejected as i64,
                error_message: RECORD_REJECTION.into(),
            }),
        }
        .encode_to_vec(),
    )
}

fn single<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>> {
    let mut values = headers.get_all(name).iter();
    let value = values
        .next()
        .map(|s| s.to_str().map_err(|_| Error::Invalid))
        .transpose()?;
    if values.next().is_some() {
        return Err(Error::Invalid);
    }
    Ok(value)
}
async fn body(req: Request<Incoming>, cap: usize) -> Result<Bytes> {
    if single(req.headers(), "content-length")?
        .is_some_and(|s| s.parse::<u64>().is_ok_and(|n| n > cap as u64))
    {
        return Err(Error::TooLarge);
    }
    let incoming = timeout(
        Duration::from_secs(2),
        Limited::new(req.into_body(), cap).collect(),
    )
    .await
    .map_err(|_| Error::ReadTimeout)?;
    incoming.map(|r| r.to_bytes()).map_err(|e| {
        if e.is::<http_body_util::LengthLimitError>() {
            Error::TooLarge
        } else {
            Error::Invalid
        }
    })
}
async fn json_body<T: DeserializeOwned>(req: Request<Incoming>) -> Result<T> {
    if !single(req.headers(), "content-type")?.is_some_and(|s| s.eq_ignore_ascii_case(JSON))
        || single(req.headers(), "content-encoding")?.is_some()
    {
        return Err(Error::Media);
    }
    serde_json::from_slice(&body(req, 4096).await?).map_err(|_| Error::Invalid)
}
fn counts(
    mut response: Reply,
    accepted: usize,
    rejected: usize,
    unknown: usize,
    unsubmitted: usize,
) -> Reply {
    for (header, value) in [
        ("x-observe-accepted-records", accepted),
        ("x-observe-rejected-records", rejected),
        ("x-observe-unknown-records", unknown),
        ("x-observe-unsubmitted-records", unsubmitted),
    ] {
        response.headers_mut().insert(header, value.into());
    }
    response
}

async fn ingest(app: Arc<App>, key: Key, req: Request<Incoming>, is_json: bool) -> Result<Reply> {
    let grant = app.authority.authorize(&key, None, Action::Ingest)?;
    let slot = app.writes.enter(grant.tenant())?;
    if !single(req.headers(), "content-type")?
        .is_some_and(|v| v.eq_ignore_ascii_case(PROTO) || v.eq_ignore_ascii_case(JSON))
    {
        return Err(Error::Media);
    }
    let gzip = match single(req.headers(), "content-encoding")? {
        None => false,
        Some(v) if v.eq_ignore_ascii_case("identity") => false,
        Some(v) if v.eq_ignore_ascii_case("gzip") => true,
        _ => return Err(Error::Media),
    };
    let raw = body(req, BYTE_LIMIT).await?;
    let decoder = app
        .decoders
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::Busy)?;
    // The task owns the reservation even if its HTTP caller disconnects during decode or storage I/O.
    let job = tokio::spawn(async move {
        let _slot = slot;
        let mut batch = tokio::task::spawn_blocking(move || {
            let _decoder = decoder;
            prepare(&raw, gzip, is_json, &grant)
        })
        .await
        .map_err(|_| Error::Internal)??;
        let grant = app.authority.authorize(&key, None, Action::Ingest)?;
        let total = batch.rejected + batch.records.len();
        let window = app.store.write_time_range(now_ns());
        batch.records.retain(|r| window.contains(&r.timestamp_ns.0));
        let n = batch.records.len();
        let rejected = total - n;
        if n == 0 {
            let resp = if rejected > 0 {
                otlp_status(400, RECORD_REJECTION, is_json)
            } else {
                otlp_ok(0, is_json)
            };
            return Ok(counts(resp, 0, rejected, 0, 0));
        }
        let worker = app.clone();
        let admitted = blocking(move || {
            worker
                .authority
                .with_writer(grant.tenant(), &grant.subject, |current| {
                    worker.spool.admit(current, batch.records)
                })
        })
        .await;
        let entry = match admitted {
            Ok(entry) => entry,
            Err(Error::Busy) => {
                let mut resp = counts(otlp_error(Error::Busy, is_json), 0, rejected, 0, n);
                resp.headers_mut()
                    .insert("x-observe-durable-records", 0.into());
                return Ok(resp);
            }
            Err(e) => return Err(e),
        };
        let mut resp = match delivery::send(app, entry, false).await {
            Ok(Outcome::Ack) => counts(otlp_ok(rejected, is_json), n, rejected, 0, 0),
            Ok(Outcome::Offline | Outcome::Held(_)) => {
                counts(otlp_error(Error::Unavailable, is_json), 0, rejected, 0, n)
            }
            Ok(Outcome::Unknown(_) | Outcome::DestinationDenied | Outcome::LostReply) | Err(_) => {
                counts(otlp_error(Error::Unavailable, is_json), 0, rejected, n, 0)
            }
        };
        resp.headers_mut()
            .insert("x-observe-durable-records", n.into());
        Ok(resp)
    });
    job.await.map_err(|_| Error::Internal)?
}

async fn read_logs(
    app: Arc<App>,
    key: Key,
    tenant: &str,
    q: Query,
    evidence: bool,
    started: Instant,
) -> Result<Reply> {
    q.validate()?;
    let grant = app.authority.authorize(&key, Some(tenant), Action::Query)?;
    let mut slot = app.reads.enter(grant.tenant())?;
    let deadline = started + Duration::from_millis(q.deadline_ms);
    let store = app.store.clone();
    let source = store.source();
    let q2 = q.clone();
    let key = Arc::new(key);
    let work_key = key.clone();
    let worker = app.clone();
    let task = tokio::spawn(async move {
        let grant = worker
            .authority
            .authorize(&work_key, Some(grant.tenant()), Action::Query)?;
        if Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        match store.scan(&grant, &q2, deadline).await {
            Ok(page) => Ok(page),
            Err(StoreError::Deadline) => Err(Error::Deadline),
            Err(StoreError::ResponseBudget) => Err(Error::SourceBudget),
            Err(e) => {
                if e == StoreError::Indeterminate {
                    slot.retain();
                }
                Err(Error::Unavailable)
            }
        }
    });
    // On client deadline the detached task still owns its lane until the source finishes.
    let result = match timeout_at(deadline, task).await {
        Ok(joined) => joined.map_err(|_| Error::Internal)?,
        Err(_) => Err(Error::Deadline),
    };
    app.authority.authorize(&key, Some(tenant), Action::Query)?;
    match result {
        Ok(page) => Ok(json_reply(
            200,
            query::result(tenant, source, &q, page, evidence),
        )),
        Err(err @ (Error::Unavailable | Error::Deadline | Error::SourceBudget)) => Ok(json_reply(
            err.response().0,
            json!({"outcome":"unavailable","reason":err.response().1,"requested_range":q.range(),
                "effective_range":null,"source_query_range":null,"coverage":"unknown","source":source}),
        )),
        Err(e) => Err(e),
    }
}

async fn api(app: Arc<App>, req: Request<Incoming>, started: Instant) -> Result<Reply> {
    let path = req.uri().path().to_owned();
    let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
    let key = Key::from_headers(req.headers())?;
    match (req.method(), parts.as_slice()) {
        (&Method::POST, ["api", "v1", "bootstrap"]) => {
            body(req, 0).await?;
            let owner = app.authority.bootstrap(&key)?;
            Ok(json_reply(201, json!(owner)))
        }
        (&Method::POST, ["api", "v1", "tenants"]) => {
            app.authority.owner(&key)?;
            app.authority.create_tenant(&key, json_body(req).await?)?;
            Ok(json_reply(201, json!({"outcome":"created"})))
        }
        (&Method::POST, ["api", "v1", "tenants", tenant, "credentials"]) => {
            app.authority.owner(&key)?;
            let credential = app.authority.issue(&key, tenant, json_body(req).await?)?;
            Ok(json_reply(201, json!(credential)))
        }
        (&Method::DELETE, ["api", "v1", "tenants", tenant, "credentials", id]) => {
            app.authority.revoke(&key, tenant, id)?;
            Ok(reply(204, JSON, Bytes::new()))
        }
        (&Method::GET, ["api", "v1", "tenants", tenant, "logs", "delivery"]) => {
            let grant = app.authority.authorize(&key, Some(tenant), Action::Query)?;
            let spool = app.spool.clone();
            let slot = app.reads.enter(grant.tenant())?;
            let status = blocking(move || {
                let _slot = slot;
                spool.status(grant.tenant())
            })
            .await?;
            app.authority.authorize(&key, Some(tenant), Action::Query)?;
            Ok(json_reply(200, status))
        }
        (&Method::POST, ["api", "v1", "tenants", tenant, family, "query"])
            if matches!(*family, "logs" | "evidence") =>
        {
            app.authority.authorize(&key, Some(tenant), Action::Query)?;
            let q = json_body(req).await?;
            read_logs(app, key, tenant, q, *family == "evidence", started).await
        }
        _ => Err(Error::Missing),
    }
}

async fn handle(app: Arc<App>, req: Request<Incoming>) -> Reply {
    let started = Instant::now();
    let otlp = req.method() == Method::POST && req.uri().path() == "/v1/logs";
    let is_json = req
        .headers()
        .get(header::CONTENT_TYPE)
        .is_some_and(|s| s.as_bytes().eq_ignore_ascii_case(JSON.as_bytes()));
    if req.uri().query().is_some() || req.headers().contains_key("x-scope-orgid") {
        return if otlp {
            otlp_error(Error::Invalid, is_json)
        } else {
            api_error(Error::Invalid)
        };
    }
    if otlp {
        let key = match Key::from_headers(req.headers()) {
            Ok(key) => key,
            Err(e) => return otlp_error(e, is_json),
        };
        ingest(app, key, req, is_json)
            .await
            .unwrap_or_else(|e| otlp_error(e, is_json))
    } else {
        api(app, req, started).await.unwrap_or_else(api_error)
    }
}

pub async fn serve(listener: TcpListener, app: Arc<App>) -> Result<()> {
    let connections = Arc::new(Semaphore::new(32));
    loop {
        let permit = connections
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Internal)?;
        let (stream, _) = listener.accept().await?;
        let app = app.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let service = service_fn(move |req| {
                let app = app.clone();
                async move { Ok::<_, Infallible>(handle(app, req).await) }
            });
            let mut http = http1::Builder::new();
            http.keep_alive(false)
                .max_buf_size(8192)
                .max_headers(32)
                .timer(TokioTimer::new())
                .header_read_timeout(Duration::from_secs(2));
            let _ = timeout(
                Duration::from_secs(10),
                http.serve_connection(TokioIo::new(stream), service),
            )
            .await;
        });
    }
}
