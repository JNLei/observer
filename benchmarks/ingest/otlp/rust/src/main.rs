//! Local OTLP/HTTP log receiver for the paired Loki trial.
mod logs;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    body::Incoming, header, server::conn::http1, service::service_fn, Method, Request, Response,
};
use hyper_util::rt::TokioIo;
use prost::Message;
use serde_json::json;
use std::{
    convert::Infallible,
    env,
    error::Error,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio::{
    net::{TcpListener, UnixStream},
    sync::Semaphore,
    time::timeout,
};

const PROTO: &str = "application/x-protobuf";
const IO_TIME: Duration = Duration::from_secs(2);
const QUERY_LIMIT: usize = 512 * 1024;
type Reply = Response<Full<Bytes>>;
type AnyError = Box<dyn Error + Send + Sync>;

#[derive(Default)]
struct Counts {
    admitted: usize,
    input_wire_bytes: usize,
    ack_bytes: usize,
    rejected: usize,
    attempts: usize,
    acknowledged: usize,
    unknown: usize,
    store_http_400_attempts: usize,
    overloads: usize,
}
struct Tenant {
    name: &'static str,
    credential: String,
    writes: Arc<Semaphore>,
    writer: Semaphore,
    reader: Arc<Semaphore>,
    counts: Mutex<Counts>,
}
struct Trial {
    socket: PathBuf,
    tenants: [Arc<Tenant>; 2],
}

fn response(code: u16, kind: &str, bytes: impl Into<Bytes>) -> Reply {
    let mut builder = Response::builder()
        .status(code)
        .header(header::CONTENT_TYPE, kind);
    if code == 429 || code == 503 {
        builder = builder.header(header::RETRY_AFTER, "1");
    }
    builder.body(Full::new(bytes.into())).unwrap()
}
fn failure(code: u16) -> Reply {
    // google.rpc.Status field 2. Only fixed messages enter protocol errors.
    #[derive(Message)]
    struct Status {
        #[prost(string, tag = "2")]
        message: String,
    }
    response(
        code,
        PROTO,
        Status {
            message: hyper::StatusCode::from_u16(code)
                .unwrap()
                .canonical_reason()
                .unwrap_or("failure")
                .into(),
        }
        .encode_to_vec(),
    )
}

async fn store(
    socket: &Path,
    tenant: &str,
    method: Method,
    path: String,
    body: Vec<u8>,
) -> Result<(u16, Bytes), AnyError> {
    let socket = UnixStream::connect(socket).await?;
    let (mut client, connection) =
        hyper::client::conn::http1::handshake(TokioIo::new(socket)).await?;
    // Dropping the set closes the transport on error, cancellation or deadline.
    let mut transport = tokio::task::JoinSet::new();
    transport.spawn(connection);
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header(header::HOST, "localhost")
        .header("X-Scope-OrgID", tenant)
        .header(header::CONTENT_TYPE, PROTO)
        .body(Full::new(Bytes::from(body)))?;
    let reply = client.send_request(request).await?;
    let code = reply.status().as_u16();
    let bytes = if code == 200 {
        Limited::new(reply.into_body(), QUERY_LIMIT)
            .collect()
            .await?
            .to_bytes()
    } else {
        Bytes::new()
    };
    Ok((code, bytes))
}

fn query_path(query: &str) -> Option<String> {
    let (a, b) = query.strip_prefix("start=")?.split_once("&end=")?;
    if ![a, b]
        .iter()
        .all(|v| !v.is_empty() && v.bytes().all(|c| c.is_ascii_digit()))
    {
        return None;
    }
    let (start, end) = (a.parse::<i64>().ok()?, b.parse::<i64>().ok()?);
    if start <= 0 || end <= start || end - start > 26 * 3600 * 1_000_000_000 {
        return None;
    }
    Some(format!("/loki/api/v1/query_range?query=%7Bservice_name%3D%22m1-bench%22%7D&start={start}&end={end}&limit=512&direction=forward"))
}

async fn ingest(req: Request<Incoming>, tenant: Arc<Tenant>, trial: Arc<Trial>) -> Reply {
    if req.headers().get_all(header::CONTENT_TYPE).iter().count() != 1
        || !req
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(PROTO.as_bytes()))
    {
        return failure(415);
    }
    let mut encodings = req.headers().get_all(header::CONTENT_ENCODING).iter();
    let gzip = match encodings.next().map(|h| h.as_bytes()) {
        None => false,
        Some(h) if h.eq_ignore_ascii_case(b"identity") => false,
        Some(h) if h.eq_ignore_ascii_case(b"gzip") => true,
        _ => return failure(415),
    };
    if encodings.next().is_some() {
        return failure(415);
    }
    if req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .is_some_and(|n| n > logs::BYTE_LIMIT as u64)
    {
        return failure(413);
    }
    let Ok(slot) = tenant.writes.clone().try_acquire_owned() else {
        tenant.counts.lock().unwrap().overloads += 1;
        return failure(429);
    };
    let wire = match timeout(
        IO_TIME,
        Limited::new(req.into_body(), logs::BYTE_LIMIT).collect(),
    )
    .await
    {
        Err(_) => return failure(408),
        Ok(Err(e)) if e.is::<http_body_util::LengthLimitError>() => return failure(413),
        Ok(Err(_)) => return failure(400),
        Ok(Ok(body)) => body.to_bytes(),
    };
    let input_bytes = wire.len();
    let decoded = tokio::task::spawn_blocking(move || (slot, logs::prepare(&wire, gzip))).await;
    let (slot, batch) = match decoded {
        Ok((slot, Ok(batch))) => (slot, batch),
        Ok((_, Err(code))) => return failure(code),
        Err(_) => return failure(503),
    };
    {
        let mut counts = tenant.counts.lock().unwrap();
        counts.rejected += batch.rejected;
        counts.admitted += batch.accepted;
        counts.input_wire_bytes += input_bytes;
    }
    if batch.accepted == 0 {
        return if batch.rejected == 0 {
            response(200, PROTO, Bytes::new())
        } else {
            failure(400)
        };
    }
    // The transaction owns its slot through the downstream outcome, even if the client leaves.
    let rejected = batch.rejected;
    let work = tokio::spawn(async move {
        let _slot = slot;
        let _active = tenant.writer.acquire().await.unwrap();
        tenant.counts.lock().unwrap().attempts += 1;
        let size = batch.wire.len();
        let outcome = timeout(
            IO_TIME,
            store(
                &trial.socket,
                tenant.name,
                Method::POST,
                "/otlp/v1/logs".into(),
                batch.wire,
            ),
        )
        .await;
        let ack = matches!(outcome, Ok(Ok((204, _))));
        let mut counts = tenant.counts.lock().unwrap();
        if ack {
            counts.acknowledged += batch.accepted;
            counts.ack_bytes += size;
        } else {
            counts.unknown += batch.accepted;
            if matches!(outcome, Ok(Ok((400, _)))) {
                // Loki can commit the valid subset before returning 400.
                counts.store_http_400_attempts += 1;
            }
        }
        ack
    });
    if matches!(work.await, Ok(true)) {
        response(200, PROTO, logs::success(rejected))
    } else {
        failure(503)
    }
}

async fn handle(req: Request<Incoming>, trial: Arc<Trial>) -> Reply {
    let mut auth = req.headers().get_all(header::AUTHORIZATION).iter();
    let Some(value) = auth.next() else {
        return failure(401);
    };
    let Some(tenant) = trial
        .tenants
        .iter()
        .find(|t| bool::from(t.credential.as_bytes().ct_eq(value.as_bytes())))
        .cloned()
    else {
        return failure(401);
    };
    if auth.next().is_some() {
        return failure(401);
    }
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/v1/logs") if req.uri().query().is_none() => {
            ingest(req, tenant, trial).await
        }
        (&Method::GET, "/bench/v1/counts") if req.uri().query().is_none() => {
            let c = tenant.counts.lock().unwrap();
            let snapshot = json!({
                "admitted_records": c.admitted,
                "input_wire_bytes": c.input_wire_bytes,
                "invalid_records": c.rejected,
                "store_attempts": c.attempts,
                "store_ack_records": c.acknowledged,
                "store_ack_bytes": c.ack_bytes,
                "store_unknown_records": c.unknown,
                "store_http_400_attempts": c.store_http_400_attempts,
                "outstanding_records": c.admitted - c.acknowledged - c.unknown,
                "overloaded_requests": c.overloads
            });
            response(200, "application/json", snapshot.to_string())
        }
        (&Method::GET, "/bench/v1/logs") => {
            let Some(path) = req.uri().query().and_then(query_path) else {
                return failure(400);
            };
            let Ok(slot) = tenant.reader.clone().try_acquire_owned() else {
                return failure(429);
            };
            // Keep the read reservation until this store exchange ends.
            let read = tokio::spawn(async move {
                let _slot = slot;
                timeout(
                    IO_TIME,
                    store(&trial.socket, tenant.name, Method::GET, path, vec![]),
                )
                .await
            });
            match read.await {
                Ok(Ok(Ok((200, data)))) => response(200, "application/json", data),
                _ => failure(503),
            }
        }
        _ => failure(404),
    }
}

fn tenant(name: &'static str, key: &str) -> Result<Arc<Tenant>, AnyError> {
    let token = env::var(key)?;
    if token.len() != 64
        || !token
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err("invalid trial credential".into());
    }
    Ok(Arc::new(Tenant {
        name,
        credential: format!("Bearer {token}"),
        writes: Arc::new(Semaphore::new(3)),
        writer: Semaphore::new(1),
        reader: Arc::new(Semaphore::new(1)),
        counts: Mutex::default(),
    }))
}

fn main() -> Result<(), AnyError> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()?
        .block_on(serve())
}

async fn serve() -> Result<(), AnyError> {
    let addr: SocketAddr = env::var("OBS_BENCH_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:0".into())
        .parse()?;
    let socket = PathBuf::from(env::var("OBS_BENCH_LOKI_SOCKET")?);
    let tenants = [
        tenant("a", "OBS_BENCH_TOKEN_A")?,
        tenant("b", "OBS_BENCH_TOKEN_B")?,
    ];
    if !addr.ip().is_loopback()
        || !socket.is_absolute()
        || tenants[0].credential == tenants[1].credential
    {
        return Err("invalid trial configuration".into());
    }
    let trial = Arc::new(Trial { socket, tenants });
    let listener = TcpListener::bind(addr).await?;
    let connections = Arc::new(Semaphore::new(12));
    println!("LISTENING {}", listener.local_addr()?);
    loop {
        let permit = connections.clone().acquire_owned().await?;
        let (stream, _) = listener.accept().await?;
        let trial = trial.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let mut http = http1::Builder::new();
            http.keep_alive(false)
                .max_buf_size(8192)
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(IO_TIME);
            let service = service_fn(move |req| {
                let trial = trial.clone();
                async move { Ok::<_, Infallible>(handle(req, trial).await) }
            });
            let _ = timeout(
                Duration::from_secs(11),
                http.serve_connection(TokioIo::new(stream), service),
            )
            .await;
        });
    }
}
