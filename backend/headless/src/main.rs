//! Headless local logs service. The operator's Unix account is the installation trust root.
mod admission;
mod api;
mod authority;
mod logs;
mod query;
mod storage;

use authority::Authority;
use std::{
    env, fs::OpenOptions, net::SocketAddr, os::unix::fs::OpenOptionsExt, path::PathBuf, sync::Arc,
};
use tokio::net::TcpListener;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Error {
    Invalid,
    Unauthenticated,
    Denied,
    Missing,
    Conflict,
    TooLarge,
    Media,
    Busy,
    ReadTimeout,
    Deadline,
    Unavailable,
    State,
    Internal,
}
type Result<T> = std::result::Result<T, Error>;
impl From<rusqlite::Error> for Error {
    fn from(_: rusqlite::Error) -> Self {
        Self::State
    }
}
impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        Self::State
    }
}

fn main() {
    if let Err(e) = run() {
        eprintln!("observe-backend: {e:?} (usage: observe-backend init|serve|recover-owner ABSOLUTE_STATE_DIR)");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut args = env::args_os().skip(1);
    let op = args
        .next()
        .and_then(|v| v.into_string().ok())
        .ok_or(Error::Invalid)?;
    let dir = PathBuf::from(args.next().ok_or(Error::Invalid)?);
    if args.next().is_some() || !matches!(op.as_str(), "init" | "recover-owner" | "serve") {
        return Err(Error::Invalid);
    }
    let auth = Authority::open(&dir, op == "init")?;
    if op != "serve" {
        return auth.provision(&dir, op == "recover-owner");
    }
    let lock = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(dir.join("server.lock"))?;
    lock.try_lock().map_err(|_| Error::State)?;
    let addr: SocketAddr = env::var("OBSERVE_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:4318".into())
        .parse()
        .map_err(|_| Error::Invalid)?;
    if !addr.ip().is_loopback() {
        return Err(Error::Invalid);
    }
    let app = Arc::new(api::App::new(
        auth,
        Arc::new(storage::Loki(dir.join("loki.sock:0"))),
    ));
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(4)
        .enable_all()
        .build()?
        .block_on(async {
            let listener = TcpListener::bind(addr).await?;
            println!("LISTENING {}", listener.local_addr()?);
            api::serve(listener, app).await
        })
}
