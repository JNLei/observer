//! A loopback-only process-liveness endpoint, not an ingestion or management API.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const MAX_HEADER_BYTES: usize = 4096;
const REQUEST_DEADLINE: Duration = Duration::from_secs(2);
const WORKERS: usize = 4;
const QUEUE_CAPACITY: usize = 8;

/// Bind only to a numeric loopback address. No public service is configured here.
pub fn bind_loopback(address: &str) -> io::Result<TcpListener> {
    let address: SocketAddr = address.parse().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid numeric socket address",
        )
    })?;
    if !address.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "health listener must use a loopback address",
        ));
    }
    TcpListener::bind(address)
}

/// Serve with a fixed worker pool, a bounded queue, and an absolute read deadline.
pub fn serve(listener: TcpListener) -> io::Result<()> {
    let (sender, receiver) = mpsc::sync_channel::<TcpStream>(QUEUE_CAPACITY);
    let receiver = Arc::new(Mutex::new(receiver));
    for _ in 0..WORKERS {
        let receiver = Arc::clone(&receiver);
        thread::Builder::new()
            .name("health-worker".into())
            .spawn(move || loop {
                let stream = match receiver.lock() {
                    Ok(receiver) => receiver.recv(),
                    Err(_) => return,
                };
                let Ok(stream) = stream else { return };
                if let Err(error) = handle_connection(stream, REQUEST_DEADLINE) {
                    eprintln!("health connection failed: {error}");
                }
            })?;
    }
    for connection in listener.incoming() {
        match connection {
            Ok(stream) => match sender.try_send(stream) {
                Ok(()) => {}
                Err(mpsc::TrySendError::Full(mut stream)) => {
                    let _ = stream.set_write_timeout(Some(REQUEST_DEADLINE));
                    let _ = response(
                        &mut stream,
                        "503 Service Unavailable",
                        "{\"error\":\"busy\"}",
                    );
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    return Err(io::Error::other("health worker pool stopped"));
                }
            },
            Err(error) => eprintln!("health accept failed: {error}"),
        }
    }
    Ok(())
}

fn response(stream: &mut TcpStream, status: &str, body: &str) -> io::Result<()> {
    let allow = if status == "405 Method Not Allowed" {
        "Allow: GET\r\n"
    } else {
        ""
    };
    let message = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{allow}Connection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(message.as_bytes())
}

/// Process one bounded HTTP/1.x request. The result is process liveness only.
pub fn handle_connection(mut stream: TcpStream, deadline: Duration) -> io::Result<()> {
    stream.set_write_timeout(Some(deadline))?;
    let started = Instant::now();
    let mut bytes = [0_u8; MAX_HEADER_BYTES + 1];
    let mut length = 0;
    let header_end = loop {
        let Some(remaining) = deadline.checked_sub(started.elapsed()) else {
            return response(
                &mut stream,
                "408 Request Timeout",
                "{\"error\":\"timeout\"}",
            );
        };
        // A zero duration is invalid on some platforms.
        if remaining.is_zero() {
            return response(
                &mut stream,
                "408 Request Timeout",
                "{\"error\":\"timeout\"}",
            );
        }
        stream.set_read_timeout(Some(remaining))?;
        match stream.read(&mut bytes[length..]) {
            Ok(0) => return Ok(()),
            Ok(read) => length += read,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) =>
            {
                return response(
                    &mut stream,
                    "408 Request Timeout",
                    "{\"error\":\"timeout\"}",
                );
            }
            Err(error) => return Err(error),
        }
        if let Some(end) = bytes[..length]
            .windows(4)
            .position(|part| part == b"\r\n\r\n")
        {
            if end + 4 > MAX_HEADER_BYTES {
                return response(
                    &mut stream,
                    "431 Request Header Fields Too Large",
                    "{\"error\":\"header too large\"}",
                );
            }
            break end + 2;
        }
        if length > MAX_HEADER_BYTES {
            return response(
                &mut stream,
                "431 Request Header Fields Too Large",
                "{\"error\":\"header too large\"}",
            );
        }
    };

    let mut lines = bytes[..header_end].split_inclusive(|&byte| byte == b'\n');
    let first = lines.next().and_then(|line| line.strip_suffix(b"\r\n"));
    if !lines.all(valid_header_field) {
        return response(
            &mut stream,
            "400 Bad Request",
            "{\"error\":\"bad request\"}",
        );
    }
    let Some(first_line) = first
        .filter(|line| !line.contains(&b'\r'))
        .and_then(|line| std::str::from_utf8(line).ok())
    else {
        return response(
            &mut stream,
            "400 Bad Request",
            "{\"error\":\"bad request\"}",
        );
    };
    let mut parts = first_line.split(' ');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(method), Some(path), Some("HTTP/1.0" | "HTTP/1.1"), None) => {
            if method != "GET" {
                response(
                    &mut stream,
                    "405 Method Not Allowed",
                    "{\"error\":\"method not allowed\"}",
                )
            } else if path == "/healthz" {
                response(&mut stream, "200 OK", "{\"status\":\"ok\"}")
            } else {
                response(&mut stream, "404 Not Found", "{\"error\":\"not found\"}")
            }
        }
        _ => response(
            &mut stream,
            "400 Bad Request",
            "{\"error\":\"bad request\"}",
        ),
    }
}

fn valid_header_field(line: &[u8]) -> bool {
    let Some(field) = line.strip_suffix(b"\r\n") else {
        return false;
    };
    let Some(colon) = field.iter().position(|&byte| byte == b':') else {
        return false;
    };
    colon > 0
        && field[..colon]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(byte))
        && field[colon + 1..]
            .iter()
            .all(|&byte| byte == b'\t' || (byte >= b' ' && byte != 0x7f))
}
