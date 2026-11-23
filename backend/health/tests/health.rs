use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn request(bytes: &[u8]) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        observe_health::handle_connection(stream, Duration::from_secs(1)).unwrap();
    });
    let mut stream = TcpStream::connect(address).unwrap();
    stream.write_all(bytes).unwrap();
    let mut reply = String::new();
    stream.read_to_string(&mut reply).unwrap();
    server.join().unwrap();
    reply
}

fn start_binary() -> (ChildGuard, TcpStream) {
    let address = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_observe-health"))
            .env("OBSERVE_HEALTH_ADDR", address.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let until = Instant::now() + Duration::from_secs(5);
    let stream = loop {
        match TcpStream::connect(address) {
            Ok(stream) => break stream,
            Err(error) if Instant::now() < until => {
                assert!(
                    child.0.try_wait().unwrap().is_none(),
                    "server exited: {error}"
                );
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => panic!("server did not start: {error}"),
        }
    };
    (child, stream)
}

#[test]
fn running_binary_serves_process_health_without_optional_components() {
    let (_child, mut stream) = start_binary();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .unwrap();
    let mut reply = String::new();
    stream.read_to_string(&mut reply).unwrap();
    assert!(reply.starts_with("HTTP/1.1 200 OK\r\n"), "{reply}");
    assert!(reply.ends_with("{\"status\":\"ok\"}"), "{reply}");
}

#[test]
fn only_health_get_is_served_and_headers_are_bounded() {
    assert!(request(b"GET /healthz HTTP/1.0\r\n\r\n").starts_with("HTTP/1.1 200"));
    assert!(request(b"GET /other HTTP/1.1\r\n\r\n").starts_with("HTTP/1.1 404"));
    let wrong_method = request(b"POST /healthz HTTP/1.1\r\n\r\n");
    assert!(wrong_method.starts_with("HTTP/1.1 405"));
    assert!(wrong_method.contains("\r\nAllow: GET\r\n"));
    assert!(request(b"malformed\r\n\r\n").starts_with("HTTP/1.1 400"));
    assert!(request(&vec![b'x'; 4097]).starts_with("HTTP/1.1 431"));
}

#[test]
fn request_line_requires_exact_crlf() {
    assert!(request(b"GET /healthz HTTP/1.1\nHost: localhost\r\n\r\n").starts_with("HTTP/1.1 400"));
    assert!(
        request(b"GET /healthz HTTP/1.1\r\r\nHost: localhost\r\n\r\n").starts_with("HTTP/1.1 400")
    );
    assert!(
        request(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n").starts_with("HTTP/1.1 200")
    );
}

#[test]
fn validates_header_fields_before_routing() {
    for field in [
        &b"No-Colon"[..],
        b": empty name",
        b"Host : localhost",
        b" folded",
        b"X: value\nNext: field",
        b"X: con\rtrol",
        b"X: a\x7fb",
        b"X: a\0b",
    ] {
        let input = [b"GET /healthz HTTP/1.1\r\n", field, b"\r\n\r\n"].concat();
        assert!(request(&input).starts_with("HTTP/1.1 400"));
    }
    assert!(
        request(b"GET /healthz HTTP/1.1\r\nX-A: \t\xff\r\nEmpty:\r\n\r\n")
            .starts_with("HTTP/1.1 200")
    );
}

#[test]
fn stalled_client_does_not_block_another_health_probe() {
    let (_child, mut stalled) = start_binary();
    let address = stalled.peer_addr().unwrap();
    stalled
        .write_all(b"GET /healthz HTTP/1.1\r\nHost:")
        .unwrap();
    thread::sleep(Duration::from_millis(100));
    let mut probe = TcpStream::connect(address).unwrap();
    probe
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    probe.write_all(b"GET /healthz HTTP/1.1\r\n\r\n").unwrap();
    let mut reply = String::new();
    probe.read_to_string(&mut reply).unwrap();
    assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
}

#[test]
fn slow_headers_cannot_extend_the_absolute_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        observe_health::handle_connection(stream, Duration::from_secs(1)).unwrap();
    });
    let mut stream = TcpStream::connect(address).unwrap();
    stream.write_all(b"G").unwrap();
    thread::sleep(Duration::from_millis(500));
    stream.write_all(b"E").unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(750)))
        .unwrap();
    let mut reply = String::new();
    stream.read_to_string(&mut reply).unwrap();
    assert!(reply.starts_with("HTTP/1.1 408"), "{reply}");
    server.join().unwrap();
}

#[test]
fn rejects_non_loopback_or_non_numeric_addresses() {
    assert!(observe_health::bind_loopback("0.0.0.0:0").is_err());
    assert!(observe_health::bind_loopback("localhost:0").is_err());
    assert!(observe_health::bind_loopback("127.0.0.1:0").is_ok());
}
