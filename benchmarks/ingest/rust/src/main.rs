//! Synthetic benchmark-only forwarding protocol; not an OTLP or production receiver.
use std::env;
use std::io::{self, BufRead, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const OK: u8 = 0;
const BUSY: u8 = 1;
const DOWNSTREAM_UNKNOWN: u8 = 2;
const INVALID: u8 = 3;
const READY: u8 = 4;

#[derive(Clone, Default)]
struct Counters {
    accepted: u64,
    admitted: u64,
    overloaded: u64,
    overload_writing: u64,
    overload_write_ok: u64,
    overload_write_failed: u64,
    queued: u64,
    in_flight: u64,
    completed: u64,
    ready_write_failed: u64,
    status_ok: u64,
    status_downstream_unknown: u64,
    status_invalid: u64,
    response_write_ok: u64,
    response_write_failed: u64,
}

fn report(kind: &str, counters: &Mutex<Counters>) {
    let c = counters.lock().unwrap().clone();
    println!(
        concat!(
            "{} {{\"accepted\":{},\"admitted\":{},\"overloaded\":{},",
            "\"overload_writing\":{},\"overload_write_ok\":{},\"overload_write_failed\":{},",
            "\"queued\":{},\"in_flight\":{},\"completed\":{},\"ready_write_failed\":{},",
            "\"status_ok\":{},\"status_downstream_unknown\":{},\"status_invalid\":{},",
            "\"response_write_ok\":{},\"response_write_failed\":{}}}"
        ),
        kind,
        c.accepted,
        c.admitted,
        c.overloaded,
        c.overload_writing,
        c.overload_write_ok,
        c.overload_write_failed,
        c.queued,
        c.in_flight,
        c.completed,
        c.ready_write_failed,
        c.status_ok,
        c.status_downstream_unknown,
        c.status_invalid,
        c.response_write_ok,
        c.response_write_failed
    );
    let _ = io::stdout().flush();
}

struct Config {
    listen: SocketAddr,
    downstream: SocketAddr,
    workers: usize,
    queue: usize,
    timeout: Duration,
    max_payload: usize,
}

fn socket_address(value: &str, allow_zero_port: bool) -> Result<SocketAddr, String> {
    let address: SocketAddr = value
        .parse()
        .map_err(|_| "invalid numeric socket address")?;
    if !address.ip().is_loopback() || (!allow_zero_port && address.port() == 0) {
        return Err("addresses must be loopback and the downstream port must be nonzero".into());
    }
    Ok(address)
}

fn number(value: &str, max: usize) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| "invalid positive integer")?;
    if parsed == 0 || parsed > max {
        return Err(format!("value must be between 1 and {max}"));
    }
    Ok(parsed)
}

fn config() -> Result<Config, String> {
    let mut config = Config {
        listen: socket_address("127.0.0.1:19091", true)?,
        downstream: socket_address("127.0.0.1:19090", false)?,
        workers: 8,
        queue: 16,
        timeout: Duration::from_millis(500),
        max_payload: 65_536,
    };
    let mut args = env::args().skip(1);
    while let Some(key) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {key}"))?;
        match key.as_str() {
            "--listen" => config.listen = socket_address(&value, true)?,
            "--downstream" => config.downstream = socket_address(&value, false)?,
            "--workers" => config.workers = number(&value, 256)?,
            "--queue" => config.queue = number(&value, 4096)?,
            "--timeout-ms" => {
                config.timeout = Duration::from_millis(number(&value, 60_000)? as u64)
            }
            "--max-payload" => config.max_payload = number(&value, 1_048_576)?,
            _ => return Err(format!("unknown argument: {key}")),
        }
    }
    Ok(config)
}

fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "request deadline exceeded"))
}

fn read_exact_until(
    stream: &mut TcpStream,
    mut bytes: &mut [u8],
    deadline: Instant,
) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.set_read_timeout(Some(remaining(deadline)?))?;
        match stream.read(bytes) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short frame")),
            Ok(length) => bytes = &mut bytes[length..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn write_all_until(stream: &mut TcpStream, mut bytes: &[u8], deadline: Instant) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.set_write_timeout(Some(remaining(deadline)?))?;
        match stream.write(bytes) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "short write")),
            Ok(length) => bytes = &bytes[length..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn forward(stream: &mut TcpStream, config: &Config, deadline: Instant) -> u8 {
    let mut header = [0_u8; 12];
    if read_exact_until(stream, &mut header, deadline).is_err() {
        return INVALID;
    }
    let length = u32::from_be_bytes(header[8..12].try_into().expect("fixed header")) as usize;
    if length > config.max_payload {
        return INVALID;
    }
    let mut payload = vec![0; length];
    if read_exact_until(stream, &mut payload, deadline).is_err() {
        return INVALID;
    }
    let result = (|| -> io::Result<()> {
        let mut downstream = TcpStream::connect_timeout(&config.downstream, remaining(deadline)?)?;
        downstream.set_nodelay(true)?;
        write_all_until(&mut downstream, &header, deadline)?;
        write_all_until(&mut downstream, &payload, deadline)?;
        let mut ack = [0_u8; 8];
        read_exact_until(&mut downstream, &mut ack, deadline)?;
        if ack != header[..8] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "wrong downstream acknowledgement",
            ));
        }
        Ok(())
    })();
    if result.is_ok() {
        OK
    } else {
        DOWNSTREAM_UNKNOWN
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(config().map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?);
    let listener = TcpListener::bind(config.listen)?;
    listener.set_nonblocking(true)?;
    let (sender, receiver) = mpsc::sync_channel::<TcpStream>(config.queue);
    let receiver = Arc::new(Mutex::new(receiver));
    let counters = Arc::new(Mutex::new(Counters::default()));
    let draining = Arc::new(AtomicBool::new(false));
    let mut workers = Vec::with_capacity(config.workers);
    for _ in 0..config.workers {
        let receiver = Arc::clone(&receiver);
        let config = Arc::clone(&config);
        let counters = Arc::clone(&counters);
        workers.push(thread::spawn(move || loop {
            let stream = match receiver.lock() {
                Ok(receiver) => receiver.recv(),
                Err(_) => return,
            };
            let Ok(mut stream) = stream else { return };
            {
                let mut c = counters.lock().unwrap();
                c.queued -= 1;
                c.in_flight += 1;
            }
            let _ = stream.set_nodelay(true);
            let deadline = Instant::now() + config.timeout;
            if write_all_until(&mut stream, &[READY], deadline).is_err() {
                drop(stream);
                let mut c = counters.lock().unwrap();
                c.in_flight -= 1;
                c.completed += 1;
                c.ready_write_failed += 1;
                continue;
            }
            let status = forward(&mut stream, &config, deadline);
            // A client timeout does not cancel the downstream operation; the fixed worker
            // remains occupied until forward() returns or its absolute deadline expires.
            let write_ok = stream
                .set_write_timeout(Some(config.timeout))
                .and_then(|()| stream.write_all(&[status]))
                .is_ok();
            drop(stream);
            let mut c = counters.lock().unwrap();
            c.in_flight -= 1;
            c.completed += 1;
            match status {
                OK => c.status_ok += 1,
                DOWNSTREAM_UNKNOWN => c.status_downstream_unknown += 1,
                INVALID => c.status_invalid += 1,
                _ => unreachable!("forward returns only final statuses"),
            }
            if write_ok {
                c.response_write_ok += 1;
            } else {
                c.response_write_failed += 1;
            }
        }));
    }
    {
        let counters = Arc::clone(&counters);
        let draining = Arc::clone(&draining);
        thread::spawn(move || {
            for line in io::stdin().lock().lines() {
                match line.as_deref() {
                    Ok("SNAPSHOT") => report("SNAPSHOT", &counters),
                    Ok("DRAIN") => draining.store(true, Ordering::SeqCst),
                    Ok(_) => eprintln!("unknown control command"),
                    Err(_) => return,
                }
            }
        });
    }
    println!("LISTENING {}", listener.local_addr()?);
    io::stdout().flush()?;
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                // Some platforms inherit the listener's nonblocking mode on accept.
                stream.set_nonblocking(false)?;
                let mut c = counters.lock().unwrap();
                c.accepted += 1;
                match sender.try_send(stream) {
                    Ok(()) => {
                        c.admitted += 1;
                        c.queued += 1;
                    }
                    Err(mpsc::TrySendError::Full(mut stream)) => {
                        c.overloaded += 1;
                        c.overload_writing += 1;
                        drop(c);
                        let write_ok = stream
                            .set_write_timeout(Some(config.timeout))
                            .and_then(|()| stream.write_all(&[BUSY]))
                            .is_ok();
                        drop(stream);
                        let mut c = counters.lock().unwrap();
                        c.overload_writing -= 1;
                        if write_ok {
                            c.overload_write_ok += 1;
                        } else {
                            c.overload_write_failed += 1;
                        }
                    }
                    Err(mpsc::TrySendError::Disconnected(_)) => {
                        return Err("worker pool stopped".into())
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if draining.load(Ordering::SeqCst) {
                    break;
                }
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    drop(listener);
    drop(sender);
    for worker in workers {
        worker.join().map_err(|_| "worker panicked")?;
    }
    report("FINAL", &counters);
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("benchmark forwarder: {error}");
            ExitCode::FAILURE
        }
    }
}
