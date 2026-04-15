//! D-δ integration test 1: nodedb binary exits within 1 second of SIGTERM.
//!
//! Spawns the real `nodedb` binary via `std::process::Command`, waits for
//! it to become ready (HTTP /healthz returns 200 via raw TCP), sends SIGTERM,
//! and asserts the process exits within 1,100 ms (1 s budget + 100 ms slack).
//!
//! Real process. Real signal. Real timer. No mocks.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

/// Allocate an ephemeral port by binding, recording the port, then releasing.
fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().expect("local_addr").port()
}

/// Send a raw HTTP GET /healthz request and return whether the response is 200.
fn check_healthz(port: u16) -> bool {
    let addr = format!("127.0.0.1:{port}");
    let mut stream = match TcpStream::connect_timeout(
        &addr.parse().expect("addr"),
        Duration::from_millis(200),
    ) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let req = b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    if stream.write_all(req).is_err() {
        return false;
    }
    let mut buf = [0u8; 256];
    match stream.read(&mut buf) {
        Ok(n) if n > 0 => {
            let resp = std::str::from_utf8(&buf[..n]).unwrap_or("");
            resp.starts_with("HTTP/1.1 200")
        }
        _ => false,
    }
}

/// Poll HTTP /healthz until 200 or deadline.
fn wait_for_healthz(port: u16, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if Instant::now() >= deadline {
            return false;
        }
        if check_healthz(port) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn real_nodedb_binary_exits_within_1_second_of_sigterm() {
    let bin = env!("CARGO_BIN_EXE_nodedb");

    // Use a unique temp dir and ephemeral ports for this test.
    let dir = tempfile::tempdir().expect("tempdir");
    let http_port = free_port();
    let pgwire_port = free_port();
    let native_port = free_port();

    let mut child = std::process::Command::new(bin)
        .env("NODEDB_DATA_DIR", dir.path())
        .env("NODEDB_DATA_PLANE_CORES", "1")
        .env("NODEDB_PORT_HTTP", http_port.to_string())
        .env("NODEDB_PORT_PGWIRE", pgwire_port.to_string())
        .env("NODEDB_PORT_NATIVE", native_port.to_string())
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to spawn nodedb binary");

    let ready = wait_for_healthz(http_port, Duration::from_secs(15));
    assert!(
        ready,
        "nodedb did not become ready within 15s — startup failure"
    );

    // Send SIGTERM and start the timer.
    let start = Instant::now();
    #[cfg(unix)]
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    #[cfg(not(unix))]
    {
        child.kill().expect("kill");
    }

    let status = child.wait().expect("wait for child");
    let elapsed = start.elapsed();

    assert!(
        status.success() || status.code() == Some(0),
        "nodedb exited with unexpected status {status:?} after SIGTERM"
    );
    assert!(
        elapsed <= Duration::from_millis(1100),
        "nodedb took {elapsed:?} to exit after SIGTERM — budget is 1s (1100ms with slack)"
    );
}
