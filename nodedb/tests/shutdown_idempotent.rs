//! D-δ integration test 3: double SIGTERM is idempotent.
//!
//! Send two SIGTERM signals in quick succession. Assert: exit code == 0,
//! no panic, no double-free. Uses real binary.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().expect("local_addr").port()
}

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
fn double_sigterm_is_idempotent_no_panic() {
    let bin = env!("CARGO_BIN_EXE_nodedb");
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
    assert!(ready, "nodedb did not become ready within 15s");

    // Send two SIGTERMs in very quick succession.
    #[cfg(unix)]
    {
        unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
        std::thread::sleep(Duration::from_millis(50));
        unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    }
    #[cfg(not(unix))]
    {
        child.kill().expect("kill");
    }

    // Must exit cleanly within 3s (generous for double-signal test).
    let deadline = Instant::now() + Duration::from_secs(3);
    let status = loop {
        match child.try_wait().expect("try_wait") {
            Some(s) => break s,
            None => {
                if Instant::now() >= deadline {
                    child.kill().ok();
                    panic!("nodedb did not exit within 3s after double SIGTERM");
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    };

    assert!(
        status.success() || status.code() == Some(0),
        "nodedb exited with status {status:?} after double SIGTERM — expected 0"
    );
}
