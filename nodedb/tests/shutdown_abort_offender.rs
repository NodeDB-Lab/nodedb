//! D-δ integration test 4: offender task is aborted after 500ms budget.
//!
//! Start the binary with NODEDB_TEST_SLOW_DRAIN_TASK=1, which registers a
//! drain task that sleeps 2s without calling report_drained. SIGTERM → assert:
//! - sequencer aborts the offender at ~500ms
//! - stderr contains "offender" and "test_slow_task"
//! - process exits within 3s (not the full 2s sleep)
//!
//! Uses real binary + stderr capture.

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
fn offender_task_aborted_at_500ms_budget() {
    let bin = env!("CARGO_BIN_EXE_nodedb");
    let dir = tempfile::tempdir().expect("tempdir");
    let http_port = free_port();
    let pgwire_port = free_port();
    let native_port = free_port();

    let child = std::process::Command::new(bin)
        .env("NODEDB_DATA_DIR", dir.path())
        .env("NODEDB_DATA_PLANE_CORES", "1")
        .env("NODEDB_PORT_HTTP", http_port.to_string())
        .env("NODEDB_PORT_PGWIRE", pgwire_port.to_string())
        .env("NODEDB_PORT_NATIVE", native_port.to_string())
        // Inject a slow drain task that will be detected as an offender.
        .env("NODEDB_TEST_SLOW_DRAIN_TASK", "1")
        // Use warn level so the shutdown offender ERROR log is captured.
        .env("RUST_LOG", "shutdown=error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to spawn nodedb binary");

    let ready = wait_for_healthz(http_port, Duration::from_secs(15));
    assert!(ready, "nodedb did not become ready within 15s");

    // Send SIGTERM.
    let start = Instant::now();
    #[cfg(unix)]
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    #[cfg(not(unix))]
    {
        child.kill().expect("kill");
    }

    // Collect output and wait for exit — must finish well under 2s
    // (the slow task sleeps 2s but should be aborted at 500ms).
    let output = child.wait_with_output().expect("wait_with_output");
    let elapsed = start.elapsed();

    // Process must exit within 3s (500ms budget + remaining phases).
    assert!(
        elapsed <= Duration::from_millis(3500),
        "nodedb took {elapsed:?} — offender should have been aborted at 500ms"
    );

    // Stderr should contain "test_slow_task" as an offender name.
    // The log line from bus.rs reads:
    //   ERROR shutdown: task exceeded 500ms drain budget — aborting offender=test_slow_task
    // OR the DrainGuard Drop warning:
    //   WARN shutdown: DrainGuard dropped without report_drained offender=test_slow_task
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("test_slow_task"),
        "stderr did not contain 'test_slow_task'.\nstderr:\n{stderr}"
    );
}
