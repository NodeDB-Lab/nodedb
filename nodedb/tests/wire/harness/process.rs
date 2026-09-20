// SPDX-License-Identifier: BUSL-1.1

//! Spawns the real `nodedb` binary as a subprocess against a data directory,
//! waits for `/healthz`, captures its stdout/stderr to a log file, and tears
//! it down with `SIGTERM` (falling back to `SIGKILL`) — mirroring
//! `tests/crash_harness/mod.rs`, the proven in-repo subprocess pattern.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use super::config_toml::{self, AuthMode, TuningOverrides};

/// Bind an ephemeral port then release it for a child process to bind
/// itself. Mirrors `crash_harness::free_port`, which accepts the same race.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener
        .local_addr()
        .expect("ephemeral port local_addr")
        .port()
}

/// Ports a spawned server binds to, allocated once before each spawn.
pub(super) struct ServerPorts {
    pub(super) http: u16,
    pub(super) pgwire: u16,
    pub(super) native: u16,
    pub(super) sync: u16,
    pub(super) resp: u16,
}

impl ServerPorts {
    fn allocate() -> Self {
        Self {
            http: free_port(),
            pgwire: free_port(),
            native: free_port(),
            sync: free_port(),
            resp: free_port(),
        }
    }
}

/// Probe `/healthz` once. It returns 503 until the gateway is enabled, so
/// polling for HTTP 200 is a correct readiness wait.
fn check_healthz(port: u16) -> bool {
    let addr = format!("127.0.0.1:{port}");
    let mut stream = match TcpStream::connect_timeout(
        &addr.parse().expect("parse loopback addr"),
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

impl SpawnedServer {
    /// Poll `/healthz` until the server answers 200, exits, or times out.
    /// The error says which.
    fn wait_until_ready(&mut self, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        loop {
            // A dead server never answers; waiting out the timeout would hide
            // the real error.
            if let Some(child) = self.child.as_mut()
                && let Ok(Some(status)) = child.try_wait()
            {
                return Err(format!("nodedb exited during startup with {status}"));
            }
            if check_healthz(self.ports.http) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "nodedb did not answer /healthz within {}s",
                    timeout.as_secs()
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// The server's captured output, or why it could not be read.
    fn read_log(&self) -> String {
        std::fs::read_to_string(&self.log_path)
            .unwrap_or_else(|e| format!("(could not read {}: {e})", self.log_path.display()))
    }
}

/// A spawned `nodedb` server process plus the ports it is listening on.
pub(super) struct SpawnedServer {
    pub(super) ports: ServerPorts,
    log_path: PathBuf,
    /// `None` once `graceful_shutdown` has taken and reaped the child.
    child: Option<Child>,
}

/// Spawn the real `nodedb` binary against `data_dir` and block until
/// `/healthz` reports ready. Retries with fresh ports when the server exits
/// during startup, since `free_port` can lose the bind race.
pub(super) fn spawn(
    data_dir: &Path,
    auth_mode: AuthMode,
    tuning: TuningOverrides,
    cores: usize,
) -> SpawnedServer {
    spawn_with_failpoints(data_dir, auth_mode, tuning, cores, None)
}

/// Spawn with `NODEDB_FAILPOINTS` set from `failpoints` (see
/// `nodedb_types::fail_point::FAILPOINTS_ENV`) — the only way a test reaches
/// into a spawned server process's fail points, since the in-process `set`
/// API cannot reach a process the test only supervises. A binary built
/// without the `failpoints` Cargo feature ignores the variable entirely.
pub(super) fn spawn_with_failpoints(
    data_dir: &Path,
    auth_mode: AuthMode,
    tuning: TuningOverrides,
    cores: usize,
    failpoints: Option<&str>,
) -> SpawnedServer {
    let config_path = config_toml::write_config(data_dir, auth_mode, tuning);
    let mut last_failure = String::new();
    for _ in 0..START_ATTEMPTS {
        match try_spawn(data_dir, &config_path, cores, failpoints) {
            Ok(server) => return server,
            Err((reason, log)) => last_failure = format!("{reason}\n--- server log ---\n{log}"),
        }
    }
    panic!("nodedb did not start in {START_ATTEMPTS} attempts.\n{last_failure}");
}

/// How many times [`spawn`] will re-allocate ports and try again.
const START_ATTEMPTS: u32 = 3;

/// One spawn attempt. `Err` carries the reason and the server's own log.
fn try_spawn(
    data_dir: &Path,
    config_path: &Path,
    cores: usize,
    failpoints: Option<&str>,
) -> Result<SpawnedServer, (String, String)> {
    let ports = ServerPorts::allocate();

    let log_path = data_dir.join("server.log");
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .expect("open server log");
    let log_err = log.try_clone().expect("clone server log handle");

    let mut command = Command::new(env!("CARGO_BIN_EXE_nodedb"));
    if let Some(spec) = failpoints {
        // Mirrors `nodedb_types::fail_point::FAILPOINTS_ENV`, not imported
        // directly: that constant only exists under the `failpoints`
        // Cargo feature, and this harness must still build without it.
        command.env("NODEDB_FAILPOINTS", spec);
    }
    let child = command
        .env("NODEDB_DATA_DIR", data_dir)
        .env("NODEDB_DATA_PLANE_CORES", cores.to_string())
        .env("NODEDB_CONFIG", config_path)
        .env("NODEDB_PORT_HTTP", ports.http.to_string())
        .env("NODEDB_PORT_PGWIRE", ports.pgwire.to_string())
        .env("NODEDB_PORT_NATIVE", ports.native.to_string())
        .env("NODEDB_PORT_SYNC", ports.sync.to_string())
        .env("NODEDB_PORT_RESP", ports.resp.to_string())
        // Pinned so the harness client can authenticate in password mode;
        // trust mode ignores it. Same value on every spawn (including a
        // restart against the same data directory).
        .env("NODEDB_SUPERUSER_PASSWORD", "nodedb")
        // `NODEDB_TEST_RUST_LOG` overrides the spawned server's `RUST_LOG`,
        // defaulting to `info`. Set it (e.g. `debug`) to see finer server
        // logs when a test needs them dumped via `NODEDB_TEST_DUMP_SERVER_LOG`.
        .env(
            "RUST_LOG",
            std::env::var("NODEDB_TEST_RUST_LOG").unwrap_or_else(|_| "info".to_string()),
        )
        .stdout(std::process::Stdio::from(log))
        .stderr(std::process::Stdio::from(log_err))
        .spawn()
        .expect("failed to spawn nodedb binary");

    let mut server = SpawnedServer {
        ports,
        log_path: log_path.clone(),
        child: Some(child),
    };

    if let Err(reason) = server.wait_until_ready(READY_TIMEOUT) {
        // The temp dir goes away when the caller panics, taking the log with
        // it. Read it now. Dropping `server` reaps the child.
        return Err((reason, server.read_log()));
    }
    Ok(server)
}

/// Budget for `/healthz` to answer. Generous: the suite runs servers in
/// parallel, so a cold boot competes for CPU.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

impl SpawnedServer {
    /// Send `SIGTERM` and wait for exit — the signal handler runs graceful
    /// shutdown (syncs the WAL). Falls back to `SIGKILL` on timeout.
    pub(super) async fn graceful_shutdown(mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        #[cfg(unix)]
        unsafe {
            libc::kill(child.id() as i32, libc::SIGTERM);
        }
        #[cfg(not(unix))]
        {
            let _ = child.kill();
        }

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    eprintln!(
                        "nodedb did not exit within 20s of SIGTERM; force-killed. See {}",
                        self.log_path.display()
                    );
                    return;
                }
                Ok(None) => tokio::time::sleep(Duration::from_millis(50)).await,
                Err(_) => return,
            }
        }
    }
}

impl Drop for SpawnedServer {
    fn drop(&mut self) {
        // `child` is `None` when `graceful_shutdown` already ran.
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        // The data directory is removed right after this drop, so a failing
        // test's server log survives only when it is copied out here.
        // `NODEDB_TEST_DUMP_SERVER_LOG` (any value) turns this dump on;
        // `read_log` never panics — a missing/unreadable log file renders as
        // an inline placeholder string instead.
        if std::env::var_os("NODEDB_TEST_DUMP_SERVER_LOG").is_some() {
            eprintln!(
                "--- server log {} ---\n{}",
                self.log_path.display(),
                self.read_log()
            );
        }
    }
}
