// SPDX-License-Identifier: BUSL-1.1

//! Real process-kill crash-recovery harness.
//!
//! Spawns the actual `nodedb` binary as a child process, drives it over
//! pgwire, then `kill -9`s it and reopens the same data directory to
//! exercise WAL replay through the normal binary boot path. Distinct from
//! the in-process `nodedb-test-support` harnesses, which cannot simulate a
//! real process crash since there is no separate process to kill.

#![allow(dead_code)] // Not every crash-test binary uses every helper.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

// `pub` so a crash test can read faultbox reports directly, not just via
// the panic-path diagnostics wired into `pgwire.rs`.
pub mod diagnostics;
// `wait_ready` and its bind-collision respawn.
mod boot;
// Boot sections and numeric fields read back from the server log.
pub mod log_fields;
// WAL segment names and checkpoint truncation lines.
pub mod wal_truncation;
// The ILP client helper lives in `nodedb-test-support` and is imported
// directly by tests that need it, not re-exported here.
mod pgwire;

// Only `crash_ilp_timeseries_write.rs` uses these directly.
#[allow(unused_imports)]
pub use pgwire::{RetryableSchemaChange, Session};
#[path = "../support/mod.rs"]
pub mod support;

/// Re-exported so a crash test can state its own filesystem precondition
/// without pulling the support module in a second time.
#[allow(unused_imports)]
pub use support::direct_io::direct_io_supported;

/// Boot budget for a server in a default-budget test. Stays well under
/// nextest's kill (`slow-timeout` 30s x 4 = 120s), since a test may boot twice
/// and still do work inside it.
pub const BOOT_READY_TIMEOUT: Duration = Duration::from_secs(45);

/// Boot budget for tests whose nextest override raises the kill to 240s
/// (`terminate-after = 8`). They run serially and boot under a loaded machine.
pub const BOOT_READY_TIMEOUT_EXTENDED: Duration = Duration::from_secs(150);

pub fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().expect("local_addr").port()
}

pub fn check_healthz(port: u16) -> bool {
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

/// Raw `/healthz` response — status line, headers, and body.
///
/// Sibling to [`check_healthz`], which caps its read at 256 bytes and can
/// truncate a body. The request sends `Connection: close`, so this reads to
/// EOF instead of racing a fixed buffer against body length.
pub fn fetch_healthz(port: u16) -> Option<String> {
    let addr = format!("127.0.0.1:{port}");
    let mut stream =
        TcpStream::connect_timeout(&addr.parse().expect("addr"), Duration::from_millis(200))
            .ok()?;
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let req = b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(req).ok()?;
    let mut body = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => body.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    Some(String::from_utf8_lossy(&body).into_owned())
}

pub fn wait_for_healthz(port: u16, timeout: Duration) -> bool {
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

/// Owns a real `nodedb` child process plus the temp data directory it was
/// started against, so a test can crash it with `kill -9` and reopen the
/// same data directory to exercise WAL replay.
pub struct CrashHarness {
    bin: &'static str,
    /// `None` only in the instant between `Drop` taking it out to decide
    /// whether to retain it (see the `Drop` impl) — always `Some` otherwise.
    tempdir: Option<tempfile::TempDir>,
    /// Cached from `tempdir.path()` at construction so callers don't need to
    /// unwrap the `Option` above for the common case of just reading the path.
    data_dir_path: std::path::PathBuf,
    /// Number of times `spawn()` has been called on this harness, used to
    /// mark each boot's lines in the server log unambiguously.
    boot_count: u32,
    pub http_port: u16,
    pub pgwire_port: u16,
    pub native_port: u16,
    /// Unique per harness — every protocol bind is boot-fatal, so two
    /// concurrent harnesses on the default sync port would collide.
    pub sync_port: u16,
    /// Always allocated, like the other ports, to avoid a bind collision.
    pub resp_port: u16,
    /// Always exported to the spawned process so `reopen` reuses the same
    /// port a pre-crash ILP connection was made against.
    pub ilp_port: u16,
    child: Option<std::process::Child>,
    /// Applied on every spawn, including `reopen`, so the restarted process
    /// boots under the same tuning as the one it killed.
    extra_env: Vec<(String, String)>,
    /// `NODEDB_WAL_DIRECT_IO` forced on every spawn, or `None` for the
    /// shipped default. Reused on `reopen` so recovery runs the same WAL
    /// mode as the crash half.
    wal_direct_io: Option<&'static str>,
}

impl CrashHarness {
    pub fn new() -> CrashHarness {
        let tempdir = tempfile::tempdir().expect("tempdir");
        Self::from_tempdir(tempdir)
    }

    /// Like [`CrashHarness::new`], but the data directory is created under
    /// `parent` — used to place it on a filesystem chosen by the test rather
    /// than on whatever `TMPDIR` points at.
    pub fn new_in(parent: &std::path::Path) -> CrashHarness {
        let tempdir = tempfile::tempdir_in(parent).expect("tempdir in parent");
        Self::from_tempdir(tempdir)
    }

    fn from_tempdir(tempdir: tempfile::TempDir) -> CrashHarness {
        let data_dir_path = tempdir.path().to_path_buf();
        // Probed once, against the directory the server will actually write to.
        let wal_direct_io = support::direct_io::wal_direct_io_override(&data_dir_path);
        CrashHarness {
            bin: env!("CARGO_BIN_EXE_nodedb"),
            tempdir: Some(tempdir),
            data_dir_path,
            boot_count: 0,
            http_port: free_port(),
            pgwire_port: free_port(),
            native_port: free_port(),
            sync_port: free_port(),
            resp_port: free_port(),
            ilp_port: free_port(),
            child: None,
            extra_env: Vec::new(),
            wal_direct_io,
        }
    }

    /// Demand direct I/O even if the probe says the filesystem cannot provide
    /// it — for the two tests whose subject is the direct-I/O path itself.
    pub fn with_direct_io_wal(mut self) -> CrashHarness {
        self.wal_direct_io = Some("true");
        self
    }

    /// Force the WAL open buffered instead of using direct I/O — only for a
    /// test whose subject is buffered I/O itself.
    pub fn with_buffered_wal(mut self) -> CrashHarness {
        self.wal_direct_io = Some("false");
        self
    }

    /// Add a server env override applied on every spawn. Call before `spawn`.
    pub fn with_env(mut self, key: &str, value: &str) -> CrashHarness {
        self.extra_env.push((key.to_string(), value.to_string()));
        self
    }

    /// Boot without the single-node Calvin stack. The node runs no Raft
    /// proposer, so every autocommit write takes the local funnel route, and
    /// two writes can apply out of LSN order. Writes a config file into the
    /// data directory and points `NODEDB_CONFIG` at it. Call before `spawn`.
    pub fn standalone(self) -> CrashHarness {
        let config = self.data_dir_path.join("standalone.toml");
        std::fs::write(&config, "[server]\nsingle_node_calvin = false\n")
            .expect("write the standalone config file");
        let path = config.to_string_lossy().into_owned();
        self.with_env("NODEDB_CONFIG", &path)
    }

    /// Set (or replace) a server env override in place, between spawns.
    /// Unlike [`CrashHarness::with_env`], this lets a crash-during-recovery
    /// test arm `NODEDB_FAILPOINTS` for exactly one boot — left armed, every
    /// later `reopen` would abort at the same point.
    pub fn set_env(&mut self, key: &str, value: &str) {
        match self.extra_env.iter_mut().find(|(k, _)| k == key) {
            Some(slot) => slot.1 = value.to_string(),
            None => self.extra_env.push((key.to_string(), value.to_string())),
        }
    }

    /// Drop a previously-set env override so subsequent spawns boot without
    /// it, leaving the variable unset rather than falling back to an
    /// inherited value.
    pub fn clear_env(&mut self, key: &str) {
        self.extra_env.retain(|(k, _)| k != key);
    }

    /// The data directory this server was started against.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir_path
    }

    /// A note appended to diagnostic panics stating where the data
    /// directory is retained, or empty when `NODEDB_TEST_KEEP_DATA_DIR` is
    /// unset. See [`diagnostics::keep_data_dir_note`].
    pub(crate) fn keep_data_dir_note(&self) -> String {
        diagnostics::keep_data_dir_note(&self.data_dir_path)
    }

    /// File names of the WAL segments currently on disk, sorted. Reads the
    /// directory directly so a truncation test can confirm a file was
    /// actually unlinked, not just what the server reports.
    pub fn wal_segments(&self) -> Vec<String> {
        let dir = self.data_dir().join("wal");
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            // The WAL directory not existing yet is a legitimate "no segments"
            // answer during startup, not a test failure.
            Err(_) => return Vec::new(),
        };
        let mut names: Vec<String> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.ends_with(".seg"))
            .collect();
        names.sort();
        names
    }

    /// The WAL segment the server appends to now. Segment names carry their
    /// zero-padded first LSN, so the last name is the active segment.
    pub fn active_wal_segment(&self) -> String {
        self.wal_segments()
            .last()
            .cloned()
            .unwrap_or_else(|| panic!("no WAL segment on disk after an acknowledged write"))
    }

    /// Path the server's stdout/stderr is appended to across every spawn.
    pub fn server_log_path(&self) -> std::path::PathBuf {
        self.data_dir_path.join("server.log")
    }

    /// The server output captured so far, or empty if nothing was written.
    pub fn server_log(&self) -> String {
        std::fs::read_to_string(self.server_log_path()).unwrap_or_default()
    }

    pub fn spawn(&mut self) {
        let mut cmd = std::process::Command::new(self.bin);
        for (k, v) in &self.extra_env {
            cmd.env(k, v);
        }
        // Capture server output so a crash-test failure has something to debug.
        // Appended, not truncated, so a `reopen` keeps the pre-crash half.
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.server_log_path())
            .expect("open server log");
        let log_err = log.try_clone().expect("clone server log handle");
        // Left unset in the common case so the child boots the same WAL mode a
        // deployment does; set only where the probe found no direct-I/O support
        // or a test asked for a specific mode.
        if let Some(value) = self.wal_direct_io {
            cmd.env("NODEDB_WAL_DIRECT_IO", value);
        }
        let child = cmd
            .env("NODEDB_DATA_DIR", &self.data_dir_path)
            .env("NODEDB_DATA_PLANE_CORES", "1")
            .env("NODEDB_PORT_HTTP", self.http_port.to_string())
            .env("NODEDB_PORT_PGWIRE", self.pgwire_port.to_string())
            .env("NODEDB_PORT_NATIVE", self.native_port.to_string())
            .env("NODEDB_PORT_SYNC", self.sync_port.to_string())
            .env("NODEDB_PORT_RESP", self.resp_port.to_string())
            // Unset by default (`config/server/env/host_ports.rs`), which
            // leaves the ILP listener disabled — a test that drives ILP must
            // set this to enable it, same as a real deployment opting in.
            .env("NODEDB_PORT_ILP", self.ilp_port.to_string())
            // Pin the superuser password; otherwise the binary auto-generates
            // one into `<data_dir>/.superuser_password` and the test can't auth.
            .env("NODEDB_SUPERUSER_PASSWORD", "nodedb")
            // A test that needs server diagnostics overrides this via
            // `with_env`, so it is set only when the test did not ask for
            // something else.
            .env(
                "RUST_LOG",
                self.extra_env
                    .iter()
                    .find(|(k, _)| k == "RUST_LOG")
                    .map(|(_, v)| v.as_str())
                    .unwrap_or("error"),
            )
            .stdout(std::process::Stdio::from(log))
            .stderr(std::process::Stdio::from(log_err))
            .spawn()
            .expect("failed to spawn nodedb binary");
        self.boot_count += 1;
        // Mark which boot these log lines belong to — the log accumulates
        // across spawns in one file, so a tail dump is otherwise ambiguous.
        diagnostics::mark_boot(&self.server_log_path(), self.boot_count, child.id());
        self.child = Some(child);
    }

    /// Spawn the server and assert that boot fails-stop rather than coming up.
    /// The server must never report `/healthz`-ready and must exit non-zero
    /// within `timeout`. Panics otherwise.
    pub fn spawn_expect_boot_failure(&mut self, timeout: Duration) {
        self.spawn();
        let deadline = Instant::now() + timeout;
        loop {
            // A fail-stopped boot must never open the gateway / report ready.
            assert!(
                !check_healthz(self.http_port),
                "server became ready despite a boot condition it must fail-stop on"
            );
            if let Some(child) = self.child.as_mut() {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        assert!(
                            !status.success(),
                            "server exited cleanly (0) on a boot condition it must fail-stop on; \
                             expected a non-zero exit (status: {status:?})"
                        );
                        return;
                    }
                    Ok(None) => {}
                    Err(e) => panic!("failed to poll server process: {e}"),
                }
            }
            assert!(
                Instant::now() < deadline,
                "server neither became ready nor exited within {timeout:?}; \
                 fail-stop boot-abort did not occur"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn pgwire_conn_str(&self) -> String {
        format!(
            "host=127.0.0.1 port={} dbname=default user=nodedb password=nodedb",
            self.pgwire_port
        )
    }

    /// Simulate a hard crash: `kill -9` with no graceful shutdown, no extra
    /// flush, then reap the zombie so the OS releases the process's ports.
    pub fn kill_9(&mut self) {
        let mut child = match self.child.take() {
            Some(c) => c,
            None => return,
        };
        #[cfg(unix)]
        unsafe {
            libc::kill(child.id() as i32, libc::SIGKILL);
        }
        #[cfg(not(unix))]
        {
            let _ = child.kill();
        }
        let _ = child.wait();
    }

    /// Wait for the server to die on its own and reap it, used with an armed
    /// `NODEDB_FAILPOINTS` abort. A timeout means the injection never fired.
    pub fn await_self_crash(&mut self, timeout: Duration) {
        let mut child = match self.child.take() {
            Some(c) => c,
            None => panic!("no server process to wait on"),
        };
        let deadline = Instant::now() + timeout;
        loop {
            match child.try_wait().expect("try_wait on server") {
                Some(_status) => return,
                None if Instant::now() >= deadline => {
                    #[cfg(unix)]
                    unsafe {
                        libc::kill(child.id() as i32, libc::SIGKILL);
                    }
                    let _ = child.wait();
                    let log = self.server_log();
                    let lines: Vec<&str> = log.lines().collect();
                    // Head shows whether the subsystem under test came up; tail
                    // shows what it was doing when the wait expired.
                    let budget = diagnostics::tail_line_count();
                    let half = budget / 2;
                    let excerpt = if lines.len() <= budget {
                        lines.join("\n")
                    } else {
                        format!(
                            "{}\n… {} lines elided …\n{}",
                            lines[..half].join("\n"),
                            lines.len() - 2 * half,
                            lines[lines.len() - half..].join("\n")
                        )
                    };
                    panic!(
                        "server was still alive after {timeout:?} — the injected fail point never \
                         fired, so this test proves NOTHING about crashing at that point.\n\
                         {}Server output ({} lines):\n{excerpt}",
                        self.keep_data_dir_note(),
                        lines.len()
                    );
                }
                None => std::thread::sleep(Duration::from_millis(100)),
            }
        }
    }

    /// Spawn a fresh process on the same data directory (WAL replay on
    /// boot) and wait for it to become ready.
    pub fn reopen(&mut self) {
        self.spawn();
        self.wait_ready();
    }
}

impl Default for CrashHarness {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for CrashHarness {
    fn drop(&mut self) {
        // Kill and reap before tempdir drops, so no orphan process runs
        // against a deleted data directory.
        if self.child.is_some() {
            self.kill_9();
        }
        // `tempdir` is always `Some` here except during this very drop, so
        // `take()` always succeeds.
        if let Some(dir) = self.tempdir.take()
            && diagnostics::keep_data_dir_requested()
        {
            // `keep()` consumes the guard without deleting the directory, so
            // the retained data survives past this drop.
            let kept = dir.keep();
            eprintln!(
                "\n\n=== NODEDB_TEST_KEEP_DATA_DIR: data directory retained at {} ===\n\n",
                kept.display()
            );
        }
        // else: `dir` drops here and removes the directory, same as today's
        // default behavior.
    }
}
