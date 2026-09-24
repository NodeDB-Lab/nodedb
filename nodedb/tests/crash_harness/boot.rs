// SPDX-License-Identifier: BUSL-1.1

//! Waiting for a spawned server to report ready.
//!
//! `free_port` releases each port before the child binds it. Another process
//! can take the port in between, and every protocol bind is boot-fatal. The
//! wait therefore watches the child as well as `/healthz`. A child that exits
//! on a bind collision is respawned on fresh ports. Any other exit, or a boot
//! that outlives its budget, panics with this boot's server output.

use std::time::{Duration, Instant};

use super::{BOOT_READY_TIMEOUT, BOOT_READY_TIMEOUT_EXTENDED, CrashHarness, check_healthz};
use super::{diagnostics, free_port};

/// Spawns one boot can use before a bind collision becomes a panic.
/// Each one allocates six fresh ports, so repeated collisions are unlikely.
const BIND_COLLISION_ATTEMPTS: u32 = 3;

/// The OS error text every listener bind failure carries in the boot error.
const ADDRESS_IN_USE: &str = "Address already in use";

/// How one wait for a boot ended.
enum BootEnd {
    Ready,
    Exited(std::process::ExitStatus),
    TimedOut,
}

impl CrashHarness {
    /// Block until `/healthz` reports ready.
    ///
    /// Panics with the server output when the child exits or the budget
    /// runs out.
    pub fn wait_ready(&mut self) {
        self.wait_ready_within(BOOT_READY_TIMEOUT);
    }

    /// [`CrashHarness::wait_ready`] for a test whose nextest kill budget is raised.
    pub fn wait_ready_extended(&mut self) {
        self.wait_ready_within(BOOT_READY_TIMEOUT_EXTENDED);
    }

    fn wait_ready_within(&mut self, budget: Duration) {
        let mut attempt = 1;
        loop {
            match self.poll_boot(budget) {
                BootEnd::Ready => return,
                BootEnd::Exited(status) => {
                    let boot_log = self.current_boot_log();
                    if boot_log.contains(ADDRESS_IN_USE) && attempt < BIND_COLLISION_ATTEMPTS {
                        attempt += 1;
                        self.reallocate_ports();
                        self.spawn();
                        continue;
                    }
                    panic!(
                        "nodedb exited during boot {} with {status} before reporting ready.\n{}{}{}",
                        self.boot_count,
                        self.keep_data_dir_note(),
                        diagnostics::faultbox_report_section(self.data_dir()),
                        diagnostics::log_tail_section(&self.server_log()),
                    );
                }
                BootEnd::TimedOut => panic!(
                    "nodedb did not become ready within {budget:?} on boot {} and is still running.\n{}{}{}",
                    self.boot_count,
                    self.keep_data_dir_note(),
                    diagnostics::faultbox_report_section(self.data_dir()),
                    diagnostics::log_tail_section(&self.server_log()),
                ),
            }
        }
    }

    /// Poll the child and `/healthz` until one of them settles the boot.
    fn poll_boot(&mut self, budget: Duration) -> BootEnd {
        let deadline = Instant::now() + budget;
        loop {
            // A dead child never answers, so waiting out the budget hides its error.
            if let Some(child) = self.child.as_mut() {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        self.child = None;
                        return BootEnd::Exited(status);
                    }
                    Ok(None) => {}
                    Err(e) => panic!("failed to poll the nodedb child process: {e}"),
                }
            }
            if check_healthz(self.http_port) {
                return BootEnd::Ready;
            }
            if Instant::now() >= deadline {
                return BootEnd::TimedOut;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Server output written since the marker of the latest boot.
    fn current_boot_log(&self) -> String {
        let log = self.server_log();
        let marker = format!("=== crash harness boot {} (pid", self.boot_count);
        match log.rfind(&marker) {
            Some(start) => log[start..].to_string(),
            None => log,
        }
    }

    /// Give every protocol a fresh port for the next spawn.
    fn reallocate_ports(&mut self) {
        self.http_port = free_port();
        self.pgwire_port = free_port();
        self.native_port = free_port();
        self.sync_port = free_port();
        self.resp_port = free_port();
        self.ilp_port = free_port();
    }
}
