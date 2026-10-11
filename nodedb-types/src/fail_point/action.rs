// SPDX-License-Identifier: Apache-2.0

//! What an armed fail point does when it fires.

use std::time::Duration;

/// Action a fail point performs when triggered.
#[derive(Debug, Clone)]
pub enum FailAction {
    /// Panic with a message that names the fail-point.
    Panic,
    /// Kill the whole process immediately via `abort()`.
    ///
    /// `Panic` only unwinds the current task, which a supervising runtime
    /// absorbs — no use for simulating a crash in a spawned server. This
    /// is the action a process-kill harness arms.
    Abort,
    /// Sleep for the given duration; execution then continues normally.
    Sleep(Duration),
    /// Return an error from the injected call site, carrying this detail.
    /// Ignored by bare `fail_point!` — use `fail_point_err!`.
    Fail(String),
    /// Park the call site until this file exists. Only an async call site
    /// that looks the action up with [`super::lookup`] can honour it.
    WaitForFile(std::path::PathBuf),
}
