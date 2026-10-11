// SPDX-License-Identifier: BUSL-1.1

//! Failpoints that hold a rebuild thread, so a test can write while a
//! rebuild runs.
//!
//! The hold runs on the rebuild's own OS thread, never on a core. Without
//! the `failpoints` feature it compiles to nothing.

use crate::fail_point::FailScope;

/// Holds a full-text rebuild thread before it reads its snapshot.
pub const FTS_BUILD_HOLD: &str = "reindex::fts_build_hold";

/// Holds a CSR rebuild thread before it compacts its snapshot.
pub const CSR_BUILD_HOLD: &str = "reindex::csr_build_hold";

/// Hold the calling rebuild thread at failpoint `name`, evaluated in
/// `scope`, the scope of the core that started the rebuild.
///
/// `WaitForFile(path)` parks the thread until `path` exists, for at most
/// two minutes. Every other action runs as at any failpoint.
pub(super) fn hold_build(scope: FailScope, name: &str) {
    #[cfg(feature = "failpoints")]
    hold_armed(scope, name);
    #[cfg(not(feature = "failpoints"))]
    let _ = (scope, name);
}

#[cfg(feature = "failpoints")]
fn hold_armed(scope: FailScope, name: &str) {
    use std::time::{Duration, Instant};

    use crate::fail_point::{FailAction, eval, lookup};

    const HOLD_LIMIT: Duration = Duration::from_secs(120);
    const POLL: Duration = Duration::from_millis(5);

    match lookup(scope, name) {
        Some(FailAction::WaitForFile(path)) => {
            let deadline = Instant::now() + HOLD_LIMIT;
            while !path.exists() && Instant::now() < deadline {
                std::thread::sleep(POLL);
            }
        }
        Some(_) => eval(scope, name),
        None => {}
    }
}
