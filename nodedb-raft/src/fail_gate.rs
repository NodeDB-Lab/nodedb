// SPDX-License-Identifier: BUSL-1.1

//! Fail gates for Raft tests.
//!
//! Compiled only with the `failpoints` feature. A gate answers without
//! waiting: Raft steps are synchronous, so a gate holds a step back and the
//! driver asks again on its next step.

use nodedb_types::fail_point::{FailAction, lookup};

/// Whether the commit gate of `group_id` on `node_id` holds now. Named
/// `raft::hold_commit::node<N>::group<G>`, armed with `wait_file(<path>)`.
/// It holds while the file is absent.
///
/// A leader under the gate counts no quorum, so it commits no new entry.
/// The entries it appends still replicate. A test keeps an entry
/// uncommitted this way, then moves the leadership: the entry commits only
/// in the next term. On its first hold the gate creates `<path>.parked`.
pub(crate) fn holds_commit(node_id: u64, group_id: u64) -> bool {
    let name = format!("raft::hold_commit::node{node_id}::group{group_id}");
    let Some(FailAction::WaitForFile(path)) = lookup(&name) else {
        return false;
    };
    if path.exists() {
        return false;
    }
    let mut parked = path.into_os_string();
    parked.push(".parked");
    let parked = std::path::PathBuf::from(parked);
    if !parked.exists()
        && let Err(e) = std::fs::write(&parked, &name)
    {
        tracing::warn!(gate = %name, error = %e, "fail gate parked marker not created");
    }
    true
}
