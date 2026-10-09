// SPDX-License-Identifier: BUSL-1.1

//! The membership pass over this node's chunked redo streams.
//!
//! - A data group this node no longer mounts gets no later entry here, so
//!   no final entry, abandon, or later term closes its streams. They drop
//!   here, with their WAL floor holds. The group then owes a snapshot
//!   install before it applies here again. A remount resumes its old log,
//!   which can still name a dropped stream.
//! - A mounted group that owes a snapshot refuses log entries until one
//!   installs, and its leadership here moves to a replica that can lead.
//!   This covers a group that mounted again, and a group whose apply here
//!   held an entry of a dropped stream.

use std::sync::Mutex;

use nodedb_cluster::multi_raft::MultiRaft;

use crate::control::state::SharedState;

/// Run the pass once. The membership reconcile calls it on every tick.
pub fn reconcile_redo_streams(shared: &SharedState, multi_raft: &Mutex<MultiRaft>) {
    drop_unmounted(shared, multi_raft);
    require_owed_snapshots(shared, multi_raft);
}

/// Drop the streams of every data group this node no longer mounts.
///
/// The `MultiRaft` lock is held throughout. A group that mounts meanwhile
/// keeps its streams, and a remount sees the group's debt. The debt write
/// is a catalog commit under that lock. It runs only when a left group
/// still holds a stream.
fn drop_unmounted(shared: &SharedState, multi_raft: &Mutex<MultiRaft>) {
    let dropped = {
        let multi_raft = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        shared.redo_chunks.drop_unhosted_groups(
            |group_id| multi_raft.contains_group(group_id),
            shared.credentials.catalog(),
        )
    };
    match dropped {
        Ok(0) => {}
        Ok(dropped) => tracing::info!(
            dropped,
            "redo chunks: dropped the open streams of data groups this node left; each such \
             group installs a snapshot before it applies here again"
        ),
        Err(error) => {
            tracing::error!(
                %error,
                "redo chunks: the snapshot debt of data groups this node left did not \
                 persist; their streams stay until a later pass records it"
            );
            crate::diag::redo_snapshot_debt_not_recorded(&error);
        }
    }
}

/// Make every mounted group that owes a snapshot refuse log entries, and
/// move a leadership of it this node holds: a leader takes no snapshot.
///
/// The debt is read under the `MultiRaft` lock. An install settles the debt
/// before it refreshes the group's requirement under the same lock, so this
/// pass never raises a requirement an install has paid.
fn require_owed_snapshots(shared: &SharedState, multi_raft: &Mutex<MultiRaft>) {
    let owed = shared.redo_chunks.owed_groups();
    if owed.is_empty() {
        return;
    }
    let mut multi_raft = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
    for group_id in owed {
        if !multi_raft.contains_group(group_id) || !shared.redo_chunks.owes_snapshot(group_id) {
            continue;
        }
        if !multi_raft.snapshot_required(group_id) {
            multi_raft.set_snapshot_required(group_id, true);
            tracing::warn!(
                group_id,
                "redo chunks: the group's replica here owes a snapshot install; it refuses \
                 log entries until one installs"
            );
        }
        if let Some(target) = multi_raft.hand_off_leadership(group_id) {
            tracing::info!(
                group_id,
                target,
                "redo chunks: the group's replica here owes a snapshot install; its \
                 leadership moves to a replica that can lead"
            );
        }
    }
}
