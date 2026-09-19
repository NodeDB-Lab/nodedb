// SPDX-License-Identifier: BUSL-1.1

//! Waiting for a read index to be confirmed by a quorum.
//!
//! The coordinator lock is taken to start the probe, released, then retaken
//! for each poll. It is never held across an await: the tick loop needs the
//! same lock to send the very responses being waited on, so holding it would
//! deadlock the confirmation it is waiting for.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nodedb_raft::ReadIndexStatus;

use crate::error::ClusterError;
use crate::multi_raft::MultiRaft;

/// How often to re-check a probe. Below the 10ms tick interval, so a
/// confirmation is noticed in the tick it arrives in.
const POLL_INTERVAL: Duration = Duration::from_millis(2);

/// Confirm this node still leads `group_id`, and return the index the read
/// may be served at.
///
/// Refuses rather than waiting out `timeout` once leadership is known to be
/// lost — a deposed leader can never confirm, and the caller retries against
/// the new one.
pub async fn confirm_read_index(
    multi_raft: &Arc<Mutex<MultiRaft>>,
    group_id: u64,
    timeout: Duration,
) -> Result<u64, ClusterError> {
    let probe = {
        let mut mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(index) = mr.leader_lease_index(group_id) {
            // The quorum window is still open: a majority acknowledged this
            // leader within an election timeout, so no successor can have
            // moved the log past this commit index. The read is linearizable
            // without the round-trip the probe below would pay for.
            return Ok(index);
        }
        mr.start_read_index(group_id)
            .ok_or(ClusterError::ReadIndexNotLeader { group_id })?
    };

    let started = Instant::now();
    loop {
        let status = {
            let mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            mr.read_index_status(group_id, &probe)
        };
        match status {
            ReadIndexStatus::Confirmed => return Ok(probe.read_index),
            ReadIndexStatus::LeadershipLost => {
                return Err(ClusterError::ReadIndexNotLeader { group_id });
            }
            ReadIndexStatus::Pending => {}
        }
        if started.elapsed() >= timeout {
            return Err(ClusterError::ReadIndexTimeout {
                group_id,
                waited_ms: started.elapsed().as_millis() as u64,
            });
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::routing::RoutingTable;

    fn coordinator(dir: &tempfile::TempDir) -> Arc<Mutex<MultiRaft>> {
        Arc::new(Mutex::new(MultiRaft::new(
            1,
            RoutingTable::uniform(1, &[1, 2, 3], 3),
            PathBuf::from(dir.path()),
        )))
    }

    #[tokio::test]
    async fn a_group_this_node_does_not_host_is_refused_immediately() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mr = coordinator(&dir);

        let started = Instant::now();
        let err = confirm_read_index(&mr, 7, Duration::from_secs(30))
            .await
            .expect_err("an unhosted group cannot confirm a read");

        assert!(
            matches!(err, ClusterError::ReadIndexNotLeader { group_id: 7 }),
            "got {err}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "refusal must not wait out the timeout"
        );
    }

    #[tokio::test]
    async fn a_follower_is_refused_rather_than_waited_on() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mr = coordinator(&dir);
        mr.lock()
            .expect("lock")
            .add_group(7, vec![1, 2, 3])
            .expect("add group");

        let err = confirm_read_index(&mr, 7, Duration::from_secs(30))
            .await
            .expect_err("a follower cannot confirm a read");

        assert!(
            matches!(err, ClusterError::ReadIndexNotLeader { group_id: 7 }),
            "got {err}"
        );
    }
}
