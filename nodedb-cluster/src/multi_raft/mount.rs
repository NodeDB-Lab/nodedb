// SPDX-License-Identifier: BUSL-1.1

//! Mounting a Raft group: the disk work apart from the `MultiRaft` lock.
//!
//! A mount opens the group's redb log and reloads its hard state and log.
//! That is disk work, so a mount on a running node takes three steps:
//! 1. [`MultiRaft::mount_spec`] under the lock copies what the group needs;
//! 2. [`GroupMountSpec::open`] off the async threads and without the lock
//!    opens and restores the group;
//! 3. [`MultiRaft::insert_opened`] under the lock mounts it.
//!
//! Boot and tests mount through [`MultiRaft::add_group`], which runs the
//! three steps in a row.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use nodedb_raft::RaftNode;
use nodedb_raft::node::RaftConfig;
use tracing::info;

use crate::error::{ClusterError, Result};
use crate::group_disk::StagedLogStorage;

use super::core::{MultiRaft, seed_watcher};

/// What a group's mount needs from its `MultiRaft`.
pub struct GroupMountSpec {
    config: RaftConfig,
    storage_path: PathBuf,
    compaction_ceiling: Option<Arc<AtomicU64>>,
    /// Notified when the group's disk makes a batch durable.
    drive_signal: Arc<tokio::sync::Notify>,
}

/// A group opened and restored from its disk, ready to mount.
pub struct OpenedGroup {
    node: RaftNode<StagedLogStorage>,
    storage_path: PathBuf,
}

impl GroupMountSpec {
    /// The group this spec mounts.
    pub fn group_id(&self) -> u64 {
        self.config.group_id
    }

    /// Open the group's log and reload its hard state and entries. Blocks on
    /// disk: call it off the async threads.
    pub fn open(self) -> Result<OpenedGroup> {
        let group_id = self.config.group_id;
        let storage = StagedLogStorage::open(group_id, &self.storage_path, self.drive_signal)
            .map_err(|e| ClusterError::Transport {
                detail: format!("failed to open raft storage for group {group_id}: {e}"),
            })?;
        let mut node = RaftNode::new(self.config, storage);
        if let Some(ceiling) = self.compaction_ceiling {
            node.set_compaction_ceiling(ceiling);
        }
        // Reload durable state (HardState + log) before mounting the group.
        // On a restart this recovers the persisted term/voted_for, so a
        // restarted voter cannot forget its vote and double-vote, and the
        // persisted log entries, so the node does not depend on full
        // re-replication from the leader. On a fresh group the storage is
        // empty and this is a no-op. Also resets the election timeout.
        node.restore()?;
        Ok(OpenedGroup {
            node,
            storage_path: self.storage_path,
        })
    }
}

impl MultiRaft {
    /// What mounting `group_id` needs: its Raft config and its log path.
    /// Touches no disk.
    ///
    /// `peers` are the other voters. A learner-start group lists every voter
    /// in `peers` and the other learners in `learners`.
    pub fn mount_spec(
        &self,
        group_id: u64,
        peers: Vec<u64>,
        learners: Vec<u64>,
        starts_as_learner: bool,
    ) -> GroupMountSpec {
        GroupMountSpec {
            config: RaftConfig {
                node_id: self.node_id,
                group_id,
                peers,
                learners,
                observers: vec![],
                starts_as_learner,
                starts_as_observer: false,
                election_timeout_min: self.election_timeout_min,
                election_timeout_max: self.election_timeout_max,
                heartbeat_interval: self.heartbeat_interval,
                log_compaction_threshold: self.log_compaction_threshold,
            },
            storage_path: crate::raft_bootstrap::group_log_path(&self.data_dir, group_id),
            compaction_ceiling: self.compaction_ceilings.get(&group_id).map(Arc::clone),
            drive_signal: Arc::clone(&self.drive_signal),
        }
    }

    /// Mount an opened group. Touches no disk. Returns the group back when
    /// it is mounted already: the caller drops it off the async threads.
    pub fn insert_opened(&mut self, opened: OpenedGroup) -> Option<OpenedGroup> {
        let group_id = opened.node.group_id();
        if self.groups.contains_key(&group_id) {
            return Some(opened);
        }
        if let Some(watchers) = self.applied_watchers.as_ref() {
            seed_watcher(watchers, group_id, opened.node.last_applied());
        }
        let as_learner = opened.node.role() == nodedb_raft::NodeRole::Learner;
        let mut node = opened.node;
        // Decided before the group takes its first entry: a replica with no
        // Calvin state to resume from would otherwise replay a log that does
        // not hold the Calvin transactions the sequencer compacted away.
        if super::core::is_data_group(group_id)
            && let Some(requirement) = self.snapshot_requirement.as_ref()
        {
            node.set_replica_state(requirement(group_id, self.sequencer_first_available()));
        }
        self.groups.insert(group_id, node);
        self.apply_gates.mount(group_id);
        info!(
            node = self.node_id,
            group = group_id,
            as_learner,
            path = %opened.storage_path.display(),
            "added raft group with persistent storage"
        );
        None
    }

    /// Open and mount a group in one call. Blocks on disk: boot and tests
    /// only. A running node mounts through [`Self::mount_spec`],
    /// [`GroupMountSpec::open`] and [`Self::insert_opened`].
    pub(super) fn add_group_inner(
        &mut self,
        group_id: u64,
        peers: Vec<u64>,
        learners: Vec<u64>,
        starts_as_learner: bool,
    ) -> Result<()> {
        let opened = self
            .mount_spec(group_id, peers, learners, starts_as_learner)
            .open()?;
        // A group mounted already keeps its replica; the one opened here
        // closes as it drops.
        drop(self.insert_opened(opened));
        Ok(())
    }
}
