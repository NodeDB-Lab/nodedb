// SPDX-License-Identifier: BUSL-1.1

//! The data-group leader proposes the timeout entry of a dependent-read
//! barrier that waited too long.
//!
//! The leader's own clock decides only when it proposes
//! `ReplicatedWrite::CalvinReadTimeout`. The entry's place in the vShard's
//! log decides what it does (see [`super::read_result`]): every replica
//! applies it after the same read results. The leader proposes straight to
//! its group with `MultiRaft::propose`, as it proposes a redo: the write
//! gate exempts the entry, which writes no row.
//!
//! A proposal re-arms the barrier's timeout. An entry a leader change
//! lost is proposed again then, and a duplicate that lands after the first
//! changes nothing.

use std::time::Instant;

use nodedb_cluster::ClusterError;
use nodedb_raft::RaftError;

use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};

/// Why a timeout entry did not reach the log.
enum TimeoutRefusal {
    /// This node does not lead the vShard's data group.
    NotLeader,
    /// The entry cannot be built, or the group refused it now.
    Failed(String),
}

impl Scheduler {
    /// Propose the timeout entry of every open barrier whose timeout passed.
    /// Runs on the leader only.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn propose_due_read_timeouts(
        &mut self,
    ) {
        if !self.role.is_leader() || self.dependent_barrier.is_empty() {
            return;
        }
        // no-determinism: the leader's clock decides only when it proposes the timeout entry; the entry's place in the log decides the barrier.
        let now = Instant::now();
        let due: Vec<TxnId> = self
            .dependent_barrier
            .iter()
            .filter(|(_, barrier)| barrier.timeout_due <= now)
            .map(|(txn_id, _)| *txn_id)
            .collect();
        for txn_id in due {
            match self.propose_read_timeout(txn_id) {
                Ok(()) => {}
                Err(TimeoutRefusal::NotLeader) => {
                    self.on_not_leader();
                    return;
                }
                Err(TimeoutRefusal::Failed(error)) => tracing::debug!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    %error,
                    "calvin: barrier timeout entry not proposed; proposing it again later"
                ),
            }
            if let Some(barrier) = self.dependent_barrier.get_mut(&txn_id) {
                barrier.timeout_due = now + self.config.passive_timeout();
            }
        }
    }

    /// The earliest instant this leader proposes a barrier's timeout entry,
    /// when it holds a barrier open.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn next_read_timeout_due(
        &self,
    ) -> Option<Instant> {
        if !self.role.is_leader() {
            return None;
        }
        self.dependent_barrier
            .values()
            .map(|barrier| barrier.timeout_due)
            .min()
    }

    /// Append the timeout entry of `txn_id`'s barrier to this vShard's
    /// data-group log.
    fn propose_read_timeout(&self, txn_id: TxnId) -> Result<(), TimeoutRefusal> {
        let Some(barrier) = self.dependent_barrier.get(&txn_id) else {
            return Ok(());
        };
        let tenant_id = barrier.txn.tx_class.tenant_id.as_u64();
        let entry = ReplicatedEntry::new(
            tenant_id,
            barrier.txn.tx_class.database_id.as_u64(),
            self.vshard_id,
            ReplicatedWrite::CalvinReadTimeout {
                epoch: txn_id.epoch,
                position: txn_id.position,
                tenant_id,
            },
        );
        let bytes = entry
            .encode()
            .map_err(|error| TimeoutRefusal::Failed(error.to_string()))?;
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        match mr.propose(self.vshard_id, bytes) {
            Ok(_) => {
                tracing::info!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    "calvin: dependent-read barrier waited past its timeout; proposed the \
                     timeout entry"
                );
                Ok(())
            }
            Err(ClusterError::Raft(RaftError::NotLeader { .. })) => Err(TimeoutRefusal::NotLeader),
            Err(error) => Err(TimeoutRefusal::Failed(error.to_string())),
        }
    }
}
