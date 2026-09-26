// SPDX-License-Identifier: BUSL-1.1

//! Coverage of the sequencer group.
//!
//! A Calvin transaction is acknowledged once every participant vShard's
//! leader proposed a `CompletionAck` to the sequencer log. Every other
//! replica of the vShard applies the transaction in its own time. This node
//! covers sequencer index `i` when it applied the sequencer log through `i`
//! and, for every ack at or below `i` naming a vShard this node replicates,
//! its own scheduler applied that transaction too.
//!
//! The sequencer state machine records each ack it applies. This tracker
//! takes them in log order and settles each against the local scheduler's
//! applied mirror. The first unsettled ack bounds the coverage.

use std::collections::VecDeque;
use std::sync::Mutex;

use nodedb_cluster::calvin::{AppliedCompletionAck, CalvinCompletionRegistry};

use crate::control::cluster::calvin::scheduler::AppliedMirrors;

/// Acks taken from the sequencer log and not yet settled.
#[derive(Debug, Default)]
pub struct CalvinAckCoverage {
    pending: Mutex<VecDeque<AppliedCompletionAck>>,
}

impl CalvinAckCoverage {
    /// The sequencer index this node covers, given that it applied the
    /// sequencer log through `applied`, read before this call.
    ///
    /// `replicates` answers whether this node replicates a vShard. An ack for
    /// a vShard it does not replicate settles at once: this node never plans
    /// against that vShard's rows.
    pub fn covered_through(
        &self,
        registry: &CalvinCompletionRegistry,
        mirrors: &AppliedMirrors,
        applied: u64,
        replicates: impl Fn(u32) -> bool,
    ) -> u64 {
        let mut pending = self.pending.lock().unwrap_or_else(|p| p.into_inner());
        pending.extend(registry.applied_acks.drain());
        while let Some(ack) = pending.front() {
            let settled = !replicates(ack.vshard_id)
                || mirrors
                    .get(ack.vshard_id)
                    .is_some_and(|mirror| mirror.is_applied(ack.txn.epoch, ack.txn.position));
            if !settled {
                break;
            }
            pending.pop_front();
        }
        match pending.front() {
            Some(ack) => applied.min(ack.index.saturating_sub(1)),
            None => applied,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use nodedb_cluster::calvin::TxnId;

    use super::*;
    use crate::control::cluster::calvin::scheduler::NOT_YET_APPLIED_EPOCH;

    fn ack(index: u64, epoch: u64, vshard_id: u32) -> AppliedCompletionAck {
        AppliedCompletionAck {
            index,
            txn: TxnId::new(epoch, 0),
            vshard_id,
        }
    }

    #[test]
    fn an_ack_the_local_replica_has_not_applied_bounds_the_coverage() {
        let registry = CalvinCompletionRegistry::new_detached();
        registry.applied_acks.enable();
        let mirrors = AppliedMirrors::default();
        let mirror = mirrors.register(7, NOT_YET_APPLIED_EPOCH, &BTreeSet::new());
        let coverage = CalvinAckCoverage::default();

        registry.applied_acks.record(ack(4, 1, 7));
        registry.applied_acks.record(ack(6, 2, 9));
        // vShard 7 is replicated here and its scheduler has not applied epoch 1.
        let replicates = |vshard: u32| vshard == 7;
        assert_eq!(
            coverage.covered_through(&registry, &mirrors, 8, replicates),
            3
        );

        mirror.mark(1, 0);
        // The ack for vShard 9 settles at once: it is not replicated here.
        assert_eq!(
            coverage.covered_through(&registry, &mirrors, 8, replicates),
            8
        );
    }

    #[test]
    fn a_replicated_vshard_without_a_scheduler_yet_is_unsettled() {
        let registry = CalvinCompletionRegistry::new_detached();
        registry.applied_acks.enable();
        let mirrors = AppliedMirrors::default();
        let coverage = CalvinAckCoverage::default();
        registry.applied_acks.record(ack(2, 1, 5));
        assert_eq!(
            coverage.covered_through(&registry, &mirrors, 3, |_| true),
            1
        );
    }

    /// A restart loses the in-memory mirror and, after a checkpoint, the WAL
    /// markers of transactions applied before it. The mirror rebuilt from the
    /// state the checkpoint saved settles every ack the sequencer log replays,
    /// so the coverage reaches the sequencer log's applied index.
    #[test]
    fn a_mirror_rebuilt_from_saved_state_settles_replayed_acks() {
        use crate::control::cluster::calvin::scheduler::recover_applied;
        use crate::control::security::catalog::SystemCatalog;
        use crate::control::security::catalog::calvin_applied::StoredCalvinApplied;

        let wal_dir = tempfile::tempdir().expect("wal dir");
        // The WAL a checkpoint truncated holds no applied marker.
        let wal = crate::wal::manager::WalManager::open(wal_dir.path(), false).expect("wal");
        let catalog_dir = tempfile::tempdir().expect("catalog dir");
        let catalog =
            SystemCatalog::open(&catalog_dir.path().join("system.redb")).expect("catalog");
        catalog
            .save_calvin_applied(vec![StoredCalvinApplied {
                vshard_id: 7,
                fully_applied_epoch: NOT_YET_APPLIED_EPOCH,
                tail: [(0, 0), (3, 1)].into_iter().collect(),
            }])
            .expect("save");

        let recovered = recover_applied(&wal, &catalog, 7).expect("recover");
        let mirrors = AppliedMirrors::default();
        mirrors.register(7, recovered.fully_applied_epoch, &recovered.applied_tail);

        let registry = CalvinCompletionRegistry::new_detached();
        registry.applied_acks.enable();
        registry.applied_acks.record(AppliedCompletionAck {
            index: 5,
            txn: TxnId::new(0, 0),
            vshard_id: 7,
        });
        registry.applied_acks.record(AppliedCompletionAck {
            index: 9,
            txn: TxnId::new(3, 1),
            vshard_id: 7,
        });
        let coverage = CalvinAckCoverage::default();
        assert_eq!(
            coverage.covered_through(&registry, &mirrors, 19, |_| true),
            19,
            "every replayed ack settles against the rebuilt mirror"
        );
    }
}
