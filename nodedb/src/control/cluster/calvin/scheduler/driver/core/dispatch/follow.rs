// SPDX-License-Identifier: BUSL-1.1

//! A granted txn on a replica that does not lead the vShard's data group.
//!
//! The follower holds the txn's locks and stages nothing. It owes no vote.
//! The txn completes on an abort verdict, on a COMMIT verdict for a slice
//! with no local write, or once the slice's stamped redo applies from the
//! log. A promotion stages it. A dependent-read txn keeps its barrier log
//! meanwhile.

use std::time::Instant;

use nodedb_cluster::calvin::types::SequencedTxn;

use super::super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::driver::types::{
    CommitState, PendingTxn, SliceScope,
};
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

impl Scheduler {
    /// Hold `txn` `Following`. A verdict stored already resolves it at once.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn follow(
        &mut self,
        txn: SequencedTxn,
        txn_id: TxnId,
        lock_owner: TxnId,
    ) {
        let scope = self.local_slice_scope(&txn, txn_id);
        let dependent = txn.tx_class.dependent_reads.is_some();
        self.pending.insert(
            txn_id,
            PendingTxn {
                txn,
                lock_owner,
                // no-determinism: dispatch time is observability only.
                dispatch_time: Instant::now(),
                has_primary_write: false,
                raises_write_mark: false,
                has_returning: false,
                commit_state: CommitState::Following,
                awaiting: None,
                verdict_deadline: None,
                stage_error: None,
                scope,
                redo: None,
                superseded: false,
                gates: Vec::new(),
                ungated: false,
            },
        );
        // A dependent txn keeps its barrier log from its grant on, so a
        // promotion decides its barrier from every entry the log holds.
        if dependent {
            self.absorb_buffered_reads(txn_id);
        }
        if let Some(verdict) = self.registry.verdict(nodedb_cluster::calvin::TxnId::new(
            txn_id.epoch,
            txn_id.position,
        )) {
            self.resume_on_verdict(txn_id, verdict);
        }
    }

    /// The scope of `txn`'s slice on this vShard, from the txn's own plans.
    /// A follower completes a COMMIT with no entry only when this scope
    /// writes nothing, so it never comes from a leader's step in progress.
    /// Plans that do not decode or route make the leader vote abort, so they
    /// hold no write here.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn local_slice_scope(
        &self,
        txn: &SequencedTxn,
        txn_id: TxnId,
    ) -> SliceScope {
        let local =
            super::super::super::helpers::decode_plans(&txn.tx_class.plans).and_then(|plans| {
                self.local_calvin_plans(
                    plans,
                    txn.tx_class.database_id,
                    txn_id.epoch,
                    txn_id.position,
                )
            });
        match local {
            Ok(local) => SliceScope::of_plans(&local),
            Err(error) => {
                tracing::debug!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    %error,
                    "calvin: follower holds a txn whose plans the leader rejects"
                );
                SliceScope::of_plans(&[])
            }
        }
    }
}
