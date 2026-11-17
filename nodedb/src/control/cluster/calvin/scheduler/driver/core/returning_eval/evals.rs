// SPDX-License-Identifier: BUSL-1.1

//! The `RETURNING` evaluations a leader runs off its scheduler loop, and the
//! transcripts it keeps for every txn that carries a contract.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use nodedb_cluster::calvin::types::SequencedTxn;
use tokio::sync::mpsc;

use super::evaluate::ReturningOutcome;
use crate::control::cluster::calvin::scheduler::driver::returning_transcripts::TxnTranscripts;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

/// One finished evaluation, as it returns to the scheduler loop.
pub(in crate::control::cluster::calvin::scheduler::driver::core) struct Evaluated {
    pub(super) txn_id: TxnId,
    /// The evaluation's sequence number. Only the evaluation the txn waits
    /// for casts its vote.
    pub(super) seq: u64,
    pub(super) outcome: ReturningOutcome,
    /// The plans of the stored reply the round followed.
    pub(super) base: BTreeSet<u32>,
    /// The plans the round evaluated.
    pub(super) round: BTreeSet<u32>,
}

/// The evaluations of one scheduler.
pub(in crate::control::cluster::calvin::scheduler::driver::core) struct ReturningEvals {
    tx: mpsc::UnboundedSender<Evaluated>,
    rx: mpsc::UnboundedReceiver<Evaluated>,
    next_seq: u64,
    /// The transcripts of every txn with a contract this scheduler holds.
    /// A txn keeps them across every stage, until it finishes.
    transcripts: BTreeMap<TxnId, Arc<TxnTranscripts>>,
}

impl ReturningEvals {
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            tx,
            rx,
            next_seq: 0,
            transcripts: BTreeMap::new(),
        }
    }

    /// The next finished evaluation. The scheduler holds a sender, so this
    /// waits until one finishes.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) async fn recv(
        &mut self,
    ) -> Option<Evaluated> {
        self.rx.recv().await
    }

    pub(super) fn sender(&self) -> mpsc::UnboundedSender<Evaluated> {
        self.tx.clone()
    }

    pub(super) fn next_seq(&mut self) -> u64 {
        self.next_seq = self.next_seq.wrapping_add(1);
        self.next_seq
    }

    /// The transcripts of `txn`, at `txn_id`. A txn with no contract makes
    /// no sequence call, so it gets fresh ones that nothing keeps.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn transcripts_for(
        &mut self,
        txn_id: TxnId,
        txn: &SequencedTxn,
    ) -> Arc<TxnTranscripts> {
        if txn.tx_class.returning.is_none() {
            return Arc::default();
        }
        Arc::clone(self.transcripts.entry(txn_id).or_default())
    }

    /// Drop the transcripts of `txn_id`, which finished.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn forget(
        &mut self,
        txn_id: TxnId,
    ) {
        self.transcripts.remove(&txn_id);
    }

    #[cfg(test)]
    pub(super) fn holds(&self, txn_id: TxnId) -> bool {
        self.transcripts.contains_key(&txn_id)
    }
}
