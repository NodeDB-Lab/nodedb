// SPDX-License-Identifier: BUSL-1.1

//! The leader's side of multi-part transactions: opening their part
//! streams, proposing the parts they stream in, and abandoning the ones no
//! stream will finish.
//!
//! The leader opens a transaction's stream once its header's epoch batch is
//! proposed. The coordinator streams the parts into the shared
//! [`PartsIntake`], whose byte cap bounds the leader's memory. The leader
//! proposes queued parts in order on each tick.
//!
//! - Fairness: one tick proposes parts up to `max_bytes_per_epoch` bytes, at
//!   least one part. Epoch batches keep their cadence between them. A part
//!   carries a position fixed by its header, so it never reorders anything on
//!   the vShards it shares with later transactions: their flushes wait for it
//!   by position.
//! - A failed proposal puts the part back for the next tick.
//! - A node that stops leading closes every stream. A leader whose term
//!   changed closes the streams of the old term: Raft can drop an earlier
//!   term's proposals. A stream that stalls is closed too.
//! - A leader abandons every open transaction whose stream it does not
//!   hold: one another leader sequenced, one this node sequenced before a
//!   restart or a term change, or one whose stream closed. The abandonment
//!   applies only while the transaction is open, so a last part that commits
//!   first wins.
//!
//! A stream stays open until every part is proposed, its header applied,
//! and the transaction closed in the state machine, so the leader never
//! abandons a transaction it is still carrying.
//!
//! [`PartsIntake`]: crate::calvin::sequencer::parts_intake::PartsIntake

use std::time::Instant;

use tracing::{debug, warn};

use crate::calvin::TxnId;
use crate::calvin::sequencer::config::SEQUENCER_GROUP_ID;
use crate::calvin::sequencer::entry::SequencerEntry;
use crate::calvin::types::MultiPartPlans;

use super::core::SequencerService;

/// The abandonments this leader proposed and has not seen applied, and the
/// term it proposed them in.
///
/// Raft can drop a proposal of an earlier term. So a term change forgets
/// them all, and the leader proposes each again, as it closes the streams
/// of an earlier term. A leader regained with no non-leader tick between is
/// covered too: the term still changed.
#[derive(Debug, Default)]
pub(crate) struct Abandoning {
    term: Option<u64>,
    txns: std::collections::BTreeSet<TxnId>,
}

impl Abandoning {
    /// Forget every abandonment unless it was proposed in `term`.
    fn keep_term(&mut self, term: Option<u64>) {
        if self.term != term {
            self.txns.clear();
            self.term = term;
        }
    }

    /// Forget every abandonment.
    fn clear(&mut self) {
        self.txns.clear();
        self.term = None;
    }
}

impl SequencerService {
    /// Open the part stream of `txn`, whose header's batch was
    /// proposed.
    pub(super) fn open_part_stream(&mut self, txn: TxnId, manifest: &MultiPartPlans) {
        let Some(term) = self.sequencer_term() else {
            warn!(
                epoch = txn.epoch,
                position = txn.position,
                "sequencer: leadership lost right after proposing a multi-part header; \
                 the next leader abandons it"
            );
            return;
        };
        self.parts_intake.open(manifest.stream, txn, term, manifest);
    }

    /// This node's term in the sequencer group while it leads, else `None`.
    fn sequencer_term(&self) -> Option<u64> {
        let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.leader_term(SEQUENCER_GROUP_ID)
    }

    /// Close every part stream: this node no longer leads.
    pub(super) fn drop_part_streams(&mut self) {
        let dropped = self.parts_intake.clear();
        self.abandoning.clear();
        if dropped > 0 {
            warn!(
                node_id = self.node_id,
                dropped,
                "sequencer: leadership lost with multi-part transactions streaming; \
                 the next leader abandons them"
            );
        }
    }

    /// Close stale streams, then propose queued parts in order, up to one
    /// epoch's byte budget and at least one part. Stops at the first failed
    /// proposal.
    pub(super) fn propose_parts(&mut self) {
        let closed = self.parts_intake.close_stale(
            self.sequencer_term(),
            // no-determinism: stall detection only, never in the log.
            Instant::now(),
        );
        if closed > 0 {
            warn!(
                node_id = self.node_id,
                closed,
                "sequencer: closed part streams of an earlier term or with a stalled \
                 coordinator; abandoning their transactions"
            );
        }
        let budget = self.config.max_bytes_per_epoch;
        let mut spent = 0usize;
        while spent < budget {
            let Some((stream, txn, streamed)) = self.parts_intake.next_part() else {
                return;
            };
            let entry = SequencerEntry::TxnPart {
                epoch: txn.epoch,
                position: txn.position,
                index: streamed.index,
                first_task: streamed.part.first_task,
                targets: streamed.targets.clone(),
                plans: streamed.part.plans.clone(),
                chunk: streamed.part.chunk,
            };
            match self.propose_entry(&entry) {
                Ok(_) => spent = spent.saturating_add(streamed.part.plans.len().max(1)),
                Err(error) => {
                    warn!(
                        epoch = txn.epoch,
                        position = txn.position,
                        part = streamed.index,
                        %error,
                        "sequencer: part proposal failed; retrying next tick"
                    );
                    self.parts_intake.return_part(stream, streamed);
                    return;
                }
            }
        }
    }

    /// Abandon every open multi-part transaction whose stream this leader
    /// does not hold, and close the streams that finished.
    ///
    /// Runs only once the epoch seed exists: the group then replayed its log,
    /// so the state machine knows every header committed before this term.
    pub(super) fn abandon_orphaned_parts(&mut self) {
        let (open, applied_epoch) = {
            let sm = self.state_machine.lock().unwrap_or_else(|p| p.into_inner());
            (sm.open_multi_part_txns(), sm.last_applied_epoch())
        };
        self.parts_intake.close_finished(|txn| {
            applied_epoch.is_some_and(|applied| applied >= txn.epoch)
                && open.binary_search(&txn).is_err()
        });
        let term = self.sequencer_term();
        self.abandoning.keep_term(term);
        self.abandoning
            .txns
            .retain(|txn| open.binary_search(txn).is_ok());
        for txn in open {
            if self.parts_intake.holds(txn) || self.abandoning.txns.contains(&txn) {
                continue;
            }
            match self.propose_entry(&SequencerEntry::TxnPartsAbandoned {
                epoch: txn.epoch,
                position: txn.position,
            }) {
                Ok(_) => {
                    debug!(
                        epoch = txn.epoch,
                        position = txn.position,
                        "sequencer: abandoning a multi-part transaction no stream carries"
                    );
                    self.abandoning.txns.insert(txn);
                }
                Err(error) => warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    %error,
                    "sequencer: abandonment proposal failed; retrying next tick"
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::calvin::sequencer::parts_intake::PartsOfferStatus;
    use crate::calvin::types::{
        EpochBatch, PartStreamId, PlanPart, SequencedTxn, StreamedPart, VShardParts,
    };

    use super::super::core::tests::{Harness, elect, make_harness, make_tx_class};
    use super::*;

    const TXN: TxnId = TxnId {
        epoch: 0,
        position: 0,
    };
    const STREAM: PartStreamId = PartStreamId { node: 1, seq: 42 };

    fn part(index: u32, bytes: usize, target: u32) -> StreamedPart {
        StreamedPart {
            index,
            targets: vec![target],
            part: PlanPart {
                first_task: index,
                plans: vec![0x90; bytes],
                chunk: None,
            },
        }
    }

    /// Apply, as a log replay does, the epoch-0 batch whose one txn is the
    /// header of `parts` parts, every one targeting the first participant.
    /// Returns the manifest and that participant.
    fn apply_header(harness: &Harness, parts: u32) -> (MultiPartPlans, u32) {
        let mut tx_class = make_tx_class(1, 2);
        tx_class.plans = Vec::new();
        let target = tx_class.participating_vshards()[0].as_u32();
        let manifest = MultiPartPlans {
            stream: STREAM,
            part_count: parts,
            total_tasks: parts,
            user_write: true,
            client_write: true,
            per_vshard: vec![VShardParts {
                vshard: target,
                parts,
            }],
        };
        tx_class.multi_part = Some(manifest.clone());
        let batch = EpochBatch {
            epoch: 0,
            txns: vec![SequencedTxn {
                epoch: 0,
                position: 0,
                tx_class: std::sync::Arc::new(tx_class),
                epoch_system_ms: 1_700_000_000_000,
                epoch_vshard_txn_count: 1,
                lock_owner: None,
                local_plans: None,
            }],
            epoch_system_ms: 1_700_000_000_000,
        };
        let bytes = zerompk::to_msgpack_vec(&SequencerEntry::EpochBatch { batch }).expect("encode");
        let mut sm = harness
            .state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        sm.apply(1, &bytes);
        assert_eq!(sm.open_multi_part_txns(), [TXN]);
        (manifest, target)
    }

    fn log_tip(harness: &Harness) -> u64 {
        harness
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_log_index(SEQUENCER_GROUP_ID)
            .expect("group is mounted")
    }

    /// A leader that holds no stream for an open txn, after a restart or a
    /// leader change, abandons it once.
    #[test]
    fn a_leader_without_the_stream_abandons_the_open_txn_once() {
        let mut harness = make_harness();
        elect(&harness.multi_raft);
        apply_header(&harness, 2);

        let before = log_tip(&harness);
        harness.service.abandon_orphaned_parts();
        assert_eq!(log_tip(&harness), before + 1, "one abandonment");
        harness.service.abandon_orphaned_parts();
        assert_eq!(log_tip(&harness), before + 1, "proposed once");
    }

    /// An abandonment proposed in an earlier term is proposed again: Raft
    /// can drop that term's proposals. A leader regained with no
    /// non-leader tick between still sees the term change.
    #[test]
    fn an_abandonment_of_an_earlier_term_is_proposed_again() {
        let mut harness = make_harness();
        elect(&harness.multi_raft);
        apply_header(&harness, 2);
        let term = harness
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .leader_term(SEQUENCER_GROUP_ID)
            .expect("leader");
        harness.service.abandoning.term = Some(term.saturating_sub(1));
        harness.service.abandoning.txns.insert(TXN);

        let before = log_tip(&harness);
        harness.service.abandon_orphaned_parts();
        assert_eq!(log_tip(&harness), before + 1, "proposed again this term");
        harness.service.abandon_orphaned_parts();
        assert_eq!(log_tip(&harness), before + 1, "once per term");
    }

    /// The leader that sequenced a txn takes its streamed parts, proposes
    /// them, and never abandons it while it is open.
    #[test]
    fn the_leader_proposes_streamed_parts_and_keeps_the_txn() {
        let mut harness = make_harness();
        elect(&harness.multi_raft);
        let (manifest, target) = apply_header(&harness, 2);
        harness.service.open_part_stream(TXN, &manifest);

        let offer = harness
            .inbox
            .offer_parts(STREAM, vec![part(0, 4, target), part(1, 4, target)]);
        assert_eq!(offer.status, PartsOfferStatus::Accepted);
        assert_eq!(offer.next_index, 2);

        let before = log_tip(&harness);
        harness.service.propose_parts();
        assert_eq!(log_tip(&harness), before + 2, "both parts");
        harness.service.abandon_orphaned_parts();
        assert_eq!(log_tip(&harness), before + 2, "no abandonment");
        assert!(harness.service.parts_intake.holds(TXN));
    }

    /// A stream opened in an earlier term is closed mid-stream: Raft can
    /// drop that term's proposals. Its coordinator's next offer is answered
    /// `Unknown`, and the open txn is abandoned.
    #[test]
    fn a_term_change_mid_stream_closes_it_and_abandons_the_txn() {
        let mut harness = make_harness();
        elect(&harness.multi_raft);
        let (manifest, target) = apply_header(&harness, 3);
        let term = harness
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .leader_term(SEQUENCER_GROUP_ID)
            .expect("leader");
        harness
            .service
            .parts_intake
            .open(STREAM, TXN, term.saturating_sub(1), &manifest);
        let offer = harness.inbox.offer_parts(STREAM, vec![part(0, 4, target)]);
        assert_eq!(offer.status, PartsOfferStatus::Accepted);

        let before = log_tip(&harness);
        harness.service.propose_parts();
        assert_eq!(log_tip(&harness), before, "no part of the old term");
        let offer = harness.inbox.offer_parts(STREAM, vec![part(1, 4, target)]);
        assert_eq!(offer.status, PartsOfferStatus::Unknown);
        harness.service.abandon_orphaned_parts();
        assert_eq!(log_tip(&harness), before + 1, "one abandonment");
    }

    /// A txn of 72 one-MiB parts, over the 64 MiB RPC limit, streams in
    /// batches. The leader's queue never holds more than its cap, tells the
    /// stream to wait when full, and proposes every part in order.
    #[test]
    fn a_stream_over_64_mib_is_bounded_and_fully_proposed() {
        const PARTS: u32 = 72;
        const PART_BYTES: usize = 1 << 20;
        let mut harness = make_harness();
        elect(&harness.multi_raft);
        let (manifest, target) = apply_header(&harness, PARTS);
        harness.service.open_part_stream(TXN, &manifest);
        let cap = harness.service.config.max_queued_part_bytes;
        let before = log_tip(&harness);

        let mut next = 0u32;
        let mut pushed_back = false;
        let mut ticks = 0;
        while next < PARTS {
            let batch: Vec<StreamedPart> = (next..PARTS.min(next + 8))
                .map(|index| part(index, PART_BYTES, target))
                .collect();
            let offer = harness.inbox.offer_parts(STREAM, batch);
            assert!(harness.service.parts_intake.queued_bytes() <= cap);
            next = offer.next_index;
            if offer.status == PartsOfferStatus::Full {
                pushed_back = true;
                harness.service.propose_parts();
                ticks += 1;
            }
            assert!(ticks < 1_000, "the stream makes progress");
        }
        while harness.service.parts_intake.queued_bytes() > 0 {
            harness.service.propose_parts();
        }

        assert!(pushed_back, "the queue pushed back before 72 MiB queued");
        assert_eq!(log_tip(&harness), before + u64::from(PARTS));
        harness.service.abandon_orphaned_parts();
        assert_eq!(
            log_tip(&harness),
            before + u64::from(PARTS),
            "a fully streamed txn is never abandoned"
        );
    }
}
