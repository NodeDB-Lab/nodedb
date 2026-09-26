// SPDX-License-Identifier: BUSL-1.1

//! Which replicated proposals this node already applied, keyed by proposal
//! identity.
//!
//! Every `ReplicatedEntry` carries an `idempotency_key` minted once by its
//! proposer. A re-proposal after `RetryableLeaderChange` sends the same bytes,
//! so both copies carry the same key. When the first copy committed at a log
//! index the proposer was not waiting on, both copies commit, and applying
//! both double-counts every non-idempotent effect (a materialized sum fold, a
//! columnar append). The apply loop checks this ledger before it applies an
//! entry and skips a copy whose key already applied.
//!
//! ## Durability
//!
//! Every WAL record an entry's apply appends carries the entry's key in its
//! header (`RecordHeader::apply_key`). The key is durable in the same write as
//! the effect it names, so no separate marker and no extra fsync is needed.
//! After a restart, [`ProposalLedger::from_records`] recovers the keys from
//! the replayed WAL:
//!
//! - A forward record the WAL cancelled with `WriteAborted` is absent from the
//!   replayed records, so a refused entry stays replayable.
//! - A final refusal's `WriteAborted` marker carries the key, so the refusal
//!   counts as the entry's outcome.
//! - An apply that writes no record of its own (a `wal=false` timeseries
//!   ingest) appends a payload-free `ProposalApplied` record in its place.
//!
//! A duplicate delivered after a restart is recognised as long as the WAL
//! still retains a record of its original.
//!
//! ## Bound
//!
//! The ledger keeps the most recent [`PROPOSAL_LEDGER_CAPACITY`] keys across
//! every group and evicts the oldest past that. Keys are random 64-bit values,
//! so one set serves every group. A re-proposal commits within the proposer's
//! retry budget, which is orders of magnitude fewer entries than the
//! capacity. A checkpoint truncates the WAL, which bounds the recovered set
//! too.
//!
//! The ledger also keeps the outcome of every proposal this process applied,
//! so the waiter of a skipped copy receives what the first copy's waiter
//! received: the same payload and write version, or the same refusal. A key
//! recovered from the WAL has no outcome: no waiter from before the restart
//! survives it.

use std::collections::{HashMap, VecDeque};

use nodedb_wal::WalRecord;

use super::propose_tracker::AppliedWrite;
use crate::bridge::envelope::ErrorCode;

/// Keys the ledger keeps before it evicts the oldest.
pub const PROPOSAL_LEDGER_CAPACITY: usize = 1 << 20;

/// What an applied proposal's waiter received: the applied write, or the
/// typed refusal a durable refusal answered with.
pub type AppliedOutcome = Result<AppliedWrite, ErrorCode>;

/// Applied proposal keys, in apply order.
#[derive(Debug)]
pub struct ProposalLedger {
    results: HashMap<u64, Option<AppliedOutcome>>,
    order: VecDeque<u64>,
    capacity: usize,
}

/// A proposal key the ledger already holds.
#[derive(Debug)]
pub enum PriorApply<'a> {
    /// Applied in this process: the outcome its first copy produced.
    Outcome(&'a AppliedOutcome),
    /// Recovered from the WAL, or applied with no outcome to share.
    NoOutcome,
}

impl ProposalLedger {
    /// An empty ledger that keeps `capacity` keys.
    pub fn new(capacity: usize) -> Self {
        Self {
            results: HashMap::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    /// Recover the ledger from the keyed records in `records`, the node's
    /// replayed WAL in WAL order, with cancelled forward records already
    /// removed. A record with key `0` belongs to no proposal.
    pub fn from_records(records: &[WalRecord], capacity: usize) -> Self {
        let mut ledger = Self::new(capacity);
        for record in records {
            ledger.note(record.apply_key(), None);
        }
        ledger
    }

    /// The prior apply of `proposal_key`, if any. Key `0` is the "no key"
    /// sentinel of a legacy or synthetic entry and never matches.
    pub fn prior(&self, proposal_key: u64) -> Option<PriorApply<'_>> {
        if proposal_key == 0 {
            return None;
        }
        Some(match self.results.get(&proposal_key)? {
            Some(outcome) => PriorApply::Outcome(outcome),
            None => PriorApply::NoOutcome,
        })
    }

    /// Record that `proposal_key` applied, with the outcome its waiter
    /// received when there is one. Key `0` is ignored. A key already held
    /// keeps its place in the eviction order.
    pub fn note(&mut self, proposal_key: u64, result: Option<AppliedOutcome>) {
        if proposal_key == 0 {
            return;
        }
        if self.results.insert(proposal_key, result).is_none() {
            self.order.push_back(proposal_key);
        }
        while self.order.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.results.remove(&oldest);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
    use crate::wal::manager::NO_APPLY_KEY;

    fn applied(payload: &[u8]) -> AppliedWrite {
        AppliedWrite {
            payload: payload.to_vec(),
            write_version: Lsn::new(9),
        }
    }

    #[test]
    fn a_second_copy_of_a_proposal_finds_the_first_copys_outcome() {
        let mut ledger = ProposalLedger::new(8);
        assert!(ledger.prior(77).is_none());
        ledger.note(77, Some(Ok(applied(b"first"))));
        match ledger.prior(77) {
            Some(PriorApply::Outcome(Ok(result))) => assert_eq!(result.payload, b"first"),
            other => panic!("expected the first copy's result, got {other:?}"),
        }
        assert!(ledger.prior(78).is_none());
    }

    #[test]
    fn the_no_key_sentinel_never_deduplicates() {
        let mut ledger = ProposalLedger::new(8);
        ledger.note(0, None);
        assert!(ledger.prior(0).is_none());
    }

    #[test]
    fn the_oldest_key_is_evicted_past_capacity() {
        let mut ledger = ProposalLedger::new(2);
        ledger.note(10, None);
        ledger.note(11, None);
        ledger.note(12, None);
        assert!(ledger.prior(10).is_none());
        assert!(ledger.prior(11).is_some());
        assert!(ledger.prior(12).is_some());
    }

    #[test]
    fn a_key_noted_twice_holds_one_eviction_slot() {
        let mut ledger = ProposalLedger::new(2);
        ledger.note(10, None);
        ledger.note(10, None);
        ledger.note(11, None);
        assert!(ledger.prior(10).is_some());
        assert!(ledger.prior(11).is_some());
    }

    fn open_wal(dir: &tempfile::TempDir) -> crate::wal::WalManager {
        crate::wal::WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open wal")
    }

    #[test]
    fn a_redelivered_key_is_skipped_after_the_ledger_rebuilds_from_keyed_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(&dir);
        let (tid, vs, db) = (TenantId::new(1), VShardId::new(0), DatabaseId::DEFAULT);
        wal.appender(0xAB)
            .with_event_source(crate::event::EventSource::User)
            .append_put(tid, vs, db, b"keyed")
            .expect("append keyed put");
        wal.appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(tid, vs, db, b"unkeyed")
            .expect("append unkeyed put");
        wal.sync().expect("sync wal");

        let ledger = ProposalLedger::from_records(
            &wal.replay().expect("replay wal"),
            PROPOSAL_LEDGER_CAPACITY,
        );
        assert!(matches!(ledger.prior(0xAB), Some(PriorApply::NoOutcome)));
        assert!(ledger.prior(0xCD).is_none());
    }

    #[test]
    fn a_cancelled_forward_record_leaves_its_proposal_replayable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(&dir);
        let (tid, vs, db) = (TenantId::new(1), VShardId::new(0), DatabaseId::DEFAULT);
        let forward = wal
            .appender(0xAB)
            .with_event_source(crate::event::EventSource::User)
            .append_put(tid, vs, db, b"refused")
            .expect("append keyed put");
        wal.appender(NO_APPLY_KEY)
            .append_write_aborted(tid, vs, db, forward)
            .expect("append unkeyed abort");
        let final_forward = wal
            .appender(0xCD)
            .with_event_source(crate::event::EventSource::User)
            .append_put(tid, vs, db, b"refused for good")
            .expect("append keyed put");
        wal.appender(0xCD)
            .append_write_aborted(tid, vs, db, final_forward)
            .expect("append keyed abort");
        wal.sync().expect("sync wal");

        let ledger = ProposalLedger::from_records(
            &wal.replay().expect("replay wal"),
            PROPOSAL_LEDGER_CAPACITY,
        );
        assert!(
            ledger.prior(0xAB).is_none(),
            "a non-final refusal stays replayable"
        );
        assert!(
            ledger.prior(0xCD).is_some(),
            "a final refusal is the proposal's outcome"
        );
    }

    #[test]
    fn a_proposal_applied_marker_is_appended_only_under_an_apply_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(&dir);
        let (tid, vs, db) = (TenantId::new(1), VShardId::new(0), DatabaseId::DEFAULT);
        assert!(
            wal.appender(NO_APPLY_KEY)
                .append_proposal_applied(tid, vs, db)
                .expect("append with no apply key")
                .is_none()
        );
        assert!(
            wal.appender(0xEF)
                .append_proposal_applied(tid, vs, db)
                .expect("append under an apply key")
                .is_some()
        );
        wal.sync().expect("sync wal");

        let ledger = ProposalLedger::from_records(
            &wal.replay().expect("replay wal"),
            PROPOSAL_LEDGER_CAPACITY,
        );
        assert!(ledger.prior(0xEF).is_some());
    }
}
