// SPDX-License-Identifier: BUSL-1.1

//! The first error of a streamed validate, ranked by the order the
//! whole-record checks run in.
//!
//! The whole-record path decodes the content, raises its stamps, probes its
//! clone copies, and then runs the commit boundary, the arms and the
//! snapshot check, each over every sub-record. Its first error is the error
//! of the earliest check that failed. A stream sees the sub-records once, in
//! record order, so each check offers its error under a [`Rank`], and the
//! verdict keeps the smallest. A decode error ends the walk and never
//! enters the verdict: it outranks every rank.

use crate::bridge::envelope::ErrorCode;

/// The step of one collection's checks, in the order they run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Phase {
    /// The stateless PUT and DELETE rules, op by op.
    Stateless,
    /// BALANCED over the collection's entries.
    Balanced,
    /// The UNIQUE image of a put, op by op.
    UniqueImage,
    /// UNIQUE on the post-state.
    Unique,
    /// The materialized-sum folds, op by op.
    Fold,
}

/// The order the snapshot write check lists a record's keyed rows in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum WriteLane {
    Document,
    Kv,
    Edge,
}

/// Where an error ranks among a record's errors. Smaller ranks first. An
/// op's `u64` is its index among the content's sub-records, a batch's
/// `u64` its index among the stream's batches.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Rank {
    /// A sub-record's stamps do not raise to the stream's floor.
    Raise(u64),
    /// A clone copy's row did not read.
    CloneProbe(u64),
    /// A document put's post-image is over the row image limit.
    DocRowImage(u64),
    /// A spilled index failed on this node. Every collection check reads
    /// the indexes, so this ranks before them.
    Local,
    /// One collection's checks, collections in name order.
    Collection(String, Phase, u64),
    /// A KV put's row is over the row image limit.
    KvRowImage(u64),
    /// A batch's record does not build.
    Record(u64),
    /// The arm pass stopped: a panic or a lost scope.
    ArmsAbort(u64),
    /// The arms' first error, or a sub-record no arm claimed.
    Arms,
    /// The snapshot trails the commit ordinal by more than the window.
    SnapshotWindow,
    /// A read moved past the snapshot, or a store did not read.
    SnapshotReads,
    /// A written row moved past the snapshot, or a store did not read.
    SnapshotWrites(WriteLane, u64),
}

/// The smallest-ranked error offered so far.
#[derive(Default)]
pub(super) struct Verdict {
    first: Option<(Rank, ErrorCode)>,
}

impl Verdict {
    /// Whether an error at `rank` replaces the kept one. Work whose
    /// rank is not admitted cannot change the verdict.
    pub(super) fn admits(&self, rank: &Rank) -> bool {
        self.first.as_ref().is_none_or(|(kept, _)| rank < kept)
    }

    /// [`Self::admits`] for `Rank::Collection(collection, phase, op)`,
    /// without building the rank.
    pub(super) fn admits_collection(&self, collection: &str, phase: Phase, op: u64) -> bool {
        let Some((kept, _)) = &self.first else {
            return true;
        };
        match kept {
            Rank::Raise(_) | Rank::CloneProbe(_) | Rank::DocRowImage(_) | Rank::Local => false,
            Rank::Collection(kept_collection, kept_phase, kept_op) => {
                (collection, phase, op) < (kept_collection.as_str(), *kept_phase, *kept_op)
            }
            Rank::KvRowImage(_)
            | Rank::Record(_)
            | Rank::ArmsAbort(_)
            | Rank::Arms
            | Rank::SnapshotWindow
            | Rank::SnapshotReads
            | Rank::SnapshotWrites(..) => true,
        }
    }

    /// Keep `code` when `rank` ranks before the kept error. A tie keeps
    /// the kept error: it was offered first.
    pub(super) fn offer(&mut self, rank: Rank, code: ErrorCode) {
        if self.admits(&rank) {
            self.first = Some((rank, code));
        }
    }

    /// The kept error, `Ok` when no check failed.
    pub(super) fn into_result(self) -> Result<(), ErrorCode> {
        match self.first {
            Some((_, code)) => Err(code),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn code(reason: &str) -> ErrorCode {
        ErrorCode::RejectedPrevalidation {
            reason: reason.into(),
        }
    }

    fn reason(verdict: Verdict) -> String {
        match verdict.into_result() {
            Err(ErrorCode::RejectedPrevalidation { reason }) => reason,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[test]
    fn collections_rank_by_name_then_phase_then_op() {
        let mut verdict = Verdict::default();
        verdict.offer(Rank::Collection("b".into(), Phase::Stateless, 1), code("b"));
        verdict.offer(Rank::Collection("a".into(), Phase::Fold, 0), code("a fold"));
        verdict.offer(
            Rank::Collection("a".into(), Phase::Unique, 9),
            code("a unique"),
        );
        assert!(!verdict.admits_collection("a", Phase::Fold, 3));
        assert!(verdict.admits_collection("a", Phase::Balanced, 7));
        assert!(!verdict.admits_collection("b", Phase::Stateless, 0));
        assert_eq!(reason(verdict), "a unique");
    }

    #[test]
    fn an_earlier_check_outranks_a_later_op_order() {
        let mut verdict = Verdict::default();
        verdict.offer(Rank::KvRowImage(0), code("kv"));
        verdict.offer(Rank::DocRowImage(5), code("doc"));
        verdict.offer(Rank::CloneProbe(9), code("probe"));
        verdict.offer(Rank::SnapshotWindow, code("window"));
        assert!(!verdict.admits_collection("a", Phase::Stateless, 0));
        assert_eq!(reason(verdict), "probe");
    }

    #[test]
    fn a_tie_keeps_the_first_offer() {
        let mut verdict = Verdict::default();
        verdict.offer(Rank::Arms, code("first"));
        verdict.offer(Rank::Arms, code("second"));
        assert_eq!(reason(verdict), "first");
    }
}
