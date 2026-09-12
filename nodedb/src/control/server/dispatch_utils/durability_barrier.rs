// SPDX-License-Identifier: BUSL-1.1

//! Enforcement for the durable-at-ack barrier in [`super::submit_write`].
//!
//! The barrier only has something to wait on when a durable LSN exists, and a
//! write-class plan reaching it with no LSN is NOT by itself a bug: most
//! engines have write ops whose durability is owned somewhere other than this
//! funnel's WAL append, and those arms are deliberate and documented. A naive
//! "write-class plan with no LSN" assertion would fire on every document
//! INSERT (the row is redb-synchronous-durable) and be switched off within a
//! day, which is strictly worse than no check.
//!
//! What IS an invariant is narrower and checkable: for the engines below,
//! EVERY write-class op mints a WAL redo record on this path. If one of them
//! reaches the acknowledgement with nothing to wait on, a write op was filed
//! under a "no durable record" arm and the acknowledgement is a lie — exactly
//! the failure class that only shows up as data loss after a `kill -9`.
//!
//! The counter mirrors [`crate::bridge::admission_chokepoint`]: debug builds
//! trip loudly, release builds keep counting so a regression stays observable
//! after the assertion is compiled out.

#![deny(clippy::wildcard_enum_match_arm)]

use std::sync::atomic::{AtomicU64, Ordering};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::write_admission::plan_is_write;
use nodedb_physical::physical_plan::GraphOp;

/// Count of writes acknowledged with no durable redo record despite belonging
/// to an engine whose every write-class op mints one on this path.
///
/// Stays at zero by construction. A non-zero value names a write op that was
/// classified as needing no durable record and is now acknowledged before it
/// is recoverable.
static WRITES_ACKED_WITHOUT_DURABILITY: AtomicU64 = AtomicU64::new(0);

/// Read the count of writes acknowledged with no durable redo record. Exposed
/// for the metrics exporter and tests.
pub fn writes_acked_without_durability() -> u64 {
    WRITES_ACKED_WITHOUT_DURABILITY.load(Ordering::Relaxed)
}

/// Whether this plan belongs to the set of writes the funnel's own WAL append
/// is required to have minted a redo record for, and under which engine name.
///
/// `None` means a missing LSN is legitimate here, for one of:
///
/// * the plan is not a base-state write at all — reads, control ops, and the
///   per-transaction overlay ops (`StageWrite`, savepoint mark / rollback),
///   all excluded by [`plan_is_write`];
/// * `Document` — every document write op is documented as redb-synchronous-
///   durable, and the one restart-fidelity gap (a secondary vector index) is
///   covered by the post-apply write-set redo, not by a forward record;
/// * `Crdt` — constraint installs are Raft-log-replay durable and
///   `RestoreToVersion` only computes a forward delta that a follow-up
///   `Apply` logs;
/// * `Meta` — COMMIT's single transaction redo, the procedural batch flush and
///   the Calvin ops each own durability on their own path and arrive with the
///   LSN they minted (or none, by design);
/// * `ClusterArray` — a coordinator-side routing wrapper: each owning shard's
///   apply mints the redo for the cells it actually holds;
/// * an empty `EdgePutBatch` / `EdgeDeleteBatch`, which has no edge to make
///   durable and appends nothing on purpose.
///
/// Exchange / gather plans never reach the barrier at all: they are resolved
/// into a merged response before `submit_write` is entered.
pub(super) fn funnel_minted_redo_engine(plan: &PhysicalPlan) -> Option<&'static str> {
    if !plan_is_write(plan) {
        return None;
    }
    // Exhaustive by engine (`wildcard_enum_match_arm` is denied) so a new
    // `PhysicalPlan` variant must be classified here by name rather than
    // silently inheriting "no durability expected".
    match plan {
        PhysicalPlan::Kv(_) => Some("kv"),
        PhysicalPlan::Vector(_) => Some("vector"),
        PhysicalPlan::Columnar(_) => Some("columnar"),
        PhysicalPlan::Timeseries(_) => Some("timeseries"),
        PhysicalPlan::Text(_) => Some("text"),
        PhysicalPlan::Spatial(_) => Some("spatial"),
        PhysicalPlan::Array(_) => Some("array"),
        // A zero-edge batch appends nothing deliberately; every other graph
        // write mints a record per edge or per label delta.
        PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges }) if edges.is_empty() => None,
        PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch { edges }) if edges.is_empty() => None,
        PhysicalPlan::Graph(_) => Some("graph"),
        PhysicalPlan::Document(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => None,
    }
}

/// Called at the durable-at-ack barrier when there is no LSN to wait on.
///
/// `missing_redo_engine` is [`funnel_minted_redo_engine`]'s verdict for the
/// plan: `None` means the absent LSN is legitimate and this is a no-op. `Some`
/// means an acknowledged write has no durable record — counted, reported, and
/// tripped in debug builds so it cannot survive a test run unnoticed.
pub(super) fn assert_durable_before_ack(missing_redo_engine: Option<&'static str>) {
    if let Some(engine) = missing_redo_engine {
        WRITES_ACKED_WITHOUT_DURABILITY.fetch_add(1, Ordering::Relaxed);
        crate::diag::write_acked_without_durability(engine);
    }
    debug_assert!(
        missing_redo_engine.is_none(),
        "a {missing_redo_engine:?} write was acknowledged with no durable redo \
         record — the durable-at-ack barrier had nothing to wait on"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{DocumentOp, KvOp, MetaOp};
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    fn kv_put() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    /// A KV write mints its record in the funnel, so it must be held to the
    /// barrier: this is the arm that turns a lost acknowledged write loud.
    #[test]
    fn kv_write_requires_a_funnel_minted_redo() {
        assert_eq!(funnel_minted_redo_engine(&kv_put()), Some("kv"));
    }

    /// A read carries no durability obligation at all.
    #[test]
    fn read_requires_no_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::EstimateCount {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            field: "f".into(),
        });
        assert_eq!(funnel_minted_redo_engine(&plan), None);
    }

    /// The false-positive guard that decides whether this check is usable at
    /// all: document writes are redb-synchronous-durable and legitimately
    /// reach the barrier with no LSN, on the hottest write path there is.
    #[test]
    fn document_write_is_not_held_to_the_barrier() {
        let plan = PhysicalPlan::Document(DocumentOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert!(plan_is_write(&plan));
        assert_eq!(funnel_minted_redo_engine(&plan), None);
    }

    /// Staged in-transaction writes are durable via the COMMIT redo funnel,
    /// never via this dispatch — `plan_is_write` already excludes them.
    #[test]
    fn staged_transaction_write_is_not_held_to_the_barrier() {
        let plan = PhysicalPlan::Meta(MetaOp::StageWrite {
            plan: Box::new(kv_put()),
        });
        assert_eq!(funnel_minted_redo_engine(&plan), None);
    }

    /// A zero-edge batch appends nothing on purpose, so it must not be held to
    /// an invariant its non-empty sibling satisfies.
    #[test]
    fn empty_edge_batch_is_exempt() {
        let empty = PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges: Vec::new() });
        assert_eq!(funnel_minted_redo_engine(&empty), None);

        let empty_delete = PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch { edges: Vec::new() });
        assert_eq!(funnel_minted_redo_engine(&empty_delete), None);
    }

    /// The guard BITES: an engine whose writes the funnel must make durable
    /// reached the acknowledgement with nothing to fsync.
    #[test]
    #[should_panic(expected = "no durable redo")]
    fn missing_redo_trips_the_barrier() {
        assert_durable_before_ack(funnel_minted_redo_engine(&kv_put()));
    }

    /// A legitimately record-less plan passes the barrier silently — the case
    /// that runs on every read and every document write.
    #[test]
    fn legitimately_record_less_plan_does_not_trip() {
        let before = writes_acked_without_durability();
        assert_durable_before_ack(None);
        assert_eq!(writes_acked_without_durability(), before);
    }
}
