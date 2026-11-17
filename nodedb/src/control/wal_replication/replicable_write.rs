// SPDX-License-Identifier: BUSL-1.1

//! A write plan proven safe to put on the Raft wire.
//!
//! A compiled RLS predicate is decided against the writing identity, which a
//! follower lacks, so it can't cross the wire. [`ReplicableWrite`] makes that
//! state unrepresentable instead of refusing it again at each encode site.

use crate::bridge::envelope::PhysicalPlan;

/// A write plan whose RLS write-check slots are all decided (never
/// `PendingInjection`, and `Predicate` only on a record that carries it to
/// every applier). Private field, only the two constructors below build
/// it, so the guarantee holds for every value that exists.
pub struct ReplicableWrite<'a>(&'a PhysicalPlan);

impl<'a> ReplicableWrite<'a> {
    /// Decide `plan` for replication, or refuse it. Control Plane only. An op
    /// with a deferred policy decision must resolve to a concrete row set first;
    /// a slot still at `PendingInjection` is refused as never policy-checked.
    pub fn decide_for_replication(plan: &'a PhysicalPlan) -> crate::Result<Self> {
        let checks = plan.rls_write_checks();
        if checks.iter().any(|c| c.is_pending_injection()) {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "internal invariant break: this write on '{}' reached the Raft propose path \
                     before RLS injection decided its write-policy check \
                     (RlsWriteCheck::PendingInjection); this is a skipped injection step, not a \
                     policy rejection.",
                    plan.collection().unwrap_or("<unknown>")
                ),
            });
        }
        if !record_carries_write_policy(plan) && checks.iter().any(|c| c.has_predicate()) {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "this write on '{}' carries an RLS write policy whose decision is deferred \
                     to the handler, and a follower has no writing identity to evaluate it \
                     against. It must be resolved to a concrete row set before it is proposed.",
                    plan.collection().unwrap_or("<unknown>")
                ),
            });
        }
        // Each replica accepts different lines against its own tag
        // cardinality and memory budget, so a timeseries ingest resolves to
        // its rows on the proposer, and the entry carries those rows.
        if crate::control::write_resolve::is_unresolved_ingest(plan) {
            return Err(crate::Error::Internal {
                detail: format!(
                    "internal invariant break: a timeseries ingest on '{}' reached the Raft \
                     propose path unresolved; the proposer resolves it with \
                     `resolve_for_log` before it builds the entry",
                    plan.collection().unwrap_or("<unknown>")
                ),
            });
        }
        Ok(Self(plan))
    }

    /// Wrap a plan rebuilt from an already-committed entry, without deciding
    /// anything. Sound only for replay/catchup: the caller owes the guarantee
    /// that checks came from the committed entry, not a live identity.
    pub fn for_already_committed_entry(plan: &'a PhysicalPlan) -> Self {
        Self(plan)
    }

    /// The wrapped plan.
    pub fn plan(&self) -> &'a PhysicalPlan {
        self.0
    }
}

/// Whether `plan`'s replicated record carries its compiled write policy.
///
/// A materialized document write (`PointPut`, `PointInsert`, `BatchInsert`)
/// travels with the predicate compiled against the writing identity. Every
/// applier decides it against the row it materializes, so no identity is
/// needed there. The compiled bytes hold literal comparisons only:
/// `substitute_to_scan_filters` binds every `$auth.*` reference and function
/// on the proposer, and the policy language has no clock, random, sequence,
/// or subquery input, so every replica reaches the same verdict. Every other op drops its check on the wire and must resolve
/// before it is proposed. Exhaustive so a new op decides here.
fn record_carries_write_policy(plan: &PhysicalPlan) -> bool {
    use nodedb_physical::physical_plan::DocumentOp;
    match plan {
        PhysicalPlan::Document(op) => match op {
            DocumentOp::PointPut { .. }
            | DocumentOp::PointInsert { .. }
            | DocumentOp::BatchInsert { .. } => true,
            DocumentOp::PointGet { .. }
            | DocumentOp::PointUpdate { .. }
            | DocumentOp::PointDelete { .. }
            | DocumentOp::Upsert { .. }
            | DocumentOp::BatchUpsert { .. }
            | DocumentOp::BulkUpdate { .. }
            | DocumentOp::BulkDelete { .. }
            | DocumentOp::Merge { .. }
            | DocumentOp::UpdateFromJoin { .. }
            | DocumentOp::ResolveWrite(_)
            | DocumentOp::ResolvedWrite { .. }
            | DocumentOp::InsertSelect { .. }
            | DocumentOp::Scan { .. }
            | DocumentOp::RangeScan { .. }
            | DocumentOp::Register { .. }
            | DocumentOp::IndexLookup { .. }
            | DocumentOp::IndexedFetch { .. }
            | DocumentOp::DropIndex { .. }
            | DocumentOp::BackfillIndex { .. }
            | DocumentOp::Truncate { .. }
            | DocumentOp::EstimateCount { .. }
            | DocumentOp::MaterializeScan { .. }
            | DocumentOp::ApplyBalanceDelta { .. } => false,
        },
        PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::ColumnarOp;
    use nodedb_types::RlsWriteCheck;

    fn delete_with(check: RlsWriteCheck) -> PhysicalPlan {
        PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "orders",
            ),
            filters: Vec::new(),
            system_from: None,
            rls_write_check: check,
            system_time: nodedb_types::SystemTimeScope::Current,
        })
    }

    #[test]
    fn a_live_predicate_cannot_be_decided_for_replication() {
        let plan = delete_with(RlsWriteCheck::from_injected(vec![1, 2, 3]));
        assert!(ReplicableWrite::decide_for_replication(&plan).is_err());
    }

    /// A materialized document write carries its compiled policy on the
    /// record, so every applier decides it against the row it stores.
    #[test]
    fn a_materialized_document_write_carries_its_predicate() {
        let plan =
            PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::PointInsert {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "orders",
                ),
                document_id: "a".into(),
                value: vec![0x80],
                surrogate: nodedb_types::Surrogate::new(1),
                if_absent: false,
                returning: None,
                rls_filters: Vec::new(),
                rls_write_check: RlsWriteCheck::from_injected(vec![1, 2, 3]),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            });
        assert!(ReplicableWrite::decide_for_replication(&plan).is_ok());
    }

    /// An un-injected slot is refused separately: that write was never
    /// policy-checked, so it is a skipped step, not a deferred decision.
    #[test]
    fn an_uninjected_check_cannot_be_decided_for_replication() {
        let plan = delete_with(RlsWriteCheck::pending_injection());
        let Err(crate::Error::PlanError { detail }) =
            ReplicableWrite::decide_for_replication(&plan)
        else {
            panic!("an un-injected write must be refused with a PlanError");
        };
        assert!(
            detail.contains("PendingInjection"),
            "the refusal must name the invariant that broke; got {detail}"
        );
    }

    #[test]
    fn a_decided_check_is_accepted() {
        let plan = delete_with(RlsWriteCheck::decided_earlier_in_request());
        let write =
            ReplicableWrite::decide_for_replication(&plan).expect("decided check must be accepted");
        assert!(matches!(write.plan(), PhysicalPlan::Columnar(_)));
    }

    fn timeseries_ingest(format: &str) -> PhysicalPlan {
        PhysicalPlan::Timeseries(nodedb_physical::physical_plan::TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "metrics",
            ),
            payload: Vec::new(),
            format: format.to_string(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    /// Each replica accepts other lines than its peers, so only the
    /// resolved rows cross the wire.
    #[test]
    fn only_a_resolved_timeseries_ingest_is_decided_for_replication() {
        for format in ["ilp", "ilp-msgpack", "json", "msgpack"] {
            assert!(
                ReplicableWrite::decide_for_replication(&timeseries_ingest(format)).is_err(),
                "an unresolved {format} ingest must be refused"
            );
        }
        let resolved =
            timeseries_ingest(crate::engine::timeseries::resolved_ingest::RESOLVED_INGEST_FORMAT);
        assert!(ReplicableWrite::decide_for_replication(&resolved).is_ok());
    }

    /// Replay never re-decides, so it wraps whatever the committed entry held.
    #[test]
    fn replay_wraps_without_deciding() {
        let plan = delete_with(RlsWriteCheck::already_decided_elsewhere());
        let write = ReplicableWrite::for_already_committed_entry(&plan);
        assert!(matches!(write.plan(), PhysicalPlan::Columnar(_)));
    }
}
