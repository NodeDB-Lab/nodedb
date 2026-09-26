// SPDX-License-Identifier: BUSL-1.1

//! The one consolidated write-class predicate.
//!
//! `plan_is_write` is the single source of truth for "does this plan mutate
//! shard key state" — used by the SPSC enqueue chokepoint and (in a follow-up
//! change) by the write-admission gate's lock acquisition. It is DERIVED from
//! the exhaustive [`required_permission`] mapping rather than re-listing the
//! write op variants, so there is exactly one place the write/read split is
//! decided and a new `PhysicalPlan` variant cannot silently miss it.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::{Permission, required_permission};
use nodedb_physical::physical_plan::MetaOp;

/// Whether a physical plan is a base-state write — one that mutates a shard's
/// committed key state and therefore must pass the write-admission gate.
///
/// Derived from the exhaustive [`required_permission`] mapping: a plan is a
/// base-state write iff it requires [`Permission::Write`], MINUS the
/// per-transaction overlay / savepoint control ops. DDL / schema changes
/// ([`Permission::Alter`], `Create`, `Drop`, `Admin`) are excluded by
/// `required_permission` already — they are not part of the transactional
/// read/write set the gate serializes.
///
/// The carve-out ops below require Write permission but stage into the
/// transaction's overlay rather than committed base state (the real base write
/// happens at COMMIT via `MetaOp::TransactionBatch`, which IS a write here), so
/// they are Exempt from the gate — not a third write classification, just the
/// overlay ops subtracted from the one write set.
pub fn plan_is_write(plan: &PhysicalPlan) -> bool {
    if matches!(
        plan,
        PhysicalPlan::Meta(
            MetaOp::StageWrite { .. }
                | MetaOp::MarkSavepoint { .. }
                | MetaOp::RollbackToSavepoint { .. }
        )
    ) {
        return false;
    }
    matches!(required_permission(plan), Permission::Write)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{DocumentOp, KvOp};

    #[test]
    fn point_get_is_not_a_write() {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            document_id: "d".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        assert!(!plan_is_write(&plan));
    }

    #[test]
    fn kv_put_is_a_write() {
        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        });
        assert!(plan_is_write(&plan));
    }

    #[test]
    fn cancel_meta_op_is_not_a_write() {
        let plan = PhysicalPlan::Meta(MetaOp::Cancel {
            target_request_id: crate::types::RequestId::new(1),
        });
        assert!(!plan_is_write(&plan));
    }
}
