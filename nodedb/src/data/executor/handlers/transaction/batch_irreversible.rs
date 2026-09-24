// SPDX-License-Identifier: BUSL-1.1

//! Sub-plans a transaction batch cannot roll back.
//!
//! A sub-plan with no engine-specific transaction handler runs through the
//! ordinary dispatch path. That path records no undo entry, so a later
//! rollback leaves its write in place. A failed batch that ran such a write
//! must not answer with a code that claims nothing applied: the Control
//! Plane cancels every record of a batch refused with such a code, and
//! recovery would then drop the write that stayed.

use nodedb_physical::physical_plan::PhysicalPlan;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::control::server::shared::write_admission::plan_is_write;
use crate::data::executor::handlers::partial_refusal::refusal_after_partial_apply;

/// Whether `plan` wrote state the undo log does not cover.
///
/// A tracked write that changed state always adds an undo entry. A write
/// plan that added none either ran untracked or changed nothing. Both count
/// here, which errs toward replaying the batch's records.
pub(super) fn applied_irreversibly(
    plan: &PhysicalPlan,
    undo_before: usize,
    undo_after: usize,
) -> bool {
    undo_after == undo_before && plan_is_write(plan)
}

/// The code a failed batch answers with once its rollback finished.
///
/// After an irreversible sub-plan applied, a definite refusal becomes
/// `Internal`, which keeps the batch's records for replay.
pub(super) fn batch_failure_code(code: ErrorCode, irreversible: bool) -> ErrorCode {
    if irreversible {
        refusal_after_partial_apply(code)
    } else {
        code
    }
}

/// [`batch_failure_code`] applied to a failed batch's response.
pub(super) fn batch_failure_response(mut response: Response, irreversible: bool) -> Response {
    if let Some(code) = response.error_code.take() {
        response.error_code = Some(Box::new(batch_failure_code(*code, irreversible)));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::dispatch_utils::write_definitely_not_applied;

    #[test]
    fn a_definite_refusal_after_an_irreversible_write_keeps_the_records() {
        let code = ErrorCode::RejectedConstraint {
            constraint: "unique".into(),
            detail: "duplicate key".into(),
        };
        let answered = batch_failure_code(code, true);
        assert!(matches!(answered, ErrorCode::Internal { .. }));
        assert!(!write_definitely_not_applied(&answered));
    }

    #[test]
    fn a_definite_refusal_with_every_write_rolled_back_stays_definite() {
        let code = ErrorCode::RejectedConstraint {
            constraint: "unique".into(),
            detail: "duplicate key".into(),
        };
        assert_eq!(batch_failure_code(code.clone(), false), code);
    }

    /// A predicate delete runs untracked. A later sub-plan refuses the batch,
    /// the rollback leaves the delete in place, and the answer must not claim
    /// that nothing applied.
    #[test]
    fn a_batch_refused_after_an_untracked_delete_keeps_its_records() {
        use std::collections::HashMap;

        use nodedb_physical::physical_plan::{CrdtOp, KvOp};
        use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate, Value};

        use crate::bridge::envelope::Status;
        use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let did = task.request.database_id.as_u64();
        let tid = task.request.tenant_id.as_u64();
        let collection = QualifiedCollection::new(DatabaseId::DEFAULT, "items");
        let value = zerompk::to_msgpack_vec(&Value::Object(HashMap::from([(
            "v".to_string(),
            Value::Integer(1),
        )])))
        .expect("encode value");
        let seed = PhysicalPlan::Kv(KvOp::Put {
            collection: collection.clone(),
            key: b"k1".to_vec(),
            value,
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
        });
        let seeded = core.execute_transaction_batch(&task, tid, &[seed], &[], None);
        assert_eq!(seeded.status, Status::Ok, "seed put must apply");

        let delete_all = PhysicalPlan::Kv(KvOp::PredicateDelete {
            collection: collection.clone(),
            filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });
        let refused = PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "doc".into(),
            delta: vec![1],
            peer_id: 1,
            mutation_id: 1,
            surrogate: Surrogate::ZERO,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        });
        let response =
            core.execute_transaction_batch(&task, tid, &[delete_all, refused], &[], None);

        assert_eq!(response.status, Status::Error);
        let code = response.error_code.map(|code| *code);
        assert!(
            matches!(code, Some(ErrorCode::Internal { .. })),
            "got {code:?}"
        );
        let now = crate::engine::kv::current_ms();
        assert_eq!(
            core.kv_engine.get(did, tid, "items", b"k1", now),
            None,
            "the untracked delete stays after the rollback"
        );
    }
}
