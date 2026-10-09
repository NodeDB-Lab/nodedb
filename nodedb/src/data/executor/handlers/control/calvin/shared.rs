// SPDX-License-Identifier: BUSL-1.1

//! `CalvinExecCtx`, the helpers the static and active Calvin stage paths
//! share, and the test fixtures every concern file's tests build on.

use crate::bridge::envelope::{ErrorCode, Response, StageVote};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Execution context shared by both static and active Calvin handler variants.
///
/// Bundles the epoch-scoped parameters that repeat across
/// `execute_calvin_execute_static` and `execute_calvin_execute_active`,
/// keeping each function's argument count within the lint budget.
pub(in crate::data::executor) struct CalvinExecCtx {
    pub epoch: u64,
    pub position: u32,
    pub epoch_system_ms: i64,
}

impl CoreLoop {
    /// Refuse a Calvin stage that staged nothing, with the vote `error` names.
    ///
    /// `OllpRetryRequired` votes `PredictionDrift`. Every other error votes
    /// `ParticipantError`.
    pub(super) fn calvin_stage_refusal(&self, task: &ExecutionTask, error: ErrorCode) -> Response {
        let vote = StageVote::of_stage_error(&error);
        let mut response = self.response_error(task, error);
        response.stage_vote = Some(vote);
        response
    }

    /// Clean failed staging and return its abort vote.
    /// Defensive removal clears document/KV and graph overlays plus their gauge.
    pub(super) fn calvin_stage_failure<E>(
        &mut self,
        task: &ExecutionTask,
        epoch: u64,
        position: u32,
        vshard_id: u32,
        error: E,
    ) -> Response
    where
        E: Into<ErrorCode>,
    {
        self.calvin
            .commit_pending
            .remove(&(epoch, position, vshard_id));
        self.drop_calvin_synthetic_overlay(epoch, position, vshard_id);
        // The scheduler proposes this abort vote and waits for the global
        // verdict before it drops anything.
        self.calvin_stage_refusal(task, error.into())
    }
}

#[cfg(test)]
pub(in crate::data::executor) mod test_support {
    use std::time::{Duration, Instant};

    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, TimeseriesOp};
    use nodedb_types::{QualifiedCollection, Surrogate, Value};

    use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::doc_format;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{DatabaseId, RequestId, TenantId, TraceId, VShardId};

    /// A minimal `ExecutionTask` homing to vShard 0, tenant 1, database
    /// DEFAULT -- everything a Calvin static-execute handler needs beyond
    /// its explicit `CalvinExecCtx` / `tenant_id` / `plans` arguments.
    pub(in crate::data::executor::handlers::control::calvin) fn make_task() -> ExecutionTask {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "x"),
            document_id: "y".into(),
            surrogate: None,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        let request = Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: crate::types::ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            commit_hlc: None,
            admission: Admission::Exempt(ExemptReason::Read),
        };
        ExecutionTask::new(request)
    }

    pub(in crate::data::executor::handlers::control::calvin) fn doc_value(
        field: &str,
        val: &str,
    ) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert(field.to_string(), Value::String(val.into()));
        nodedb_types::value_to_msgpack(&Value::Object(obj)).unwrap()
    }

    pub(in crate::data::executor) fn point_insert_plan(
        collection: &str,
        document_id: &str,
        surrogate: u32,
    ) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            document_id: document_id.to_string(),
            value: doc_value("a", "1"),
            if_absent: false,
            surrogate: Surrogate::new(surrogate),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        })
    }

    pub(in crate::data::executor::handlers::control::calvin) fn canonical_ilp_plan(
        collection: &str,
        lines: Vec<&str>,
        tokens: Vec<u32>,
    ) -> PhysicalPlan {
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            payload: zerompk::to_msgpack_vec(&lines).expect("canonical ILP payload"),
            format: "ilp-msgpack".to_owned(),
            wal_lsn: None,
            surrogates: tokens.into_iter().map(Surrogate::new).collect(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    pub(in crate::data::executor) fn bulk_delete_plan(
        collection: &str,
        predicted: Option<Vec<u32>>,
    ) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            filters: Vec::new(),
            returning: None,
            ollp_predicted_surrogates: predicted,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    /// Seed a row directly into base storage (bypassing Calvin staging), the
    /// pre-existing state the active-path OLLP verifier scans against.
    pub(in crate::data::executor) fn seed_row(
        core: &mut CoreLoop,
        collection: &str,
        surrogate: u32,
    ) {
        let doc_id = nodedb_types::StorageKey::for_surrogate(Surrogate::new(surrogate));
        let body = doc_format::canonicalize_document_for_storage(&doc_value("a", "1"));
        core.sparse
            .put(DatabaseId::DEFAULT.as_u64(), 1, collection, &doc_id, &body)
            .expect("seed row");
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::make_task;
    use crate::bridge::envelope::{ErrorCode, StageVote, Status};
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::control::calvin_txn_id::calvin_synthetic_txn_id;

    #[test]
    fn a_stage_drift_refusal_votes_prediction_drift_and_drops_staged_state() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();
        let vshard = task.request.vshard_id.as_u32();
        let synthetic = calvin_synthetic_txn_id(4, 2, vshard).unwrap();
        core.graph_txn_overlay_mut(synthetic);

        let response = core.calvin_stage_failure(&task, 4, 2, vshard, ErrorCode::OllpRetryRequired);

        assert_eq!(response.status, Status::Error);
        assert_eq!(response.stage_vote, Some(StageVote::PredictionDrift));
        assert_eq!(
            response.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired)
        );
        assert!(!core.graph_txn_overlays.contains_key(&synthetic));
    }

    #[test]
    fn any_other_stage_refusal_votes_participant_error() {
        let dir = tempfile::tempdir().unwrap();
        let (core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();

        let response = core.calvin_stage_refusal(
            &task,
            ErrorCode::Internal {
                detail: "stage".into(),
            },
        );

        assert_eq!(response.status, Status::Error);
        assert_eq!(response.stage_vote, Some(StageVote::ParticipantError));
    }
}
