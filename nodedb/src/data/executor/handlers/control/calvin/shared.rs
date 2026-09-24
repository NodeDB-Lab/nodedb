// SPDX-License-Identifier: BUSL-1.1

//! `CalvinExecCtx`, the helpers the static and active Calvin stage paths
//! share, and the test fixtures every concern file's tests build on.

use crate::bridge::envelope::{ErrorCode, Response};
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
    pub is_group_leader: bool,
}

impl CoreLoop {
    /// Clean failed static staging and return an explicit abort vote.
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
        self.calvin
            .fence
            .note_resolved((epoch, position, vshard_id), None);
        let mut response = self.response_error(task, error.into());
        // Scheduler treats this as a durable local abort vote and still waits
        // for the authoritative global verdict before issuing any drop.
        response.read_set_valid = Some(false);
        response
    }
}

#[cfg(test)]
pub(in crate::data::executor::handlers::control::calvin) mod test_support {
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
            surrogate: Surrogate::ZERO,
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
        zerompk::to_msgpack_vec(&Value::Object(obj)).unwrap()
    }

    pub(in crate::data::executor::handlers::control::calvin) fn point_insert_plan(
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

    pub(in crate::data::executor::handlers::control::calvin) fn bulk_delete_plan(
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
    pub(in crate::data::executor::handlers::control::calvin) fn seed_row(
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
