// SPDX-License-Identifier: BUSL-1.1

//! The `DataPlaneArrayExecutor` type and its shared SPSC dispatch scaffolding.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb_array::types::ArrayId;
use nodedb_cluster::error::{ClusterError, Result};
use nodedb_cluster::rpc_codec::DataPlaneErrorCode;

use super::refusal::execution_error;
use crate::bridge::envelope::{Priority, Request, Response};
use crate::control::cluster::linearizable_read::{
    confirm_linearizable_read, groups_of_vshards, statement_read_deadline,
};
use crate::control::server::shared::write_admission::plan_is_write;
use crate::control::state::SharedState;
use crate::event::types::EventSource;
use crate::types::{ReadConsistency, RequestId, TraceId, TxnId, VShardId};
use nodedb_physical::physical_plan::PhysicalPlan;

/// Timeout for a single shard-side array operation dispatched through the
/// local SPSC bridge. This bounds how long the cluster handler waits for the
/// Data Plane to respond before returning an error to the coordinator.
const LOCAL_DISPATCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Concrete implementation of `ArrayLocalExecutor` backed by the local Data Plane.
///
/// Holds a reference to `SharedState` so it can dispatch `PhysicalPlan::Array`
/// variants through the SPSC bridge and await their responses via the
/// `RequestTracker`.
pub struct DataPlaneArrayExecutor {
    pub(super) state: Arc<SharedState>,
}

impl DataPlaneArrayExecutor {
    /// Construct an executor backed by the given shared state.
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }

    /// Dispatch a `PhysicalPlan` through the local SPSC bridge and await the
    /// single (non-streaming) response.
    ///
    /// `txn_id` is the reading transaction's id for read-your-own-writes
    /// against this shard's staging overlay. `None` for an autocommit read
    /// and for every write: a cluster array write is never inside a
    /// transaction block, where it is staged per shard instead.
    pub(super) async fn dispatch_and_await(
        &self,
        array_id: &ArrayId,
        local_vshard_id: VShardId,
        plan: PhysicalPlan,
        txn_id: Option<TxnId>,
    ) -> Result<Response> {
        // Array reads have no weaker consistency: a shard read confirms its
        // group on this node before it reads.
        if !plan_is_write(&plan) {
            let groups = groups_of_vshards(&self.state, [local_vshard_id.as_u32()])
                .map_err(|e| execution_error("array read confirmation", e))?;
            confirm_linearizable_read(&self.state, &groups, statement_read_deadline(&self.state))
                .await
                .map_err(|e| execution_error("array read confirmation", e))?;
        }
        let request_id = self.state.next_request_id();
        let request = local_request(request_id, array_id, local_vshard_id, plan, txn_id);

        let mut rx = self.state.tracker.register(request_id);

        // A shard fan-out sends one request per shard, which can exceed the
        // tenant's in-flight cap: each request waits for a freed slot until
        // its deadline instead of refusing the statement.
        let dispatch_result = crate::control::server::dispatch_utils::dispatch_when_capacity_frees(
            &self.state,
            request,
            None,
        )
        .await;

        // A dispatch refusal, such as a capacity limit past the deadline,
        // keeps its own class.
        if let Err(e) = dispatch_result {
            return Err(execution_error("array executor dispatch", e));
        }

        await_local_response(rx.recv()).await
    }
}

/// Await the local Data Plane's response. A timeout is the typed
/// `DeadlineExceeded` verdict, which the coordinator renders as `57014`.
async fn await_local_response(
    rx: impl std::future::Future<Output = Option<Response>>,
) -> Result<Response> {
    match tokio::time::timeout(LOCAL_DISPATCH_TIMEOUT, rx).await {
        Ok(Some(resp)) => Ok(resp),
        Ok(None) => Err(ClusterError::Storage {
            detail: "array executor: response channel closed".into(),
        }),
        Err(_) => Err(ClusterError::DataPlane {
            code: DataPlaneErrorCode::DeadlineExceeded,
        }),
    }
}

/// Build the local bridge request from the decoded array identity. The wire
/// requests carry no independent tenant/database envelope, so deriving both
/// fields from this one canonical identity prevents same-name arrays in
/// different scopes from being redirected into a default namespace.
fn local_request(
    request_id: RequestId,
    array_id: &ArrayId,
    local_vshard_id: VShardId,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
) -> Request {
    Request {
        request_id,
        tenant_id: array_id.tenant_id,
        database_id: array_id.database_id,
        vshard_id: local_vshard_id,
        plan,
        deadline: Instant::now() + LOCAL_DISPATCH_TIMEOUT,
        priority: Priority::Normal,
        trace_id: TraceId::generate(),
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id,
        wal_lsn: None,
        resolved_now_ms: None,
        commit_hlc: None,
        entry_version: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::AlreadyOrdered,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::{ReplicableWrite, to_replicated_entry};
    use crate::types::{DatabaseId, TenantId};
    use nodedb_physical::physical_plan::{ArrayOp, PhysicalPlan};

    #[test]
    fn non_default_same_name_array_keeps_scope_in_replication_and_local_request() {
        let tenant_id = TenantId::new(41);
        let database_id = DatabaseId::new(73);
        let array_id = ArrayId::in_database(tenant_id, database_id, "measurements");
        let default_scope_array = ArrayId::new(tenant_id, "measurements");
        assert_eq!(array_id.name, default_scope_array.name);
        assert_eq!(array_id.tenant_id, default_scope_array.tenant_id);
        assert_ne!(array_id.database_id, default_scope_array.database_id);

        let vshard_id = VShardId::new(19);
        let plan = PhysicalPlan::Array(ArrayOp::Delete {
            array_id: array_id.clone(),
            coords_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
            vshard_id: vshard_id.as_u32(),
        });
        let replicable = ReplicableWrite::decide_for_replication(&plan)
            .expect("array write plan carries no live RLS predicate");
        let entry = to_replicated_entry(tenant_id, database_id, vshard_id, &replicable)
            .expect("array write plan encode must not error")
            .expect("array write plan must be replicated");
        assert_eq!(entry.tenant_id, tenant_id.as_u64());
        assert_eq!(entry.database_id, database_id.as_u64());

        let request = local_request(RequestId::new(7), &array_id, vshard_id, plan, None);
        assert_eq!(request.tenant_id, tenant_id);
        assert_eq!(request.database_id, database_id);
        assert_eq!(request.vshard_id, vshard_id);
    }

    /// A local timeout crosses as the typed deadline verdict, and the
    /// coordinator renders it as `57014`.
    #[tokio::test(start_paused = true)]
    async fn a_local_timeout_is_a_typed_deadline() {
        let error = await_local_response(std::future::pending::<Option<Response>>())
            .await
            .expect_err("a pending response must time out");
        assert!(
            matches!(
                &error,
                ClusterError::DataPlane {
                    code: DataPlaneErrorCode::DeadlineExceeded
                }
            ),
            "expected a typed deadline, got {error:?}"
        );
        let rebuilt = crate::control::cluster::array_cluster_helpers::cluster_err(error);
        let (_, state, _) = crate::control::server::pgwire::types::error_to_sqlstate(&rebuilt);
        assert_eq!(state, nodedb_types::error::sqlstate::QUERY_CANCELED.0);
    }

    #[test]
    fn nonzero_vshard_is_preserved_for_read_and_write_requests() {
        let array_id = ArrayId::new(TenantId::new(41), "measurements");
        let vshard_id = VShardId::new(19);
        let read = PhysicalPlan::Array(ArrayOp::SurrogateBitmapScan {
            array_id: array_id.clone(),
            slice_msgpack: Vec::new(),
        });
        let write = PhysicalPlan::Array(ArrayOp::Delete {
            array_id: array_id.clone(),
            coords_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
            vshard_id: vshard_id.as_u32(),
        });

        let read_request = local_request(RequestId::new(8), &array_id, vshard_id, read, None);
        let write_request = local_request(RequestId::new(9), &array_id, vshard_id, write, None);

        assert_eq!(read_request.vshard_id, vshard_id);
        assert_eq!(write_request.vshard_id, vshard_id);
    }
}
