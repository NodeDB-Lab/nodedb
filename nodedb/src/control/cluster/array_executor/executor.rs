// SPDX-License-Identifier: BUSL-1.1

//! The `DataPlaneArrayExecutor` type and its shared SPSC dispatch scaffolding.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb_array::types::ArrayId;
use nodedb_cluster::error::{ClusterError, Result};

use crate::bridge::envelope::{Priority, Request};
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
    ) -> Result<crate::bridge::envelope::Response> {
        let request_id = self.state.next_request_id();
        let request = local_request(request_id, array_id, local_vshard_id, plan, txn_id);

        let mut rx = self.state.tracker.register(request_id);

        let dispatch_result = match self.state.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };

        if let Err(e) = dispatch_result {
            return Err(ClusterError::Storage {
                detail: format!("array executor dispatch: {e}"),
            });
        }

        match tokio::time::timeout(LOCAL_DISPATCH_TIMEOUT, async { rx.recv().await.ok_or(()) })
            .await
        {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err(ClusterError::Storage {
                detail: "array executor: response channel closed".into(),
            }),
            Err(_) => Err(ClusterError::Storage {
                detail: "array executor: local dispatch timed out".into(),
            }),
        }
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

        let plan = PhysicalPlan::Array(ArrayOp::Delete {
            array_id: array_id.clone(),
            coords_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
        });

        let vshard_id = VShardId::new(19);
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
        });

        let read_request = local_request(RequestId::new(8), &array_id, vshard_id, read, None);
        let write_request = local_request(RequestId::new(9), &array_id, vshard_id, write, None);

        assert_eq!(read_request.vshard_id, vshard_id);
        assert_eq!(write_request.vshard_id, vshard_id);
    }
}
