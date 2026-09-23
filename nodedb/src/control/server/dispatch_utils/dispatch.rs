// SPDX-License-Identifier: BUSL-1.1

//! The dispatch core: resolves Exchange data-movement nodes, then hands the
//! plan to the shared Control-Plane write funnel (`submit_write`), which owns
//! write admission, the WAL append, the enqueue, and the response collect.

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::server::shared::clone_write::CloneCheckedTask;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

use super::submit_write::{
    ChangeFeedOwner, SubmitWrite, WalDurability, WriteOrdering, submit_write,
};
use super::types::{AutocommitWrite, DataPlaneDispatch, WriteDispatch};

/// Dispatch a clone-checked, capability-bearing external task to the Data Plane.
pub async fn dispatch_authorized_to_data_plane(
    shared: &SharedState,
    checked: CloneCheckedTask,
    trace_id: TraceId,
) -> crate::Result<Response> {
    let task = checked.into_authorized().into_physical_task();
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id: task.tenant_id,
            database_id: task.database_id,
            vshard_id: task.vshard_id,
            plan: task.plan,
            trace_id,
            event_source: crate::event::EventSource::User,
            txn_id: task.txn_id,
            durability: WalDurability::CallerSupplied {
                wal_lsn: None,
                resolved_now_ms: None,
            },
        },
    )
    .await
}

/// Dispatch a clone-checked, capability-bearing external autocommit write.
pub async fn dispatch_authorized_autocommit_write(
    shared: &SharedState,
    checked: CloneCheckedTask,
    trace_id: TraceId,
) -> crate::Result<Response> {
    let task = checked.into_authorized().into_physical_task();
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id: task.tenant_id,
            database_id: task.database_id,
            vshard_id: task.vshard_id,
            plan: task.plan,
            trace_id,
            event_source: crate::event::EventSource::User,
            txn_id: task.txn_id,
            durability: WalDurability::AppendHere {
                now_override: None,
                apply_key: 0,
            },
        },
    )
    .await
}

/// Dispatch a capability-bearing external autocommit write with an explicit
/// event source.
///
/// Same durability contract as [`dispatch_authorized_autocommit_write`] — the
/// funnel mints the redo under the write-admission guard and the durable-at-ack
/// barrier covers it — but the write is tagged with the caller's event source so
/// a synced write does not re-fire AFTER triggers on the receiving node.
pub(crate) async fn dispatch_authorized_autocommit_write_with_source(
    shared: &SharedState,
    checked: CloneCheckedTask,
    trace_id: TraceId,
    event_source: crate::event::EventSource,
) -> crate::Result<Response> {
    let task = checked.into_authorized().into_physical_task();
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id: task.tenant_id,
            database_id: task.database_id,
            vshard_id: task.vshard_id,
            plan: task.plan,
            trace_id,
            event_source,
            txn_id: task.txn_id,
            durability: WalDurability::AppendHere {
                now_override: None,
                apply_key: 0,
            },
        },
    )
    .await
}

/// Dispatch a trusted internal physical plan to the Data Plane and await the response.
///
/// Creates a request envelope, registers with the tracker for correlation,
/// dispatches via the SPSC bridge, and awaits the response with a timeout.
pub(crate) async fn dispatch_to_data_plane(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<Response> {
    dispatch_to_data_plane_with_source(
        shared,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        crate::event::EventSource::User,
    )
    .await
}

/// Dispatch a physical plan to the Data Plane with an explicit event source.
///
/// Trigger-generated writes pass `EventSource::Trigger` so the Data Plane
/// emits WriteEvents with the correct source tag (preventing cascade
/// re-triggering in the Event Plane).
pub(crate) async fn dispatch_to_data_plane_with_source(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    event_source: crate::event::EventSource,
) -> crate::Result<Response> {
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source,
            txn_id: None,
            // The caller (trigger / sync / internal funnel) owns durability on its
            // own path; the funnel does not append here.
            durability: WalDurability::CallerSupplied {
                wal_lsn: None,
                resolved_now_ms: None,
            },
        },
    )
    .await
}

/// Dispatch a write to the Data Plane carrying the WAL LSN allocated for it.
///
/// Used by autocommit write endpoints that call `wal_append_if_write` and then
/// dispatch: the returned LSN is stamped onto the `Request` so the Data Plane
/// records the committed per-key / per-collection write version. The write's
/// identity and LSN travel in a [`WriteDispatch`] to keep the argument list
/// short; `wal_lsn` is `None` when the write was WAL-bypassed (e.g.
/// `timeseries` `wal=false`). `resolved_now_ms` carries the wall-clock instant
/// the Control Plane resolved for a TTL-bearing KV write's `expire_at_ms` — see
/// [`WriteDispatch::resolved_now_ms`].
pub(crate) async fn dispatch_trusted_internal_write_to_data_plane(
    shared: &SharedState,
    write: WriteDispatch,
) -> crate::Result<Response> {
    let WriteDispatch {
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
        txn_id,
        wal_lsn,
        resolved_now_ms,
    } = write;
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source,
            txn_id,
            // Caller pre-appended and supplied `wal_lsn` (e.g. the procedural
            // batch-flush path whose dispatched plan is a `TransactionBatch`
            // whose per-task records were appended upstream): the funnel must not
            // append again.
            durability: WalDurability::CallerSupplied {
                wal_lsn,
                resolved_now_ms,
            },
        },
    )
    .await
}

/// Dispatch an autocommit write whose WAL append the funnel performs *under the
/// write-admission guard*, immediately before the enqueue.
///
/// This is the entry point for single-node local writes that own their own
/// autocommit durability (the native SQL / direct-op boot path, HTTP query,
/// RESP KV write, protocol-neutral INSERT/UPSERT). The WAL LSN must be minted
/// after admission and just before the dispatcher enqueue so that WAL-LSN order
/// equals Data-Plane apply order per key; performing the append inside the
/// funnel (rather than at the caller, before admission) is what closes that
/// ordering gap. `wal_lsn` / `resolved_now_ms` are therefore *not* caller
/// inputs — the funnel resolves them.
pub(crate) async fn dispatch_autocommit_write(
    shared: &SharedState,
    write: AutocommitWrite,
) -> crate::Result<Response> {
    let AutocommitWrite {
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
        txn_id,
    } = write;
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source,
            txn_id,
            // The funnel appends the WAL record under the admission guard just
            // before enqueue and stamps the minted LSN onto the `Request`.
            durability: WalDurability::AppendHere {
                now_override: None,
                apply_key: 0,
            },
        },
    )
    .await
}

/// Dispatch a physical plan to the Data Plane carrying an explicit transaction
/// id so the Data Plane can resolve this transaction's staging overlay
/// (read-your-own-writes) and route `StageWrite`. Used by the native endpoint,
/// whose in-transaction tasks flow through this shared path.
pub(crate) async fn dispatch_to_data_plane_with_txn(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<crate::types::TxnId>,
) -> crate::Result<Response> {
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source: crate::event::EventSource::User,
            txn_id,
            // Staged in-transaction writes are not yet durably committed; the
            // committed write version is recorded at COMMIT via the batch funnel,
            // so durability is not the funnel's to append here.
            durability: WalDurability::CallerSupplied {
                wal_lsn: None,
                resolved_now_ms: None,
            },
        },
    )
    .await
}

async fn dispatch_to_data_plane_inner(
    shared: &SharedState,
    params: DataPlaneDispatch,
) -> crate::Result<Response> {
    let DataPlaneDispatch {
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
        txn_id,
        durability,
    } = params;
    // Resolve any Exchange data-movement nodes before dispatch: a root-level
    // Gather fans the child to all cores and returns the merged response here;
    // a Broadcast join child is gathered and embedded so the plan reaching a
    // core is self-contained. Safe no-op for the many non-Exchange callers
    // (writes, metrics, triggers). Catalog materialization is identity-scoped
    // and already done upstream on the pgwire/native paths.
    // Internal funnel (COPY, cursors, materialized-view refresh, constraint
    // subqueries): not session-transaction-scoped, so `None`.
    let plan = match crate::control::server::exchange::resolve_exchange_in_plan(
        shared,
        database_id,
        tenant_id,
        plan,
        trace_id,
        None,
    )
    .await?
    {
        crate::control::server::exchange::Resolved::Gathered(
            resp,
            _shard_watermarks,
            _shuffle_reads,
        ) => {
            return Ok(resp);
        }
        crate::control::server::exchange::Resolved::Plan(p) => *p,
        // Internal funnel callers want a fully-collected Response, not a lazy
        // stream: materialize the stream into one merged-array Response,
        // preserving the prior gather-then-return behaviour on this path.
        crate::control::server::exchange::Resolved::Stream(s) => {
            return crate::control::server::exchange::gather::stream_to_response(s).await;
        }
    };

    submit_write(
        shared,
        SubmitWrite {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source,
            txn_id,
            // Internal / autocommit funnel: no session user to attribute.
            user_id: None,
            durability,
            ordering: WriteOrdering::Gate,
            // The autocommit / internal funnel is the path that feeds `/cdc`
            // and WS-RPC subscribers; every other caller of `submit_write` is
            // `Unowned`.
            change_feed: ChangeFeedOwner::Funnel,
        },
    )
    .await
    .map(|outcome| outcome.response)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use nodedb_array::types::ArrayId;
    use nodedb_physical::physical_plan::ArrayOp;
    use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

    use super::dispatch_authorized_autocommit_write_with_source;
    use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide, Dispatcher};
    use crate::bridge::envelope::{Payload, Status};
    use crate::control::state::SharedState;
    use crate::engine::array::wal::ArrayPutCell;
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
    use crate::wal::WalManager;

    const ARRAY: &str = "grid";

    fn fixture() -> (Arc<SharedState>, CoreChannelDataSide, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("autocommit.wal"))
                .expect("test WAL"),
        );
        let (dispatcher, mut sides) = Dispatcher::new(1, 64);
        let side = sides.pop().expect("one data side");
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        (state, side, directory)
    }

    fn array_put_task(tenant_id: TenantId) -> PhysicalTask {
        // An empty cell batch is a valid encoding; what this exercises is the
        // durability handling of the plan shape, not the cells.
        let cells: Vec<ArrayPutCell> = Vec::new();
        PhysicalTask {
            tenant_id,
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::from_collection_in_database(DatabaseId::DEFAULT, ARRAY),
            plan: crate::bridge::envelope::PhysicalPlan::Array(ArrayOp::Put {
                array_id: ArrayId::in_database(tenant_id, DatabaseId::DEFAULT, ARRAY),
                cells_msgpack: zerompk::to_msgpack_vec(&cells).expect("encode cells"),
                wal_lsn: 0,
                provenance: None,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    /// Answer one request, returning the plan's stamped LSN to the caller.
    async fn respond_once_capturing_lsn(
        state: Arc<SharedState>,
        mut side: CoreChannelDataSide,
        stamped: Arc<std::sync::Mutex<Option<u64>>>,
    ) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut handled = false;
        while !handled && Instant::now() < deadline {
            if let Ok(request) = side.request_rx.try_pop() {
                if let crate::bridge::envelope::PhysicalPlan::Array(ArrayOp::Put {
                    wal_lsn, ..
                }) = &request.inner.plan
                {
                    *stamped.lock().expect("stamped lock") = Some(*wal_lsn);
                }
                side.response_tx
                    .try_push(BridgeResponse {
                        inner: crate::bridge::envelope::Response {
                            request_id: request.inner.request_id,
                            status: Status::Ok,
                            attempt: 1,
                            partial: false,
                            payload: Payload::empty(),
                            watermark_lsn: Lsn::ZERO,
                            error_code: None,
                            read_set_valid: None,
                            read_version_lsn: Lsn::ZERO,
                            write_set: Vec::new(),
                        },
                    })
                    .expect("fake data-plane response queue has capacity");
                handled = true;
            }
            state.poll_and_route_responses();
            tokio::task::yield_now().await;
        }
        assert!(handled, "fake data plane received the dispatched request");
        state.poll_and_route_responses();
    }

    /// The array sync inbound path acks its peer off this dispatch and nothing
    /// upstream appends a redo for it, so the funnel must own the record: mint
    /// it, stamp it into the plan (the array engine versions its tiles from the
    /// LSN carried there, and replay stamps the same version off the record
    /// header — a zero would make the two disagree), and hold the reply behind
    /// the durable-at-ack barrier.
    #[tokio::test]
    async fn an_autocommit_write_mints_stamps_and_fsyncs_its_own_redo() {
        let (state, side, _directory) = fixture();
        let tenant_id = TenantId::new(1);
        let task = array_put_task(tenant_id);
        let identity =
            crate::control::security::identity::AuthenticatedIdentity::new_internal_service(
                1,
                "array-durability-test",
                tenant_id,
                Vec::new(),
                true,
                None,
                crate::control::security::identity::AuthenticatedIdentity::default_database_set(
                    true,
                ),
            );
        let checked = match crate::control::server::shared::clone_write::intercept_and_authorize(
            crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                state: &state,
                task,
                identity: &identity,
                tenant_id,
                permissions: &state.permissions,
                roles: &state.roles,
                emitter: &crate::control::security::audit::NoopAuditEmitter,
            },
        )
        .await
        .expect("clone-check and authorize test task")
        {
            crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(t) => t,
            crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(_) => {
                panic!("array put must not be clone-intercepted")
            }
        };

        let stamped = Arc::new(std::sync::Mutex::new(None));
        let responder = tokio::spawn(respond_once_capturing_lsn(
            Arc::clone(&state),
            side,
            Arc::clone(&stamped),
        ));
        let response = dispatch_authorized_autocommit_write_with_source(
            &state,
            checked,
            crate::types::TraceId::ZERO,
            crate::event::EventSource::CrdtSync,
        )
        .await
        .expect("autocommit array write succeeds");
        responder.await.expect("responder completes");

        assert_eq!(response.status, Status::Ok);
        let stamped = stamped.lock().expect("stamped lock").expect("an array put");
        assert!(
            stamped > 0,
            "the plan the Data Plane executes must carry the minted LSN, not a zero"
        );
        assert!(
            state.wal.durable_through() >= stamped,
            "the minted redo must be fsync-durable before the write is acknowledged"
        );
    }
}
