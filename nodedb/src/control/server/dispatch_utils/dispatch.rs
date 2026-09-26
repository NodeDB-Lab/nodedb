// SPDX-License-Identifier: BUSL-1.1

//! The dispatch core: resolves Exchange data-movement nodes, then hands the
//! plan to the shared Control-Plane write funnel (`submit_write`), which owns
//! write admission, the WAL append, the enqueue, and the response collect.

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::server::shared::clone_write::CloneCheckedTask;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

use super::minted::{MintedRecords, RecordOwner};
use super::submit_write::{
    ChangeFeedOwner, SubmitWrite, WalDurability, WriteOrdering, submit_write,
};
use super::types::{AutocommitWrite, DataPlaneDispatch, WriteDispatch};

/// Dispatch a clone-checked, capability-bearing external task to the Data Plane.
///
/// The read route: it appends no WAL record. A write whose caller owns no
/// record for it goes through `dispatch_authorized_durable_write`, or
/// `dispatch_authorized_task_by_class` where one call site carries both.
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
                minted: None,
            },
        },
    )
    .await
}

/// Dispatch a clone-checked task whose records the caller already appended
/// under `minted`. The request carries the highest of their LSNs, so the
/// write is durable before it is acknowledged. The funnel closes their
/// outcome-floor window from the task's outcome.
pub(crate) async fn dispatch_authorized_minted_to_data_plane(
    shared: &SharedState,
    checked: CloneCheckedTask,
    trace_id: TraceId,
    minted: MintedRecords,
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
                wal_lsn: minted.highest(),
                resolved_now_ms: None,
                minted: Some(minted),
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
                commit_hlc: None,
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
                commit_hlc: None,
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
                minted: None,
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
        minted,
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
            // Caller pre-appended and supplied `wal_lsn`: the funnel must not
            // append again.
            durability: WalDurability::CallerSupplied {
                wal_lsn,
                resolved_now_ms,
                minted,
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
                commit_hlc: None,
            },
        },
    )
    .await
}

/// Dispatch a physical plan to the Data Plane carrying an explicit transaction
/// id so the Data Plane can resolve this transaction's staging overlay
/// (read-your-own-writes) and route `StageWrite`. Used by the native endpoint,
/// whose in-transaction tasks flow through this shared path.
///
/// It appends no WAL record, so it refuses a write that only the funnel's
/// `AppendHere` route logs. A staged write is not such a write: COMMIT logs it.
pub(crate) async fn dispatch_to_data_plane_with_txn(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<crate::types::TxnId>,
) -> crate::Result<Response> {
    super::durability_barrier::refuse_unlogged_write(&plan)?;
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
                minted: None,
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
        mut durability,
    } = params;
    let owner = RecordOwner {
        tenant_id,
        database_id,
        vshard_id,
    };
    // A write that carries its own records is never a query. Only a query
    // plan holds Exchange nodes, and resolving one fans it out to the cores,
    // so a record-carrying query would reach the cores before any close.
    let plan = if durability.has_minted() {
        if matches!(plan, PhysicalPlan::Query(_)) {
            if let Some(minted) = durability.take_minted() {
                minted.cancel(&shared.wal, owner, 0).await?;
            }
            return Err(crate::Error::Internal {
                detail: "a write carrying WAL records reached the funnel as a query plan; \
                         nothing was dispatched"
                    .into(),
            });
        }
        plan
    } else {
        // Resolve any Exchange data-movement nodes before dispatch: a
        // root-level Gather fans the child to all cores and returns the merged
        // response here. A Broadcast join child is gathered and embedded so
        // the plan reaching a core is self-contained. Plans with no Exchange
        // node pass through unchanged. Catalog materialization is
        // identity-scoped and already done upstream on the pgwire and native
        // paths. The internal funnel is not session-transaction-scoped, so the
        // transaction id is `None`.
        let resolved = crate::control::server::exchange::resolve_exchange_in_plan(
            shared,
            database_id,
            tenant_id,
            plan,
            trace_id,
            None,
        )
        .await?;
        match resolved {
            crate::control::server::exchange::Resolved::Plan(p) => *p,
            crate::control::server::exchange::Resolved::Gathered(
                resp,
                _shard_watermarks,
                _shuffle_reads,
            ) => return Ok(resp),
            // Internal funnel callers want one merged `Response`, not a lazy
            // stream.
            crate::control::server::exchange::Resolved::Stream(s) => {
                return crate::control::server::exchange::gather::stream_to_response(s).await;
            }
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

    // --- Caller records under the outcome floor ---

    /// A read plan: the funnel admits it without a gate, so these tests reach
    /// the dispatch and response paths with the caller's records attached.
    fn point_get_plan() -> crate::bridge::envelope::PhysicalPlan {
        crate::bridge::envelope::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::PointGet {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
                document_id: "u1".into(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            },
        )
    }

    fn minted_record(state: &SharedState) -> (super::MintedRecords, Lsn) {
        let minted = super::MintedRecords::open(&state.outcome_floor);
        let lsn = minted
            .appender(&state.wal, crate::wal::manager::NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"row",
            )
            .expect("append");
        (minted, lsn)
    }

    fn write_with(minted: super::MintedRecords, lsn: Lsn) -> super::WriteDispatch {
        super::WriteDispatch {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: point_get_plan(),
            trace_id: crate::types::TraceId::ZERO,
            event_source: crate::event::EventSource::User,
            txn_id: None,
            wal_lsn: Some(lsn),
            resolved_now_ms: None,
            minted: Some(minted),
        }
    }

    fn replayed(state: &SharedState) -> Vec<u64> {
        state.wal.sync().expect("sync");
        state
            .wal
            .replay()
            .expect("replay")
            .iter()
            .map(|record| record.header.lsn)
            .collect()
    }

    /// Answer one request with `status` and `code`.
    async fn respond_once_with(
        state: Arc<SharedState>,
        mut side: CoreChannelDataSide,
        status: Status,
        code: Option<crate::bridge::envelope::ErrorCode>,
    ) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut handled = false;
        while !handled && Instant::now() < deadline {
            if let Ok(request) = side.request_rx.try_pop() {
                side.response_tx
                    .try_push(BridgeResponse {
                        inner: crate::bridge::envelope::Response {
                            request_id: request.inner.request_id,
                            status,
                            attempt: 1,
                            partial: false,
                            payload: Payload::empty(),
                            watermark_lsn: Lsn::ZERO,
                            error_code: code.clone().map(Box::new),
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

    #[tokio::test]
    async fn a_refused_dispatch_cancels_the_callers_records() {
        let (state, _side, _directory) = fixture();
        let (minted, lsn) = minted_record(&state);
        state
            .dispatcher
            .lock()
            .expect("dispatcher")
            .begin_data_plane_drain();

        let result =
            super::dispatch_trusted_internal_write_to_data_plane(&state, write_with(minted, lsn))
                .await;

        assert!(result.is_err(), "a draining dispatcher refuses the request");
        assert!(!replayed(&state).contains(&lsn.as_u64()));
        assert!(state.outcome_floor.floor() >= lsn);
        assert_eq!(state.outcome_floor.leaked_windows(), 0);
    }

    #[tokio::test]
    async fn a_refusal_that_applied_nothing_cancels_the_callers_records() {
        let (state, side, _directory) = fixture();
        let (minted, lsn) = minted_record(&state);
        let responder = tokio::spawn(respond_once_with(
            Arc::clone(&state),
            side,
            Status::Error,
            Some(crate::bridge::envelope::ErrorCode::RejectedConstraint {
                constraint: "unique".into(),
                detail: "duplicate key".into(),
            }),
        ));

        let response =
            super::dispatch_trusted_internal_write_to_data_plane(&state, write_with(minted, lsn))
                .await
                .expect("the refusal is a response");
        responder.await.expect("responder completes");

        assert_eq!(response.status, Status::Error);
        assert!(!replayed(&state).contains(&lsn.as_u64()));
        assert!(state.outcome_floor.floor() >= lsn);
    }

    #[tokio::test]
    async fn an_applied_write_settles_the_callers_records() {
        let (state, side, _directory) = fixture();
        let (minted, lsn) = minted_record(&state);
        let responder = tokio::spawn(respond_once_with(
            Arc::clone(&state),
            side,
            Status::Ok,
            None,
        ));

        let response =
            super::dispatch_trusted_internal_write_to_data_plane(&state, write_with(minted, lsn))
                .await
                .expect("the write applies");
        responder.await.expect("responder completes");

        assert_eq!(response.status, Status::Ok);
        assert!(replayed(&state).contains(&lsn.as_u64()));
        assert!(state.outcome_floor.floor() >= lsn);
        assert_eq!(state.outcome_floor.leaked_windows(), 0);
    }

    /// A point write the admission gate serializes on its key.
    fn incr_plan() -> crate::bridge::envelope::PhysicalPlan {
        crate::bridge::envelope::PhysicalPlan::Kv(nodedb_physical::physical_plan::KvOp::Incr {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
            key: b"k1".to_vec(),
            delta: 1,
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::new(1),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            shape: nodedb_physical::physical_plan::KvCounterShape::Raw,
        })
    }

    /// Wait until the floor passes `lsn`, routing responses meanwhile.
    async fn floor_passes(state: &SharedState, lsn: Lsn) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while state.outcome_floor.floor() < lsn && Instant::now() < deadline {
            state.poll_and_route_responses();
            tokio::task::yield_now().await;
        }
        state.outcome_floor.floor() >= lsn
    }

    #[tokio::test]
    async fn a_caller_dropped_while_waiting_for_admission_cancels_its_records() {
        let (state, _side, _directory) = fixture();
        let (minted, lsn) = minted_record(&state);
        let plan = incr_plan();
        let (_, keys) =
            crate::control::server::shared::write_admission::lock_keys::plan_lock_keys(&plan)
                .expect("a point write has a lock key");
        let key = keys.into_iter().next().expect("one key");
        let held = state.write_order_locks.lock_owned(key).await;
        let mut write = write_with(minted, lsn);
        write.plan = plan;

        let waited = tokio::time::timeout(
            Duration::from_millis(50),
            super::dispatch_trusted_internal_write_to_data_plane(&state, write),
        )
        .await;
        drop(held);

        assert!(waited.is_err(), "the write waits behind the held key");
        assert!(
            !replayed(&state).contains(&lsn.as_u64()),
            "a marker names it"
        );
        assert!(state.outcome_floor.floor() >= lsn);
        assert_eq!(state.outcome_floor.leaked_windows(), 0);
        assert_eq!(state.outcome_floor.held_windows(), 0);
    }

    /// Drop the caller once its write is enqueued, then answer the write.
    async fn drop_after_dispatch_then_answer(
        status: Status,
        code: Option<crate::bridge::envelope::ErrorCode>,
    ) -> (Arc<SharedState>, Lsn, tempfile::TempDir) {
        let (state, side, directory) = fixture();
        let (minted, lsn) = minted_record(&state);
        let waited = tokio::time::timeout(
            Duration::from_millis(50),
            super::dispatch_trusted_internal_write_to_data_plane(&state, write_with(minted, lsn)),
        )
        .await;
        assert!(waited.is_err(), "no response arrived before the drop");
        assert!(
            state.outcome_floor.floor() < lsn,
            "the core holds the records"
        );
        respond_once_with(Arc::clone(&state), side, status, code).await;
        assert!(
            floor_passes(&state, lsn).await,
            "the final response closed the window"
        );
        assert_eq!(state.outcome_floor.leaked_windows(), 0);
        (state, lsn, directory)
    }

    #[tokio::test]
    async fn a_caller_dropped_after_dispatch_settles_its_records_from_the_answer() {
        let (state, lsn, _directory) = drop_after_dispatch_then_answer(Status::Ok, None).await;
        assert!(replayed(&state).contains(&lsn.as_u64()));
    }

    #[tokio::test]
    async fn a_caller_dropped_after_dispatch_cancels_its_records_on_a_refusal() {
        let (state, lsn, _directory) = drop_after_dispatch_then_answer(
            Status::Error,
            Some(crate::bridge::envelope::ErrorCode::RejectedConstraint {
                constraint: "unique".into(),
                detail: "duplicate key".into(),
            }),
        )
        .await;
        assert!(!replayed(&state).contains(&lsn.as_u64()));
    }

    /// A record-carrying write never resolves an Exchange, so nothing of it
    /// reaches a core before its records close.
    #[tokio::test]
    async fn a_record_carrying_query_is_refused_before_any_fan_out() {
        let (state, mut side, _directory) = fixture();
        let (minted, lsn) = minted_record(&state);
        let mut write = write_with(minted, lsn);
        write.plan = crate::bridge::envelope::PhysicalPlan::Query(
            nodedb_physical::physical_plan::QueryOp::Exchange(
                nodedb_physical::physical_plan::ExchangeOp {
                    child: Box::new(point_get_plan()),
                    mode: nodedb_physical::physical_plan::ExchangeMode::Gather {
                        as_aggregate: false,
                    },
                },
            ),
        );

        let result = super::dispatch_trusted_internal_write_to_data_plane(&state, write).await;

        assert!(result.is_err(), "a query plan cannot carry records");
        assert!(
            side.request_rx.try_pop().is_err(),
            "no request reached a core"
        );
        assert!(
            !replayed(&state).contains(&lsn.as_u64()),
            "a marker names it"
        );
        assert!(state.outcome_floor.floor() >= lsn);
        assert_eq!(state.outcome_floor.leaked_windows(), 0);
    }

    /// A committed proposal refused for good is refused on every replica, so
    /// its abort marker carries the proposal key and a redelivered copy finds
    /// the refusal in the ledger rebuilt after a restart.
    #[tokio::test]
    async fn a_final_refusal_of_a_keyed_proposal_is_its_ledger_outcome() {
        const KEY: u64 = 0xC0FF_EE01;
        let (state, side, _directory) = fixture();
        let plan =
            crate::bridge::envelope::PhysicalPlan::Kv(nodedb_physical::physical_plan::KvOp::Put {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ttl_ms: 0,
                surrogate: nodedb_types::Surrogate::new(1),
                returning: None,
                rls_filters: Vec::new(),
            });
        let responder = tokio::spawn(respond_once_with(
            Arc::clone(&state),
            side,
            Status::Error,
            Some(crate::bridge::envelope::ErrorCode::RejectedConstraint {
                constraint: "unique".into(),
                detail: "duplicate key".into(),
            }),
        ));

        let outcome = super::submit_write(
            &state,
            super::SubmitWrite {
                tenant_id: TenantId::new(1),
                database_id: DatabaseId::DEFAULT,
                vshard_id: VShardId::new(0),
                plan,
                trace_id: crate::types::TraceId::ZERO,
                event_source: crate::event::EventSource::User,
                txn_id: None,
                user_id: None,
                durability: super::WalDurability::AppendHere {
                    now_override: None,
                    apply_key: KEY,
                    commit_hlc: None,
                },
                ordering: super::WriteOrdering::AlreadyOrdered,
                change_feed: super::ChangeFeedOwner::Unowned,
            },
        )
        .await
        .expect("the refusal is a response");
        responder.await.expect("responder completes");
        assert_eq!(outcome.response.status, Status::Error);

        state.wal.sync().expect("sync");
        let ledger = crate::control::distributed_applier::ProposalLedger::from_records(
            &state.wal.replay().expect("replay"),
            8,
        );
        assert!(
            ledger.prior(KEY).is_some(),
            "the refusal's marker names the proposal"
        );
    }
}
