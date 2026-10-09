// SPDX-License-Identifier: BUSL-1.1

//! The dispatch core: resolves Exchange data-movement nodes, then hands the
//! plan to the shared Control-Plane write funnel (`submit_write`), which owns
//! write admission, the WAL append, the enqueue, and the response collect.

use futures::future::BoxFuture;

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::server::shared::clone_write::CloneCheckedTask;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

use super::minted::RecordOwner;
use super::submit_write::{
    ChangeFeedOwner, SubmitWrite, WalDurability, WriteOrdering, submit_write,
};
use super::types::{AutocommitWrite, DataPlaneDispatch, ReadRoute, WriteDispatch};

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
    // The write lease lives until the dispatch returns its outcome.
    let (authorized, _lease) = checked.into_parts();
    let task = authorized.into_physical_task();
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
            change_feed: ChangeFeedOwner::LocalApply,
            read_route: ReadRoute::Owned,
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
    // The write lease lives until the dispatch returns its outcome.
    let (authorized, _lease) = checked.into_parts();
    let task = authorized.into_physical_task();
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
                change_position: None,
            },
            change_feed: ChangeFeedOwner::LocalApply,
            read_route: ReadRoute::Owned,
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
    // The write lease lives until the dispatch returns its outcome.
    let (authorized, _lease) = checked.into_parts();
    let task = authorized.into_physical_task();
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
                change_position: None,
            },
            change_feed: ChangeFeedOwner::LocalApply,
            read_route: ReadRoute::Owned,
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
            // Trusted internal plumbing: reads, maintenance, and node-local
            // state. It publishes no change event.
            change_feed: ChangeFeedOwner::Unowned,
            read_route: ReadRoute::Owned,
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
            change_feed: ChangeFeedOwner::LocalApply,
            read_route: ReadRoute::Owned,
        },
    )
    .await
}

/// Dispatch a write that replays a WAL record this node already applied
/// the effects of once: the record is durable, and its change events were
/// published when it first applied. It publishes none.
pub(crate) async fn dispatch_replayed_write_to_data_plane(
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
            durability: WalDurability::CallerSupplied {
                wal_lsn,
                resolved_now_ms,
                minted,
            },
            read_route: ReadRoute::Owned,
            change_feed: ChangeFeedOwner::Unowned,
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
/// after admission and right before the dispatcher enqueue so that WAL-LSN order
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
            // The funnel appends the WAL record under the admission guard right
            // before enqueue and stamps the minted LSN onto the `Request`.
            durability: WalDurability::AppendHere {
                now_override: None,
                apply_key: 0,
                commit_hlc: None,
                change_position: None,
            },
            change_feed: ChangeFeedOwner::LocalApply,
            read_route: ReadRoute::Owned,
        },
    )
    .await
}

/// [`dispatch_step`] behind a boxed future.
///
/// Every Data Plane dispatch passes this funnel: protocol writes, the
/// transaction route, clone copy-ups, the gateway's local leg, and the sync
/// and array inbound handlers. Under it sit Exchange resolution, the
/// cross-node gather, the array coordinator and the write funnel. Unboxed,
/// that chain nests inside every caller's future and overflows the
/// compiler's layout depth limit. The box ends it at this shared node.
pub(super) fn dispatch_to_data_plane_inner(
    shared: &SharedState,
    params: DataPlaneDispatch,
) -> BoxFuture<'_, crate::Result<Response>> {
    Box::pin(dispatch_step(shared, params))
}

async fn dispatch_step(shared: &SharedState, params: DataPlaneDispatch) -> crate::Result<Response> {
    let DataPlaneDispatch {
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
        txn_id,
        mut durability,
        read_route,
        change_feed,
    } = params;
    let owner = RecordOwner {
        tenant_id,
        database_id,
        vshard_id,
    };
    // A write that carries its own records is never a query. Only a query
    // plan holds Exchange nodes, and resolving one fans it out to the cores,
    // so a record-carrying query will reach the cores before any close.
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
        // transaction id is `None`. An owned read (COPY, cursors, view refresh,
        // constraint subqueries) is strong, so every leg confirms.
        let resolved = crate::control::server::exchange::resolve_exchange_in_plan(
            shared,
            plan,
            crate::control::server::exchange::ReadScope {
                database_id,
                tenant_id,
                trace_id,
                txn_id: None,
                linearizable: read_route == ReadRoute::Owned,
            },
        )
        .await?;
        match resolved {
            crate::control::server::exchange::Resolved::Plan(p)
                if read_route == ReadRoute::Owned =>
            {
                let scope = super::owner_read::OwnedReadScope {
                    tenant_id,
                    database_id,
                    vshard_id,
                    trace_id,
                    txn_id,
                    linearizable: true,
                };
                match super::owner_read::route_owned_read(shared, scope, *p).await? {
                    super::owner_read::OwnedRead::Local(plan) => *plan,
                    super::owner_read::OwnedRead::Served(response) => return Ok(response),
                }
            }
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
            // Each entry point declares whether its writes publish.
            change_feed,
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

    use super::SubmitWrite;
    use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide, Dispatcher};
    use crate::bridge::envelope::{Payload, Status};
    use crate::control::server::dispatch_utils::{MintedRecords, enqueue_write};
    use crate::control::state::SharedState;
    use crate::engine::array::wal::ArrayPutCell;
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
    use crate::wal::WalManager;

    const ARRAY: &str = "grid";

    fn fixture() -> (Arc<SharedState>, CoreChannelDataSide, tempfile::TempDir) {
        fixture_with_capacity(64)
    }

    /// One core whose queue, and each tenant's in-flight cap, hold `capacity`
    /// requests.
    fn fixture_with_capacity(
        capacity: usize,
    ) -> (Arc<SharedState>, CoreChannelDataSide, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("autocommit.wal"))
                .expect("test WAL"),
        );
        let (dispatcher, mut sides) = Dispatcher::new(1, capacity);
        let side = sides.pop().expect("one data side");
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        (state, side, directory)
    }

    /// The `CREATE ARRAY` task for the test array: one Int64 dimension and
    /// one Int64 attribute.
    fn array_create_task(tenant_id: TenantId) -> PhysicalTask {
        use nodedb_array::schema::ArraySchemaBuilder;
        use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
        use nodedb_array::schema::dim_spec::{DimSpec, DimType};
        use nodedb_array::types::domain::{Domain, DomainBound};

        let schema = ArraySchemaBuilder::new(ARRAY)
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4])
            .build()
            .expect("build test array schema");
        PhysicalTask {
            tenant_id,
            database_id: DatabaseId::DEFAULT,
            vshard_id: nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, ARRAY).vshard(),
            plan: crate::bridge::envelope::PhysicalPlan::Array(ArrayOp::OpenArray {
                array_id: ArrayId::in_database(tenant_id, DatabaseId::DEFAULT, ARRAY),
                schema_msgpack: zerompk::to_msgpack_vec(&schema).expect("encode schema"),
                schema_hash: 0xA11CE,
                prefix_bits: 8,
                audit_retain_ms: None,
                minimum_audit_retain_ms: None,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    fn array_put_task(tenant_id: TenantId) -> PhysicalTask {
        // An empty cell batch is a valid encoding; what this exercises is the
        // durability handling of the plan shape, not the cells.
        let cells: Vec<ArrayPutCell> = Vec::new();
        PhysicalTask {
            tenant_id,
            database_id: DatabaseId::DEFAULT,
            vshard_id: nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, ARRAY).vshard(),
            plan: crate::bridge::envelope::PhysicalPlan::Array(ArrayOp::Put {
                array_id: ArrayId::in_database(tenant_id, DatabaseId::DEFAULT, ARRAY),
                cells_msgpack: zerompk::to_msgpack_vec(&cells).expect("encode cells"),
                wal_lsn: 0,
                provenance: None,
                vshard_id: nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, ARRAY)
                    .vshard()
                    .as_u32(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    /// Answer every request with `Ok`, the Raft applies of the one-node
    /// cluster included. Records the stamped LSN of each array put.
    async fn answer_capturing_array_lsn(
        state: Arc<SharedState>,
        mut side: CoreChannelDataSide,
        stamped: Arc<std::sync::Mutex<Option<u64>>>,
    ) {
        loop {
            while let Ok(request) = side.request_rx.try_pop() {
                if let crate::bridge::envelope::PhysicalPlan::Array(ArrayOp::Put {
                    wal_lsn, ..
                }) = &request.inner.plan
                {
                    *stamped.lock().unwrap_or_else(|p| p.into_inner()) = Some(*wal_lsn);
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
                            stage_vote: None,
                            read_version_lsn: Lsn::ZERO,
                            write_set: Vec::new(),
                        },
                    })
                    .expect("fake data-plane response queue has capacity");
            }
            state.poll_and_route_responses();
            tokio::task::yield_now().await;
        }
    }

    /// A synced array put applies through its replicated entry, and nothing
    /// upstream appends a redo for it, so the apply's funnel must own the
    /// record: mint it, stamp it into the plan (the array engine versions its
    /// tiles from the LSN carried there, and replay stamps the same version
    /// off the record header — a zero will make the two disagree), and hold
    /// the reply behind the durable-at-ack barrier.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_autocommit_write_mints_stamps_and_fsyncs_its_own_redo() {
        let stamped = Arc::new(std::sync::Mutex::new(None));
        let core_stamped = Arc::clone(&stamped);
        let cluster = crate::control::cluster::test_one_node::boot_with_core(
            |_| {},
            move |state, side| tokio::spawn(answer_capturing_array_lsn(state, side, core_stamped)),
        )
        .await;
        let state = Arc::clone(&cluster.state);
        let tenant_id = TenantId::new(1);
        // The replicated apply routes a cell write by the array's catalog
        // incarnation, and an array absent from the catalog is superseded.
        crate::control::array_catalog::ddl::run_trusted_array_ddl(
            &state,
            array_create_task(tenant_id),
        )
        .await
        .expect("create the test array");
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

        // An array put yields change events, so it applies through its
        // replicated entry: the apply's funnel mints and stamps the redo.
        let response =
            crate::control::server::dispatch_utils::dispatch_authorized_durable_write_with_source(
                &state,
                checked,
                crate::types::TraceId::ZERO,
                crate::event::EventSource::CrdtSync,
            )
            .await
            .expect("replicated array write succeeds");

        assert_eq!(response.status, Status::Ok);
        let stamped = stamped
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .expect("the core saw an array put");
        assert!(
            stamped > 0,
            "the plan the Data Plane executes must carry the minted LSN, not a zero"
        );
        assert!(
            state.wal.durable_through() >= stamped,
            "the minted redo must be fsync-durable before the write is acknowledged"
        );
        drop(state);
        cluster.shutdown().await;
    }

    // --- Caller records under the outcome floor ---

    /// A read plan: the funnel admits it without a gate, so these tests reach
    /// the dispatch and response paths with the caller's records attached.
    fn point_get_plan() -> crate::bridge::envelope::PhysicalPlan {
        crate::bridge::envelope::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::PointGet {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
                document_id: "u1".into(),
                surrogate: None,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            },
        )
    }

    fn minted_record(state: &SharedState) -> (MintedRecords, Lsn) {
        let minted = MintedRecords::open(&state.outcome_floor);
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

    fn write_with(minted: MintedRecords, lsn: Lsn) -> super::WriteDispatch {
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
                            stage_vote: None,
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

    /// A row write whose caller appended its records outside the funnel is
    /// refused, and the records are cancelled: a row write's records are
    /// appended inside the funnel, under its vShard's write-order fence.
    #[tokio::test]
    async fn a_row_write_the_caller_journalled_is_refused_and_cancelled() {
        let (state, _side, _directory) = fixture();
        let (minted, lsn) = minted_record(&state);
        let mut write = write_with(minted, lsn);
        write.plan = crate::bridge::envelope::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
                document_id: "u1".into(),
                value: Vec::new(),
                surrogate: nodedb_types::Surrogate::new(1),
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            },
        );

        let result = super::dispatch_trusted_internal_write_to_data_plane(&state, write).await;

        assert!(
            matches!(result, Err(crate::Error::Internal { .. })),
            "a row write the caller journalled must be refused with an internal error"
        );
        assert!(!replayed(&state).contains(&lsn.as_u64()));
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

    /// A point write the admission gate serializes on its key. A vector index
    /// insert yields no change event, so the funnel admits it on a route no
    /// other replica applies.
    fn vector_insert_plan() -> crate::bridge::envelope::PhysicalPlan {
        crate::bridge::envelope::PhysicalPlan::Vector(
            nodedb_physical::physical_plan::VectorOp::Insert {
                collection: nodedb_types::QualifiedCollection::new(
                    DatabaseId::DEFAULT,
                    "embeddings",
                ),
                vector: vec![1.0, 0.0],
                dim: 2,
                field_name: String::new(),
                surrogate: nodedb_types::Surrogate::new(1),
                pk_bytes: None,
                provenance: None,
            },
        )
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
        let plan = vector_insert_plan();
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
    /// A KV put of `key` with surrogate `surrogate`.
    fn kv_put_plan(key: &[u8], surrogate: u32) -> crate::bridge::envelope::PhysicalPlan {
        crate::bridge::envelope::PhysicalPlan::Kv(nodedb_physical::physical_plan::KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: key.to_vec(),
            value: b"v1".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::new(surrogate),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        })
    }

    /// A committed write the funnel appends under `apply_key` and enqueues
    /// without the admission gate.
    fn ordered_write(plan: crate::bridge::envelope::PhysicalPlan, apply_key: u64) -> SubmitWrite {
        SubmitWrite {
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
                apply_key,
                commit_hlc: None,
                change_position: None,
            },
            ordering: super::WriteOrdering::AlreadyOrdered,
            change_feed: super::ChangeFeedOwner::Unowned,
        }
    }

    #[tokio::test]
    async fn a_final_refusal_of_a_keyed_proposal_is_its_ledger_outcome() {
        const KEY: u64 = 0xC0FF_EE01;
        let (state, side, _directory) = fixture();
        let responder = tokio::spawn(respond_once_with(
            Arc::clone(&state),
            side,
            Status::Error,
            Some(crate::bridge::envelope::ErrorCode::RejectedConstraint {
                constraint: "unique".into(),
                detail: "duplicate key".into(),
            }),
        ));

        let outcome = super::submit_write(&state, ordered_write(kv_put_plan(b"k1", 1), KEY))
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

    /// The enqueue returned, and the `PendingWrite` is dropped before its
    /// response phase runs. The task the enqueue handed the records to still
    /// settles them from the answer.
    #[tokio::test]
    async fn a_pending_write_dropped_before_its_response_phase_closes_its_records() {
        let (state, side, _directory) = fixture();
        let pending = enqueue_write(&state, ordered_write(kv_put_plan(b"k1", 1), 0))
            .await
            .expect("the write is enqueued");
        let lsn = Lsn::new(
            replayed(&state)
                .into_iter()
                .max()
                .expect("the funnel appended the record"),
        );
        assert!(
            state.outcome_floor.floor() < lsn,
            "the core holds the record"
        );

        drop(pending);
        respond_once_with(Arc::clone(&state), side, Status::Ok, None).await;

        assert!(
            floor_passes(&state, lsn).await,
            "the answer closed the window"
        );
        assert!(
            replayed(&state).contains(&lsn.as_u64()),
            "no marker names it"
        );
        assert_eq!(state.outcome_floor.leaked_windows(), 0);
        assert_eq!(state.outcome_floor.held_windows(), 0);
    }

    /// A committed write waits for a free dispatch slot. Its caller is dropped
    /// during that wait: no core holds the write, so its record is cancelled
    /// and its response-tracker entry is removed.
    #[tokio::test]
    async fn a_write_dropped_while_waiting_for_capacity_leaves_nothing_open() {
        let (state, _side, _directory) = fixture_with_capacity(2);
        let first = enqueue_write(&state, ordered_write(kv_put_plan(b"k1", 1), 0))
            .await
            .expect("the first write is enqueued");
        let second = enqueue_write(&state, ordered_write(kv_put_plan(b"k2", 2), 0))
            .await
            .expect("the second write is enqueued");
        let in_flight = state.tracker.in_flight();
        let leaked = state.outcome_floor.leaked_windows();
        let lsn = state.wal.next_lsn();

        let waited = tokio::time::timeout(
            Duration::from_millis(50),
            enqueue_write(&state, ordered_write(kv_put_plan(b"k3", 3), 0)),
        )
        .await;

        assert!(waited.is_err(), "the write waits for a free slot");
        assert!(state.wal.next_lsn() > lsn, "the write appended its record");
        assert!(
            !replayed(&state).contains(&lsn.as_u64()),
            "a marker names it"
        );
        assert_eq!(
            state.tracker.in_flight(),
            in_flight,
            "no entry waits for a response that never comes"
        );
        assert_eq!(state.outcome_floor.leaked_windows(), leaked);
        assert_eq!(state.outcome_floor.held_windows(), 0);
        drop((first, second));
    }

    // --- Array DDL never reaches a core through the funnel ---

    /// Array DDL runs through the replicated catalog. The funnel refuses it
    /// before any record is minted, any catalog row is written, or any core
    /// holds it.
    #[tokio::test]
    async fn the_funnel_refuses_array_ddl() {
        let (state, mut side, _directory) = fixture();
        let array_id = ArrayId::in_database(TenantId::new(1), DatabaseId::DEFAULT, "refused");
        let plan = crate::bridge::envelope::PhysicalPlan::Array(ArrayOp::OpenArray {
            array_id: array_id.clone(),
            schema_msgpack: vec![0x90],
            schema_hash: 7,
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
        });
        let lsn = state.wal.next_lsn();

        let refused = enqueue_write(&state, ordered_write(plan, 0)).await;

        assert!(refused.is_err(), "array DDL must not enter the funnel");
        assert!(side.request_rx.try_pop().is_err(), "no core holds it");
        assert_eq!(state.wal.next_lsn(), lsn, "no record was minted");
        assert!(
            state
                .credentials
                .catalog()
                .get_array_in_database(TenantId::new(1), DatabaseId::DEFAULT, &array_id.name)
                .expect("catalog read")
                .is_none()
        );
        assert!(
            state
                .array_catalog
                .read()
                .expect("array catalog")
                .lookup_by_id(&array_id)
                .is_none()
        );
    }
}
