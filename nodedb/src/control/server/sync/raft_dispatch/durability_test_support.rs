// SPDX-License-Identifier: BUSL-1.1

//! Fixture for the sync-dispatch durable-at-ack tests.
//!
//! Both dispatch shapes (`response.rs` and `write.rs`) make the same promise:
//! when the caller hands them the LSN of a record it appended, that record is
//! fsync-durable before the call returns — because the sync handlers turn the
//! return value straight into the peer's "applied" ack. Testing that needs a
//! `SharedState` with a fake Data Plane on the other side of the bridge, which
//! is the same fixture for both files.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::TextOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide, Dispatcher};
use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::security::audit::NoopAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::authorization::{AuthorizedTask, authorize_task_set};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::WalManager;

pub(super) const COLLECTION: &str = "docs";

pub(super) fn tenant() -> TenantId {
    TenantId::new(1)
}

pub(super) fn vshard() -> VShardId {
    VShardId::from_collection_in_database(DatabaseId::DEFAULT, COLLECTION)
}

/// A `SharedState` whose bridge's Data-Plane side the test drives by hand.
pub(super) fn fixture() -> (Arc<SharedState>, CoreChannelDataSide, tempfile::TempDir) {
    let directory = tempfile::tempdir().expect("temporary WAL directory");
    let wal = Arc::new(
        WalManager::open_for_testing(&directory.path().join("sync-durability.wal"))
            .expect("test WAL"),
    );
    let (dispatcher, mut sides) = Dispatcher::new(1, 64);
    let side = sides.pop().expect("one data side");
    let state = SharedState::new(dispatcher, wal).expect("shared state");
    (state, side, directory)
}

/// Append a real FTS-delete redo and return its LSN, buffered but not durable.
pub(super) fn append_buffered_record(state: &SharedState) -> Lsn {
    append_fts_delete(state.wal.appender(crate::wal::manager::NO_APPLY_KEY))
}

/// Append a real FTS-delete redo under an outcome-floor window, as a caller
/// that dispatches it does. The record is buffered but not durable.
pub(super) fn minted_buffered_record(
    state: &SharedState,
) -> (crate::control::server::dispatch_utils::MintedRecords, Lsn) {
    let minted = crate::control::server::dispatch_utils::MintedRecords::open(&state.outcome_floor);
    let lsn = append_fts_delete(minted.appender(&state.wal, crate::wal::manager::NO_APPLY_KEY));
    (minted, lsn)
}

fn append_fts_delete(wal: crate::wal::manager::WalAppender<'_>) -> Lsn {
    let payload = nodedb_wal::record::FtsDeletePayload::new(
        nodedb_types::sync::wire::SyncProvenance {
            producer_id: 1,
            epoch: 1,
            stream_id: 1,
            seq: 1,
        },
        COLLECTION,
        "00000001",
    );
    crate::control::server::wal_dispatch::wal_append_fts_delete(
        wal,
        tenant(),
        vshard(),
        DatabaseId::DEFAULT,
        &payload,
    )
    .expect("append test redo")
}

/// A write-class plan matching the appended record, authorized for dispatch.
pub(super) fn authorized_write(state: &SharedState) -> AuthorizedTask {
    authorized_plan(
        state,
        PhysicalPlan::Text(TextOp::FtsDeleteDoc {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            surrogate: nodedb_types::Surrogate::ZERO,
            provenance: None,
        }),
    )
}

/// `plan` on the test collection, authorized for dispatch.
pub(super) fn authorized_plan(state: &SharedState, plan: PhysicalPlan) -> AuthorizedTask {
    let task = PhysicalTask {
        tenant_id: tenant(),
        database_id: DatabaseId::DEFAULT,
        vshard_id: vshard(),
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let identity = AuthenticatedIdentity::new_internal_service(
        1,
        "sync-durability-test",
        tenant(),
        Vec::new(),
        true,
        None,
        AuthenticatedIdentity::default_database_set(true),
    );
    authorize_task_set(
        &identity,
        std::slice::from_ref(&task),
        &state.permissions,
        &state.roles,
        &NoopAuditEmitter,
    )
    .expect("authorize test task")
    .into_tasks()
    .into_iter()
    .next()
    .expect("one authorized task")
}

/// Answer exactly one Data-Plane request with a bare `Ok`, then stop.
pub(super) async fn respond_once(state: Arc<SharedState>, mut side: CoreChannelDataSide) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut handled = false;
    while !handled && Instant::now() < deadline {
        if let Ok(request) = side.request_rx.try_pop() {
            side.response_tx
                .try_push(BridgeResponse {
                    inner: Response {
                        request_id: request.inner.request_id,
                        status: Status::Ok,
                        attempt: 1,
                        partial: false,
                        payload: Vec::new().into(),
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
