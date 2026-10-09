// SPDX-License-Identifier: BUSL-1.1

//! A typed Data-Plane refusal keeps its SQLSTATE through the system door.
//!
//! `CREATE VECTOR INDEX` registers its parameters through `apply_in_engine`,
//! which dispatches `VectorOp::SetParams` through `dispatch_system`. A core
//! whose index already holds vectors refuses that plan with
//! `ErrorCode::Unsupported`. A fake core gives that exact refusal here, so the
//! test runs the real dispatch, response routing and DDL error mapping with no
//! real Data-Plane core.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::VectorOp;
use nodedb_types::error::sqlstate;

use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide, Dispatcher};
use crate::bridge::envelope::{ErrorCode, Payload, PhysicalPlan, Response, Status};
use crate::control::server::shared::ddl::engine_apply::apply_in_engine;
use crate::control::server::shared::ddl::sync_dispatch::{
    SystemReason, SystemTask, dispatch_system,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId};
use crate::wal::WalManager;

const COLLECTION: &str = "vectors";

/// The refusal a core gives `SetParams` on an index that holds vectors.
fn materialized_refusal() -> ErrorCode {
    ErrorCode::Unsupported {
        detail: "changing vector index params after the index holds vectors is not \
                 supported; drop and recreate the collection"
            .into(),
    }
}

fn set_params_plan() -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::SetParams {
        collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
        field_name: "emb".into(),
        dim: 3,
        m: 16,
        ef_construction: 200,
        metric: "cosine".into(),
        index_type: "hnsw".into(),
        pq_m: 0,
        ivf_cells: 0,
        ivf_nprobe: 0,
    })
}

/// State plus the fake core's data side. The caller keeps the `TempDir`
/// alive for as long as `state` is in use.
fn fixture() -> (Arc<SharedState>, CoreChannelDataSide, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("create test directory");
    let wal = Arc::new(
        WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
    );
    let (dispatcher, mut sides) = Dispatcher::new(1, 64);
    let side = sides.pop().expect("one data side");
    let state = SharedState::new(dispatcher, wal).expect("construct shared state");
    (state, side, dir)
}

/// Fake core: pops the one dispatched request and answers it with an error
/// status carrying `code`.
async fn refuse_once(
    mut side: CoreChannelDataSide,
    state: Arc<SharedState>,
    code: Option<ErrorCode>,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut handled = false;
    while !handled && Instant::now() < deadline {
        if let Ok(request) = side.request_rx.try_pop() {
            side.response_tx
                .try_push(BridgeResponse {
                    inner: Response {
                        request_id: request.inner.request_id,
                        status: Status::Error,
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
                .expect("fake core response queue has capacity");
            handled = true;
        }
        state.poll_and_route_responses();
        tokio::task::yield_now().await;
    }
    assert!(handled, "fake core received the dispatched request");
    state.poll_and_route_responses();
}

/// The system door returns the refusal's own code, never `Internal`.
#[tokio::test]
async fn dispatch_system_keeps_the_refusal_code() {
    let (state, side, _dir) = fixture();
    let responder = tokio::spawn(refuse_once(
        side,
        Arc::clone(&state),
        Some(materialized_refusal()),
    ));
    let result = dispatch_system(
        &state,
        SystemTask::new(
            SystemReason::DdlApply,
            TenantId::new(1),
            nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, COLLECTION),
            set_params_plan(),
        ),
        Duration::from_secs(5),
    )
    .await;
    responder.await.expect("responder completes");

    match result {
        Err(crate::Error::DataPlane(code)) => assert_eq!(code, materialized_refusal()),
        other => panic!("expected the typed Data-Plane refusal, got {other:?}"),
    }
}

/// The DDL statement answers the refusal's SQLSTATE (`0A000`) and class, with
/// the statement context before the message.
#[tokio::test]
async fn create_vector_index_refusal_keeps_its_sqlstate() {
    let (state, side, _dir) = fixture();
    let responder = tokio::spawn(refuse_once(
        side,
        Arc::clone(&state),
        Some(materialized_refusal()),
    ));
    let result = apply_in_engine(
        &state,
        TenantId::new(1),
        DatabaseId::DEFAULT,
        COLLECTION,
        set_params_plan(),
        "CREATE VECTOR INDEX",
    )
    .await;
    responder.await.expect("responder completes");

    let err = result.expect_err("the refused SetParams must fail the statement");
    assert_eq!(err.sqlstate, sqlstate::FEATURE_NOT_SUPPORTED, "{err:?}");
    assert_ne!(err.sqlstate, sqlstate::INTERNAL_ERROR);
    assert_eq!(err.code, nodedb_types::error::ErrorCode::SQL_NOT_ENABLED);
    assert!(
        err.message.starts_with("CREATE VECTOR INDEX: "),
        "context leads the message: {}",
        err.message
    );
    assert!(
        err.message.contains("holds vectors"),
        "the refusal detail survives: {}",
        err.message
    );
}

/// A refusal with no code has no class of its own, so it is the one case
/// that stays internal.
#[tokio::test]
async fn refusal_without_a_code_is_internal() {
    let (state, side, _dir) = fixture();
    let responder = tokio::spawn(refuse_once(side, Arc::clone(&state), None));
    let result = apply_in_engine(
        &state,
        TenantId::new(1),
        DatabaseId::DEFAULT,
        COLLECTION,
        set_params_plan(),
        "CREATE VECTOR INDEX",
    )
    .await;
    responder.await.expect("responder completes");

    let err = result.expect_err("an uncoded refusal must fail the statement");
    assert_eq!(err.sqlstate, sqlstate::INTERNAL_ERROR, "{err:?}");
}
