// SPDX-License-Identifier: BUSL-1.1

//! A system transaction refuses a write it cannot buffer before BEGIN, and
//! runs a read, whose error fails the transaction.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, VectorOp};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate, TenantId};

use super::{SystemTxnError, SystemTxnStatement, run_statements_atomically};
use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide, Dispatcher};
use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::control::lease::QueryLeaseScope;
use crate::control::security::identity::{AuthenticatedIdentity, DatabaseSet, Role};
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::{Lsn, VShardId};
use crate::wal::WalManager;

fn fixture() -> (Arc<SharedState>, CoreChannelDataSide, tempfile::TempDir) {
    let directory = tempfile::tempdir().expect("temporary WAL directory");
    let wal = Arc::new(
        WalManager::open_for_testing(&directory.path().join("system_txn.wal")).expect("test WAL"),
    );
    let (dispatcher, mut sides) = Dispatcher::new(1, 64);
    let side = sides.pop().expect("one data side");
    let state = SharedState::new(dispatcher, wal).expect("shared state");
    (state, side, directory)
}

fn identity() -> AuthenticatedIdentity {
    AuthenticatedIdentity::new_internal_service(
        0,
        "_system_txn_test",
        TenantId::new(1),
        vec![Role::Superuser],
        true,
        None,
        DatabaseSet::All,
    )
}

fn statement(plan: PhysicalPlan) -> Vec<SystemTxnStatement> {
    vec![SystemTxnStatement {
        tasks: vec![PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        }],
        lease_scope: Arc::new(QueryLeaseScope::empty()),
    }]
}

#[tokio::test]
async fn a_write_it_cannot_buffer_is_refused_before_begin() {
    let (state, mut side, _directory) = fixture();
    let plan = PhysicalPlan::Vector(VectorOp::DropIndex {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
        field_name: String::new(),
    });

    let result =
        run_statements_atomically(&state, &identity(), statement(plan), EventSource::Trigger).await;

    assert!(
        matches!(
            result,
            Err(SystemTxnError::Statement {
                index: 0,
                source: crate::Error::NotInTransactionBlock { .. },
                ..
            })
        ),
        "{result:?}"
    );
    assert!(
        side.request_rx.try_pop().is_err(),
        "nothing reaches the data plane"
    );
}

/// Answer every data-plane request until `stop` is set: the read with an
/// error, everything else with `Ok`.
async fn answer_all(state: Arc<SharedState>, mut side: CoreChannelDataSide, stop: Arc<AtomicBool>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        if let Ok(request) = side.request_rx.try_pop() {
            let request = request.inner;
            let (status, error_code) = match request.plan {
                PhysicalPlan::Document(DocumentOp::PointGet { .. }) => (
                    Status::Error,
                    Some(Box::new(ErrorCode::Internal {
                        detail: "read failed".into(),
                    })),
                ),
                _ => (Status::Ok, None),
            };
            let response = Response {
                request_id: request.request_id,
                status,
                attempt: 1,
                partial: false,
                payload: Payload::empty(),
                watermark_lsn: Lsn::ZERO,
                error_code,
                read_set_valid: None,
                read_version_lsn: Lsn::ZERO,
                write_set: Vec::new(),
            };
            side.response_tx
                .try_push(BridgeResponse { inner: response })
                .expect("fake data-plane response queue has capacity");
        }
        state.poll_and_route_responses();
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn a_read_runs_and_its_error_fails_the_transaction() {
    let (state, side, _directory) = fixture();
    let stop = Arc::new(AtomicBool::new(false));
    let responder = tokio::spawn(answer_all(Arc::clone(&state), side, Arc::clone(&stop)));
    let plan = PhysicalPlan::Document(DocumentOp::PointGet {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
        document_id: "d1".into(),
        surrogate: Surrogate::new(1),
        pk_bytes: Vec::new(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });

    let result =
        run_statements_atomically(&state, &identity(), statement(plan), EventSource::Trigger).await;
    stop.store(true, Ordering::Relaxed);
    responder.await.expect("responder completes");

    assert!(
        matches!(
            result,
            Err(SystemTxnError::Statement {
                index: 0,
                source: crate::Error::DataPlane(ErrorCode::Internal { .. }),
                ..
            })
        ),
        "the read ran and its error failed the transaction: {result:?}"
    );
}
