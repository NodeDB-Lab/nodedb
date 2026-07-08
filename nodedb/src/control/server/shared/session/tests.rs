// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for session store (transaction lifecycle, params, cursors, live).

use crate::control::server::shared::session::state::TransactionState;
use crate::control::server::shared::session::store::SessionStore;

#[test]
fn transaction_lifecycle() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5000".parse().unwrap();
    store.ensure_session(addr);

    assert_eq!(store.transaction_state(&addr), TransactionState::Idle);

    store.begin(&addr, crate::types::Lsn::new(1), 0, 0).unwrap();
    assert_eq!(store.transaction_state(&addr), TransactionState::InBlock);

    store.commit(&addr).unwrap();
    assert_eq!(store.transaction_state(&addr), TransactionState::Idle);

    store.begin(&addr, crate::types::Lsn::new(1), 0, 0).unwrap();
    store.fail_transaction(&addr);
    assert_eq!(store.transaction_state(&addr), TransactionState::Failed);

    store.rollback(&addr).unwrap();
    assert_eq!(store.transaction_state(&addr), TransactionState::Idle);
}

#[test]
fn session_parameters() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5000".parse().unwrap();
    store.ensure_session(addr);

    assert_eq!(
        store.get_parameter(&addr, "client_encoding"),
        Some("UTF8".into())
    );

    store.set_parameter(&addr, "application_name".into(), "test_app".into());
    assert_eq!(
        store.get_parameter(&addr, "application_name"),
        Some("test_app".into())
    );
}

#[test]
fn session_cleanup() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5000".parse().unwrap();
    store.ensure_session(addr);
    assert_eq!(store.count(), 1);

    store.remove(&addr);
    assert_eq!(store.count(), 0);
}

#[test]
fn live_subscription_store_and_check() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5001".parse().unwrap();
    store.ensure_session(addr);

    assert!(!store.has_live_subscriptions(&addr));

    let stream = crate::control::change_stream::ChangeStream::new(64);
    let sub = stream.subscribe(Some("orders".into()), None);
    store.add_live_subscription(&addr, "live_orders".into(), sub);

    assert!(store.has_live_subscriptions(&addr));
}

#[test]
fn live_subscription_drain_empty() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5002".parse().unwrap();
    store.ensure_session(addr);

    let stream = crate::control::change_stream::ChangeStream::new(64);
    let sub = stream.subscribe(Some("orders".into()), None);
    store.add_live_subscription(&addr, "live_orders".into(), sub);

    // No events published — drain returns empty.
    let notifications = store.drain_live_notifications(&addr);
    assert!(notifications.is_empty());
}

#[test]
fn live_subscription_drain_receives_events() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5003".parse().unwrap();
    store.ensure_session(addr);

    let stream = crate::control::change_stream::ChangeStream::new(64);
    let sub = stream.subscribe(Some("orders".into()), None);
    store.add_live_subscription(&addr, "live_orders".into(), sub);

    // Publish a matching event.
    stream.publish(crate::control::change_stream::ChangeEvent {
        lsn: crate::types::Lsn::new(1),
        tenant_id: crate::types::TenantId::new(1),
        collection: "orders".into(),
        document_id: "o42".into(),
        operation: crate::control::change_stream::ChangeOperation::Insert,
        timestamp_ms: 0,
        after: None,
    });

    let notifications = store.drain_live_notifications(&addr);
    assert_eq!(notifications.len(), 1);
    assert_eq!(notifications[0].0, "live_orders");
    assert_eq!(notifications[0].1, "INSERT:o42");
}

#[test]
fn live_subscription_filters_by_collection() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5004".parse().unwrap();
    store.ensure_session(addr);

    let stream = crate::control::change_stream::ChangeStream::new(64);
    let sub = stream.subscribe(Some("orders".into()), None);
    store.add_live_subscription(&addr, "live_orders".into(), sub);

    // Publish event for a different collection — should be filtered out.
    stream.publish(crate::control::change_stream::ChangeEvent {
        lsn: crate::types::Lsn::new(1),
        tenant_id: crate::types::TenantId::new(1),
        collection: "users".into(),
        document_id: "u1".into(),
        operation: crate::control::change_stream::ChangeOperation::Update,
        timestamp_ms: 0,
        after: None,
    });

    let notifications = store.drain_live_notifications(&addr);
    assert!(notifications.is_empty());
}

#[test]
fn live_subscription_no_session_returns_empty() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5005".parse().unwrap();
    // No session created — should return empty, not panic.
    let notifications = store.drain_live_notifications(&addr);
    assert!(notifications.is_empty());
    assert!(!store.has_live_subscriptions(&addr));
}

/// `run_begin` anchors the session's cross-shard snapshot to the last
/// globally-applied Calvin epoch from `SharedState::last_applied_calvin_epoch`.
#[tokio::test]
async fn run_begin_anchors_snapshot_epoch() {
    use std::sync::atomic::Ordering;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::server::shared::session::lifecycle::run_begin;
    use crate::control::state::SharedState;
    use crate::wal::WalManager;

    let dir = tempfile::tempdir().unwrap();
    let wal =
        std::sync::Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap());
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let state = SharedState::new(dispatcher, wal).unwrap();

    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5100".parse().unwrap();
    store.ensure_session(addr);

    // Seed the applied epoch to 7 and BEGIN — the session anchors to 7.
    state.last_applied_calvin_epoch.store(7, Ordering::Release);
    run_begin(&store, &addr, &state).unwrap();
    assert_eq!(store.snapshot_epoch(&addr), Some(7));
    store.commit(&addr).unwrap();
    assert_eq!(store.snapshot_epoch(&addr), None);

    // Unset (single-node / no-Calvin): BEGIN anchors to 0.
    state.last_applied_calvin_epoch.store(0, Ordering::Release);
    run_begin(&store, &addr, &state).unwrap();
    assert_eq!(store.snapshot_epoch(&addr), Some(0));
}

// ── Multi-vShard overlay teardown + per-vShard savepoint markers ──

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, DatabaseSet};
use crate::control::server::shared::session::outcome::TxnDataPlane;
use crate::control::server::shared::session::savepoint_ops;
use crate::types::{DatabaseId, Lsn, RequestId, TenantId, VShardId};
use nodedb_physical::physical_plan::MetaOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// A `TxnDataPlane` that records every dispatched overlay meta-op (per vShard)
/// instead of touching a real core. `MarkSavepoint` replies with a 16-byte
/// composite marker whose value component is `vshard + 1`, so a later
/// ROLLBACK TO can be asserted to thread each vShard's own saved marker.
#[derive(Default)]
struct RecordingDp {
    ops: Mutex<Vec<(VShardId, MetaOp)>>,
}

impl TxnDataPlane for RecordingDp {
    fn dispatch_no_wal<'a>(
        &'a self,
        task: PhysicalTask,
        _wal_lsn: Option<Lsn>,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Response>> + Send + 'a>> {
        let vshard = task.vshard_id;
        let payload = if let PhysicalPlan::Meta(op) = &task.plan {
            self.ops.lock().unwrap().push((vshard, op.clone()));
            match op {
                MetaOp::MarkSavepoint { .. } => {
                    let value = (vshard.as_u32() as u64) + 1;
                    let graph = 0u64;
                    let mut bytes = Vec::with_capacity(16);
                    bytes.extend_from_slice(&value.to_le_bytes());
                    bytes.extend_from_slice(&graph.to_le_bytes());
                    Payload::from_vec(bytes)
                }
                _ => Payload::empty(),
            }
        } else {
            Payload::empty()
        };
        Box::pin(async move {
            Ok(Response {
                request_id: RequestId::new(1),
                status: Status::Ok,
                attempt: 1,
                partial: false,
                payload,
                watermark_lsn: Lsn::ZERO,
                error_code: None,
            })
        })
    }
}

/// A benign staged write task homed on `vshard`. The plan content is irrelevant
/// to overlay teardown — only the vShard it stages to is tracked.
fn staged_task(vshard: u32) -> PhysicalTask {
    PhysicalTask {
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(vshard),
        database_id: DatabaseId::DEFAULT,
        plan: PhysicalPlan::Meta(MetaOp::WalAppend {
            payload: Vec::new(),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }
}

fn test_identity() -> AuthenticatedIdentity {
    AuthenticatedIdentity {
        user_id: 1,
        username: "tester".into(),
        tenant_id: TenantId::new(1),
        auth_method: AuthMethod::Trust,
        roles: Vec::new(),
        is_superuser: true,
        default_database: None,
        accessible_databases: DatabaseSet::All,
    }
}

/// ROLLBACK of a transaction that staged writes to TWO vShards must drop the
/// staging overlay on BOTH — the pre-fix single-`tx_vshard` code leaked the
/// second core's overlay.
#[tokio::test]
async fn multi_vshard_rollback_drops_every_overlay() {
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::server::shared::session::lifecycle::{run_begin, run_rollback};
    use crate::control::state::SharedState;
    use crate::wal::WalManager;

    let dir = tempfile::tempdir().unwrap();
    let wal =
        std::sync::Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap());
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let state = SharedState::new(dispatcher, wal).unwrap();

    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5200".parse().unwrap();
    store.ensure_session(addr);
    run_begin(&store, &addr, &state).unwrap();

    // Stage to two distinct vShards/cores.
    assert!(store.buffer_write(&addr, staged_task(3)));
    assert!(store.buffer_write(&addr, staged_task(9)));

    let identity = test_identity();
    let dp = RecordingDp::default();
    run_rollback(&store, &addr, &identity, &state, &dp).await;

    let ops = dp.ops.lock().unwrap();
    let drops: Vec<VShardId> = ops
        .iter()
        .filter_map(|(v, op)| matches!(op, MetaOp::DropTxnOverlay { .. }).then_some(*v))
        .collect();
    assert!(drops.contains(&VShardId::new(3)), "core A overlay dropped");
    assert!(
        drops.contains(&VShardId::new(9)),
        "core B overlay dropped (would leak pre-fix)"
    );
    assert_eq!(drops.len(), 2, "exactly the two staged overlays dropped");
}

/// A vShard first staged AFTER a savepoint must have ALL its staged writes
/// rewound on ROLLBACK TO — its overlay is rewound to marker `(0, 0)`, while a
/// vShard present at savepoint time rewinds to its own saved marker.
#[tokio::test]
async fn multi_vshard_rollback_to_savepoint_rewinds_each_vshard() {
    let store = SessionStore::new();
    let addr: std::net::SocketAddr = "127.0.0.1:5201".parse().unwrap();
    store.ensure_session(addr);
    store.begin(&addr, Lsn::new(1), 0, 0).unwrap();
    let tenant = TenantId::new(1);
    let dp = RecordingDp::default();

    // Stage on core A (3), then SAVEPOINT — only A is marked.
    assert!(store.buffer_write(&addr, staged_task(3)));
    savepoint_ops::run_savepoint(&store, &addr, tenant, &dp, "s1")
        .await
        .expect("savepoint");

    // Stage on core B (9) AFTER the savepoint.
    assert!(store.buffer_write(&addr, staged_task(9)));

    // ROLLBACK TO s1 — A rewinds to its saved marker, B rewinds to (0, 0).
    savepoint_ops::run_rollback_to_savepoint(&store, &addr, tenant, &dp, "s1")
        .await
        .expect("rollback to savepoint");

    let ops = dp.ops.lock().unwrap();

    // Only core A was marked at savepoint time (B was not yet staged).
    let marks: Vec<u32> = ops
        .iter()
        .filter_map(|(v, op)| matches!(op, MetaOp::MarkSavepoint { .. }).then_some(v.as_u32()))
        .collect();
    assert_eq!(marks, vec![3], "only the pre-savepoint vShard is marked");

    // Both staged vShards are rewound; A to its saved marker (3+1), B to zero.
    let rewinds: std::collections::BTreeMap<u32, (u64, u64)> = ops
        .iter()
        .filter_map(|(v, op)| match op {
            MetaOp::RollbackToSavepoint {
                value_marker,
                graph_marker,
                ..
            } => Some((v.as_u32(), (*value_marker, *graph_marker))),
            _ => None,
        })
        .collect();
    assert_eq!(
        rewinds.get(&3),
        Some(&(4, 0)),
        "core A rewinds to its saved marker"
    );
    assert_eq!(
        rewinds.get(&9),
        Some(&(0, 0)),
        "core B (staged after savepoint) rewinds to empty"
    );
}
