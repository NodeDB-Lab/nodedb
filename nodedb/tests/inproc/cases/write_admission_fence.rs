// SPDX-License-Identifier: BUSL-1.1

//! Write-admission fence tests.
//!
//! The fast-path point-write gate and the deterministic Calvin scheduler share
//! ONE per-vShard lock table (`CalvinLocalState::lock_managers`). These tests
//! drive the gate directly against that shared table to prove the fence:
//!
//! - A point write whose key is held by a pending commit (a normal Calvin-band
//!   `TxnId` acquired and not released) is NOT admitted to the fast path — it is
//!   routed to the scheduler, which queues it FIFO behind the holder. Once the
//!   commit releases, the same write is admitted fast.
//! - Two concurrent point writes to the same key serialize: the first takes the
//!   fast path holding the lock; the second observes that lock and is routed.
//!
//! The routed write's actual apply is delegated to the existing
//! `submit_calvin_routed` primitive (exercised end-to-end by the Calvin executor
//! test suite); these tests assert the gate's fence DECISION, which is the piece
//! this change introduces.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::cluster::calvin::scheduler::lock_manager::{
    AcquireOutcome, LockKey, LockManager, TxnId,
};
use nodedb::control::server::shared::write_admission::{
    WriteAdmission, WriteTarget, admit, cp_routed_to_calvin,
};
use nodedb::control::state::SharedState;
use nodedb::types::{DatabaseId, TenantId, VShardId};
use nodedb::wal::WalManager;
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
use nodedb_types::{QualifiedCollection, Surrogate};

/// Build a single-node `SharedState` over a throwaway WAL. The returned
/// `TempDir` must be kept alive for the WAL's lifetime.
fn build_shared() -> (Arc<SharedState>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal =
        Arc::new(WalManager::open_for_testing(&dir.path().join("fence.wal")).expect("open wal"));
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let shared = SharedState::new(dispatcher, wal).expect("shared state");
    (shared, dir)
}

/// Register an empty lock table for `collection`'s vShard and return both the
/// shared `Arc` and the vShard the gate will resolve the plan to.
fn register_lock_manager(
    shared: &SharedState,
    collection: &str,
) -> (Arc<Mutex<LockManager>>, VShardId) {
    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, collection);
    let lm = Arc::new(Mutex::new(LockManager::new()));
    shared
        .calvin
        .lock_managers
        .lock()
        .expect("lock managers")
        .insert(vshard.as_u32(), Arc::clone(&lm));
    (lm, vshard)
}

/// Register a promotion channel for `vshard` and return the receiver. The gate
/// clones the sender into any fast-path guard it builds for this vShard, so the
/// guard's drop delivers promoted scheduler waiters here.
fn register_promotion_channel(
    shared: &SharedState,
    vshard: VShardId,
) -> tokio::sync::mpsc::UnboundedReceiver<Vec<TxnId>> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    shared
        .calvin
        .promotion_senders
        .lock()
        .expect("promotion senders")
        .insert(vshard.as_u32(), tx);
    rx
}

/// A single-key KV point write to `collection` / `key`.
fn kv_put(collection: &str, key: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Put {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
        key: key.to_vec(),
        value: b"v".to_vec(),
        ttl_ms: 0,
        surrogate: Surrogate::ZERO,
        returning: None,
        rls_filters: Vec::new(),
        provenance: None,
    })
}

/// The deterministic lock key the gate derives for `kv_put(collection, key)`.
fn kv_lock_key(collection: &str, key: &[u8]) -> LockKey {
    LockKey::Kv {
        collection: Arc::from(collection),
        key: Arc::from(key),
    }
}

fn target<'a>(vshard: VShardId, plan: &'a PhysicalPlan) -> WriteTarget<'a> {
    WriteTarget {
        tenant_id: TenantId::new(1),
        database_id: DatabaseId::DEFAULT,
        vshard_id: vshard,
        plan,
    }
}

/// A point write whose key a pending commit already holds is routed to the
/// scheduler (not fast-pathed); once the commit releases, it is admitted fast.
#[tokio::test]
async fn fence_write_blocks_behind_held_commit_lock() {
    let (shared, _dir) = build_shared();
    let coll = "fence_coll";
    let (lm, vshard) = register_lock_manager(&shared, coll);

    // A pending commit (a normal Calvin-band txn) holds the fence on key K.
    let commit_txn = TxnId::new(5, 0);
    let held: BTreeSet<LockKey> = [kv_lock_key(coll, b"K")].into();
    assert_eq!(
        lm.lock().expect("lm").acquire(commit_txn, held),
        AcquireOutcome::Ready,
        "the simulated commit takes the lock first"
    );

    // The autocommit point write to K cannot fast-path — it is routed behind the
    // holder via the scheduler's FIFO queue.
    let plan = kv_put(coll, b"K");
    let before = cp_routed_to_calvin();
    match admit(&shared, &target(vshard, &plan)) {
        WriteAdmission::RouteToCalvin => {}
        _ => panic!("a point write behind a held commit lock must route to Calvin"),
    }
    assert!(
        cp_routed_to_calvin() > before,
        "the routed write must bump the routed-to-Calvin counter"
    );

    // The commit releases; the same write is now admitted to the fast path.
    let _ = lm.lock().expect("lm").release(commit_txn);
    match admit(&shared, &target(vshard, &plan)) {
        WriteAdmission::FastPath { guard: Some(_) } => {}
        _ => panic!("after release, the point write must fast-path with a real lock guard"),
    }
}

/// Two concurrent point writes to the same key serialize: the first holds the
/// lock on the fast path; the second observes it and is routed; after the first
/// releases, the key is admitted fast again.
#[tokio::test]
async fn two_concurrent_same_key_point_writes_serialize() {
    let (shared, _dir) = build_shared();
    let coll = "serialize_coll";
    let (_lm, vshard) = register_lock_manager(&shared, coll);
    let plan = kv_put(coll, b"K");

    // First write takes the fast path and holds the lock via its RAII guard.
    let guard1 = match admit(&shared, &target(vshard, &plan)) {
        WriteAdmission::FastPath { guard: Some(g) } => g,
        _ => panic!("first same-key write must fast-path with a real lock guard"),
    };

    // Second write to the same key observes the held lock and is routed.
    let before = cp_routed_to_calvin();
    match admit(&shared, &target(vshard, &plan)) {
        WriteAdmission::RouteToCalvin => {}
        _ => panic!("second same-key write must route to Calvin behind the first"),
    }
    assert!(
        cp_routed_to_calvin() > before,
        "the second write must bump the routed-to-Calvin counter"
    );

    // First write completes (guard drops, releasing the lock); the key is free.
    drop(guard1);
    match admit(&shared, &target(vshard, &plan)) {
        WriteAdmission::FastPath { guard: Some(_) } => {}
        _ => panic!("after the first write releases, the key must fast-path again"),
    }
}

/// Single-node (NO Calvin lock manager registered for the vShard): a point write
/// is admitted with the global keyed order-lock and its own point key, so
/// concurrent same-key writes can serialize even with no lock table to fence
/// against. This is the UA2 hole the keyed lock closes.
#[tokio::test]
async fn single_node_point_write_uses_global_keyed_order_lock() {
    let (shared, _dir) = build_shared();
    let coll = "single_node_coll";
    // Deliberately DO NOT register a lock manager — the single-node path.
    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, coll);
    let plan = kv_put(coll, b"K");

    let lock = match admit(&shared, &target(vshard, &plan)) {
        WriteAdmission::FastPathBlocking { key, keyed_lock } => {
            assert_eq!(
                key,
                kv_lock_key(coll, b"K"),
                "the admission must carry the write's exact point key"
            );
            keyed_lock
        }
        _ => panic!("a single-node point write must return FastPathBlocking"),
    };
    assert!(
        Arc::ptr_eq(&lock, &shared.write_order_locks),
        "the gate must hand out the one global SharedState keyed order-lock"
    );
}

/// Single-node concurrent same-key writes serialize in FIFO arrival order via the
/// keyed order-lock the gate hands out. Deterministic under the current-thread
/// test runtime: each spawned waiter is polled to its park point before the next.
#[tokio::test]
async fn single_node_same_key_serializes_fifo() {
    let (shared, _dir) = build_shared();
    let coll = "single_node_fifo";
    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, coll);
    let plan = kv_put(coll, b"K");

    let (key, lock) = match admit(&shared, &target(vshard, &plan)) {
        WriteAdmission::FastPathBlocking { key, keyed_lock } => (key, keyed_lock),
        _ => panic!("single-node point write must return FastPathBlocking"),
    };

    let order = Arc::new(Mutex::new(Vec::<u32>::new()));
    // The holder acquires first, parking the two waiters behind it.
    let held = lock.lock_owned(key.clone()).await;

    let mut handles = Vec::new();
    for id in [1u32, 2u32] {
        let lock = Arc::clone(&lock);
        let order = Arc::clone(&order);
        let key = key.clone();
        handles.push(tokio::spawn(async move {
            let _g = lock.lock_owned(key).await;
            order.lock().expect("order").push(id);
        }));
        // Poll the waiter to its park point, fixing its FIFO queue position.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
    }

    assert!(
        order.lock().expect("order").is_empty(),
        "same-key waiters must block while the holder is live"
    );

    drop(held);
    for h in handles {
        h.await.expect("waiter task");
    }
    assert_eq!(
        *order.lock().expect("order"),
        vec![1, 2],
        "concurrent same-key writes must acquire in FIFO arrival order"
    );
}

/// A fast-path point write that promotes a blocked multi-vShard scheduler txn on
/// drop MUST hand the promoted `TxnId` to the scheduler over the promotion
/// channel — never discard it.
///
/// Regression for the Calvin lock-leak: a scheduler transaction that queued
/// behind an uncontended fast-path key was promoted to holder by `release` when
/// the fast-path guard dropped, but the promoted id was thrown away (`let _ =
/// ...`). The scheduler never learned to dispatch it, so it sat in `blocked`
/// forever holding the key — every later txn on that key stalled behind a zombie
/// holder. The guard now forwards the promoted ids to the owning scheduler.
#[tokio::test]
async fn fast_path_drop_delivers_promoted_scheduler_txn() {
    let (shared, _dir) = build_shared();
    let coll = "promotion_coll";
    let (lm, vshard) = register_lock_manager(&shared, coll);
    let mut promotion_rx = register_promotion_channel(&shared, vshard);

    // A fast-path point write on K takes the fence via its RAII guard (an
    // autocommit-band holder). K was uncontended at acquire time.
    let plan = kv_put(coll, b"K");
    let guard = match admit(&shared, &target(vshard, &plan)) {
        WriteAdmission::FastPath { guard: Some(g) } => g,
        _ => panic!("uncontended point write must fast-path with a real lock guard"),
    };

    // AFTER the fast path acquired K, a multi-vShard Calvin scheduler txn T needs
    // K and queues behind the fast-path holder (Blocked) — the exact contention
    // the old "fast-path keys are never contended" assumption ignored.
    let scheduler_txn = TxnId::new(7, 0);
    let want: BTreeSet<LockKey> = [kv_lock_key(coll, b"K")].into();
    assert_eq!(
        lm.lock().expect("lm").acquire(scheduler_txn, want.clone()),
        AcquireOutcome::Blocked,
        "the scheduler txn must block behind the fast-path holder"
    );
    assert!(
        promotion_rx.try_recv().is_err(),
        "no promotion may be delivered while the fast-path holder is live"
    );

    // The fast-path write completes: the guard drops, `release` promotes T to
    // holder of K and returns [T], and the guard forwards it to the scheduler.
    drop(guard);

    let promoted = promotion_rx
        .try_recv()
        .expect("the promoted scheduler txn must be delivered over the promotion channel");
    assert!(
        promoted.contains(&scheduler_txn),
        "the delivered promotion set must name the unblocked scheduler txn"
    );
    // The promotion is real: T is now holder of K in the shared lock table, ready
    // for the scheduler to dispatch — not stranded in `blocked`.
    assert!(
        lm.lock().expect("lm").is_ready(scheduler_txn, &want),
        "release must have installed the scheduler txn as holder of the freed key"
    );
}

/// Single-node writes to DISTINCT keys never block each other — both order-lock
/// guards are held simultaneously.
#[tokio::test]
async fn single_node_distinct_keys_do_not_block() {
    let (shared, _dir) = build_shared();
    let coll = "single_node_distinct";
    let lock = Arc::clone(&shared.write_order_locks);

    let g_a = lock.lock_owned(kv_lock_key(coll, b"A")).await;
    let g_b = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        lock.lock_owned(kv_lock_key(coll, b"B")),
    )
    .await
    .expect("a distinct key must not block on a held key");

    // Both alive at once — proof the keys map to independent mutexes.
    drop(g_b);
    drop(g_a);
}
