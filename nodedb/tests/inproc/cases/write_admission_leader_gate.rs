// SPDX-License-Identifier: BUSL-1.1

//! The data-group leader's write gate.
//!
//! A replicated write takes its lock keys on its leader before its propose.
//! These tests drive `LeaderWriteGate` with encoded entries against the lock
//! table a Calvin scheduler shares:
//!
//! - An uncontended entry holds its keys until the leader's apply loop starts
//!   the entry. A Calvin truncate of the collection waits until then.
//! - A contended entry a Calvin transaction can sequence is refused with
//!   `RouteToSequencer`.
//! - A contended entry no Calvin transaction sequences waits for its keys,
//!   until the proposer's deadline at the latest.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use nodedb::control::cluster::calvin::scheduler::lock_manager::{
    AcquireOutcome, LockKey, LockMode, TxnId,
};
use nodedb::control::server::shared::write_admission::LeaderWriteGate;
use nodedb::control::wal_replication::{ReplicableWrite, to_replicated_entry};
use nodedb::types::{DatabaseId, TenantId, VShardId};
use nodedb_cluster::{CalvinError, ClusterError, DataProposeGate};
use nodedb_physical::physical_plan::{KvOp, KvResolvedMutation, PhysicalPlan};
use nodedb_types::{QualifiedCollection, Surrogate};

use super::write_admission_fence::{build_shared, kv_put, register_lock_manager};

/// The encoded replicated entry of `plan` on `vshard`.
fn entry_bytes(vshard: VShardId, plan: &PhysicalPlan) -> Vec<u8> {
    let replicable = ReplicableWrite::decide_for_replication(plan).expect("replicable");
    to_replicated_entry(TenantId::new(1), DatabaseId::DEFAULT, vshard, &replicable)
        .expect("encode")
        .expect("the plan has a replicated form")
        .encode()
        .expect("entry bytes")
}

/// A proposer deadline no test reaches.
fn far_deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(60)
}

fn truncate_keys(collection: &str) -> BTreeMap<LockKey, LockMode> {
    [(
        LockKey::Collection {
            collection: Arc::from(collection),
        },
        LockMode::Exclusive,
    )]
    .into()
}

/// An admitted entry holds its keys until the leader starts its apply. A
/// Calvin truncate sequenced meanwhile runs after it.
#[tokio::test]
async fn an_admitted_entry_holds_its_keys_until_its_apply_starts() {
    let (shared, _dir) = build_shared();
    let coll = "leader_gate_hold";
    let (lm, vshard) = register_lock_manager(&shared, coll);
    let gate = LeaderWriteGate::new(&shared);

    let hold = gate
        .admit(
            vshard.as_u32(),
            &entry_bytes(vshard, &kv_put(coll, b"K")),
            far_deadline(),
        )
        .await
        .expect("admit")
        .expect("an uncontended row write holds its keys");
    let (group_id, log_index) = (3, 10);
    hold.landed(group_id, log_index);

    let truncate = TxnId::new(9, 0);
    let keys = truncate_keys(coll);
    assert_eq!(
        lm.lock().expect("lm").acquire(truncate, keys.clone()),
        AcquireOutcome::Blocked,
        "a Calvin truncate waits for the admitted write"
    );
    shared
        .calvin
        .admission_holds
        .release_through(group_id, log_index - 1);
    assert!(
        !lm.lock().expect("lm").is_ready(truncate, &keys),
        "an earlier entry's start releases nothing of this one"
    );
    shared
        .calvin
        .admission_holds
        .release_through(group_id, log_index);
    assert!(
        lm.lock().expect("lm").is_ready(truncate, &keys),
        "the entry's start releases its keys to the truncate"
    );
}

/// A contended entry a Calvin transaction can sequence is refused, so its
/// proposer submits it to the sequencer.
#[tokio::test]
async fn a_contended_sequenced_entry_routes_to_the_sequencer() {
    let (shared, _dir) = build_shared();
    let coll = "leader_gate_route";
    let (lm, vshard) = register_lock_manager(&shared, coll);
    assert_eq!(
        lm.lock()
            .expect("lm")
            .acquire(TxnId::new(9, 0), truncate_keys(coll)),
        AcquireOutcome::Ready
    );
    let gate = LeaderWriteGate::new(&shared);
    let refused = gate
        .admit(
            vshard.as_u32(),
            &entry_bytes(vshard, &kv_put(coll, b"K")),
            far_deadline(),
        )
        .await;
    assert!(matches!(
        refused,
        Err(ClusterError::Calvin(CalvinError::RouteToSequencer))
    ));
}

/// A contended resolved write, which no Calvin transaction sequences, waits
/// for its keys and takes them once the holder releases.
#[tokio::test]
async fn a_contended_resolved_entry_waits_for_its_keys() {
    let (shared, _dir) = build_shared();
    let coll = "leader_gate_wait";
    let (lm, vshard) = register_lock_manager(&shared, coll);
    let truncate = TxnId::new(9, 0);
    assert_eq!(
        lm.lock()
            .expect("lm")
            .acquire(truncate, truncate_keys(coll)),
        AcquireOutcome::Ready
    );
    let resolved = PhysicalPlan::Kv(KvOp::ResolvedWrite {
        mutations: vec![KvResolvedMutation::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, coll),
            key: b"K".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            expire_at_ms: 0,
            surrogate: Surrogate::new(5),
            precondition: None,
        }],
        response_payload: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
    });
    let bytes = entry_bytes(vshard, &resolved);

    let waiter = {
        let gate = LeaderWriteGate::new(&shared);
        let vshard = vshard.as_u32();
        tokio::spawn(async move { gate.admit(vshard, &bytes, far_deadline()).await })
    };
    tokio::task::yield_now().await;
    assert!(
        !waiter.is_finished(),
        "the entry waits while the truncate holds"
    );
    let _ = lm.lock().expect("lm").release(truncate);
    let hold = waiter
        .await
        .expect("join")
        .expect("the keys free up")
        .expect("the resolved write holds its row");
    drop(hold);
    assert_eq!(
        lm.lock()
            .expect("lm")
            .acquire(TxnId::new(10, 0), truncate_keys(coll)),
        AcquireOutcome::Ready,
        "dropping the hold frees every key it held"
    );
}

/// A contended entry waits no longer than the proposer's deadline. A
/// forwarded entry carries a short budget, and the gate refuses it
/// unproposed once the budget runs out.
#[tokio::test]
async fn a_contended_entry_waits_no_longer_than_its_budget() {
    let (shared, _dir) = build_shared();
    let coll = "leader_gate_budget";
    let (lm, vshard) = register_lock_manager(&shared, coll);
    assert_eq!(
        lm.lock()
            .expect("lm")
            .acquire(TxnId::new(9, 0), truncate_keys(coll)),
        AcquireOutcome::Ready
    );
    let resolved = PhysicalPlan::Kv(KvOp::ResolvedWrite {
        mutations: vec![KvResolvedMutation::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, coll),
            key: b"K".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            expire_at_ms: 0,
            surrogate: Surrogate::new(5),
            precondition: None,
        }],
        response_payload: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
    });
    let budget_ms = 50;
    let deadline = nodedb_cluster::rpc_codec::forwarded_deadline(budget_ms).expect("a live budget");
    let gate = LeaderWriteGate::new(&shared);
    // The node default deadline is seconds long. The gate must end at the
    // budget, so this bound fails a gate that waits for the default.
    let refused = tokio::time::timeout(
        Duration::from_secs(5),
        gate.admit(vshard.as_u32(), &entry_bytes(vshard, &resolved), deadline),
    )
    .await
    .expect("the gate ends at the forwarded budget, not the node default");
    assert!(matches!(
        refused,
        Err(ClusterError::Calvin(CalvinError::AdmissionTimedOut))
    ));
    assert!(
        tokio::time::Instant::now() >= deadline,
        "the gate waits until the budget runs out"
    );
}
