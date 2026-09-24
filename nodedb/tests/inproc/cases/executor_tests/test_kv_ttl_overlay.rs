// SPDX-License-Identifier: BUSL-1.1

//! In-transaction read-your-own-writes for KV TTL (`KvOp::Expire` /
//! `KvOp::Persist`, and the `GetTtl` read that must observe them).
//!
//! `KvOp::Expire` / `KvOp::Persist` / `KvOp::GetTtl` have no SQL or
//! native-DSL surface in this codebase today (no `KV_EXPIRE` / `KV_PERSIST`
//! / `KV_GET_TTL` function, unlike `KV_INCR` / `KV_CAS` / `KV_GETSET` --
//! see `control/server/shared/ddl/neutral/kv_atomic.rs`), so a pgwire
//! `TestServer` end-to-end test (as `sql_transactions_kv_overlay.rs` /
//! `sql_transactions_kv_atomic_overlay.rs` use) cannot exercise them --
//! the same gap `BatchPut` was already flagged with in
//! `sql_transactions_kv_atomic_overlay.rs`. This suite instead drives the
//! Data Plane directly through the SPSC bridge: it builds
//! `MetaOp::StageWrite { plan: KvOp::Expire/Persist }` tasks stamped with a
//! `txn_id` (mirroring what the pgwire/native staging gate does at
//! `BEGIN..COMMIT` time) and reads back through `KvOp::GetTtl` / `KvOp::Get`
//! with the same `txn_id`, then `MetaOp::DropTxnOverlay` to simulate
//! ROLLBACK/COMMIT releasing the overlay.

use nodedb::bridge::envelope::{Request, Status};
use nodedb::types::TxnId;
use nodedb_physical::physical_plan::{KvOp, MetaOp, PhysicalPlan};

use super::helpers::*;

/// Push a request carrying `txn_id`, tick, return the raw response.
fn send_txn(
    core: &mut nodedb::data::executor::core_loop::CoreLoop,
    req_tx: &mut nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    resp_rx: &mut nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    txn_id: TxnId,
    plan: PhysicalPlan,
) -> nodedb::bridge::envelope::Response {
    let request = Request {
        txn_id: Some(txn_id),
        ..make_request(plan)
    };
    req_tx
        .try_push(nodedb::bridge::dispatch::BridgeRequest::unfloored(request))
        .unwrap();
    core.tick();
    resp_rx.try_pop().unwrap().inner
}

fn stage_expire(collection: &str, key: &[u8], ttl_ms: u64) -> PhysicalPlan {
    PhysicalPlan::Meta(MetaOp::StageWrite {
        plan: Box::new(PhysicalPlan::Kv(KvOp::Expire {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            key: key.to_vec(),
            ttl_ms,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        })),
    })
}

fn stage_persist(collection: &str, key: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Meta(MetaOp::StageWrite {
        plan: Box::new(PhysicalPlan::Kv(KvOp::Persist {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            key: key.to_vec(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        })),
    })
}

fn get_ttl_ms(payload: &[u8]) -> i64 {
    payload_value(payload)["ttl_ms"]
        .as_i64()
        .expect("ttl_ms must be an integer")
}

#[test]
fn staged_expire_is_observed_by_in_tx_get_ttl_then_reverts_on_rollback() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let txn_id = TxnId::new(1);

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    // Stage EXPIRE inside the "transaction".
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        stage_expire("c", b"k", 60_000),
    );
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);

    // In-tx GetTtl must observe the staged remaining TTL (~60s), not the
    // base engine's "no TTL" state.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        PhysicalPlan::Kv(KvOp::GetTtl {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"k".to_vec(),
        }),
    );
    let ttl_ms = get_ttl_ms(resp.payload.as_ref());
    assert!(
        (0..=60_000).contains(&ttl_ms),
        "expected staged remaining TTL in (0, 60000], got {ttl_ms}"
    );

    // "ROLLBACK": drop the overlay without ever replaying the staged plan.
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::DropTxnOverlay { txn_id }),
    );
    assert_eq!(resp.status, Status::Ok);

    // Post-rollback GetTtl (even stamped with the now-defunct txn_id) must
    // revert to the base engine's "no TTL" state -- the overlay is gone.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        PhysicalPlan::Kv(KvOp::GetTtl {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"k".to_vec(),
        }),
    );
    assert_eq!(
        get_ttl_ms(resp.payload.as_ref()),
        -1,
        "ROLLBACK must discard the staged EXPIRE"
    );
}

#[test]
fn staged_persist_hides_base_ttl_then_reverts_on_rollback() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let txn_id = TxnId::new(2);

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 60_000,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    // Base GetTtl (no txn) confirms the base row does carry a TTL.
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::GetTtl {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"k".to_vec(),
        }),
    );
    assert!(get_ttl_ms(resp.payload.as_ref()) > 0);

    // Stage PERSIST inside the "transaction".
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        stage_persist("c", b"k"),
    );
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);

    // In-tx GetTtl must observe "no TTL" (-1), not the base row's TTL.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        PhysicalPlan::Kv(KvOp::GetTtl {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"k".to_vec(),
        }),
    );
    assert_eq!(
        get_ttl_ms(resp.payload.as_ref()),
        -1,
        "in-tx GetTtl must observe the staged PERSIST"
    );

    // "ROLLBACK".
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::DropTxnOverlay { txn_id }),
    );
    assert_eq!(resp.status, Status::Ok);

    // Base TTL is restored — the staged PERSIST never touched the base row.
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::GetTtl {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"k".to_vec(),
        }),
    );
    assert!(
        get_ttl_ms(resp.payload.as_ref()) > 0,
        "ROLLBACK must discard the staged PERSIST"
    );
}

#[test]
fn staged_expire_with_zero_ttl_makes_key_appear_absent_to_in_tx_get() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let txn_id = TxnId::new(3);

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    // Stage an already-expired EXPIRE (ttl_ms == 0 -> expire_at == now_ms).
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        stage_expire("c", b"k", 0),
    );
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);

    // In-tx GET must treat the key as absent (empty payload).
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        PhysicalPlan::Kv(KvOp::Get {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"k".to_vec(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        }),
    );
    assert_eq!(resp.status, Status::Ok);
    assert!(
        resp.payload.as_ref().is_empty(),
        "an already-expired staged EXPIRE must hide the key from an in-tx GET"
    );

    // In-tx GetTtl must report -2 (does not exist / expired).
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        PhysicalPlan::Kv(KvOp::GetTtl {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"k".to_vec(),
        }),
    );
    assert_eq!(get_ttl_ms(resp.payload.as_ref()), -2);

    // "ROLLBACK" restores the base row.
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::DropTxnOverlay { txn_id }),
    );
    assert_eq!(resp.status, Status::Ok);

    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Kv(KvOp::Get {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"k".to_vec(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        }),
    );
    assert_eq!(
        payload, b"v",
        "ROLLBACK must restore the base (non-expired) row"
    );
}

#[test]
fn staged_incr_with_ttl_is_observed_by_in_tx_get_ttl() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let txn_id = TxnId::new(4);

    // Stage an INCR that also carries a TTL, on a fresh key.
    let stage_incr = PhysicalPlan::Meta(MetaOp::StageWrite {
        plan: Box::new(PhysicalPlan::Kv(KvOp::Incr {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"ctr".to_vec(),
            delta: 5,
            ttl_ms: 30_000,
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            shape: nodedb_physical::physical_plan::KvCounterShape::Raw,
        })),
    });
    let resp = send_txn(&mut core, &mut tx, &mut rx, txn_id, stage_incr);
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);

    // In-tx GetTtl must observe the TTL side-effect the staged INCR carried.
    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        PhysicalPlan::Kv(KvOp::GetTtl {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "c",
            ),
            key: b"ctr".to_vec(),
        }),
    );
    let ttl_ms = get_ttl_ms(resp.payload.as_ref());
    assert!(
        (0..=30_000).contains(&ttl_ms),
        "expected staged INCR's TTL side-effect in (0, 30000], got {ttl_ms}"
    );

    // "ROLLBACK".
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::DropTxnOverlay { txn_id }),
    );
    assert_eq!(resp.status, Status::Ok);
}

#[test]
fn stage_expire_on_absent_key_is_not_found() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let txn_id = TxnId::new(5);

    let resp = send_txn(
        &mut core,
        &mut tx,
        &mut rx,
        txn_id,
        stage_expire("c", b"never-existed", 1_000),
    );
    assert_eq!(resp.status, Status::Error);
}
