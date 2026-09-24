// SPDX-License-Identifier: BUSL-1.1

//! Crash-injection tests for `MetaOp::TransactionBatch`.
//!
//! Compiled only with `--features failpoints`. Each test arms a panic on
//! the `transaction_batch::between_subapply` fail point so the second
//! sub-apply triggers a panic. The batch handler catches the unwind,
//! routes through the typed-rollback path, and returns an
//! `ErrorCode::Internal` response. Side-effects of the first (already
//! applied) sub-plan must be rolled back before the response is returned —
//! the all-or-nothing guarantee.

#[allow(unused_imports)]
use nodedb_test_support::tx_batch_helpers::*;

#[cfg(feature = "failpoints")]
use nodedb::bridge::dispatch::BridgeRequest;
#[cfg(feature = "failpoints")]
use nodedb::bridge::envelope::{ErrorCode, Status};
#[cfg(feature = "failpoints")]
use nodedb::fail_point::{FailAction, FailGuard};
#[cfg(feature = "failpoints")]
use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

/// Push a TransactionBatch through the bridge, tick once, return the response.
/// Panic-from-fail-point is now handled inside the handler — `tick()` never
/// unwinds for a controlled fail point installed at the sub-apply boundary.
#[cfg(feature = "failpoints")]
fn send_batch_expecting_panic_rollback(
    core: &mut nodedb::data::executor::core_loop::CoreLoop,
    tx: &mut nodedb_bridge::buffer::Producer<BridgeRequest>,
    rx: &mut nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    plans: Vec<PhysicalPlan>,
) -> nodedb::bridge::envelope::Response {
    tx.try_push(BridgeRequest::unfloored(make_request(PhysicalPlan::Meta(
        MetaOp::TransactionBatch {
            plans,
            txn_id: None,
        },
    ))))
    .unwrap();
    core.tick();
    rx.try_pop().unwrap().inner
}

#[cfg(feature = "failpoints")]
fn assert_panic_rollback_response(resp: &nodedb::bridge::envelope::Response) {
    assert_eq!(
        resp.status,
        Status::Error,
        "expected Status::Error after panic-rollback, got {:?}",
        resp.status
    );
    match resp.error_code.as_deref() {
        Some(ErrorCode::Internal { detail }) => {
            assert!(
                detail.contains("panic in sub-apply"),
                "error detail did not name the panic site: {detail}"
            );
        }
        other => panic!("expected ErrorCode::Internal, got {other:?}"),
    }
}

#[cfg(feature = "failpoints")]
fn seed_vec(
    core: &mut nodedb::data::executor::core_loop::CoreLoop,
    tx: &mut nodedb_bridge::buffer::Producer<BridgeRequest>,
    rx: &mut nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    coll: &str,
) {
    send_ok(core, tx, rx, vector_set_params(coll));
    send_ok(core, tx, rx, vector_seed(coll));
}

// ── Doc + Vector crash ────────────────────────────────────────────────────────

#[cfg(feature = "failpoints")]
#[test]
fn crash_between_subapply_doc_vector() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed_vec(&mut core, &mut tx, &mut rx, "vec");

    let _guard = FailGuard::install("transaction_batch::between_subapply", FailAction::Panic);

    let resp = send_batch_expecting_panic_rollback(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            doc_put("docs", "crash_doc", b"should_not_persist"),
            vector_insert_ok("vec"),
        ],
    );
    assert_panic_rollback_response(&resp);

    // Drop the guard before issuing further requests so subsequent ticks
    // don't trip the fail point.
    drop(_guard);
    assert_doc_absent(&mut core, &mut tx, &mut rx, "docs", "crash_doc");
}

// ── Doc + Graph crash ─────────────────────────────────────────────────────────

#[cfg(feature = "failpoints")]
#[test]
fn crash_between_subapply_doc_graph() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let _guard = FailGuard::install("transaction_batch::between_subapply", FailAction::Panic);

    let resp = send_batch_expecting_panic_rollback(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            doc_put("docs", "crash_doc2", b"should_not_persist"),
            edge_put("g", "crash_src", "crash_dst"),
        ],
    );
    assert_panic_rollback_response(&resp);

    drop(_guard);
    assert_doc_absent(&mut core, &mut tx, &mut rx, "docs", "crash_doc2");
    assert_edge_absent(&mut core, &mut tx, &mut rx, "g", "crash_src");
}

// ── Doc + KV crash ────────────────────────────────────────────────────────────

#[cfg(feature = "failpoints")]
#[test]
fn crash_between_subapply_doc_kv() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let _guard = FailGuard::install("transaction_batch::between_subapply", FailAction::Panic);

    let resp = send_batch_expecting_panic_rollback(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            doc_put("docs", "crash_doc3", b"should_not_persist"),
            kv_put(b"crash_k1", b"should_not_persist"),
        ],
    );
    assert_panic_rollback_response(&resp);

    drop(_guard);
    assert_doc_absent(&mut core, &mut tx, &mut rx, "docs", "crash_doc3");
    assert_kv_absent(&mut core, &mut tx, &mut rx, b"crash_k1");
}

// ── Vector + Graph crash ──────────────────────────────────────────────────────

#[cfg(feature = "failpoints")]
#[test]
fn crash_between_subapply_vector_graph() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed_vec(&mut core, &mut tx, &mut rx, "vec");

    let _guard = FailGuard::install("transaction_batch::between_subapply", FailAction::Panic);

    let resp = send_batch_expecting_panic_rollback(
        &mut core,
        &mut tx,
        &mut rx,
        vec![vector_insert_ok("vec"), edge_put("g", "vg_src", "vg_dst")],
    );
    assert_panic_rollback_response(&resp);

    drop(_guard);
    // The edge (second op) was never applied. The vector insert (first op)
    // is rolled back via the undo log.
    assert_edge_absent(&mut core, &mut tx, &mut rx, "g", "vg_src");
}
