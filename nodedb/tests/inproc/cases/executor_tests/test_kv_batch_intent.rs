// SPDX-License-Identifier: BUSL-1.1

//! `KvOp::BatchPut` judges each entry's key by its intent, the way the
//! point op of a multi-row SQL statement judges each row.

use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb_physical::physical_plan::{KvBatchIntent, KvOp, PhysicalPlan};

use super::helpers::*;

const COLLECTION: &str = "batch_intent";

fn collection() -> nodedb_types::QualifiedCollection {
    nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, COLLECTION)
}

fn batch(intent: KvBatchIntent, entries: &[(&[u8], &[u8])]) -> PhysicalPlan {
    let entries: Vec<(Vec<u8>, Vec<u8>)> = entries
        .iter()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect();
    let surrogates = entries
        .iter()
        .map(|(key, _)| nodedb_test_support::kv_rows::kv_row_surrogate(key))
        .collect();
    PhysicalPlan::Kv(KvOp::BatchPut {
        collection: collection(),
        entries,
        ttl_ms: 0,
        intent,
        surrogates,
        returning: None,
        rls_filters: Vec::new(),
    })
}

fn get(key: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Get {
        collection: collection(),
        key: key.to_vec(),
        rls_filters: Vec::new(),
        surrogate_ceiling: None,
        system_time: nodedb_types::SystemTimeScope::Current,
    })
}

/// A get of an absent key answers `Ok` with an empty payload; a stored
/// key answers its body.
fn assert_absent(ctx: &mut TestCtx, key: &[u8]) {
    let resp = send_raw(&mut ctx.core, &mut ctx.tx, &mut ctx.rx, get(key));
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    assert!(
        resp.payload.as_bytes().is_empty(),
        "the refused batch wrote `{}`",
        String::from_utf8_lossy(key)
    );
}

/// An `Insert` batch over a stored key writes nothing and refuses with a
/// unique violation.
#[test]
fn an_insert_batch_over_a_stored_key_writes_nothing() {
    let mut ctx = make_ctx();
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        batch(KvBatchIntent::Insert, &[(b"b", b"old")]),
    );

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        batch(KvBatchIntent::Insert, &[(b"a", b"1"), (b"b", b"2")]),
    );
    assert_eq!(resp.status, Status::Error);
    assert!(
        matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::RejectedConstraint { constraint, .. }) if constraint == "unique"
        ),
        "{:?}",
        resp.error_code
    );

    assert_absent(&mut ctx, b"a");
    let kept = send_ok(&mut ctx.core, &mut ctx.tx, &mut ctx.rx, get(b"b"));
    assert_eq!(kept, b"old");
}

/// An `Insert` batch naming one key twice refuses, as the second row of a
/// statement does.
#[test]
fn an_insert_batch_repeating_a_key_refuses() {
    let mut ctx = make_ctx();
    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        batch(KvBatchIntent::Insert, &[(b"k", b"1"), (b"k", b"2")]),
    );
    assert_eq!(resp.status, Status::Error);
    assert_absent(&mut ctx, b"k");
}

/// An `InsertIfAbsent` batch skips a stored or repeated key and counts the
/// rows it wrote.
#[test]
fn an_insert_if_absent_batch_skips_stored_and_repeated_keys() {
    let mut ctx = make_ctx();
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        batch(KvBatchIntent::Insert, &[(b"b", b"old")]),
    );

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        batch(
            KvBatchIntent::InsertIfAbsent,
            &[(b"a", b"1"), (b"b", b"2"), (b"a", b"3"), (b"c", b"4")],
        ),
    );
    assert_eq!(payload_value(&payload)["affected"], 2);

    let a = send_ok(&mut ctx.core, &mut ctx.tx, &mut ctx.rx, get(b"a"));
    assert_eq!(a, b"1");
    let b = send_ok(&mut ctx.core, &mut ctx.tx, &mut ctx.rx, get(b"b"));
    assert_eq!(b, b"old");
    let c = send_ok(&mut ctx.core, &mut ctx.tx, &mut ctx.rx, get(b"c"));
    assert_eq!(c, b"4");
}

/// An `Upsert` batch overwrites and counts every row written.
#[test]
fn an_upsert_batch_overwrites_and_counts_every_row() {
    let mut ctx = make_ctx();
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        batch(KvBatchIntent::Insert, &[(b"b", b"old")]),
    );

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        batch(KvBatchIntent::Upsert, &[(b"a", b"1"), (b"b", b"2")]),
    );
    assert_eq!(payload_value(&payload)["affected"], 2);
    let b = send_ok(&mut ctx.core, &mut ctx.tx, &mut ctx.rx, get(b"b"));
    assert_eq!(b, b"2");
}
