// SPDX-License-Identifier: BUSL-1.1

//! A whole committed redo that arrives while a stream install is parked.
//!
//! The parked install writes [`FILL`] in the default database, with its
//! events on vShard 0. A whole record installs to its end inside its task.
//! It waits only when it touches what the install holds. A second stream
//! marker always waits, since one install parks at a time.

use nodedb_physical::physical_plan::{
    DocumentOp, MetaOp, PhysicalPlan, RedoOrigin, RedoSumTargets, ResolvedSumTarget,
};
use nodedb_types::{QualifiedCollection, StorageKey, Surrogate};

use crate::bridge::envelope::Request;
use crate::data::executor::core_loop::CoreLoop;
use crate::types::DatabaseId;
use crate::wal::RedoSubRecord;

use super::super::stream_validate::tests::{LSN, TID, plain_core, record};
use super::super::test_commit::doc_put_sub_record;
use super::equivalence_tests::{FILL, FILL_FROM, fill};
use super::isolation_tests::OTHER;
use super::yield_tests::{
    INSTALL, OTHER_VSHARD, ROWS, Ring, assert_ok, install_request, installed_rows, request,
    tick_until_answered_while_waiting,
};

/// The small committed redo each case queues behind the install.
const SMALL: u64 = 30;
/// A read queued behind the small redo.
const LATER_READ: u64 = 31;
/// A second stream marker, or a duplicate of the parked one.
const SECOND: u64 = 32;
/// The surrogate of the row the small redo writes: no filler row's.
const SMALL_SURROGATE: u32 = 31_500;
/// The first surrogate the second marker's rows take.
const SECOND_FROM: u32 = 40_000;
/// The idempotency key the parked install and its duplicate share.
const IDEMPOTENCY_KEY: u64 = 0xd0b1e;

/// One document put of row `s1` into `collection`.
fn small_put(collection: &str) -> RedoSubRecord {
    let body = nodedb_types::json_to_msgpack(&serde_json::json!({"n": 1})).expect("body");
    doc_put_sub_record(collection, "s1", &body, SMALL_SURROGATE)
}

/// The whole-record install of `ops` into the stored `collections`, folding
/// into `sum_targets`, at `lsn`. Its events carry another vShard than the
/// install's.
fn small_redo(
    ops: Vec<RedoSubRecord>,
    collections: &[&str],
    sum_targets: Vec<RedoSumTargets>,
    lsn: u64,
) -> Request {
    let plan = PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo: record(ops).to_bytes().expect("encode redo"),
        collections: collections.iter().map(|name| name.to_string()).collect(),
        sum_targets,
        origin: RedoOrigin::Commit,
        calvin: None,
        reads: Vec::new(),
        snapshot_ordinal: None,
        pins: Default::default(),
    });
    let mut request = install_request(SMALL, plan, lsn);
    request.vshard_id = OTHER_VSHARD;
    request
}

/// A read of row `s1` of `collection`.
fn point_get(id: u64, collection: &str) -> Request {
    request(
        id,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            document_id: "s1".into(),
            surrogate: None,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        }),
    )
}

/// Whether `core` holds the row the small redo writes into `collection`.
fn holds_small_row(core: &CoreLoop, collection: &str) -> bool {
    core.sparse
        .get(
            0,
            TID,
            collection,
            &StorageKey::for_surrogate(Surrogate::new(SMALL_SURROGATE)),
        )
        .expect("read row")
        .is_some()
}

/// A core whose install of [`FILL`] parked, with `queued` pushed behind it.
fn parked_with(queued: Vec<Request>) -> Ring {
    let mut ring = Ring::open(plain_core);
    ring.push_fill_install();
    for request in queued {
        ring.push(request, None);
    }
    ring.tick();
    assert!(ring.core.has_parked_install(), "the install parked");
    ring
}

/// A small redo of another collection and vShard installs while the large
/// install stays parked. A later read of an unrelated collection runs
/// behind it.
#[test]
fn a_small_redo_of_another_collection_installs_while_parked() {
    let mut ring = parked_with(vec![
        small_redo(vec![small_put(OTHER)], &[OTHER], Vec::new(), LSN + 1),
        point_get(LATER_READ, "c"),
    ]);
    assert_ok(ring.answer(SMALL).expect("the small redo answered"));
    assert!(holds_small_row(&ring.core, OTHER), "the small redo applied");
    assert!(
        ring.answer(LATER_READ).is_some(),
        "the later read ran behind the small redo"
    );
    assert!(ring.core.has_parked_install(), "the install stays parked");
    assert!(ring.answer(INSTALL).is_none());
    assert!(
        !ring.core.redo_apply.yield_grant,
        "no grant outlives the small redo"
    );

    assert_ok(ring.tick_until_answered(INSTALL));
    assert_eq!(installed_rows(&ring.core), ROWS as usize);
    assert!(!ring.core.has_install_fence());
}

/// A small redo that writes the install's collection waits until the
/// install ends, then applies.
#[test]
fn a_small_redo_of_the_installs_collection_waits() {
    let mut ring = parked_with(vec![small_redo(
        vec![small_put(FILL)],
        &[FILL],
        Vec::new(),
        LSN + 1,
    )]);
    assert!(!holds_small_row(&ring.core, FILL), "the small redo waits");
    tick_until_answered_while_waiting(&mut ring, INSTALL, &[SMALL]);
    assert_ok(ring.tick_until_answered(SMALL));
    assert!(holds_small_row(&ring.core, FILL), "the small redo applied");
    assert_eq!(installed_rows(&ring.core), ROWS as usize);
}

/// A small redo whose sum target is the install's collection waits until
/// the install ends.
#[test]
fn a_small_redo_whose_sum_target_is_the_installs_collection_waits() {
    let targets = vec![RedoSumTargets {
        collection: OTHER.into(),
        resolved: vec![ResolvedSumTarget::new(
            FILL,
            "k",
            Surrogate::new(SMALL_SURROGATE + 1),
        )],
    }];
    let mut ring = parked_with(vec![small_redo(
        vec![small_put(OTHER)],
        &[OTHER],
        targets,
        LSN + 1,
    )]);
    assert!(!holds_small_row(&ring.core, OTHER), "the small redo waits");
    tick_until_answered_while_waiting(&mut ring, INSTALL, &[SMALL]);
    ring.tick_until_answered(SMALL);
}

/// A second stream marker waits for the parked install, though it writes
/// another collection: one install parks at a time. Once the first
/// install ends, the second installs.
#[test]
fn a_second_stream_marker_waits_for_the_parked_install() {
    let mut ring = Ring::open(plain_core);
    ring.push_fill_install();
    let rows: Vec<RedoSubRecord> = (SECOND_FROM..SECOND_FROM + ROWS)
        .map(|surrogate| {
            let body = nodedb_types::json_to_msgpack(&serde_json::json!({"n": surrogate}))
                .expect("encode body");
            doc_put_sub_record(OTHER, &format!("s{surrogate}"), &body, surrogate)
        })
        .collect();
    let plan = ring.marker_plan(&record(rows), vec![OTHER.to_string()], Vec::new(), None);
    let mut second = install_request(SECOND, plan, LSN + 1);
    second.vshard_id = OTHER_VSHARD;
    ring.push(second, None);
    ring.tick();
    assert!(ring.core.has_parked_install(), "the first install parked");

    tick_until_answered_while_waiting(&mut ring, INSTALL, &[SECOND]);
    assert_ok(ring.answer(INSTALL).expect("the first install answered"));
    assert_ok(ring.tick_until_answered(SECOND));
    let second_rows = (SECOND_FROM..SECOND_FROM + ROWS)
        .filter(|surrogate| {
            ring.core
                .sparse
                .get(
                    0,
                    TID,
                    OTHER,
                    &StorageKey::for_surrogate(Surrogate::new(*surrogate)),
                )
                .expect("read row")
                .is_some()
        })
        .count();
    assert_eq!(second_rows, ROWS as usize, "the second install landed");
    assert!(!ring.core.has_parked_install());
}

/// A duplicate of the parked install, at its LSN with its idempotency key,
/// waits until the install ends. It then answers from the idempotency
/// cache and installs nothing again.
#[test]
fn a_duplicate_of_the_parked_install_answers_from_idempotency() {
    let mut ring = Ring::open(plain_core);
    let plan = ring.marker_plan(
        &record(fill(FILL_FROM, ROWS)),
        vec![FILL.to_string()],
        Vec::new(),
        None,
    );
    let mut first = install_request(INSTALL, plan.clone(), LSN);
    first.idempotency_key = Some(IDEMPOTENCY_KEY);
    let mut duplicate = install_request(SECOND, plan, LSN);
    duplicate.idempotency_key = Some(IDEMPOTENCY_KEY);
    ring.push(first, None);
    ring.push(duplicate, None);
    ring.tick();
    assert!(ring.core.has_parked_install(), "the install parked");
    assert!(ring.answer(SECOND).is_none(), "the duplicate waits");

    tick_until_answered_while_waiting(&mut ring, INSTALL, &[SECOND]);
    assert_ok(ring.answer(INSTALL).expect("the install answered"));
    let duplicate = ring.tick_until_answered(SECOND).clone();
    assert_ok(&duplicate);
    assert_eq!(installed_rows(&ring.core), ROWS as usize);
    assert!(!ring.core.has_parked_install());
}
