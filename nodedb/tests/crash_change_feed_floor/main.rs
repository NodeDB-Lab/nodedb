// SPDX-License-Identifier: BUSL-1.1

//! A crash after an entry applied and before its group's applied floor is
//! saved loses no change event and publishes none twice.
//!
//! Each test arms `apply_loop::before_floor_save::<collection>` with a long
//! sleep. The point fires once boot's replay finished, for a pass that
//! published new events of the collection. The apply loop settles the
//! write's entry, publishes its change events, and sleeps before it journals
//! them and saves the floor. The test
//! reads the published events from the change feed, then kills the server
//! inside the sleep. After the restart the entry is delivered again as a
//! repeat, and its events stage again from the WAL. The feed then holds
//! each event exactly once, with the position, LSN and timestamp it had
//! before the crash. A second restart changes nothing.
//!
//! One test per source of change events:
//!
//! - a plan entry: a point insert, update and delete, each named by its plan;
//! - an inline transaction redo: rows named by the redo record;
//! - a Calvin cross-shard transaction: one slice per data group;
//! - a chunked redo: rows read back from the stream's chunks;
//! - a chunked redo past the change stream's spill bound: its pages stage
//!   to the change journal and publish from it.
//!
//! [`staging`] crashes a large chunked redo while its events stage, before
//! its entry settles.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

#[path = "../crash_harness/mod.rs"]
mod crash_harness;

mod feed;
mod staging;

use crash_harness::CrashHarness;
use crash_harness::vshards::names_in_distinct_data_groups;

use feed::{
    CrashCase, Expected, ROWS_PER_STATEMENT, armed, chunked_harness, chunked_statements,
    crash_before_the_floor_save, document_collection,
};

/// Statements of a chunked redo whose events pass the change stream's spill
/// bound of 1024 events.
pub(crate) const SPILLED_STATEMENTS: usize = 25;

#[tokio::test(flavor = "multi_thread")]
async fn a_point_insert_publishes_once_across_a_crash_before_its_floor() {
    crash_before_the_floor_save(CrashCase {
        harness: CrashHarness::new(),
        create: vec![document_collection("plan_insert")],
        before: Vec::new(),
        crashed: vec!["INSERT INTO plan_insert (id, value) VALUES ('p1', 'a')".into()],
        expected: vec![armed("plan_insert", 1)],
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_point_update_publishes_once_across_a_crash_before_its_floor() {
    crash_before_the_floor_save(CrashCase {
        harness: CrashHarness::new(),
        create: vec![document_collection("plan_update")],
        before: vec!["INSERT INTO plan_update (id, value) VALUES ('p1', 'a')".into()],
        crashed: vec!["UPDATE plan_update SET value = 'b' WHERE id = 'p1'".into()],
        expected: vec![armed("plan_update", 2)],
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_point_delete_publishes_once_across_a_crash_before_its_floor() {
    crash_before_the_floor_save(CrashCase {
        harness: CrashHarness::new(),
        create: vec![document_collection("plan_delete")],
        before: vec!["INSERT INTO plan_delete (id, value) VALUES ('p1', 'a')".into()],
        crashed: vec!["DELETE FROM plan_delete WHERE id = 'p1'".into()],
        expected: vec![armed("plan_delete", 2)],
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_inline_redo_publishes_once_across_a_crash_before_its_floor() {
    crash_before_the_floor_save(CrashCase {
        harness: CrashHarness::new(),
        create: vec![document_collection("inline_redo")],
        before: Vec::new(),
        crashed: vec![
            "BEGIN".into(),
            "INSERT INTO inline_redo (id, value) VALUES ('r1', 'a'), ('r2', 'b')".into(),
            "INSERT INTO inline_redo (id, value) VALUES ('r3', 'c')".into(),
            "COMMIT".into(),
        ],
        expected: vec![armed("inline_redo", 3)],
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_calvin_transaction_publishes_once_across_a_crash_before_its_floors() {
    let [left, right] = names_in_distinct_data_groups(["calvin_left", "calvin_right"]);
    crash_before_the_floor_save(CrashCase {
        harness: CrashHarness::new(),
        create: vec![document_collection(&left), document_collection(&right)],
        before: Vec::new(),
        crashed: vec![
            "BEGIN".into(),
            format!("INSERT INTO {left} (id, value) VALUES ('c1', 'a')"),
            format!("INSERT INTO {right} (id, value) VALUES ('c2', 'b')"),
            "COMMIT".into(),
        ],
        expected: vec![
            armed(&left, 1),
            Expected {
                collection: &right,
                count: 1,
                armed: false,
            },
        ],
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_chunked_redo_publishes_once_across_a_crash_before_its_floor() {
    const STATEMENTS: usize = 6;
    crash_before_the_floor_save(CrashCase {
        harness: chunked_harness(),
        create: vec![document_collection("chunked_redo")],
        before: Vec::new(),
        crashed: chunked_statements("chunked_redo", STATEMENTS),
        expected: vec![armed("chunked_redo", STATEMENTS * ROWS_PER_STATEMENT)],
    })
    .await;
}

/// A chunked redo whose events pass the change stream's spill bound stages
/// them in pages to the change journal. Its settle publishes them from
/// there, and the crash before its floor loses none and doubles none.
#[tokio::test(flavor = "multi_thread")]
async fn a_spilled_chunked_redo_publishes_once_across_a_crash_before_its_floor() {
    crash_before_the_floor_save(CrashCase {
        harness: chunked_harness(),
        create: vec![document_collection("spilled_redo")],
        before: Vec::new(),
        crashed: chunked_statements("spilled_redo", SPILLED_STATEMENTS),
        expected: vec![armed(
            "spilled_redo",
            SPILLED_STATEMENTS * ROWS_PER_STATEMENT,
        )],
    })
    .await;
}
