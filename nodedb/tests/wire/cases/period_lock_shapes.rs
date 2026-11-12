// SPDX-License-Identifier: BUSL-1.1

//! Period-locked writes of other shapes than the insert in `period_lock`: an
//! unknown period, a point delete, a predicate update, a strict reference
//! collection, a close that comes first in the same block, two Data Plane
//! cores, and two blocks that share one open period.
//!
//! Every block writes two collections on two vShards, so its COMMIT is one
//! Calvin transaction.

#![cfg(feature = "failpoints")]

use super::period_lock::{
    ORD_ANSWER_WITHIN, block, create_side, is_period_refusal, name_off, seed_period, setup,
    spawn_request, vshard,
};
use crate::harness::TestServer;

/// Run `sql` as one block on its own connection and return the answer. The
/// answer must come within the statement deadline: a halted vShard answers
/// `57014` after 30 s.
async fn answer_of(server: &TestServer, sql: String) -> Result<(), String> {
    let task = spawn_request(server, block(&sql)).await;
    tokio::time::timeout(ORD_ANSWER_WITHIN, task)
        .await
        .expect("the block must answer within the deadline")
        .expect("block task")
}

/// The block must be refused with the period-locked answer.
fn assert_period_refused(answer: &Result<(), String>, what: &str) {
    match answer {
        Ok(()) => panic!("{what} applied"),
        Err(message) => assert!(
            is_period_refusal(message),
            "{what}: the refusal must name the period lock or a serialization conflict: {message}"
        ),
    }
}

async fn ids(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT id FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"))
}

/// A write into a period that has no row. The reference row does not exist,
/// so the write is refused before it is sequenced. No row applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_that_writes_into_an_unknown_period_is_refused() {
    let server = TestServer::start().await;
    let periods = "pl_un_periods";
    let entries = name_off("pl_un_entries", &[periods]);
    let side = name_off("pl_un_side", &[periods, entries.as_str()]);
    setup(&server, periods, &entries).await;
    seed_period(&server, periods, "2026-01", "OPEN").await;
    create_side(&server, &side).await;

    let answer = answer_of(
        &server,
        format!(
            "INSERT INTO {entries} (id, fiscal_period, amount) VALUES ('e1', '2099-01', 5); \
             INSERT INTO {side} (id, v) VALUES ('w1', 1)"
        ),
    )
    .await;
    match &answer {
        Ok(()) => panic!("a block applied into a period with no row"),
        Err(message) => assert!(
            is_period_refusal(message) || message.to_lowercase().contains("period"),
            "the refusal must name the period: {message}"
        ),
    }
    assert!(ids(&server, &entries).await.is_empty());
    assert!(ids(&server, &side).await.is_empty());
}

/// A point delete of an entry whose period closed after the entry landed. The
/// delete is refused and the entry stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_that_deletes_an_entry_of_a_closed_period_is_refused() {
    let server = TestServer::start().await;
    let periods = "pl_dl_periods";
    let entries = name_off("pl_dl_entries", &[periods]);
    let side = name_off("pl_dl_side", &[periods, entries.as_str()]);
    setup(&server, periods, &entries).await;
    seed_period(&server, periods, "2026-01", "OPEN").await;
    create_side(&server, &side).await;
    server
        .exec(&format!(
            "INSERT INTO {entries} (id, fiscal_period, amount) VALUES ('e1', '2026-01', 5)"
        ))
        .await
        .expect("seed the entry");
    server
        .exec(&format!(
            "UPDATE {periods} SET status = 'CLOSED' WHERE period_key = '2026-01'"
        ))
        .await
        .expect("close the period");

    let answer = answer_of(
        &server,
        format!(
            "DELETE FROM {entries} WHERE id = 'e1'; \
             INSERT INTO {side} (id, v) VALUES ('w1', 1)"
        ),
    )
    .await;
    assert_period_refused(&answer, "a delete in a closed period");
    assert_eq!(ids(&server, &entries).await, ["e1"]);
    assert!(ids(&server, &side).await.is_empty());
}

/// A predicate update of entries in a closed period. The update resolves its
/// rows first, then writes them. The refusal comes with the period-locked
/// answer, and the amount stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_with_a_predicate_update_in_a_closed_period_is_refused() {
    let server = TestServer::start().await;
    let periods = "pl_pu_periods";
    let entries = name_off("pl_pu_entries", &[periods]);
    let side = name_off("pl_pu_side", &[periods, entries.as_str()]);
    setup(&server, periods, &entries).await;
    seed_period(&server, periods, "2026-01", "OPEN").await;
    create_side(&server, &side).await;
    server
        .exec(&format!(
            "INSERT INTO {entries} (id, fiscal_period, amount) VALUES ('e1', '2026-01', 5)"
        ))
        .await
        .expect("seed the entry");
    server
        .exec(&format!(
            "UPDATE {periods} SET status = 'CLOSED' WHERE period_key = '2026-01'"
        ))
        .await
        .expect("close the period");

    let answer = answer_of(
        &server,
        format!(
            "UPDATE {entries} SET amount = 9 WHERE amount > 0; \
             INSERT INTO {side} (id, v) VALUES ('w1', 1)"
        ),
    )
    .await;
    assert_period_refused(&answer, "a predicate update in a closed period");
    let amounts = server
        .query_text(&format!("SELECT amount FROM {entries} WHERE id = 'e1'"))
        .await
        .unwrap();
    assert_eq!(amounts, ["5"], "a refused block changes no amount");
    assert!(ids(&server, &side).await.is_empty());
}

/// The reference collection is strict. The status read relies on the point
/// read answering a normalized row. A write into a closed period is refused,
/// and a write into an open one applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_strict_reference_collection_judges_the_period() {
    let server = TestServer::start().await;
    let periods = "pl_st_periods";
    let entries = name_off("pl_st_entries", &[periods]);
    let side = name_off("pl_st_side", &[periods, entries.as_str()]);
    server
        .exec(&format!(
            "CREATE COLLECTION {periods} (period_key TEXT PRIMARY KEY, status TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create the strict period collection");
    server
        .exec(&format!(
            "CREATE COLLECTION {entries} (id TEXT PRIMARY KEY, fiscal_period TEXT, amount INT) \
             WITH (engine='document_schemaless')"
        ))
        .await
        .expect("create the entries collection");
    server
        .exec(&format!(
            "ALTER COLLECTION {entries} ADD PERIOD LOCK ON fiscal_period \
             REFERENCES {periods}(period_key) STATUS status \
             ALLOW WRITE WHEN status IN ('OPEN')"
        ))
        .await
        .expect("add the period lock");
    seed_period(&server, periods, "2026-01", "CLOSED").await;
    seed_period(&server, periods, "2026-02", "OPEN").await;
    create_side(&server, &side).await;

    let refused = answer_of(
        &server,
        format!(
            "INSERT INTO {entries} (id, fiscal_period, amount) VALUES ('e1', '2026-01', 5); \
             INSERT INTO {side} (id, v) VALUES ('w1', 1)"
        ),
    )
    .await;
    assert_period_refused(&refused, "a write into a closed strict period");
    assert!(ids(&server, &entries).await.is_empty());

    let applied = answer_of(
        &server,
        format!(
            "INSERT INTO {entries} (id, fiscal_period, amount) VALUES ('e2', '2026-02', 5); \
             INSERT INTO {side} (id, v) VALUES ('w2', 2)"
        ),
    )
    .await;
    assert_eq!(applied, Ok(()), "a write into an open strict period");
    assert_eq!(ids(&server, &entries).await, ["e2"]);
}

/// `BEGIN; UPDATE periods (close); INSERT entries; COMMIT`, ten rounds. The
/// write comes after the close in the same block, and the lock judges the
/// committed row, which is open. The answer is a commit or a period refusal,
/// never a hang. The rows of one round apply together or not at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_that_closes_a_period_then_writes_into_it_answers_ten_times() {
    let server = TestServer::start().await;
    let periods = "pl_cw_periods";
    let entries = name_off("pl_cw_entries", &[periods]);
    setup(&server, periods, &entries).await;
    for round in 0..10 {
        let key = format!("2026-{:02}", round + 1);
        seed_period(&server, periods, &key, "OPEN").await;
        let answer = answer_of(
            &server,
            format!(
                "UPDATE {periods} SET status = 'CLOSED' WHERE period_key = '{key}'; \
                 INSERT INTO {entries} (id, fiscal_period, amount) \
                 VALUES ('e{round}', '{key}', 5)"
            ),
        )
        .await;
        let stored = server
            .query_text(&format!("SELECT id FROM {entries} WHERE id = 'e{round}'"))
            .await
            .unwrap();
        let status = server
            .query_text(&format!(
                "SELECT status FROM {periods} WHERE period_key = '{key}'"
            ))
            .await
            .unwrap();
        match answer {
            Ok(()) => {
                assert_eq!(stored, [format!("e{round}")], "round {round}: entry");
                assert_eq!(status, ["CLOSED"], "round {round}: status");
            }
            Err(message) => {
                assert!(is_period_refusal(&message), "round {round}: {message}");
                assert!(stored.is_empty(), "round {round}: refused, entry applied");
                assert_eq!(status, ["OPEN"], "round {round}: refused, period closed");
            }
        }
    }
}

/// A name that starts with `prefix` and homes on a vShard whose core, with
/// two cores, differs from the core of every name in `taken`.
fn name_on_other_core(prefix: &str, taken: &[&str]) -> String {
    let cores: Vec<u32> = taken.iter().map(|name| vshard(name) % 2).collect();
    (0..)
        .map(|n| format!("{prefix}{n}"))
        .find(|name| !cores.contains(&(vshard(name) % 2)))
        .expect("a name on the other core")
}

/// Two Data Plane cores: the period row lives on one core and the entries on
/// the other. A write into a closed period is refused with the period-locked
/// answer. An install that reads its own core's store finds no period row and
/// halts instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_into_a_closed_period_is_refused_when_the_period_is_on_another_core() {
    let server = TestServer::start_multicores(2).await;
    let periods = "pl_c2_periods";
    let entries = name_on_other_core("pl_c2_entries", &[periods]);
    let side = name_off("pl_c2_side", &[periods, entries.as_str()]);
    setup(&server, periods, &entries).await;
    seed_period(&server, periods, "2026-01", "CLOSED").await;
    seed_period(&server, periods, "2026-02", "OPEN").await;
    create_side(&server, &side).await;

    let refused = answer_of(
        &server,
        format!(
            "INSERT INTO {entries} (id, fiscal_period, amount) VALUES ('e1', '2026-01', 5); \
             INSERT INTO {side} (id, v) VALUES ('w1', 1)"
        ),
    )
    .await;
    assert_period_refused(&refused, "a write into a closed period on another core");
    assert!(ids(&server, &entries).await.is_empty());

    let applied = answer_of(
        &server,
        format!(
            "INSERT INTO {entries} (id, fiscal_period, amount) VALUES ('e2', '2026-02', 5); \
             INSERT INTO {side} (id, v) VALUES ('w2', 2)"
        ),
    )
    .await;
    assert_eq!(
        applied,
        Ok(()),
        "a write into an open period on another core"
    );
    assert_eq!(ids(&server, &entries).await, ["e2"]);
}

/// Guard for the extra read. Two blocks write into one open period at once.
/// The read locks the period row `Shared`, so the blocks do not conflict, and
/// both apply. Expected green on base: B1 drops it if so.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_blocks_that_write_into_one_open_period_both_apply() {
    let server = TestServer::start().await;
    let periods = "pl_sh_periods";
    let entries = name_off("pl_sh_entries", &[periods]);
    let side = name_off("pl_sh_side", &[periods, entries.as_str()]);
    setup(&server, periods, &entries).await;
    seed_period(&server, periods, "2026-01", "OPEN").await;
    create_side(&server, &side).await;

    let sql = |n: u32| {
        block(&format!(
            "INSERT INTO {entries} (id, fiscal_period, amount) VALUES ('e{n}', '2026-01', {n}); \
             INSERT INTO {side} (id, v) VALUES ('w{n}', {n})"
        ))
    };
    let first = spawn_request(&server, sql(1)).await;
    let second = spawn_request(&server, sql(2)).await;
    for (what, task) in [("first", first), ("second", second)] {
        let answer = tokio::time::timeout(ORD_ANSWER_WITHIN, task)
            .await
            .unwrap_or_else(|_| panic!("the {what} block must answer within the deadline"))
            .expect("block task");
        assert_eq!(answer, Ok(()), "the {what} block");
    }
    let mut stored = ids(&server, &entries).await;
    stored.sort();
    assert_eq!(stored, ["e1", "e2"]);
}
