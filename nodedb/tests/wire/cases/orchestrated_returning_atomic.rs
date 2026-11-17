// SPDX-License-Identifier: BUSL-1.1

//! An autocommit `INSERT ... SELECT`, `UPDATE ... FROM` or `MERGE` whose
//! `RETURNING` list can fail runs as one implicit transaction, as in
//! PostgreSQL: an error in the list fails the statement and changes no row,
//! and a list that succeeds answers the rows the statement wrote.

use crate::harness::TestServer;

use super::returning_vote_single_shard::{create, rows_of};
use super::returning_vote_single_shard_pgwire::simple_error;

/// Create `target` and `source`, each `(id TEXT PRIMARY KEY, n INT)`, and
/// seed them with `target_rows` and `source_rows`.
async fn seed(
    server: &TestServer,
    [target, source]: [&str; 2],
    target_rows: &[(&str, i64)],
    source_rows: &[(&str, i64)],
) {
    for (collection, rows) in [(target, target_rows), (source, source_rows)] {
        create(
            server,
            &format!("CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, n INT)"),
        )
        .await;
        for (id, n) in rows {
            server
                .exec(&format!(
                    "INSERT INTO {collection} (id, n) VALUES ('{id}', {n})"
                ))
                .await
                .unwrap_or_else(|e| panic!("seed {collection}: {e}"));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_list_on_insert_select_writes_no_row() {
    let server = TestServer::start().await;
    seed(
        &server,
        ["ora_is_dst", "ora_is_src"],
        &[],
        &[("a", 1), ("b", 2), ("c", 3)],
    )
    .await;

    let (code, message) = simple_error(
        &server.client,
        "INSERT INTO ora_is_dst (id, n) SELECT id, n FROM ora_is_src RETURNING 1 / (n - n)",
    )
    .await;
    assert_eq!(code, "22012", "{message}");
    assert!(
        rows_of(&server, "SELECT id FROM ora_is_dst")
            .await
            .is_empty(),
        "an error in the list stores no copied row"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_list_on_update_from_changes_nothing() {
    let server = TestServer::start().await;
    seed(
        &server,
        ["ora_uf_tgt", "ora_uf_src"],
        &[("a", 1), ("b", 2)],
        &[("a", 10), ("b", 20)],
    )
    .await;

    let (code, message) = simple_error(
        &server.client,
        "UPDATE ora_uf_tgt SET n = ora_uf_src.n FROM ora_uf_src \
         WHERE ora_uf_tgt.id = ora_uf_src.id RETURNING 1 / (n - n)",
    )
    .await;
    assert_eq!(code, "22012", "{message}");
    assert_eq!(
        rows_of(&server, "SELECT id, n FROM ora_uf_tgt").await,
        vec!["a\t1".to_owned(), "b\t2".to_owned()],
        "an error in the list changes no row"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_list_on_merge_changes_nothing() {
    let server = TestServer::start().await;
    seed(
        &server,
        ["ora_mg_tgt", "ora_mg_src"],
        &[("a", 1), ("b", 2)],
        &[("a", 10), ("c", 30)],
    )
    .await;

    let (code, message) = simple_error(
        &server.client,
        "MERGE INTO ora_mg_tgt t USING ora_mg_src s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET n = s.n \
         WHEN NOT MATCHED THEN INSERT (id, n) VALUES (s.id, s.n) \
         RETURNING 1 / (n - n)",
    )
    .await;
    assert_eq!(code, "22012", "{message}");
    assert_eq!(
        rows_of(&server, "SELECT id, n FROM ora_mg_tgt").await,
        vec!["a\t1".to_owned(), "b\t2".to_owned()],
        "an error in the list neither updates nor inserts"
    );
}

/// A list that succeeds answers every copied row, in copy order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_select_answers_its_rows_in_copy_order() {
    let server = TestServer::start().await;
    seed(
        &server,
        ["ora_iso_dst", "ora_iso_src"],
        &[],
        &[("b", 2), ("c", 3), ("a", 1)],
    )
    .await;

    let rows = server
        .query_rows(
            "INSERT INTO ora_iso_dst (id, n) SELECT id, n FROM ora_iso_src ORDER BY n DESC \
             RETURNING id, n * 2 AS d",
        )
        .await
        .expect("the copy answers its rows");
    assert_eq!(
        rows,
        vec![
            vec!["c".to_owned(), "6".to_owned()],
            vec!["b".to_owned(), "4".to_owned()],
            vec!["a".to_owned(), "2".to_owned()],
        ]
    );
    assert_eq!(
        rows_of(&server, "SELECT id, n FROM ora_iso_dst").await,
        vec!["a\t1".to_owned(), "b\t2".to_owned(), "c\t3".to_owned()]
    );
}

/// A list that succeeds answers each updated row's new image.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_from_answers_its_updated_rows() {
    let server = TestServer::start().await;
    seed(
        &server,
        ["ora_ufo_tgt", "ora_ufo_src"],
        &[("a", 1), ("b", 2), ("c", 3)],
        &[("a", 10), ("b", 20)],
    )
    .await;

    let mut rows = server
        .query_rows(
            "UPDATE ora_ufo_tgt SET n = ora_ufo_src.n FROM ora_ufo_src \
             WHERE ora_ufo_tgt.id = ora_ufo_src.id RETURNING id, n + 1 AS m",
        )
        .await
        .expect("the update answers its rows");
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec!["a".to_owned(), "11".to_owned()],
            vec!["b".to_owned(), "21".to_owned()],
        ]
    );
    assert_eq!(
        rows_of(&server, "SELECT id, n FROM ora_ufo_tgt").await,
        vec!["a\t10".to_owned(), "b\t20".to_owned(), "c\t3".to_owned()]
    );
}

/// A MERGE that updates, inserts and deletes answers its rows in source
/// order, as PostgreSQL answers them in join order: the source rows are
/// `c` (deleted), `x` (inserted) and `a` (updated), in that order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_answers_its_rows_in_source_order() {
    let server = TestServer::start().await;
    seed(
        &server,
        ["ora_mgo_tgt", "ora_mgo_src"],
        &[("a", 1), ("b", 2), ("c", 3)],
        &[("c", 0), ("x", 30), ("a", 10)],
    )
    .await;

    let rows = server
        .query_rows(
            "MERGE INTO ora_mgo_tgt t USING ora_mgo_src s ON t.id = s.id \
             WHEN MATCHED AND s.n = 0 THEN DELETE \
             WHEN MATCHED THEN UPDATE SET n = s.n \
             WHEN NOT MATCHED THEN INSERT (id, n) VALUES (s.id, s.n) \
             RETURNING id, n + 1 AS m",
        )
        .await
        .expect("the merge answers its rows");
    assert_eq!(
        rows,
        vec![
            vec!["c".to_owned(), "4".to_owned()],
            vec!["x".to_owned(), "31".to_owned()],
            vec!["a".to_owned(), "11".to_owned()],
        ]
    );
    assert_eq!(
        rows_of(&server, "SELECT id, n FROM ora_mgo_tgt").await,
        vec!["a\t10".to_owned(), "b\t2".to_owned(), "x\t30".to_owned()]
    );
}

/// Two sessions update the same row through `UPDATE ... FROM` with a list
/// that can fail. Each statement runs in its own implicit transaction, and
/// a COMMIT that loses to the other runs again on a new snapshot, so no
/// statement answers `40001`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_update_from_on_one_row_both_succeed() {
    let server = TestServer::start().await;
    seed(
        &server,
        ["ora_cc_tgt", "ora_cc_src"],
        &[("a", 0)],
        &[("a", 1)],
    )
    .await;
    let (other, _other_task) = server
        .connect_as("nodedb", "nodedb")
        .await
        .expect("a second session");

    let statement = "UPDATE ora_cc_tgt SET n = ora_cc_src.n FROM ora_cc_src \
                     WHERE ora_cc_tgt.id = ora_cc_src.id RETURNING id, n + 1 AS m";
    tokio::join!(
        update_repeatedly(&server.client, statement),
        update_repeatedly(&other, statement)
    );
    assert_eq!(
        rows_of(&server, "SELECT id, n FROM ora_cc_tgt").await,
        vec!["a\t1".to_owned()]
    );
}

/// Run `statement` 20 times on `client`. Each run answers one row.
async fn update_repeatedly(client: &tokio_postgres::Client, statement: &str) {
    for attempt in 0..20 {
        let messages = client
            .simple_query(statement)
            .await
            .unwrap_or_else(|e| panic!("attempt {attempt}: {e:?}"));
        let rows = messages
            .iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count();
        assert_eq!(rows, 1, "attempt {attempt} answers the row it updated");
    }
}
