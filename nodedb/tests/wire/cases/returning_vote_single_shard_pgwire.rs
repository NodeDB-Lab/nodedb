// SPDX-License-Identifier: BUSL-1.1

//! An autocommit write on one vShard whose `RETURNING` list can fail commits
//! through Calvin over pgwire, on the simple and the extended query
//! protocol: an error in the list fails the statement and stores nothing.

use crate::harness::TestServer;

use super::returning_vote_single_shard::{PREDICATE_COLLECTIONS, create, rows_of};
use super::returning_vote_single_shard_engines::ENGINES;

/// The SQLSTATE and message `sql` fails with on the simple query protocol.
pub(super) async fn simple_error(client: &tokio_postgres::Client, sql: &str) -> (String, String) {
    let error = match client.simple_query(sql).await {
        Ok(_) => panic!("the statement fails: {sql}"),
        Err(error) => error,
    };
    db_error(&error, sql)
}

/// The SQLSTATE and message `sql` fails with on the extended query
/// protocol: Parse, Bind and Execute.
pub(super) async fn extended_error(client: &tokio_postgres::Client, sql: &str) -> (String, String) {
    let error = match client.query(sql, &[]).await {
        Ok(_) => panic!("the statement fails: {sql}"),
        Err(error) => error,
    };
    db_error(&error, sql)
}

fn db_error(error: &tokio_postgres::Error, sql: &str) -> (String, String) {
    let db = error
        .as_db_error()
        .unwrap_or_else(|| panic!("{sql}: a server error, got {error:?}"));
    (db.code().code().to_owned(), db.message().to_owned())
}

/// Assert `sql` fails with `code` on both query protocols. `sql` holds a
/// `{k}` the simple run binds to `simple` and the extended run to `extended`.
pub(super) async fn fails_on_both(
    client: &tokio_postgres::Client,
    sql: &str,
    [simple, extended]: [&str; 2],
    code: &str,
) {
    let simple_sql = sql.replace("{k}", simple);
    let (found, message) = simple_error(client, &simple_sql).await;
    assert_eq!(found, code, "simple: {simple_sql}: {message}");
    let extended_sql = sql.replace("{k}", extended);
    let (found, message) = extended_error(client, &extended_sql).await;
    assert_eq!(found, code, "extended: {extended_sql}: {message}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_list_on_an_insert_stores_nothing() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION rvp_ins (id TEXT PRIMARY KEY, n INT)",
    )
    .await;

    fails_on_both(
        &server.client,
        "INSERT INTO rvp_ins (id, n) VALUES ('{k}', 5) RETURNING 1 / (n - n)",
        ["a", "b"],
        "22012",
    )
    .await;

    assert!(rows_of(&server, "SELECT id FROM rvp_ins").await.is_empty());
}

/// The list draws `nextval`, then the division fails. The drawn value
/// stands in the pgwire session, on each protocol.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_list_keeps_the_values_it_drew() {
    let server = TestServer::start().await;
    create(
        &server,
        "CREATE COLLECTION rvp_seq (id TEXT PRIMARY KEY, n INT)",
    )
    .await;
    for sequence in ["s_simple", "s_extended"] {
        create(&server, &format!("CREATE SEQUENCE {sequence}")).await;
    }

    let (code, message) = simple_error(
        &server.client,
        "INSERT INTO rvp_seq (id, n) VALUES ('a', 5) RETURNING nextval('s_simple'), 1 / (n - n)",
    )
    .await;
    assert_eq!(code, "22012", "{message}");
    let (code, message) = extended_error(
        &server.client,
        "INSERT INTO rvp_seq (id, n) VALUES ('b', 5) RETURNING nextval('s_extended'), 1 / (n - n)",
    )
    .await;
    assert_eq!(code, "22012", "{message}");

    for sequence in ["s_simple", "s_extended"] {
        let currval = server
            .query_text(&format!("SELECT currval('{sequence}')"))
            .await
            .expect("currval after the refusal");
        assert_eq!(currval, ["1"], "{sequence}: the refused list drew 1");
        let next = server
            .query_text(&format!("SELECT nextval('{sequence}')"))
            .await
            .expect("nextval after the refusal");
        assert_eq!(next, ["2"], "{sequence}: the drawn value stands");
    }
    assert!(rows_of(&server, "SELECT id FROM rvp_seq").await.is_empty());
}

/// Three rows on one vShard pass `max_returning_rows = 2`. The update fails
/// with `54000` and changes no row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returning_past_the_row_limit_changes_no_row() {
    let server = TestServer::start_multicores_with_max_returning_rows(2, 2).await;
    create(
        &server,
        "CREATE COLLECTION rvp_lim (id TEXT PRIMARY KEY, n INT) WITH (engine='document_strict')",
    )
    .await;
    server
        .exec("INSERT INTO rvp_lim (id, n) VALUES ('r1', 0), ('r2', 0), ('r3', 0)")
        .await
        .expect("seed rvp_lim");

    let sql = "UPDATE rvp_lim SET n = 5 WHERE n = 0 RETURNING id";
    let (code, message) = simple_error(&server.client, sql).await;
    assert_eq!(code, "54000", "{message}");
    assert!(message.contains("exceeds limit 2"), "{message}");
    let (code, message) = extended_error(&server.client, sql).await;
    assert_eq!(code, "54000", "{message}");

    assert_eq!(
        rows_of(&server, "SELECT id, n FROM rvp_lim").await,
        ["r1\t0", "r2\t0", "r3\t0"],
        "no row changed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_list_stores_nothing_on_every_engine() {
    let server = TestServer::start().await;
    for (index, engine) in ENGINES.iter().enumerate() {
        let collection = format!("rvp_engine_{index}");
        let insert = engine.insert.replace("{c}", &collection);
        create(&server, &engine.ddl.replace("{c}", &collection)).await;
        fails_on_both(&server.client, &insert, ["1000", "2000"], "22012").await;
        assert!(
            rows_of(&server, &engine.read.replace("{c}", &collection))
                .await
                .is_empty(),
            "{}: the refusals stored nothing",
            engine.engine
        );
    }
}

/// An update and a delete by predicate fail in their list on both
/// protocols. Neither changes a row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predicate_write_with_a_failing_list_changes_nothing() {
    let server = TestServer::start().await;
    for (collection, ddl) in PREDICATE_COLLECTIONS {
        create(&server, ddl).await;
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, n) VALUES ('r1', 0), ('r2', 0)"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {collection}: {e}"));

        for sql in [
            format!("UPDATE {collection} SET n = 5 WHERE n = 0 RETURNING 1 / (n - n)"),
            format!("DELETE FROM {collection} WHERE n = 0 RETURNING 1 / (n - n)"),
        ] {
            fails_on_both(&server.client, &sql, ["", ""], "22012").await;
        }

        assert_eq!(
            rows_of(&server, &format!("SELECT id, n FROM {collection}")).await,
            ["r1\t0", "r2\t0"],
            "{collection}: no row changed"
        );
    }
}
