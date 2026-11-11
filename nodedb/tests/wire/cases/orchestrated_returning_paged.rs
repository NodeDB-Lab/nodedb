// SPDX-License-Identifier: BUSL-1.1

//! `RETURNING` on an `INSERT ... SELECT`, `UPDATE ... FROM` and `MERGE`
//! inside a transaction block, on a paged copy, and at the limits that
//! bound them.
//!
//! - In a block, each answers the rows it staged, and ROLLBACK undoes them.
//! - On a node with no response limit, a copy whose list cannot fail writes
//!   page by page and answers one result set of every copied row, in copy
//!   order.
//! - Under any limit, a copy with `RETURNING` runs in an implicit
//!   transaction. Past a response limit or the staging budget it fails with
//!   `54000` and stores nothing.
//! - A copy that copies nothing answers its columns and no row.

use crate::harness::TestServer;

use super::returning_vote_single_shard::{create, rows_of};
use super::returning_vote_single_shard_pgwire::simple_error;

/// The rows a page source batch holds: the copy writes one page per batch.
const PAGE_ROWS: usize = 4_096;

/// Create `collection` as `(id TEXT PRIMARY KEY, n INT)` and seed rows
/// `r00000 .. r{count}` with `n` set to each row's index.
async fn seed_numbered(server: &TestServer, collection: &str, count: usize) {
    create(
        server,
        &format!("CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, n INT)"),
    )
    .await;
    let mut start = 0;
    while start < count {
        let end = (start + 500).min(count);
        let values: Vec<String> = (start..end)
            .map(|index| format!("('r{index:05}', {index})"))
            .collect();
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, n) VALUES {}",
                values.join(", ")
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {collection}: {e}"));
        start = end;
    }
}

/// The number of rows `collection` holds.
async fn count_of(server: &TestServer, collection: &str) -> usize {
    rows_of(server, &format!("SELECT id FROM {collection}"))
        .await
        .len()
}

/// In a block, each orchestrated write answers the rows it staged, and
/// ROLLBACK undoes all three.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_in_a_block_answer_their_rows_and_roll_back() {
    let server = TestServer::start().await;
    for collection in ["orp_blk_tgt", "orp_blk_src"] {
        create(
            &server,
            &format!("CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, n INT)"),
        )
        .await;
    }
    server
        .exec("INSERT INTO orp_blk_tgt (id, n) VALUES ('a', 1)")
        .await
        .expect("seed target");
    server
        .exec("INSERT INTO orp_blk_src (id, n) VALUES ('a', 10), ('b', 20)")
        .await
        .expect("seed source");

    server.exec("BEGIN").await.expect("BEGIN");
    let copied = server
        .query_rows(
            "INSERT INTO orp_blk_tgt (id, n) SELECT id, n FROM orp_blk_src \
             WHERE id = 'b' RETURNING id, n",
        )
        .await
        .expect("an INSERT ... SELECT in a block answers its rows");
    assert_eq!(copied, vec![vec!["b".to_owned(), "20".to_owned()]]);
    let mut updated = server
        .query_rows(
            "UPDATE orp_blk_tgt SET n = orp_blk_src.n + 1 FROM orp_blk_src \
             WHERE orp_blk_tgt.id = orp_blk_src.id RETURNING id, n",
        )
        .await
        .expect("an UPDATE ... FROM in a block answers its rows");
    updated.sort();
    assert_eq!(
        updated,
        vec![
            vec!["a".to_owned(), "11".to_owned()],
            vec!["b".to_owned(), "21".to_owned()],
        ],
        "the update reads the row the copy staged"
    );
    let merged = server
        .query_rows(
            "MERGE INTO orp_blk_tgt t USING orp_blk_src s ON t.id = s.id \
             WHEN MATCHED THEN DELETE RETURNING id",
        )
        .await
        .expect("a MERGE in a block answers its rows");
    let mut merged: Vec<String> = merged.into_iter().map(|row| row.join("\t")).collect();
    merged.sort();
    assert_eq!(merged, vec!["a".to_owned(), "b".to_owned()]);
    server.exec("ROLLBACK").await.expect("ROLLBACK");

    assert_eq!(
        rows_of(&server, "SELECT id, n FROM orp_blk_tgt").await,
        vec!["a\t1".to_owned()],
        "ROLLBACK undoes the copy, the update and the merge"
    );
}

/// On a node with no response limit, a copy across several pages with a
/// list that cannot fail answers every copied row, in copy order, as one
/// result set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paged_copy_answers_every_row_in_copy_order() {
    let server = TestServer::start_with_unlimited_returning().await;
    let count = 2 * PAGE_ROWS + 808;
    seed_numbered(&server, "orp_pg_src", count).await;
    create(
        &server,
        "CREATE COLLECTION orp_pg_dst (id TEXT PRIMARY KEY, n INT)",
    )
    .await;

    let rows = server
        .query_rows(
            "INSERT INTO orp_pg_dst (id, n) SELECT id, n FROM orp_pg_src ORDER BY n \
             RETURNING id, n",
        )
        .await
        .expect("the paged copy answers its rows");
    assert_eq!(rows.len(), count, "every copied row answers");
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(
            row,
            &vec![format!("r{index:05}"), index.to_string()],
            "row {index} answers in copy order"
        );
    }
    assert_eq!(count_of(&server, "orp_pg_dst").await, count);
}

/// Under a row limit, a copy across several pages with `RETURNING` runs in
/// one transaction. Past the limit it fails with `54000` and stores no row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_past_the_row_limit_stores_nothing() {
    let limit = PAGE_ROWS + 904;
    let server = TestServer::start_multicores_with_max_returning_rows(2, limit as u64).await;
    seed_numbered(&server, "orp_lim_src", 2 * PAGE_ROWS + 10).await;
    create(
        &server,
        "CREATE COLLECTION orp_lim_dst (id TEXT PRIMARY KEY, n INT)",
    )
    .await;

    let (code, message) = simple_error(
        &server.client,
        "INSERT INTO orp_lim_dst (id, n) SELECT id, n FROM orp_lim_src ORDER BY n RETURNING id",
    )
    .await;
    assert_eq!(code, "54000", "{message}");
    assert!(
        message.contains(&format!("exceeds limit {limit}")),
        "{message}"
    );
    assert_eq!(
        count_of(&server, "orp_lim_dst").await,
        0,
        "a limit error rolls the whole copy back"
    );
}

/// A copy whose list can fail stages every row in one transaction. Past the
/// per-core staging budget it fails with `54000` and stores nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_past_the_staging_budget_fails_with_54000() {
    let server = TestServer::start_with_max_txn_overlay_bytes(64 * 1024).await;
    for collection in ["orp_big_src", "orp_big_dst"] {
        create(
            &server,
            &format!("CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, b TEXT)"),
        )
        .await;
    }
    // 50 rows of 4 KiB stage past the 64 KiB budget.
    let body = "x".repeat(4 * 1024);
    for index in 0..50 {
        server
            .exec(&format!(
                "INSERT INTO orp_big_src (id, b) VALUES ('r{index:02}', '{body}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed row {index}: {e}"));
    }

    let (code, message) = simple_error(
        &server.client,
        "INSERT INTO orp_big_dst (id, b) SELECT id, b FROM orp_big_src RETURNING length(b)",
    )
    .await;
    assert_eq!(code, "54000", "{message}");
    assert_eq!(
        count_of(&server, "orp_big_dst").await,
        0,
        "the refused transaction stores no row"
    );
}

/// The columns `sql`'s RowDescription names, and the rows it answers, on the
/// simple query protocol. Panics when it sends no RowDescription.
async fn described(server: &TestServer, sql: &str) -> (Vec<String>, usize) {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut columns = None;
    let mut rows = 0;
    for message in messages {
        match message {
            tokio_postgres::SimpleQueryMessage::RowDescription(described) => {
                columns = Some(described.iter().map(|c| c.name().to_owned()).collect());
            }
            tokio_postgres::SimpleQueryMessage::Row(_) => rows += 1,
            _ => {}
        }
    }
    (
        columns.unwrap_or_else(|| panic!("{sql}: no RowDescription")),
        rows,
    )
}

/// A copy that copies nothing still answers its columns, with no row, on
/// the transaction route and on the paged route.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_copy_answers_its_columns() {
    for server in [
        TestServer::start().await,
        TestServer::start_with_unlimited_returning().await,
    ] {
        seed_numbered(&server, "orp_none_src", 3).await;
        create(
            &server,
            "CREATE COLLECTION orp_none_dst (id TEXT PRIMARY KEY, n INT)",
        )
        .await;
        let (columns, rows) = described(
            &server,
            "INSERT INTO orp_none_dst (id, n) SELECT id, n FROM orp_none_src WHERE n < 0 \
             RETURNING id, n",
        )
        .await;
        assert_eq!(columns, vec!["id".to_owned(), "n".to_owned()]);
        assert_eq!(rows, 0);
    }
}
