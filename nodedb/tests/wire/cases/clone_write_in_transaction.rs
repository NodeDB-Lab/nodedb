// SPDX-License-Identifier: BUSL-1.1

//! A write to a clone inside BEGIN..COMMIT takes the same copy-on-write
//! steps as an autocommit one. Its copy-up and tombstones commit with the
//! transaction and roll back with it, and the source never changes.

use crate::harness::TestServer;

/// A source database with `p1` and `p2`, and a clone of it. Leaves the
/// session in the clone.
async fn clone_with_two_rows(server: &TestServer, source: &str, clone: &str) {
    for sql in [
        format!("CREATE DATABASE {source}"),
        format!("USE DATABASE {source}"),
        "CREATE COLLECTION products (key STRING PRIMARY KEY, price STRING) \
         WITH (engine='kv')"
            .to_string(),
        "INSERT INTO products (key, price) VALUES ('p1', '10')".to_string(),
        "INSERT INTO products (key, price) VALUES ('p2', '20')".to_string(),
        "USE DATABASE default".to_string(),
        format!("CLONE DATABASE {clone} FROM {source} LATEST"),
        format!("USE DATABASE {clone}"),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

async fn prices(server: &TestServer, database: &str) -> Vec<Vec<String>> {
    server
        .exec(&format!("USE DATABASE {database}"))
        .await
        .unwrap_or_else(|e| panic!("USE {database}: {e}"));
    server
        .query_rows("SELECT key, price FROM products ORDER BY key")
        .await
        .unwrap_or_else(|e| panic!("read {database}: {e}"))
}

fn row(key: &str, price: &str) -> Vec<String> {
    vec![key.to_string(), price.to_string()]
}

/// A committed update and delete on source-only rows land in the clone, and
/// the source keeps its rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committed_clone_write_lands_in_the_clone_only() {
    let server = TestServer::start().await;
    clone_with_two_rows(&server, "cwt_src", "cwt_clone").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE products SET price = '99' WHERE key = 'p1'")
        .await
        .unwrap();
    server
        .exec("DELETE FROM products WHERE key = 'p2'")
        .await
        .unwrap();
    assert_eq!(
        server
            .query_rows("SELECT key, price FROM products ORDER BY key")
            .await
            .unwrap(),
        vec![row("p1", "99")],
        "the transaction reads its own clone writes"
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(prices(&server, "cwt_clone").await, vec![row("p1", "99")]);
    assert_eq!(
        prices(&server, "cwt_src").await,
        vec![row("p1", "10"), row("p2", "20")],
        "the source never changes"
    );
}

/// A rolled-back update and delete leave the clone reading the source rows:
/// no copy-up and no tombstone survives the rollback.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rolled_back_clone_write_leaves_nothing() {
    let server = TestServer::start().await;
    clone_with_two_rows(&server, "cwr_src", "cwr_clone").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE products SET price = '99' WHERE key = 'p1'")
        .await
        .unwrap();
    server
        .exec("DELETE FROM products WHERE key = 'p2'")
        .await
        .unwrap();
    server.exec("ROLLBACK").await.unwrap();

    assert_eq!(
        prices(&server, "cwr_clone").await,
        vec![row("p1", "10"), row("p2", "20")]
    );
    assert_eq!(
        prices(&server, "cwr_src").await,
        vec![row("p1", "10"), row("p2", "20")]
    );
}

const PASSWORD: &str = "clone-copyup-snapshot-3";

/// The first column of every row `sql` returns.
async fn column(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .collect()
}

/// A concurrent copy-up installs the source row's own version in the clone.
/// A block that read the row through the source before the copy-up keeps
/// reading the same row from its snapshot, and commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_concurrent_copy_up_keeps_a_block_snapshot_stable() {
    let server = TestServer::start().await;
    clone_with_two_rows(&server, "cws_src", "cws_clone").await;
    server
        .exec(&format!("CREATE USER cws_writer PASSWORD '{PASSWORD}'"))
        .await
        .expect("CREATE USER");
    // The copy-up reads the source, so the writer needs both databases.
    for database in ["cws_clone", "cws_src"] {
        server
            .exec(&format!("GRANT ALL ON DATABASE {database} TO cws_writer"))
            .await
            .unwrap_or_else(|e| panic!("GRANT ON {database}: {e}"));
    }
    let (writer, _connection) = server
        .connect_as("cws_writer", PASSWORD)
        .await
        .expect("second connection");
    writer
        .simple_query("USE DATABASE cws_clone")
        .await
        .expect("USE the clone");

    let price = "SELECT price FROM products WHERE key = 'p1'";
    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        column(&server.client, price).await,
        ["10"],
        "the block reads the source row through the clone"
    );

    // The autocommit UPDATE copies `p1` up into the clone, then writes it.
    writer
        .simple_query("UPDATE products SET price = '99' WHERE key = 'p1'")
        .await
        .expect("autocommit update of a source-only row");

    assert_eq!(
        column(&server.client, price).await,
        ["10"],
        "the copy-up keeps the row at its snapshot: same value, same version"
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        prices(&server, "cws_clone").await,
        vec![row("p1", "99"), row("p2", "20")]
    );
    assert_eq!(
        prices(&server, "cws_src").await,
        vec![row("p1", "10"), row("p2", "20")],
        "the source never changes"
    );
}
