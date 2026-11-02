// SPDX-License-Identifier: BUSL-1.1

//! Every read inside a transaction block reads one snapshot: the cut its
//! first statement took. A concurrent commit after that cut stays invisible
//! to the block, and the session reports `serializable`.

use crate::harness::TestServer;

const PASSWORD: &str = "txn-snapshot-7";

const ENGINES: &[(&str, &str)] = &[
    (
        "strict",
        "(id TEXT PRIMARY KEY, n BIGINT) WITH (engine='document_strict')",
    ),
    ("schemaless", ""),
    ("kv", "(id TEXT PRIMARY KEY, n BIGINT) WITH (engine='kv')"),
    (
        "columnar",
        "COLUMNS (id TEXT, n BIGINT) WITH (engine='columnar')",
    ),
];

async fn writer(
    server: &TestServer,
    user: &str,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .expect("CREATE USER");
    server
        .exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .expect("GRANT ROLE");
    server
        .connect_as(user, PASSWORD)
        .await
        .expect("second connection")
}

async fn exec(client: &tokio_postgres::Client, sql: &str) {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

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

/// A concurrent commit after the block's first read is invisible to the
/// block's later reads, and a write based on the stale read fails `40001`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_keeps_reading_its_snapshot() {
    let server = TestServer::start().await;
    let (b, handle) = writer(&server, "snapshot_b").await;
    for (engine, shape) in ENGINES {
        let coll = format!("snap_{engine}");
        exec(&server.client, &format!("CREATE COLLECTION {coll} {shape}")).await;
        exec(
            &server.client,
            &format!("INSERT INTO {coll} (id, n) VALUES ('c', 0)"),
        )
        .await;

        let a = &server.client;
        let read = format!("SELECT n FROM {coll} WHERE id = 'c'");
        exec(a, "BEGIN").await;
        assert_eq!(column(a, &read).await, vec!["0"], "{engine}");
        exec(&b, &format!("UPDATE {coll} SET n = 42 WHERE id = 'c'")).await;
        assert_eq!(
            column(a, &read).await,
            vec!["0"],
            "{engine}: the block reads its snapshot, not the concurrent commit"
        );
        exec(a, &format!("UPDATE {coll} SET n = 7 WHERE id = 'c'")).await;
        let error = a
            .simple_query("COMMIT")
            .await
            .expect_err("a write over a stale read must not commit");
        let code = error.as_db_error().map(|e| e.code().code().to_string());
        assert_eq!(code.as_deref(), Some("40001"), "{engine}: {error:?}");
        assert_eq!(column(a, &read).await, vec!["42"], "{engine}");
    }
    drop(b);
    handle.abort();
}

/// A read-only block that reads its snapshot commits, even after a
/// concurrent write to what it read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_only_block_commits_over_a_concurrent_write() {
    let server = TestServer::start().await;
    let (b, handle) = writer(&server, "readonly_b").await;
    exec(
        &server.client,
        "CREATE COLLECTION ro (id TEXT PRIMARY KEY, n BIGINT) WITH (engine='document_strict')",
    )
    .await;
    exec(&server.client, "INSERT INTO ro (id, n) VALUES ('c', 0)").await;

    let a = &server.client;
    exec(a, "BEGIN").await;
    assert_eq!(
        column(a, "SELECT n FROM ro WHERE id = 'c'").await,
        vec!["0"]
    );
    exec(&b, "UPDATE ro SET n = 5 WHERE id = 'c'").await;
    assert_eq!(
        column(a, "SELECT n FROM ro WHERE id = 'c'").await,
        vec!["0"]
    );
    exec(a, "COMMIT").await;
    drop(b);
    handle.abort();
}

/// Paging through a table inside one block reads one cut: a concurrent
/// insert and delete between pages change no page.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pages_of_one_block_read_one_cut() {
    let server = TestServer::start().await;
    let (b, handle) = writer(&server, "paging_b").await;
    exec(
        &server.client,
        "CREATE COLLECTION pages (id TEXT PRIMARY KEY, n BIGINT) WITH (engine='document_strict')",
    )
    .await;
    for i in 0..10 {
        exec(
            &server.client,
            &format!("INSERT INTO pages (id, n) VALUES ('r{i}', {i})"),
        )
        .await;
    }

    let a = &server.client;
    exec(a, "BEGIN").await;
    let first = column(a, "SELECT id FROM pages ORDER BY id LIMIT 5").await;
    assert_eq!(first, vec!["r0", "r1", "r2", "r3", "r4"]);
    exec(&b, "INSERT INTO pages (id, n) VALUES ('r4a', 40)").await;
    exec(&b, "DELETE FROM pages WHERE id = 'r7'").await;
    let second = column(
        a,
        "SELECT id FROM pages WHERE id > 'r4' ORDER BY id LIMIT 5",
    )
    .await;
    assert_eq!(second, vec!["r5", "r6", "r7", "r8", "r9"]);
    let count = column(a, "SELECT COUNT(*) FROM pages").await;
    assert_eq!(count, vec!["10"]);
    exec(a, "COMMIT").await;

    let after = column(a, "SELECT COUNT(*) FROM pages").await;
    assert_eq!(
        after,
        vec!["10"],
        "one insert and one delete after the block"
    );
    drop(b);
    handle.abort();
}

/// The session reports serializable, and every isolation level `BEGIN`
/// names is accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_session_reports_serializable() {
    let server = TestServer::start().await;
    let a = &server.client;
    assert_eq!(
        column(a, "SHOW transaction_isolation").await,
        vec!["serializable"]
    );
    for level in ["SERIALIZABLE", "REPEATABLE READ", "READ COMMITTED"] {
        exec(a, &format!("BEGIN ISOLATION LEVEL {level}")).await;
        exec(a, "COMMIT").await;
    }
}
