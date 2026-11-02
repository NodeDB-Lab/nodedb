// SPDX-License-Identifier: BUSL-1.1

//! Interactive transactions are serializable: a transaction whose reads a
//! concurrent commit changed fails its COMMIT with `40001`.
//!
//! Each case runs on every engine a read-modify-write can target: strict and
//! schemaless documents, Key-Value, and columnar.

use crate::harness::TestServer;

const PASSWORD: &str = "txn-serializable-7";

/// One collection shape per engine, keyed by `id` with an integer `n`.
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

async fn create(server: &TestServer, coll: &str, shape: &str) {
    server
        .exec(&format!("CREATE COLLECTION {coll} {shape}"))
        .await
        .unwrap_or_else(|e| panic!("CREATE COLLECTION {coll}: {e}"));
}

async fn put(server: &TestServer, coll: &str, id: &str, n: i64) {
    server
        .exec(&format!("INSERT INTO {coll} (id, n) VALUES ('{id}', {n})"))
        .await
        .unwrap_or_else(|e| panic!("INSERT {id} into {coll}: {e}"));
}

/// A second connection, as a user that can read and write.
async fn second_session(
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

async fn read_n(client: &tokio_postgres::Client, coll: &str, id: &str) -> i64 {
    let rows = client
        .simple_query(&format!("SELECT n FROM {coll} WHERE id = '{id}'"))
        .await
        .unwrap_or_else(|e| panic!("SELECT n of {id}: {e}"));
    rows.iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no row {id} in {coll}"))
        .parse()
        .expect("n is an integer")
}

async fn exec(client: &tokio_postgres::Client, sql: &str) {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn sqlstate(error: &tokio_postgres::Error) -> Option<String> {
    error.as_db_error().map(|e| e.code().code().to_string())
}

/// Assert `COMMIT` on `client` fails with `40001`.
async fn assert_commit_conflicts(client: &tokio_postgres::Client, case: &str) {
    let error = client
        .simple_query("COMMIT")
        .await
        .expect_err(&format!("{case}: the second committer must not commit"));
    assert_eq!(
        sqlstate(&error).as_deref(),
        Some("40001"),
        "{case}: {error:?}"
    );
}

/// Two transactions read the same row, then both write it. The second
/// committer fails with `40001`, so no update is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_second_read_modify_write_of_a_row_fails_40001() {
    let server = TestServer::start().await;
    let (b, handle) = second_session(&server, "lost_update_b").await;
    for (engine, shape) in ENGINES {
        let coll = format!("lost_update_{engine}");
        create(&server, &coll, shape).await;
        put(&server, &coll, "c", 0).await;

        let a = &server.client;
        exec(a, "BEGIN").await;
        assert_eq!(read_n(a, &coll, "c").await, 0);
        exec(&b, "BEGIN").await;
        assert_eq!(read_n(&b, &coll, "c").await, 0);
        exec(a, &format!("UPDATE {coll} SET n = 1 WHERE id = 'c'")).await;
        exec(a, "COMMIT").await;
        exec(&b, &format!("UPDATE {coll} SET n = 1 WHERE id = 'c'")).await;
        assert_commit_conflicts(&b, engine).await;

        assert_eq!(read_n(a, &coll, "c").await, 1, "{engine}");
    }
    drop(b);
    handle.abort();
}

/// Two spends of 700 from a balance of 1000. Each reads the balance and
/// checks it covers the spend. Exactly one commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_spends_from_one_balance_commit_one() {
    let server = TestServer::start().await;
    let (b, handle) = second_session(&server, "double_spend_b").await;
    for (engine, shape) in ENGINES {
        let coll = format!("double_spend_{engine}");
        create(&server, &coll, shape).await;
        put(&server, &coll, "acct", 1000).await;

        let a = &server.client;
        exec(a, "BEGIN").await;
        exec(&b, "BEGIN").await;
        let seen_a = read_n(a, &coll, "acct").await;
        let seen_b = read_n(&b, &coll, "acct").await;
        assert!(
            seen_a >= 700 && seen_b >= 700,
            "{engine}: both see the full balance"
        );
        exec(
            a,
            &format!("UPDATE {coll} SET n = {} WHERE id = 'acct'", seen_a - 700),
        )
        .await;
        exec(
            &b,
            &format!("UPDATE {coll} SET n = {} WHERE id = 'acct'", seen_b - 700),
        )
        .await;
        exec(a, "COMMIT").await;
        assert_commit_conflicts(&b, engine).await;

        assert_eq!(read_n(a, &coll, "acct").await, 300, "{engine}");
    }
    drop(b);
    handle.abort();
}

/// Write skew: two accounts must keep a non-negative sum. Each transaction
/// reads both, sees a sum of 1000, and withdraws 800 from a different
/// account. Snapshot isolation alone commits both. Serializable commits one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_skew_across_two_rows_commits_one() {
    let server = TestServer::start().await;
    let (b, handle) = second_session(&server, "write_skew_b").await;
    for (engine, shape) in ENGINES {
        let coll = format!("write_skew_{engine}");
        create(&server, &coll, shape).await;
        put(&server, &coll, "x", 500).await;
        put(&server, &coll, "y", 500).await;

        let a = &server.client;
        exec(a, "BEGIN").await;
        exec(&b, "BEGIN").await;
        let sum_a = read_n(a, &coll, "x").await + read_n(a, &coll, "y").await;
        let sum_b = read_n(&b, &coll, "x").await + read_n(&b, &coll, "y").await;
        assert_eq!((sum_a, sum_b), (1000, 1000), "{engine}");
        exec(a, &format!("UPDATE {coll} SET n = -300 WHERE id = 'x'")).await;
        exec(&b, &format!("UPDATE {coll} SET n = -300 WHERE id = 'y'")).await;
        exec(a, "COMMIT").await;
        assert_commit_conflicts(&b, engine).await;

        let sum = read_n(a, &coll, "x").await + read_n(a, &coll, "y").await;
        assert_eq!(sum, 200, "{engine}: the invariant holds");
    }
    drop(b);
    handle.abort();
}

/// Tasks that each increment one counter by read-then-write, retrying on
/// `40001`, count every commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_read_modify_write_increments_count_every_commit() {
    const TASKS: i64 = 16;
    let server = TestServer::start().await;
    create(
        &server,
        "counter",
        "(id TEXT PRIMARY KEY, n BIGINT) WITH (engine='document_strict')",
    )
    .await;
    put(&server, "counter", "c", 0).await;
    server
        .exec(&format!("CREATE USER counter_w PASSWORD '{PASSWORD}'"))
        .await
        .expect("CREATE USER");
    server
        .exec("GRANT ROLE readwrite TO counter_w")
        .await
        .expect("GRANT ROLE");

    let mut tasks = Vec::new();
    for _ in 0..TASKS {
        let (client, handle) = server
            .connect_as("counter_w", PASSWORD)
            .await
            .expect("connect");
        tasks.push(tokio::spawn(async move {
            let mut conflicts = 0u32;
            loop {
                exec(&client, "BEGIN").await;
                let n = read_n(&client, "counter", "c").await;
                let wrote = client
                    .simple_query(&format!("UPDATE counter SET n = {} WHERE id = 'c'", n + 1))
                    .await;
                let result = match wrote {
                    Ok(_) => client.simple_query("COMMIT").await.map(|_| ()),
                    Err(error) => {
                        let _ = client.simple_query("ROLLBACK").await;
                        Err(error)
                    }
                };
                match result {
                    Ok(()) => break,
                    Err(error) if sqlstate(&error).as_deref() == Some("40001") => {
                        conflicts += 1;
                        assert!(conflicts < 1000, "a task never committed");
                    }
                    Err(error) => panic!("increment: {error:?}"),
                }
            }
            drop(client);
            handle.abort();
        }));
    }
    for task in tasks {
        task.await.expect("increment task");
    }
    assert_eq!(read_n(&server.client, "counter", "c").await, TASKS);
}
