// SPDX-License-Identifier: BUSL-1.1

//! LATERAL on a multi-core server whose outer and inner collections live on
//! different cores. The inner side reads its owning core, and the outer side
//! reads its own, so no outer row loses its inner rows.

use nodedb_types::id::DatabaseId;

use crate::harness::TestServer;

/// Data Plane cores the server runs.
const CORES: usize = 2;

fn core_of(name: &str) -> usize {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32() as usize
        % CORES
}

/// An outer and an inner collection name on different cores.
fn collections_on_two_cores() -> (String, String) {
    let outer = "lmc_users".to_owned();
    let inner = (0..)
        .map(|n| format!("lmc_events_{n}"))
        .find(|name| core_of(name) != core_of(&outer))
        .expect("a name on the other core");
    (outer, inner)
}

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Two users and five events. u1 has e1..e3, u2 has e4 and e5.
async fn seed(server: &TestServer, users: &str, events: &str) {
    exec(
        server,
        &format!(
            "CREATE COLLECTION {users} (id TEXT PRIMARY KEY, name TEXT, created_at BIGINT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        server,
        &format!(
            "CREATE COLLECTION {events} (id TEXT PRIMARY KEY, user_id TEXT, score INT, \
             log_time BIGINT) WITH (engine='document_strict')"
        ),
    )
    .await;
    for (id, name, created) in [("u1", "Alice", 100), ("u2", "Bob", 200)] {
        exec(
            server,
            &format!(
                "INSERT INTO {users} (id, name, created_at) VALUES ('{id}', '{name}', {created})"
            ),
        )
        .await;
    }
    for (id, uid, score, log_time) in [
        ("e1", "u1", 10, 150),
        ("e2", "u1", 30, 160),
        ("e3", "u1", 20, 170),
        ("e4", "u2", 50, 250),
        ("e5", "u2", 40, 260),
    ] {
        exec(
            server,
            &format!(
                "INSERT INTO {events} (id, user_id, score, log_time) \
                 VALUES ('{id}', '{uid}', {score}, {log_time})"
            ),
        )
        .await;
    }
}

async fn sorted_text(server: &TestServer, sql: &str) -> Vec<String> {
    let mut rows = server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.sort_unstable();
    rows
}

/// A top-k LATERAL joins each user to its best event.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lateral_top_k_reads_an_inner_collection_on_another_core() {
    let server = TestServer::start_multicores(CORES).await;
    let (users, events) = collections_on_two_cores();
    seed(&server, &users, &events).await;

    let rows = sorted_text(
        &server,
        &format!(
            "SELECT u.id FROM {users} u, \
             LATERAL (SELECT e.id FROM {events} e WHERE e.user_id = u.id \
             ORDER BY e.score DESC LIMIT 1) best"
        ),
    )
    .await;
    assert_eq!(rows, vec!["u1", "u2"]);
}

/// A non-equi LATERAL loop joins each user to every later event.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lateral_loop_reads_an_inner_collection_on_another_core() {
    let server = TestServer::start_multicores(CORES).await;
    let (users, events) = collections_on_two_cores();
    seed(&server, &users, &events).await;

    let rows = sorted_text(
        &server,
        &format!(
            "SELECT u.id FROM {users} u \
             JOIN LATERAL (SELECT e.id FROM {events} e WHERE e.log_time > u.created_at) \
             recent ON true"
        ),
    )
    .await;
    // u1 (created_at 100) sees all five events, u2 (created_at 200) sees two.
    let mut expected = vec!["u1"; 5];
    expected.extend(["u2"; 2]);
    assert_eq!(rows, expected);
}
