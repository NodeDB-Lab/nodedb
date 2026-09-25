// SPDX-License-Identifier: BUSL-1.1

//! An autocommit SQL-function write is WAL-durable, and replay reproduces it.
//!
//! `KV_INCR`, `KV_INCR_FLOAT`, `KV_CAS`, `KV_GETSET`, `TRANSFER`,
//! `TRANSFER_ITEM`, `CREATE SORTED INDEX`, `RATE_CHECK`, `RATE_RESET` and an
//! audited `WEIGHTED_PICK` build their `KvOp` by hand instead of planning a
//! statement. Outside a transaction block each one must take the durable
//! route a planned write takes: the write funnel appends a WAL record for it.
//!
//! Each case runs the function, checks the WAL holds the record for it, then
//! restarts on the same data directory. The harness restores a core from WAL
//! replay alone, so the value read after the restart is the value replay
//! computed. A write dispatched on the read route has no record, and the
//! restart loses it.

use nodedb_test_support::pgwire_harness::{TestDataDir, TestServer};

/// The MessagePack encoding of the short string `s`.
fn fixstr(s: &str) -> Vec<u8> {
    assert!(s.len() < 32, "a fixstr holds at most 31 bytes: {s}");
    let mut encoded = vec![0xa0 | s.len() as u8];
    encoded.extend_from_slice(s.as_bytes());
    encoded
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Whether the WAL holds a record of the KV op `op` on `collection`.
fn wal_holds(server: &TestServer, op: &str, collection: &str) -> bool {
    server.shared.wal.sync().expect("sync the WAL");
    let records = server.shared.wal.replay().expect("read the WAL");
    let (op, collection) = (fixstr(op), fixstr(collection));
    records
        .iter()
        .any(|record| contains(&record.payload, &op) && contains(&record.payload, &collection))
}

/// Stop the server and start a new one on its data directory.
async fn restart(server: TestServer) -> (TestServer, TestDataDir) {
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    TestServer::open_on_path(dir).await
}

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// The first column of every row `sql` returns.
async fn column(server: &TestServer, sql: &str) -> Vec<String> {
    server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// The JSON document a single-row KV function returns.
async fn json(server: &TestServer, sql: &str) -> serde_json::Value {
    let rows = column(server, sql).await;
    assert_eq!(rows.len(), 1, "{sql} returns one row: {rows:?}");
    serde_json::from_str(&rows[0]).unwrap_or_else(|e| panic!("{sql} returns JSON: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_incr_replays_to_the_typed_and_the_raw_value() {
    let server = TestServer::start().await;
    exec(
        &server,
        "CREATE COLLECTION wal_incr (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')",
    )
    .await;
    exec(
        &server,
        "CREATE COLLECTION wal_incr_raw (key TEXT PRIMARY KEY, value TEXT) WITH (engine='kv')",
    )
    .await;

    json(&server, "SELECT KV_INCR('wal_incr', 'k', 5)").await;
    let typed = json(&server, "SELECT KV_INCR('wal_incr', 'k', 2)").await;
    assert_eq!(typed["value"], 7, "{typed}");
    json(&server, "SELECT KV_INCR('wal_incr_raw', 'k', 4)").await;
    let raw = json(&server, "SELECT KV_INCR('wal_incr_raw', 'k', 3)").await;
    assert_eq!(raw["value"], 7, "{raw}");
    assert!(wal_holds(&server, "kv_incr", "wal_incr"));
    assert!(wal_holds(&server, "kv_incr", "wal_incr_raw"));
    let typed_live = column(&server, "SELECT n FROM wal_incr WHERE key = 'k'").await;

    let (server, _dir) = restart(server).await;
    assert_eq!(
        column(&server, "SELECT n FROM wal_incr WHERE key = 'k'").await,
        typed_live,
        "replay must rebuild the typed counter row the live increments wrote"
    );
    assert_eq!(
        json(&server, "SELECT KV_INCR('wal_incr_raw', 'k', 0)").await["value"],
        raw["value"],
        "replay must rebuild the raw counter the live increments wrote"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_incr_float_replays_to_the_exact_decimal_value() {
    let server = TestServer::start().await;
    exec(
        &server,
        "CREATE COLLECTION wal_score (key TEXT PRIMARY KEY, score FLOAT) WITH (engine='kv')",
    )
    .await;
    exec(
        &server,
        "CREATE COLLECTION wal_score_raw (key TEXT PRIMARY KEY, value TEXT) WITH (engine='kv')",
    )
    .await;

    json(&server, "SELECT KV_INCR_FLOAT('wal_score', 's', 0.1)").await;
    json(&server, "SELECT KV_INCR_FLOAT('wal_score', 's', 0.2)").await;
    json(&server, "SELECT KV_INCR_FLOAT('wal_score_raw', 's', 0.1)").await;
    json(&server, "SELECT KV_INCR_FLOAT('wal_score_raw', 's', 0.2)").await;
    assert!(wal_holds(&server, "kv_incr_float", "wal_score"));
    assert!(wal_holds(&server, "kv_incr_float", "wal_score_raw"));
    let typed_live = column(&server, "SELECT score FROM wal_score WHERE key = 's'").await;
    let raw_live = json(&server, "SELECT KV_INCR_FLOAT('wal_score_raw', 's', 0)").await;

    let (server, _dir) = restart(server).await;
    assert_eq!(
        column(&server, "SELECT score FROM wal_score WHERE key = 's'").await,
        typed_live,
        "replay must add the same decimal digits the live increments added"
    );
    assert_eq!(
        json(&server, "SELECT KV_INCR_FLOAT('wal_score_raw', 's', 0)").await["value"],
        raw_live["value"],
        "replay must store the same decimal text the live increments stored"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_cas_and_kv_getset_replay_to_the_value_they_set() {
    let server = TestServer::start().await;
    exec(
        &server,
        "CREATE COLLECTION wal_swap (key TEXT PRIMARY KEY, value TEXT) WITH (engine='kv')",
    )
    .await;

    let cas = json(&server, "SELECT KV_CAS('wal_swap', 'state', '', 'idle')").await;
    assert_eq!(cas["success"], true, "{cas}");
    json(
        &server,
        "SELECT KV_GETSET('wal_swap', 'tok', 'first-token')",
    )
    .await;
    assert!(wal_holds(&server, "kv_cas", "wal_swap"));
    assert!(wal_holds(&server, "kv_getset", "wal_swap"));

    let (server, _dir) = restart(server).await;
    let cas = json(
        &server,
        "SELECT KV_CAS('wal_swap', 'state', 'idle', 'ended')",
    )
    .await;
    assert_eq!(
        cas["success"], true,
        "replay must restore the value the live KV_CAS set: {cas}"
    );
    let getset = json(
        &server,
        "SELECT KV_GETSET('wal_swap', 'tok', 'second-token')",
    )
    .await;
    let old = getset["old_value"]
        .as_str()
        .unwrap_or_else(|| panic!("replay must restore the KV_GETSET value: {getset}"));
    let old = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, old)
        .expect("old_value is base64");
    assert_eq!(old, b"first-token");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfer_and_transfer_item_replay_to_the_moved_state() {
    let server = TestServer::start().await;
    exec(
        &server,
        "CREATE COLLECTION wal_acct (key TEXT PRIMARY KEY, balance INT) WITH (engine='kv')",
    )
    .await;
    exec(
        &server,
        "CREATE COLLECTION wal_items (key TEXT PRIMARY KEY, name TEXT) WITH (engine='kv')",
    )
    .await;
    exec(
        &server,
        "INSERT INTO wal_acct (key, balance) VALUES ('a', 100)",
    )
    .await;
    exec(
        &server,
        "INSERT INTO wal_acct (key, balance) VALUES ('b', 10)",
    )
    .await;
    exec(
        &server,
        "INSERT INTO wal_items (key, name) VALUES ('ownerA:sword', 'Sword')",
    )
    .await;

    json(
        &server,
        "SELECT TRANSFER('wal_acct', 'a', 'b', 'balance', 30)",
    )
    .await;
    json(
        &server,
        "SELECT TRANSFER_ITEM('wal_items', 'wal_items', 'sword', 'ownerA', 'ownerB')",
    )
    .await;
    assert!(wal_holds(&server, "kv_transfer", "wal_acct"));
    assert!(wal_holds(&server, "kv_transfer_item", "wal_items"));
    let source = column(&server, "SELECT balance FROM wal_acct WHERE key = 'a'").await;
    let dest = column(&server, "SELECT balance FROM wal_acct WHERE key = 'b'").await;

    let (server, _dir) = restart(server).await;
    assert_eq!(
        column(&server, "SELECT balance FROM wal_acct WHERE key = 'a'").await,
        source
    );
    assert_eq!(
        column(&server, "SELECT balance FROM wal_acct WHERE key = 'b'").await,
        dest
    );
    assert_eq!(
        column(
            &server,
            "SELECT name FROM wal_items WHERE key = 'ownerB:sword'"
        )
        .await,
        vec!["Sword".to_string()],
        "replay must move the item to its destination key"
    );
    assert!(
        column(
            &server,
            "SELECT name FROM wal_items WHERE key = 'ownerA:sword'"
        )
        .await
        .is_empty(),
        "replay must remove the item from its source key"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sorted_index_registration_replays_to_its_tree() {
    let server = TestServer::start().await;
    exec(
        &server,
        "CREATE COLLECTION wal_board (k TEXT PRIMARY KEY, score INT) WITH (engine='kv')",
    )
    .await;
    for (key, score) in [("p0", 10), ("p1", 20), ("p2", 30)] {
        exec(
            &server,
            &format!("INSERT INTO wal_board (k, score) VALUES ('{key}', {score})"),
        )
        .await;
    }
    exec(
        &server,
        "CREATE SORTED INDEX wal_board_idx ON wal_board (score DESC) KEY k",
    )
    .await;
    assert!(wal_holds(&server, "kv_register_sorted_index", "wal_board"));

    let (server, _dir) = restart(server).await;
    let count = json(&server, "SELECT SORTED_COUNT(wal_board_idx)").await;
    assert_eq!(
        count["count"], 3,
        "replay must rebuild the index tree over every row: {count}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rate_gate_counter_replays_through_check_and_reset() {
    let server = TestServer::start().await;
    json(&server, "SELECT RATE_CHECK('wal_gate', 'u1', 5, 600)").await;
    json(&server, "SELECT RATE_CHECK('wal_gate', 'u1', 5, 600)").await;
    assert!(wal_holds(&server, "kv_incr", "_system_rate_gates"));

    let (server, dir) = restart(server).await;
    let remaining = json(&server, "SELECT RATE_REMAINING('wal_gate', 'u1', 5, 600)").await;
    assert_eq!(
        remaining["current"], 2,
        "replay must restore both counted calls: {remaining}"
    );

    json(&server, "SELECT RATE_RESET('wal_gate', 'u1')").await;
    assert!(wal_holds(&server, "kv_delete", "_system_rate_gates"));
    let (server, _dir) = restart_again(server, dir).await;
    let remaining = json(&server, "SELECT RATE_REMAINING('wal_gate', 'u1', 5, 600)").await;
    assert_eq!(
        remaining["current"], 0,
        "replay must apply the reset after the counted calls: {remaining}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_audited_weighted_pick_logs_its_audit_record() {
    let server = TestServer::start().await;
    exec(
        &server,
        "CREATE COLLECTION wal_wp (key TEXT PRIMARY KEY, w FLOAT) WITH (engine='kv')",
    )
    .await;
    exec(&server, "INSERT INTO wal_wp (key, w) VALUES ('x', 1.0)").await;

    let picked = column(
        &server,
        "SELECT * FROM WEIGHTED_PICK('wal_wp', weight => 'w', count => 1, AUDIT => TRUE)",
    )
    .await;
    assert_eq!(picked.len(), 1, "one pick: {picked:?}");
    assert!(
        wal_holds(&server, "kv_put", "_system_random_audit"),
        "an audited pick answers only once its audit record is in the WAL"
    );
}

/// A pick draws the stored key by its stored weight: a zero-weight row is
/// never drawn, and the reported key and weight are the row's own.
#[tokio::test]
async fn a_weighted_pick_draws_the_stored_key_by_its_weight() {
    let server = TestServer::start().await;
    exec(
        &server,
        "CREATE COLLECTION wp_draw (key TEXT PRIMARY KEY, w FLOAT) WITH (engine='kv')",
    )
    .await;
    exec(
        &server,
        "INSERT INTO wp_draw (key, w) VALUES ('never', 0.0)",
    )
    .await;
    exec(
        &server,
        "INSERT INTO wp_draw (key, w) VALUES ('always', 2.5)",
    )
    .await;

    let sql = "SELECT * FROM WEIGHTED_PICK('wp_draw', weight => 'w', count => 5, \
               WITH REPLACEMENT)";
    let rows = server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(rows.len(), 5, "five draws: {rows:?}");
    for row in &rows {
        assert_eq!(row[1], "always", "only the weighted row is drawn: {rows:?}");
        assert_eq!(row[2], "2.5", "the stored weight is reported: {rows:?}");
    }
}

/// Restart a server that itself came from a restart. Such a server holds a
/// placeholder directory, so the data directory the earlier restart returned
/// is the one reopened.
async fn restart_again(server: TestServer, dir: TestDataDir) -> (TestServer, TestDataDir) {
    server.graceful_shutdown().await;
    TestServer::open_on_path(dir).await
}
