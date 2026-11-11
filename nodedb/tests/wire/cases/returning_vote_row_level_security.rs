// SPDX-License-Identifier: BUSL-1.1

//! A write that commits through Calvin keeps its row-level-security write
//! policy: a row the policy refuses fails the statement with `42501` and
//! stores nothing, as on the direct path.
//!
//! A `RETURNING` list with an operator can fail, so its one-vShard write
//! commits through Calvin. One request over two vShards commits through
//! Calvin too. The policy limits writes to rows the writer owns.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use nodedb_types::id::DatabaseId;
use tokio::net::TcpStream;

use super::returning_vote_single_shard::rows_of;
use crate::harness::TestServer;

const PASSWORD: &str = "rv-rls-secret-42";

fn vshard_of(name: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32()
}

/// A collection name on another vShard than `first`.
fn other(first: &str) -> String {
    (0..)
        .map(|n| format!("{first}_b{n}"))
        .find(|name| vshard_of(name) != vshard_of(first))
        .expect("a name on another vShard")
}

/// Create `user` with the `readwrite` role.
async fn create_user(server: &TestServer, user: &str) {
    for sql in [
        format!("CREATE USER {user} PASSWORD '{PASSWORD}'"),
        format!("GRANT ROLE readwrite TO {user}"),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// A strict collection with rows `r_mine` (owned by `user`) and `r_theirs`
/// (owned by `alice`), both `n = 0`, under a write policy that admits only
/// rows `user` owns.
async fn policed(server: &TestServer, collection: &str, user: &str) {
    for sql in [
        format!(
            "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, owner TEXT, n INT) \
             WITH (engine='document_strict')"
        ),
        format!(
            "INSERT INTO {collection} (id, owner, n) VALUES \
             ('r_mine', '{user}', 0), ('r_theirs', 'alice', 0)"
        ),
        format!(
            "CREATE RLS POLICY {collection}_owner ON {collection} FOR WRITE \
             USING (owner = $auth.username)"
        ),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// The SQLSTATE `sql` fails with, sent by `stream`'s user over native.
async fn refused(stream: &mut TcpStream, seq: u64, sql: &str) -> String {
    let response = send_sql(stream, seq, sql).await;
    let error = response
        .error
        .unwrap_or_else(|| panic!("the policy refuses the write: {sql}"));
    assert!(error.message.contains("RLS"), "{sql}: {}", error.message);
    error.code
}

/// An insert of a row another user owns fails with `42501` and stores
/// nothing. The same insert of an owned row commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_insert_on_one_vshard_stores_nothing() {
    let server = TestServer::start().await;
    let user = "rv_rls_ins_user";
    create_user(&server, user).await;
    policed(&server, "rv_rls_ins", user).await;
    let mut stream = open_trust_session(server.native_port, user).await;

    let code = refused(
        &mut stream,
        2,
        "INSERT INTO rv_rls_ins (id, owner, n) VALUES ('x', 'alice', 5) RETURNING id, n * 2",
    )
    .await;
    assert_eq!(code, "42501");

    let response = send_sql(
        &mut stream,
        3,
        &format!(
            "INSERT INTO rv_rls_ins (id, owner, n) VALUES ('y', '{user}', 5) RETURNING id, n * 2"
        ),
    )
    .await;
    assert!(response.error.is_none(), "{:?}", response.error);

    assert_eq!(
        rows_of(&server, "SELECT id FROM rv_rls_ins").await,
        ["r_mine", "r_theirs", "y"],
        "the refused row is absent"
    );
}

/// One request writes an owned row on one vShard and another user's row on
/// a second. It fails with `42501`, and neither vShard stores its row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_write_across_vshards_stores_nothing() {
    let server = TestServer::start_multicores(2).await;
    let user = "rv_rls_ms_user";
    create_user(&server, user).await;
    let first = "rv_rls_ms";
    let second = other(first);
    policed(&server, first, user).await;
    policed(&server, &second, user).await;
    let mut stream = open_trust_session(server.native_port, user).await;

    let code = refused(
        &mut stream,
        2,
        &format!(
            "INSERT INTO {first} (id, owner, n) VALUES ('m', '{user}', 1); \
             INSERT INTO {second} (id, owner, n) VALUES ('m', 'alice', 1)"
        ),
    )
    .await;
    assert_eq!(code, "42501");

    for collection in [first, second.as_str()] {
        assert_eq!(
            rows_of(&server, &format!("SELECT id FROM {collection}")).await,
            ["r_mine", "r_theirs"],
            "{collection} stored nothing"
        );
    }
}

/// An update by predicate that matches a row the policy refuses fails with
/// `42501` through Calvin, and changes no row: neither the refused row nor
/// the owned one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_predicate_update_changes_no_row() {
    let server = TestServer::start().await;
    let user = "rv_rls_upd_user";
    create_user(&server, user).await;
    policed(&server, "rv_rls_upd", user).await;
    let mut stream = open_trust_session(server.native_port, user).await;

    let code = refused(
        &mut stream,
        2,
        "UPDATE rv_rls_upd SET n = 9 WHERE n = 0 RETURNING id, n * 2",
    )
    .await;
    assert_eq!(code, "42501");

    assert_eq!(
        rows_of(&server, "SELECT id, n FROM rv_rls_upd").await,
        ["r_mine\t0", "r_theirs\t0"],
        "no row changed"
    );
}

/// An HTTP request signed in as the policed user with its API key. The
/// insert of another user's row commits through Calvin and is refused, and
/// the row is absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_insert_over_http_stores_nothing() {
    let server = TestServer::start().await;
    let user = "rv_rls_http_user";
    create_user(&server, user).await;
    policed(&server, "rv_rls_http", user).await;
    let key_rows = server
        .query_named_rows(&format!("CREATE API KEY FOR {user}"))
        .await
        .expect("create the user's API key");
    let token = key_rows
        .first()
        .and_then(|row| row.get("api_key"))
        .cloned()
        .unwrap_or_else(|| panic!("an api_key column: {key_rows:?}"));

    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/v1/query", server.http_port))
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "sql": "INSERT INTO rv_rls_http (id, owner, n) VALUES ('x', 'alice', 5) \
                    RETURNING id, n * 2"
        }))
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST /v1/query: {e}"));
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    assert!(
        !(200..300).contains(&status),
        "the policy refuses: {status} {body}"
    );
    assert!(body.contains("RLS"), "{body}");

    assert_eq!(
        rows_of(&server, "SELECT id FROM rv_rls_http").await,
        ["r_mine", "r_theirs"],
        "the refused row is absent"
    );
}

/// Inside a `BEGIN` block an insert of another user's row is staged, and
/// staging decides the policy: the statement fails with `42501`, and COMMIT
/// writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_insert_in_a_block_fails_at_the_statement() {
    let server = TestServer::start().await;
    let user = "rv_rls_blk_user";
    create_user(&server, user).await;
    policed(&server, "rv_rls_blk", user).await;
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .expect("connect as the policed user");

    client.simple_query("BEGIN").await.expect("begin");
    let error = client
        .simple_query("INSERT INTO rv_rls_blk (id, owner, n) VALUES ('x', 'alice', 5)")
        .await
        .expect_err("the staged row violates the policy, so the statement fails");
    let db_error = error
        .as_db_error()
        .unwrap_or_else(|| panic!("a server error: {error:?}"));
    assert_eq!(db_error.code().code(), "42501", "{}", db_error.message());
    assert!(db_error.message().contains("RLS"), "{}", db_error.message());
    // COMMIT may answer the failed block with a rollback or an error. Either
    // way it writes nothing, which the read below checks.
    let commit = client
        .simple_query("COMMIT")
        .await
        .map(|_| ())
        .map_err(|e| e.to_string());
    drop(client);
    handle.abort();

    assert_eq!(
        rows_of(&server, "SELECT id FROM rv_rls_blk").await,
        ["r_mine", "r_theirs"],
        "COMMIT wrote nothing: {commit:?}"
    );
}
