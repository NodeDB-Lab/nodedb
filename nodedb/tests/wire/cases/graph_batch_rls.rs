// SPDX-License-Identifier: BUSL-1.1

//! Batch write paths must evaluate the write policy **per edge**, and a
//! statement is atomic. A batch that mixes an allowed and a denied edge is
//! refused as a whole and writes nothing; a batch of only allowed edges lands
//! in full. Deciding policy once for the whole statement would be an isolation
//! bug.
//!
//! The endpoints here are co-resident on one vShard, so a refusal can only come
//! from policy, never from cross-shard routing.

use nodedb_types::id::VShardId;

use crate::harness::TestServer;

const PASSWORD: &str = "graph-batch-rls-probe-secret-9";
const ROLE: &str = "readwrite";

/// SQLSTATE of a write refused by policy (`insufficient_privilege`).
const DENIED_SQLSTATE: &str = "42501";

/// Endpoints chosen so both hash to one vShard. Mirrors the single-edge probe
/// premise; a cross-shard edge would be refused before the policy gate.
const EDGE_SRC: &str = "a";
const EDGE_DST: &str = "xy";

#[test]
fn batch_endpoints_are_co_resident() {
    assert_eq!(
        VShardId::from_key(EDGE_SRC.as_bytes()),
        VShardId::from_key(EDGE_DST.as_bytes()),
        "the batch RLS tests must exercise the SINGLE-SHARD path; \
         rename the endpoints until the two hashes agree again"
    );
}

async fn create_user(server: &TestServer, user: &str) {
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    server
        .exec(&format!("GRANT ROLE {ROLE} TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant {ROLE} to {user}: {e}"));
}

async fn write_policy(server: &TestServer, policy: &str, collection: &str) {
    server
        .exec(&format!(
            "CREATE RLS POLICY {policy} ON {collection} FOR WRITE \
             USING (owner = $auth.username)"
        ))
        .await
        .unwrap_or_else(|e| panic!("create write policy {policy}: {e}"));
}

/// Run `sql` as `user`, returning `(sqlstate, message)` on failure.
async fn run_as(server: &TestServer, user: &str, sql: &str) -> Result<(), (String, String)> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let result = client.simple_query(sql).await.map(|_| ()).map_err(|e| {
        e.as_db_error()
            .map(|d| (d.code().code().to_string(), d.message().to_string()))
            .unwrap_or_else(|| (String::new(), e.to_string()))
    });
    drop(client);
    handle.abort();
    result
}

/// True when an edge with `label` leaves `EDGE_SRC` in the `out` direction.
///
/// `GRAPH NEIGHBORS` returns a JSON array per row, and an absent edge answers
/// with an empty array rather than no row: parse the array, never test the raw
/// text, or "present" is reported for everything.
///
/// A read error or an unparsable row panics. Returning `false` for either would
/// make the negative assertions below unfalsifiable — they would pass when the
/// read path broke, which is the opposite of what they pin.
async fn has_labeled_edge(server: &TestServer, collection: &str, label: &str) -> bool {
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN '{collection}' OF '{EDGE_SRC}' DIRECTION out LABEL '{label}'"
        ))
        .await
        .unwrap_or_else(|e| panic!("neighbors read for label {label} failed: {e:?}"));
    rows.iter().any(|row| {
        serde_json::from_str::<serde_json::Value>(row)
            .unwrap_or_else(|e| panic!("neighbors row is not JSON: {row}: {e}"))
            .as_array()
            .unwrap_or_else(|| panic!("neighbors row is not a JSON array: {row}"))
            .iter()
            .any(|v| v.get("label").and_then(|l| l.as_str()) == Some(label))
    })
}

/// Seed one edge per `(label, owner)` pair between the co-resident endpoints.
async fn seed_owned_edges(server: &TestServer, collection: &str, edges: &[(&str, &str)]) {
    for (label, owner) in edges {
        server
            .exec(&format!(
                "GRAPH INSERT EDGE IN '{collection}' FROM '{EDGE_SRC}' TO '{EDGE_DST}' \
                 TYPE '{label}' PROPERTIES '{{\"owner\":\"{owner}\"}}'"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {label}: {e}"));
        assert!(has_labeled_edge(server, collection, label).await);
    }
}

/// A batch delete under a `FOR WRITE` owner policy is ONE atomic statement: a
/// batch holding two conforming edges and one violating edge is refused as a
/// whole. No edge is deleted, the two allowed ones included, and the error
/// comes from the policy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_delete_with_a_denied_edge_deletes_nothing() {
    let server = TestServer::start().await;
    let user = "g_batch_rls_delete_user";
    let collection = "g_batch_rls_delete";
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .expect("create edge collection");
    create_user(&server, user).await;
    write_policy(&server, "g_batch_rls_delete_policy", collection).await;
    seed_owned_edges(
        &server,
        collection,
        &[("owns1", user), ("steals", "someone-else"), ("owns2", user)],
    )
    .await;

    // The denied edge sits between the allowed ones, so a handler that applied
    // edges in order until the first denial would delete `owns1` and keep the
    // rest: a partial effect this test makes observable.
    let result = run_as(
        &server,
        user,
        &format!(
            "GRAPH DELETE EDGES IN '{collection}' VALUES \
             ('{EDGE_SRC}','{EDGE_DST}','owns1'), ('{EDGE_SRC}','{EDGE_DST}','steals'), \
             ('{EDGE_SRC}','{EDGE_DST}','owns2')"
        ),
    )
    .await;
    let (sqlstate, message) =
        result.expect_err("the denied edge must make the statement report an error");
    assert_eq!(
        sqlstate, DENIED_SQLSTATE,
        "batch refusal SQLSTATE: {message}"
    );
    assert!(
        message.contains("policy") || message.contains("RLS"),
        "the error must come from the write policy: {message}"
    );

    for label in ["owns1", "steals", "owns2"] {
        assert!(
            has_labeled_edge(&server, collection, label).await,
            "edge {label} must survive: a refused batch deletes nothing"
        );
    }
}

/// A batch of only conforming edges under the same policy deletes every edge in
/// one statement and reports the count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_delete_of_only_allowed_edges_deletes_all() {
    let server = TestServer::start().await;
    let user = "g_batch_rls_delete_ok_user";
    let collection = "g_batch_rls_delete_ok";
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .expect("create edge collection");
    create_user(&server, user).await;
    write_policy(&server, "g_batch_rls_delete_ok_policy", collection).await;
    seed_owned_edges(
        &server,
        collection,
        &[("owns1", user), ("owns2", user), ("steals", "someone-else")],
    )
    .await;

    let result = run_as(
        &server,
        user,
        &format!(
            "GRAPH DELETE EDGES IN '{collection}' VALUES \
             ('{EDGE_SRC}','{EDGE_DST}','owns1'), ('{EDGE_SRC}','{EDGE_DST}','owns2')"
        ),
    )
    .await;
    assert!(
        result.is_ok(),
        "a batch of conforming edges must be admitted: {result:?}"
    );

    assert!(!has_labeled_edge(&server, collection, "owns1").await);
    assert!(!has_labeled_edge(&server, collection, "owns2").await);
    assert!(
        has_labeled_edge(&server, collection, "steals").await,
        "an edge outside the batch must survive"
    );
}

/// A batch insert with no property image cannot satisfy an owner policy, so
/// the whole statement is refused and no edge lands. Per-edge evaluation is
/// what makes this deterministic; a statement-level shortcut would let a
/// half-batch through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_insert_is_refused_when_the_policy_needs_a_property() {
    let server = TestServer::start().await;
    let user = "g_batch_rls_insert_user";
    let collection = "g_batch_rls_insert";
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .expect("create edge collection");
    create_user(&server, user).await;
    write_policy(&server, "g_batch_rls_insert_policy", collection).await;

    let result = run_as(
        &server,
        user,
        &format!(
            "GRAPH INSERT EDGES IN '{collection}' VALUES \
             ('{EDGE_SRC}','{EDGE_DST}','l1'), ('{EDGE_SRC}','{EDGE_DST}','l2')"
        ),
    )
    .await;
    let (sqlstate, message) =
        result.expect_err("a property-less batch under an owner policy must be refused");
    assert_eq!(
        sqlstate, DENIED_SQLSTATE,
        "batch refusal SQLSTATE: {message}"
    );
    assert!(
        !has_labeled_edge(&server, collection, "l1").await
            && !has_labeled_edge(&server, collection, "l2").await,
        "no edge from the refused batch may land"
    );
}

/// A single-edge delete of a denied edge under the same policy carries the
/// same SQLSTATE as the refused batch delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_edge_delete_under_the_same_policy_gives_the_same_sqlstate() {
    let server = TestServer::start().await;
    let user = "g_batch_rls_same_user";
    let collection = "g_batch_rls_same";
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .expect("create edge collection");
    create_user(&server, user).await;
    write_policy(&server, "g_batch_rls_same_policy", collection).await;
    seed_owned_edges(&server, collection, &[("steals", "someone-else")]).await;

    let single = run_as(
        &server,
        user,
        &format!(
            "GRAPH DELETE EDGE IN '{collection}' FROM '{EDGE_SRC}' TO '{EDGE_DST}' TYPE 'steals'"
        ),
    )
    .await;
    let (single_state, single_message) =
        single.expect_err("the single-edge delete of a denied edge must be refused");

    let batch = run_as(
        &server,
        user,
        &format!(
            "GRAPH DELETE EDGES IN '{collection}' VALUES ('{EDGE_SRC}','{EDGE_DST}','steals')"
        ),
    )
    .await;
    let (batch_state, _) = batch.expect_err("the batch delete of a denied edge must be refused");

    assert_eq!(
        single_state, DENIED_SQLSTATE,
        "single refusal SQLSTATE: {single_message}"
    );
    assert_eq!(single_state, batch_state, "single and batch SQLSTATE");
    assert!(
        has_labeled_edge(&server, collection, "steals").await,
        "the refused deletes must leave the edge in place"
    );
}
