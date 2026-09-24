// SPDX-License-Identifier: BUSL-1.1

//! Batch write paths must evaluate the write policy **per edge**. A batch that
//! mixes an allowed and a denied edge must land the allowed one and refuse the
//! denied one; deciding policy once for the whole statement would be an
//! isolation bug, and no test would catch it today.
//!
//! The endpoints here are co-resident on one vShard, so a refusal can only come
//! from policy, never from cross-shard routing.

use nodedb_types::id::VShardId;

use crate::harness::TestServer;

const PASSWORD: &str = "graph-batch-rls-probe-secret-9";
const ROLE: &str = "readwrite";

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

/// Run `sql` as `user`, returning the server's error message on failure.
async fn run_as(server: &TestServer, user: &str, sql: &str) -> Result<(), String> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let result = client.simple_query(sql).await.map(|_| ()).map_err(|e| {
        e.as_db_error()
            .map(|db| db.message().to_string())
            .unwrap_or_else(|| e.to_string())
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
async fn has_labeled_edge(server: &TestServer, collection: &str, label: &str) -> bool {
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN '{collection}' OF '{EDGE_SRC}' DIRECTION out LABEL '{label}'"
        ))
        .await
        .unwrap_or_default();
    rows.iter().any(|row| {
        serde_json::from_str::<serde_json::Value>(row)
            .ok()
            .and_then(|v| v.as_array().map(|a| !a.is_empty()))
            .unwrap_or(false)
    })
}

/// A batch delete under a `FOR WRITE` owner policy must delete the conforming
/// edge and refuse the violating one, both inside the same statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_delete_evaluates_the_write_policy_per_edge() {
    let server = TestServer::start().await;
    let user = "g_batch_rls_delete_user";
    let collection = "g_batch_rls_delete";
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .expect("create edge collection");
    create_user(&server, user).await;
    write_policy(&server, "g_batch_rls_delete_policy", collection).await;

    // Two edges between the same endpoints, distinguished by label and owner:
    // one the user may write, one they may not.
    for (label, owner) in [("owns", user), ("steals", "someone-else")] {
        server
            .exec(&format!(
                "GRAPH INSERT EDGE IN '{collection}' FROM '{EDGE_SRC}' TO '{EDGE_DST}' \
                 TYPE '{label}' PROPERTIES '{{\"owner\":\"{owner}\"}}'"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {label}: {e}"));
    }
    assert!(has_labeled_edge(&server, collection, "owns").await);
    assert!(has_labeled_edge(&server, collection, "steals").await);

    // The allowed edge comes first so a partial effect is observable: if the
    // handler decided policy once for the statement, both or neither would go.
    let result = run_as(
        &server,
        user,
        &format!(
            "GRAPH DELETE EDGES IN '{collection}' VALUES \
             ('{EDGE_SRC}','{EDGE_DST}','owns'), ('{EDGE_SRC}','{EDGE_DST}','steals')"
        ),
    )
    .await;
    assert!(
        result.is_err(),
        "the denied edge must make the statement report an error: {result:?}"
    );

    assert!(
        !has_labeled_edge(&server, collection, "owns").await,
        "the conforming edge must be deleted"
    );
    assert!(
        has_labeled_edge(&server, collection, "steals").await,
        "the denied edge must survive, and the error must come from policy, not from the first edge"
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
    assert!(
        result.is_err(),
        "a property-less batch under an owner policy must be refused: {result:?}"
    );
    assert!(
        !has_labeled_edge(&server, collection, "l1").await
            && !has_labeled_edge(&server, collection, "l2").await,
        "no edge from the refused batch may land"
    );
}
