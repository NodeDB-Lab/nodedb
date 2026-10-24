// SPDX-License-Identifier: BUSL-1.1

//! The native client's batch delete under a row-level-security write policy.
//!
//! A restricted principal connects over the native protocol with a password. A
//! batch holding one denied edge is refused as a whole with the
//! authorization-denied error code and deletes nothing. A batch of only allowed
//! edges lands. Edge state is read through the superuser pgwire connection.
//!
//! The endpoints here are co-resident on one vShard, so a refusal can only come
//! from policy, never from cross-shard routing.

use nodedb_client::NativeClient;
use nodedb_types::error::ErrorCode;
use nodedb_types::id::VShardId;
use nodedb_types::protocol::AuthMethod;

use crate::harness::TestServer;

const PASSWORD: &str = "graph-batch-native-rls-probe-secret-9";
const ROLE: &str = "readwrite";

/// Endpoints chosen so both hash to one vShard.
const EDGE_SRC: &str = "a";
const EDGE_DST: &str = "xy";

#[test]
fn native_batch_endpoints_are_co_resident() {
    assert_eq!(
        VShardId::from_key(EDGE_SRC.as_bytes()),
        VShardId::from_key(EDGE_DST.as_bytes()),
        "the native batch RLS tests must exercise the SINGLE-SHARD path; \
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

/// A native client logged in as `user` with a password.
fn native_client_as(server: &TestServer, user: &str) -> NativeClient {
    NativeClient::connect_as(
        &format!("127.0.0.1:{}", server.native_port),
        AuthMethod::Password {
            username: user.to_string(),
            password: PASSWORD.to_string(),
        },
    )
}

/// A native batch delete holding one denied edge is refused as a whole. The
/// error carries `ErrorCode::AUTHORIZATION_DENIED` and no edge is deleted, the
/// allowed one included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_native_batch_delete_with_a_denied_edge_is_refused_and_deletes_nothing() {
    let server = TestServer::start().await;
    let user = "g_native_rls_delete_user";
    let collection = "g_native_rls_delete";
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .expect("create edge collection");
    create_user(&server, user).await;
    write_policy(&server, "g_native_rls_delete_policy", collection).await;
    seed_owned_edges(
        &server,
        collection,
        &[("owns1", user), ("steals", "someone-else")],
    )
    .await;

    let client = native_client_as(&server, user);
    let err = client
        .batch_graph_delete_edges(
            collection,
            &[
                (EDGE_SRC, EDGE_DST, "owns1"),
                (EDGE_SRC, EDGE_DST, "steals"),
            ],
        )
        .await
        .expect_err("the denied edge must make the native batch report an error");
    assert_eq!(
        err.code(),
        ErrorCode::AUTHORIZATION_DENIED,
        "native batch refusal code: {err}"
    );
    assert!(err.is_auth_denied(), "native batch refusal kind: {err}");

    for label in ["owns1", "steals"] {
        assert!(
            has_labeled_edge(&server, collection, label).await,
            "edge {label} must survive: a refused native batch deletes nothing"
        );
    }
}

/// A native batch of only conforming edges is admitted and deletes them. An
/// edge outside the batch survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_native_batch_delete_of_only_allowed_edges_deletes_them() {
    let server = TestServer::start().await;
    let user = "g_native_rls_delete_ok_user";
    let collection = "g_native_rls_delete_ok";
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .expect("create edge collection");
    create_user(&server, user).await;
    write_policy(&server, "g_native_rls_delete_ok_policy", collection).await;
    seed_owned_edges(
        &server,
        collection,
        &[("owns1", user), ("owns2", user), ("steals", "someone-else")],
    )
    .await;

    let client = native_client_as(&server, user);
    client
        .batch_graph_delete_edges(
            collection,
            &[(EDGE_SRC, EDGE_DST, "owns1"), (EDGE_SRC, EDGE_DST, "owns2")],
        )
        .await
        .expect("a native batch of conforming edges must be admitted");

    assert!(!has_labeled_edge(&server, collection, "owns1").await);
    assert!(!has_labeled_edge(&server, collection, "owns2").await);
    assert!(
        has_labeled_edge(&server, collection, "steals").await,
        "an edge outside the batch must survive"
    );
}
