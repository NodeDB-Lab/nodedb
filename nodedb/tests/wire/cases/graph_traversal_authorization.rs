// SPDX-License-Identifier: BUSL-1.1

//! Authorization and policy handling for graph traversals.
//!
//! A traversal reads a collection's edges, so it needs the collection's read
//! grant — and it has to name a collection at all, or there is nothing to
//! authorize against. The CSR partition holds every collection's edges under
//! one shared node space, so an unscoped traversal would span all of them.
//!
//! Traversals return node ids and edge labels rather than row bodies, so a row
//! filter has nothing to evaluate. They therefore refuse under a read policy
//! rather than leaking the topology of rows whose contents are protected.

use crate::harness::TestServer;

const PASSWORD: &str = "probe-secret-99";

/// A collection with two connected nodes, plus a principal with no grant on it.
async fn seed(server: &TestServer, collection: &str, stranger: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    for node in ["n1", "n2"] {
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, owner) VALUES ('{node}', 'alice')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {node}: {e}"));
    }
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{collection}' FROM 'n1' TO 'n2' TYPE 'knows'"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert edge: {e}"));
    // `CREATE USER` defaults to ReadWrite, and `monitor` still confers
    // `Permission::Read` (`identity/permission.rs:80`), so neither is an
    // unprivileged principal. A custom role confers nothing without an explicit
    // grant, which is what "no access to this collection" actually looks like.
    // The role is defined and granted nothing: a user may hold only a
    // defined role.
    server
        .exec("CREATE ROLE IF NOT EXISTS graph_nobody")
        .await
        .unwrap_or_else(|e| panic!("create role graph_nobody: {e}"));
    server
        .exec(&format!(
            "CREATE USER {stranger} PASSWORD '{PASSWORD}' ROLE graph_nobody"
        ))
        .await
        .unwrap_or_else(|e| panic!("create user {stranger}: {e}"));
}

/// Run `sql` as `user`, returning `Err` with the server's message on refusal.
async fn run_as(server: &TestServer, user: &str, sql: &str) -> Result<usize, String> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .map_err(|e| format!("connect as {user}: {e}"))?;
    let result = client.simple_query(sql).await;
    drop(client);
    handle.abort();
    match result {
        Ok(messages) => Ok(messages
            .iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count()),
        // `tokio_postgres::Error`'s Display is just "db error"; the server's
        // message lives on the DbError payload.
        Err(e) => Err(e
            .as_db_error()
            .map(|db| db.message().to_string())
            .unwrap_or_else(|| e.to_string())),
    }
}

/// A traversal must name the collection it walks. Without one there is nothing
/// to authorize, so the statement must not parse at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traversal_without_a_collection_is_rejected() {
    let server = TestServer::start().await;
    seed(&server, "graph_authz_scope", "graph_scope_user").await;

    let result = server
        .query_text("GRAPH NEIGHBORS OF 'n1' DIRECTION out")
        .await;

    assert!(
        result.is_err(),
        "a traversal naming no collection was accepted: {result:?}"
    );
}

/// A principal with no read grant on the collection gets no topology from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn neighbors_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed(&server, "graph_authz_ngh", "graph_ngh_stranger").await;

    let result = run_as(
        &server,
        "graph_ngh_stranger",
        "GRAPH NEIGHBORS IN 'graph_authz_ngh' OF 'n1' DIRECTION out",
    )
    .await;

    match result {
        Err(message) => assert!(
            message.to_lowercase().contains("permission denied"),
            "expected a permission denial, got: {message}"
        ),
        Ok(rows) => panic!("ungranted principal traversed the graph and saw {rows} row(s)"),
    }
}

/// `GRAPH PATH` is the same read through a different statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn path_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed(&server, "graph_authz_path", "graph_path_stranger").await;

    let result = run_as(
        &server,
        "graph_path_stranger",
        "GRAPH PATH IN 'graph_authz_path' FROM 'n1' TO 'n2'",
    )
    .await;

    assert!(
        result.is_err(),
        "ungranted principal completed a GRAPH PATH: {result:?}"
    );
}

/// `GRAPH TRAVERSE` likewise.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traverse_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed(&server, "graph_authz_trv", "graph_trv_stranger").await;

    let result = run_as(
        &server,
        "graph_trv_stranger",
        "GRAPH TRAVERSE IN 'graph_authz_trv' FROM 'n1' DEPTH 2",
    )
    .await;

    assert!(
        result.is_err(),
        "ungranted principal completed a GRAPH TRAVERSE: {result:?}"
    );
}

/// A read policy makes the traversal refuse: the rows' contents are protected,
/// so the shape of those rows is not disclosed either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traversal_refuses_while_a_read_policy_exists() {
    let server = TestServer::start().await;
    seed(&server, "graph_authz_rls", "graph_rls_user").await;
    server
        .exec("GRANT ROLE readwrite TO graph_rls_user")
        .await
        .expect("grant readwrite");
    server
        .exec(
            "CREATE RLS POLICY graph_owner ON graph_authz_rls FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create policy");

    let result = run_as(
        &server,
        "graph_rls_user",
        "GRAPH NEIGHBORS IN 'graph_authz_rls' OF 'n1' DIRECTION out",
    )
    .await;

    match result {
        Err(message) => assert!(
            message.to_lowercase().contains("rls") || message.to_lowercase().contains("polic"),
            "expected the refusal to name the policy, got: {message}"
        ),
        Ok(rows) => panic!(
            "traversal returned {rows} row(s) of topology from a collection under a read policy"
        ),
    }
}

/// The grant is what makes it work: a principal that may read the collection
/// still gets its topology.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn granted_principal_can_traverse() {
    let server = TestServer::start().await;
    seed(&server, "graph_authz_ok", "graph_ok_user").await;
    server
        .exec("GRANT ROLE readwrite TO graph_ok_user")
        .await
        .expect("grant readwrite");

    let result = run_as(
        &server,
        "graph_ok_user",
        "GRAPH NEIGHBORS IN 'graph_authz_ok' OF 'n1' DIRECTION out",
    )
    .await;

    assert!(
        result.is_ok(),
        "granted principal was refused its own collection's topology: {result:?}"
    );
}
