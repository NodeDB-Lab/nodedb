// SPDX-License-Identifier: BUSL-1.1

//! Authorization and policy handling for `MATCH` pattern queries.
//!
//! A MATCH walks a collection's edges and returns bindings over its nodes, so
//! it carries the same read grant the collection's rows do. Both dispatch
//! shapes (single-node broadcast, cluster scatter) reach the Data Plane without
//! a plan the planner's authorization and RLS passes can inspect, so the
//! handler has to reach those verdicts itself.
//!
//! Bindings are topology, not row bodies, so there is nothing for a row filter
//! to evaluate: a read policy refuses the query outright. A pattern with no
//! `IN '<collection>'` may walk anything the tenant holds, so it is authorized
//! against every collection it could touch and refuses while any read policy
//! applies to the caller.

use crate::harness::TestServer;

const PASSWORD: &str = "match-authz-secret-7";

/// A collection with one edge, plus a principal holding no grant on it.
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
    // `CREATE USER` defaults to ReadWrite and `monitor` still confers
    // `Permission::Read`, so neither is an unprivileged principal. A custom
    // role confers nothing without an explicit grant, which is what "no access
    // to this collection" actually looks like.
    // The role is defined and granted nothing: a user may hold only a
    // defined role.
    server
        .exec("CREATE ROLE IF NOT EXISTS match_nobody")
        .await
        .unwrap_or_else(|e| panic!("create role match_nobody: {e}"));
    server
        .exec(&format!(
            "CREATE USER {stranger} PASSWORD '{PASSWORD}' ROLE match_nobody"
        ))
        .await
        .unwrap_or_else(|e| panic!("create user {stranger}: {e}"));
}

/// Run `sql` as `user`, returning `Err((sqlstate, message))` on refusal.
async fn run_as(server: &TestServer, user: &str, sql: &str) -> Result<usize, (String, String)> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let result = client.simple_query(sql).await;
    drop(client);
    handle.abort();
    match result {
        Ok(messages) => Ok(messages
            .iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count()),
        // `tokio_postgres::Error`'s Display is just "db error"; the SQLSTATE and
        // the server's message both live on the DbError payload.
        Err(e) => Err(e.as_db_error().map_or_else(
            || (String::new(), e.to_string()),
            |db| (db.code().code().to_string(), db.message().to_string()),
        )),
    }
}

/// A principal with no read grant on the collection gets no bindings from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_match_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed(&server, "match_authz_grant", "match_grant_stranger").await;

    let result = run_as(
        &server,
        "match_grant_stranger",
        "MATCH (x)-[:knows]->(y) IN 'match_authz_grant' RETURN x, y",
    )
    .await;

    match result {
        Err((sqlstate, message)) => {
            assert_eq!(sqlstate, "42501", "expected an RBAC denial, got: {message}");
            assert!(
                message.to_lowercase().contains("permission denied"),
                "expected a permission denial, got: {message}"
            );
        }
        Ok(rows) => panic!("ungranted principal matched the graph and saw {rows} row(s)"),
    }
}

/// A read policy makes the scoped match refuse: the rows' contents are
/// protected, so the shape of those rows is not disclosed either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_match_refuses_while_a_read_policy_exists() {
    let server = TestServer::start().await;
    seed(&server, "match_authz_rls", "match_rls_user").await;
    server
        .exec("GRANT ROLE readwrite TO match_rls_user")
        .await
        .expect("grant readwrite");
    server
        .exec(
            "CREATE RLS POLICY match_owner ON match_authz_rls FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create policy");

    let result = run_as(
        &server,
        "match_rls_user",
        "MATCH (x)-[:knows]->(y) IN 'match_authz_rls' RETURN x, y",
    )
    .await;

    match result {
        Err((sqlstate, message)) => assert_eq!(
            sqlstate, "0A000",
            "expected the policy refusal, got: {message}"
        ),
        Ok(rows) => {
            panic!("match returned {rows} row(s) from a collection under a read policy")
        }
    }
}

/// An unscoped pattern may walk any collection the tenant holds, so a principal
/// that cannot read them all is refused before it walks any.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unscoped_match_without_grants_on_every_collection_is_denied() {
    let server = TestServer::start().await;
    seed(&server, "match_authz_open", "match_open_stranger").await;

    let result = run_as(
        &server,
        "match_open_stranger",
        "MATCH (x)-[:knows]->(y) RETURN x, y",
    )
    .await;

    match result {
        Err((sqlstate, message)) => {
            assert_eq!(sqlstate, "42501", "expected an RBAC denial, got: {message}");
        }
        Ok(rows) => panic!("ungranted principal ran an unscoped match and saw {rows} row(s)"),
    }
}

/// …and one that can read them all is still refused while any read policy
/// applies to it, because the pattern names no collection the refusal could be
/// narrowed to.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unscoped_match_refuses_while_any_read_policy_applies() {
    let server = TestServer::start().await;
    seed(&server, "match_authz_open_rls", "match_open_rls_user").await;
    server
        .exec("GRANT ROLE readwrite TO match_open_rls_user")
        .await
        .expect("grant readwrite");
    server
        .exec(
            "CREATE RLS POLICY match_open_owner ON match_authz_open_rls FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create policy");

    let result = run_as(
        &server,
        "match_open_rls_user",
        "MATCH (x)-[:knows]->(y) RETURN x, y",
    )
    .await;

    match result {
        Err((sqlstate, message)) => assert_eq!(
            sqlstate, "0A000",
            "expected the tenant-wide policy refusal, got: {message}"
        ),
        Ok(rows) => panic!("unscoped match returned {rows} row(s) while a read policy applies"),
    }
}

/// The grant is what makes it work: a principal that may read the collection
/// still gets its bindings, scoped and unscoped, when no policy applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn granted_principal_can_match() {
    let server = TestServer::start().await;
    seed(&server, "match_authz_ok", "match_ok_user").await;
    server
        .exec("GRANT ROLE readwrite TO match_ok_user")
        .await
        .expect("grant readwrite");

    let scoped = run_as(
        &server,
        "match_ok_user",
        "MATCH (x)-[:knows]->(y) IN 'match_authz_ok' RETURN x, y",
    )
    .await;
    assert_eq!(
        scoped,
        Ok(1),
        "granted principal was refused its own collection's bindings"
    );

    let unscoped = run_as(
        &server,
        "match_ok_user",
        "MATCH (x)-[:knows]->(y) RETURN x, y",
    )
    .await;
    assert_eq!(
        unscoped,
        Ok(1),
        "granted principal was refused an unscoped match with no policy in place"
    );
}
