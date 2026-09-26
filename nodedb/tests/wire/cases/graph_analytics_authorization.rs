// SPDX-License-Identifier: BUSL-1.1

//! Authorization and policy handling for `GRAPH ALGO` and `SHOW GRAPH STATS`.
//!
//! Both reach the Data Plane through `broadcast_to_all_cores`, which builds no
//! plan for the planner's authorization and RLS passes to inspect. Both also
//! return values derived from every row of the collection — per-node ranks and
//! component ids, edge counts, edge label names — through payloads that carry
//! no row for a filter to apply to. So both need the collection's read grant,
//! and both refuse rather than filter while a read policy applies.
//!
//! Tenant-wide `SHOW GRAPH STATS` names no collection: it refuses while any
//! read policy applies to the caller, and narrows its rows to the collections
//! the caller may actually read.

use crate::harness::TestServer;

const PASSWORD: &str = "analytics-authz-secret-7";

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
    // A custom role confers nothing without an explicit grant, which is what
    // "no access to this collection" actually looks like — `CREATE USER`
    // defaults to ReadWrite and `monitor` still confers `Permission::Read`.
    // The role is defined and granted nothing: a user may hold only a
    // defined role.
    server
        .exec("CREATE ROLE IF NOT EXISTS analytics_nobody")
        .await
        .unwrap_or_else(|e| panic!("create role analytics_nobody: {e}"));
    server
        .exec(&format!(
            "CREATE USER {stranger} PASSWORD '{PASSWORD}' ROLE analytics_nobody"
        ))
        .await
        .unwrap_or_else(|e| panic!("create user {stranger}: {e}"));
}

/// Run `sql` as `user`, returning `Err((sqlstate, message))` on refusal.
async fn run_as(
    server: &TestServer,
    user: &str,
    sql: &str,
) -> Result<Vec<String>, (String, String)> {
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
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    Some(row.get(0).unwrap_or_default().to_string())
                }
                _ => None,
            })
            .collect()),
        Err(e) => Err(e.as_db_error().map_or_else(
            || (String::new(), e.to_string()),
            |db| (db.code().code().to_string(), db.message().to_string()),
        )),
    }
}

/// An algorithm run over a collection the caller cannot read is denied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn algo_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed(&server, "algo_authz_grant", "algo_grant_stranger").await;

    let result = run_as(
        &server,
        "algo_grant_stranger",
        "GRAPH ALGO PAGERANK ON algo_authz_grant",
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
        Ok(rows) => panic!(
            "ungranted principal ran PageRank and saw {} row(s)",
            rows.len()
        ),
    }
}

/// A rank is computed over every edge, including the ones a policy hides, and
/// arrives with no row to filter — so a read policy refuses the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn algo_refuses_while_a_read_policy_exists() {
    let server = TestServer::start().await;
    seed(&server, "algo_authz_rls", "algo_rls_user").await;
    server
        .exec("GRANT ROLE readwrite TO algo_rls_user")
        .await
        .expect("grant readwrite");
    server
        .exec(
            "CREATE RLS POLICY algo_owner ON algo_authz_rls FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create policy");

    let result = run_as(
        &server,
        "algo_rls_user",
        "GRAPH ALGO PAGERANK ON algo_authz_rls",
    )
    .await;

    match result {
        Err((sqlstate, message)) => assert_eq!(
            sqlstate, "0A000",
            "expected the policy refusal, got: {message}"
        ),
        Ok(rows) => panic!(
            "PageRank returned {} row(s) over a collection under a read policy",
            rows.len()
        ),
    }
}

/// A granted principal with no policy in place still gets its ranks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn granted_principal_can_run_algo() {
    let server = TestServer::start().await;
    seed(&server, "algo_authz_ok", "algo_ok_user").await;
    server
        .exec("GRANT ROLE readwrite TO algo_ok_user")
        .await
        .expect("grant readwrite");

    let result = run_as(
        &server,
        "algo_ok_user",
        "GRAPH ALGO PAGERANK ON algo_authz_ok",
    )
    .await;

    assert!(
        result.is_ok(),
        "granted principal was refused its own collection's ranks: {result:?}"
    );
}

/// Edge counts and label names describe rows, so the collection-scoped counter
/// needs the collection's read grant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_stats_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed(&server, "stats_authz_grant", "stats_grant_stranger").await;

    let result = run_as(
        &server,
        "stats_grant_stranger",
        "SHOW GRAPH STATS 'stats_authz_grant'",
    )
    .await;

    match result {
        Err((sqlstate, message)) => {
            assert_eq!(sqlstate, "42501", "expected an RBAC denial, got: {message}");
        }
        Ok(rows) => panic!(
            "ungranted principal read graph stats and saw {} row(s)",
            rows.len()
        ),
    }
}

/// …and refuses under a read policy: a counter has no row to filter, and it
/// counts the edges of the rows the policy hides.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_stats_refuses_while_a_read_policy_exists() {
    let server = TestServer::start().await;
    seed(&server, "stats_authz_rls", "stats_rls_user").await;
    server
        .exec("GRANT ROLE readwrite TO stats_rls_user")
        .await
        .expect("grant readwrite");
    server
        .exec(
            "CREATE RLS POLICY stats_owner ON stats_authz_rls FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create policy");

    let result = run_as(
        &server,
        "stats_rls_user",
        "SHOW GRAPH STATS 'stats_authz_rls'",
    )
    .await;

    match result {
        Err((sqlstate, message)) => assert_eq!(
            sqlstate, "0A000",
            "expected the policy refusal, got: {message}"
        ),
        Ok(rows) => panic!(
            "graph stats returned {} row(s) for a collection under a read policy",
            rows.len()
        ),
    }
}

/// The tenant-wide form names no collection to narrow the refusal to, so any
/// read policy applying to the caller refuses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_wide_stats_refuses_while_any_read_policy_applies() {
    let server = TestServer::start().await;
    seed(&server, "stats_authz_wide", "stats_wide_user").await;
    server
        .exec("GRANT ROLE readwrite TO stats_wide_user")
        .await
        .expect("grant readwrite");
    server
        .exec(
            "CREATE RLS POLICY stats_wide_owner ON stats_authz_wide FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create policy");

    let result = run_as(&server, "stats_wide_user", "SHOW GRAPH STATS").await;

    match result {
        Err((sqlstate, message)) => assert_eq!(
            sqlstate, "0A000",
            "expected the tenant-wide policy refusal, got: {message}"
        ),
        Ok(rows) => panic!(
            "tenant-wide graph stats returned {} row(s) while a read policy applies",
            rows.len()
        ),
    }
}

/// A granted principal with no policy in place sees the collection in both the
/// scoped and the tenant-wide form, exactly as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn granted_principal_can_read_stats() {
    let server = TestServer::start().await;
    seed(&server, "stats_authz_ok", "stats_ok_user").await;
    server
        .exec("GRANT ROLE readwrite TO stats_ok_user")
        .await
        .expect("grant readwrite");

    let scoped = run_as(
        &server,
        "stats_ok_user",
        "SHOW GRAPH STATS 'stats_authz_ok'",
    )
    .await
    .expect("granted principal was refused its own collection's graph stats");
    assert!(
        scoped.iter().any(|name| name == "stats_authz_ok"),
        "scoped stats lost the collection's row: {scoped:?}"
    );

    let wide = run_as(&server, "stats_ok_user", "SHOW GRAPH STATS")
        .await
        .expect("granted principal was refused tenant-wide graph stats");
    assert!(
        wide.iter().any(|name| name == "stats_authz_ok"),
        "tenant-wide stats dropped a collection the caller may read: {wide:?}"
    );
}
