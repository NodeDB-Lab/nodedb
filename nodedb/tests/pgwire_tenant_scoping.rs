//! pgwire wire-level tests for per-connection tenant scoping and Trust-mode
//! identity resolution.
//!
//! Covers the class of bug where the pgwire handler's planning context is
//! built once with a fixed tenant id, so queries from tenant-scoped users
//! plan against the wrong tenant's catalog. Also covers Trust-mode accepting
//! usernames that were never created as if they were superusers.

mod common;

use common::pgwire_harness::TestServer;

/// Helper: superuser-side bootstrap — create a tenant and a tenant-scoped
/// user, plus a collection owned by that tenant. The harness's default
/// connection is Trust/superuser (tenant 1) and is used for setup.
async fn bootstrap_tenant_user(server: &TestServer, user: &str, collection: &str) {
    server
        .exec("CREATE TENANT acme ID 2")
        .await
        .expect("CREATE TENANT");
    server
        .exec(&format!(
            "CREATE USER {user} WITH PASSWORD 'x' ROLE readwrite TENANT 2"
        ))
        .await
        .expect("CREATE USER");
    // Create the collection as the tenant-scoped user so ownership lands on
    // tenant 2. This itself exercises the DDL path, which already reads
    // identity.tenant_id correctly.
    let (svc, _h) = server
        .connect_as(user, "x")
        .await
        .expect("tenant user connect");
    svc.simple_query(&format!(
        "CREATE COLLECTION {collection} TYPE DOCUMENT STRICT \
         (id TEXT PRIMARY KEY, content TEXT NOT NULL)"
    ))
    .await
    .expect("tenant user CREATE COLLECTION");
    drop(svc);
}

/// Run a simple query under a freshly opened tenant-user connection and
/// return either the rows (first column) or the server error message.
async fn query_as(server: &TestServer, user: &str, sql: &str) -> Result<Vec<String>, String> {
    let (client, _h) = server.connect_as(user, "x").await?;
    match client.simple_query(sql).await {
        Ok(msgs) => {
            let mut rows = Vec::new();
            for msg in msgs {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                    rows.push(row.get(0).unwrap_or("").to_string());
                }
            }
            Ok(rows)
        }
        Err(e) => Err(pg_err(&e)),
    }
}

fn pg_err(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e:?}")
    }
}

// ── tenant-scoped planning via shared query_ctx ────────────────────────

#[tokio::test]
async fn tenant_user_can_select_own_collection() {
    let server = TestServer::start().await;
    bootstrap_tenant_user(&server, "svc_sel", "t2_sel").await;

    let (svc, _h) = server.connect_as("svc_sel", "x").await.unwrap();
    svc.simple_query("INSERT INTO t2_sel (id, content) VALUES ('a', 'alpha')")
        .await
        .expect("INSERT under tenant user");

    let rows = query_as(&server, "svc_sel", "SELECT id FROM t2_sel")
        .await
        .expect("SELECT under tenant user must not fail with 'unknown table'");
    assert_eq!(rows.len(), 1, "expected 1 row, got {rows:?}");
    assert!(
        rows[0].contains("\"a\""),
        "row should contain id 'a': {rows:?}"
    );
}

#[tokio::test]
async fn tenant_user_can_insert_into_own_collection() {
    let server = TestServer::start().await;
    bootstrap_tenant_user(&server, "svc_ins", "t2_ins").await;

    let (svc, _h) = server.connect_as("svc_ins", "x").await.unwrap();
    svc.simple_query("INSERT INTO t2_ins (id, content) VALUES ('k', 'v')")
        .await
        .expect("INSERT must succeed for tenant-owned collection");
}

#[tokio::test]
async fn tenant_user_can_update_own_collection() {
    let server = TestServer::start().await;
    bootstrap_tenant_user(&server, "svc_upd", "t2_upd").await;

    let (svc, _h) = server.connect_as("svc_upd", "x").await.unwrap();
    svc.simple_query("INSERT INTO t2_upd (id, content) VALUES ('a', 'old')")
        .await
        .unwrap();
    svc.simple_query("UPDATE t2_upd SET content = 'new' WHERE id = 'a'")
        .await
        .expect("UPDATE must not fail with 'unknown table'");

    let rows = query_as(
        &server,
        "svc_upd",
        "SELECT content FROM t2_upd WHERE id = 'a'",
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("\"new\""),
        "row should reflect updated content: {rows:?}"
    );
}

#[tokio::test]
async fn tenant_user_can_delete_from_own_collection() {
    let server = TestServer::start().await;
    bootstrap_tenant_user(&server, "svc_del", "t2_del").await;

    let (svc, _h) = server.connect_as("svc_del", "x").await.unwrap();
    svc.simple_query("INSERT INTO t2_del (id, content) VALUES ('a', 'x')")
        .await
        .unwrap();
    svc.simple_query("DELETE FROM t2_del WHERE id = 'a'")
        .await
        .expect("DELETE must not fail with 'unknown table'");

    let rows = query_as(&server, "svc_del", "SELECT id FROM t2_del")
        .await
        .unwrap();
    assert!(rows.is_empty(), "row should be deleted, got {rows:?}");
}

#[tokio::test]
async fn tenant_user_prepared_select_resolves_own_collection() {
    // Extended protocol goes through NodeDbQueryParser::parse_sql, which
    // builds its own OriginCatalog with a hardcoded tenant id. A tenant-2
    // user must still be able to prepare and execute a statement against
    // a tenant-2 collection.
    let server = TestServer::start().await;
    bootstrap_tenant_user(&server, "svc_prep", "t2_prep").await;

    let (svc, _h) = server.connect_as("svc_prep", "x").await.unwrap();
    svc.simple_query("INSERT INTO t2_prep (id, content) VALUES ('a', 'alpha')")
        .await
        .unwrap();

    // `prepare` drives Parse + Describe through NodeDbQueryParser::parse_sql,
    // which constructs an OriginCatalog. Before the fix this catalog was
    // hardcoded to tenant 1 and could not see tenant-2 collections —
    // `prepare` would surface the server's "unknown table" error.
    svc.prepare("SELECT content FROM t2_prep WHERE id = 'a'")
        .await
        .expect("prepare must resolve tenant-owned collection via parser.rs");
}

#[tokio::test]
async fn tenant_user_cannot_see_other_tenants_collection_as_empty() {
    // Asymmetric-isolation guard: a tenant-2 user issuing SELECT against a
    // tenant-1 collection must NOT silently return an empty result set.
    // The correct behavior is the same "unknown table" a cross-tenant
    // planner produces the other direction. Silent empty is a data-shape
    // leak vector even though no rows cross.
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION t1_only TYPE DOCUMENT STRICT \
             (id TEXT PRIMARY KEY, secret TEXT NOT NULL)",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO t1_only (id, secret) VALUES ('a', 'classified')")
        .await
        .unwrap();

    server.exec("CREATE TENANT acme ID 2").await.unwrap();
    server
        .exec("CREATE USER svc_xtn WITH PASSWORD 'x' ROLE readwrite TENANT 2")
        .await
        .unwrap();

    let result = query_as(&server, "svc_xtn", "SELECT secret FROM t1_only").await;
    match result {
        Err(msg) => {
            assert!(
                msg.to_lowercase().contains("unknown table"),
                "expected 'unknown table' error, got: {msg}"
            );
        }
        Ok(rows) => panic!(
            "tenant-2 user must not see tenant-1 collection via silent empty result; got rows={rows:?}"
        ),
    }
}

// ── Trust-mode identity resolution ─────────────────────────────────────

#[tokio::test]
async fn trust_mode_rejects_unknown_username() {
    // Under Trust mode NodeDB skips password verification, but it must
    // still resolve the connecting username against the credential store.
    // Accepting a fabricated username silently promotes arbitrary clients
    // to tenant-1 superuser (see core.rs resolve_identity fallback).
    let server = TestServer::start().await;

    let result = server
        .connect_as("nosuchuser_ever_created", "anything")
        .await;
    assert!(
        result.is_err(),
        "Trust mode must reject a username that was never CREATE USER'd; got an accepted connection"
    );
}

#[tokio::test]
async fn trust_mode_unknown_user_cannot_run_superuser_ddl() {
    // Defense-in-depth regression guard for the same root cause: even if
    // a connection is somehow permitted, an unknown username MUST NOT be
    // silently fabricated as a superuser identity capable of tenant/user
    // management DDL. Today core.rs sets `is_superuser: true` in the
    // fallback branch, so `CREATE USER` from a fabricated name succeeds.
    let server = TestServer::start().await;

    let Ok((client, _h)) = server.connect_as("ghost_admin", "anything").await else {
        // If connect_as is already rejected by the prior test's fix, that
        // is a strictly stronger guarantee — the bug is still captured.
        return;
    };

    let result = client
        .simple_query("CREATE USER mallory WITH PASSWORD 'y' ROLE readwrite TENANT 1")
        .await;
    assert!(
        result.is_err(),
        "unknown Trust-mode user must not be granted superuser privileges; CREATE USER succeeded"
    );
    if let Err(e) = result {
        let msg = pg_err(&e);
        assert!(
            msg.contains("42501") || msg.to_lowercase().contains("permission"),
            "expected a permission-denied error, got: {msg}"
        );
    }
}
