// SPDX-License-Identifier: BUSL-1.1

//! Dispatcher correctness for administrative `SHOW` commands over pgwire.
//!
//! The pgwire entry point intercepts every statement starting with `SHOW `
//! and, unless the command is on a hard-coded allowlist, routes it to the
//! PostgreSQL session-parameter fallback. That fallback returns a single
//! row with one column named after the parameter and an empty string as
//! its value — making an unrouted administrative command look like a
//! successful but empty result instead of erroring or reaching its real
//! handler.
//!
//! The tests below assert each administrative `SHOW` reaches its handler
//! by checking the schema and row count of the response. Each test
//! includes a regression guard against the session-parameter fallback
//! signature: a single-column response whose column name equals the
//! lowercased parameter tail (e.g. `databases`, `roles`, `stats`) with
//! an empty value.

use crate::harness::TestServer;

/// Returns true if the response is the pgwire session-parameter fallback:
/// exactly one row, exactly one column, column name equal to `param`
/// (case-insensitive), and an empty value.
async fn is_session_param_fallback(server: &TestServer, sql: &str, param: &str) -> bool {
    let rows = match server.query_named_rows(sql).await {
        Ok(r) => r,
        Err(_) => return false,
    };
    if rows.len() != 1 {
        return false;
    }
    let row = &rows[0];
    if row.len() != 1 {
        return false;
    }
    row.iter()
        .any(|(k, v)| k.eq_ignore_ascii_case(param) && v.is_empty())
}

// ── SHOW DATABASES ───────────────────────────────────────────────────

/// After `CREATE DATABASE`, `SHOW DATABASES` must return at least two rows
/// (default + new) and expose the per-database schema (`name`, `status`,
/// `collection_count`, ...) — never the single-column session-parameter
/// fallback.
#[tokio::test]
async fn show_databases_lists_created_database() {
    let server = TestServer::start().await;
    server
        .exec("CREATE DATABASE show_dispatch_alpha")
        .await
        .expect("CREATE DATABASE must succeed");

    let rows = server
        .query_named_rows("SHOW DATABASES")
        .await
        .expect("SHOW DATABASES must not error");

    assert!(
        rows.len() >= 2,
        "SHOW DATABASES must list both `default` and the created database; got {} row(s): {:?}",
        rows.len(),
        rows
    );
    assert!(
        rows[0].contains_key("name"),
        "SHOW DATABASES must expose a `name` column (got columns: {:?})",
        rows[0].keys().collect::<Vec<_>>()
    );
    assert!(
        rows.iter().any(|r| r
            .get("name")
            .map(|n| n == "show_dispatch_alpha")
            .unwrap_or(false)),
        "SHOW DATABASES must include the row created by CREATE DATABASE: {rows:?}"
    );

    // Regression guard: single-column response named "databases" with an
    // empty value is the session-parameter fallback signature.
    assert!(
        !is_session_param_fallback(&server, "SHOW DATABASES", "databases").await,
        "SHOW DATABASES must not be routed to the session-parameter fallback"
    );
}

// ── SHOW ROLES ───────────────────────────────────────────────────────

/// `SHOW ROLES` after `CREATE ROLE` must list the new role with a typed
/// schema, not the single-column session-parameter fallback.
#[tokio::test]
async fn show_roles_lists_created_role() {
    let server = TestServer::start().await;
    server
        .exec("CREATE ROLE show_dispatch_role")
        .await
        .expect("CREATE ROLE must succeed");

    let rows = server
        .query_named_rows("SHOW ROLES")
        .await
        .expect("SHOW ROLES must not error");

    assert!(
        !rows.is_empty(),
        "SHOW ROLES must return at least one row; got empty result"
    );
    assert!(
        rows[0].len() >= 2 || !rows[0].contains_key("roles"),
        "SHOW ROLES must use a typed multi-column schema, not the single \
         `roles` column session-parameter fallback (got columns: {:?})",
        rows[0].keys().collect::<Vec<_>>()
    );
    assert!(
        rows.iter()
            .any(|r| r.values().any(|v| v == "show_dispatch_role")),
        "SHOW ROLES must include the created role: {rows:?}"
    );

    assert!(
        !is_session_param_fallback(&server, "SHOW ROLES", "roles").await,
        "SHOW ROLES must not be routed to the session-parameter fallback"
    );
}

/// After `DROP ROLE`, `SHOW ROLES` must not list the dropped role. A
/// `DROP` that leaves the entry visible in `SHOW` is a ghost — the
/// catalog and the introspection view disagree on whether the role
/// exists.
#[tokio::test]
async fn show_roles_omits_dropped_role() {
    let server = TestServer::start().await;
    server
        .exec("CREATE ROLE ghost_role_to_drop")
        .await
        .expect("CREATE ROLE must succeed");
    server
        .exec("DROP ROLE ghost_role_to_drop")
        .await
        .expect("DROP ROLE must succeed");

    let rows = server
        .query_named_rows("SHOW ROLES")
        .await
        .expect("SHOW ROLES must not error");

    assert!(
        !rows
            .iter()
            .any(|r| r.values().any(|v| v == "ghost_role_to_drop")),
        "SHOW ROLES must not list a role that was dropped: {rows:?}"
    );
}

// ── SHOW TENANTS after DROP TENANT ───────────────────────────────────

/// After `DROP TENANT`, `SHOW TENANTS` must not list the dropped tenant
/// — neither by name nor as a ghost id-slot.
///
/// `CREATE TENANT` auto-provisions a `<name>_admin` user in the new
/// tenant. `SHOW TENANTS` derives its row set from the union of
/// catalog-registered tenants and every user's `tenant_id`, so an
/// orphaned admin user resurrects the dropped tenant as a ghost row
/// whose `name` is empty (the catalog row is gone) but whose
/// `tenant_id` slot is retained. The regression guard below asserts no
/// such empty-named ghost survives for the dropped id.
#[tokio::test]
async fn show_tenants_omits_dropped_tenant() {
    let server = TestServer::start().await;
    // Explicit high id so the new tenant does not collide with the
    // bootstrap superuser's home tenant; the auto-provisioned
    // `<name>_admin` is then the only user in it.
    server
        .exec("CREATE TENANT ghost_tenant_to_drop ID 4242")
        .await
        .expect("CREATE TENANT must succeed");

    let before = server
        .query_named_rows("SHOW TENANTS")
        .await
        .expect("SHOW TENANTS must not error");
    let tenant_id = before
        .iter()
        .find(|r| {
            r.get("name")
                .map(|n| n == "ghost_tenant_to_drop")
                .unwrap_or(false)
        })
        .and_then(|r| r.get("tenant_id"))
        .cloned()
        .expect("created tenant must be visible in SHOW TENANTS before drop");

    server
        .exec("DROP TENANT ghost_tenant_to_drop")
        .await
        .expect("DROP TENANT must succeed");

    let after = server
        .query_named_rows("SHOW TENANTS")
        .await
        .expect("SHOW TENANTS must not error");

    assert!(
        !after.iter().any(|r| r
            .get("name")
            .map(|n| n == "ghost_tenant_to_drop")
            .unwrap_or(false)),
        "SHOW TENANTS must not list a tenant dropped by name: {after:?}"
    );
    // Regression guard for the specific silent failure mode: a retained
    // id-slot with a cleared (empty) name is the ghost signature.
    assert!(
        !after.iter().any(|r| r.get("tenant_id") == Some(&tenant_id)),
        "SHOW TENANTS must not retain the id-slot of a dropped tenant \
         (ghost row with empty name): {after:?}"
    );
}

/// `DROP TENANT` must refuse (`42501`) when an operator-owned user
/// still belongs to the tenant, rather than orphaning it (which leaves
/// a `SHOW TENANTS` ghost) or silently hard-deleting it. The user and
/// the tenant must both survive the refused drop.
#[tokio::test]
async fn drop_tenant_refuses_when_real_users_remain() {
    let server = TestServer::start().await;
    server
        .exec("CREATE TENANT keep_tenant ID 4243")
        .await
        .expect("CREATE TENANT must succeed");
    server
        .exec("CREATE USER keep_tenant_member WITH PASSWORD 'pw' TENANT 4243")
        .await
        .expect("CREATE USER must succeed");

    let err = server
        .client
        .simple_query("DROP TENANT keep_tenant")
        .await
        .expect_err("DROP TENANT must be refused while a real user remains");
    let code = err.code().map(|c| c.code().to_string()).unwrap_or_default();
    assert_eq!(
        code, "42501",
        "DROP TENANT with a remaining real user must fail with 42501, got: {err}"
    );

    // No silent deletion: the real user must still exist.
    let users = server
        .query_named_rows("SHOW USERS")
        .await
        .expect("SHOW USERS must not error");
    assert!(
        users.iter().any(|r| r
            .get("username")
            .map(|n| n == "keep_tenant_member")
            .unwrap_or(false)),
        "the operator-owned user must survive a refused DROP TENANT: {users:?}"
    );
}

// ── SHOW STATS / SHOW SERVER STATS / SHOW METRICS / SHOW MEMORY ──────

/// `SHOW STATS` must reach a real handler. The session-parameter fallback
/// is a silent misroute — it returns a single-column row named `stats`
/// with an empty string, which is indistinguishable from a working
/// "no stats collected" response unless explicitly guarded.
#[tokio::test]
async fn show_stats_is_not_session_param_fallback() {
    let server = TestServer::start().await;
    assert!(
        !is_session_param_fallback(&server, "SHOW STATS", "stats").await,
        "SHOW STATS must not be routed to the session-parameter fallback \
         (single-column `stats` with empty value)"
    );
}

#[tokio::test]
async fn show_server_stats_is_not_session_param_fallback() {
    let server = TestServer::start().await;
    // The fallback tokenises by the param tail; `SHOW SERVER STATS`
    // becomes parameter `server stats`.
    assert!(
        !is_session_param_fallback(&server, "SHOW SERVER STATS", "server stats").await,
        "SHOW SERVER STATS must not be routed to the session-parameter fallback"
    );
}

#[tokio::test]
async fn show_metrics_is_not_session_param_fallback() {
    let server = TestServer::start().await;
    assert!(
        !is_session_param_fallback(&server, "SHOW METRICS", "metrics").await,
        "SHOW METRICS must not be routed to the session-parameter fallback"
    );
}

#[tokio::test]
async fn show_memory_is_not_session_param_fallback() {
    let server = TestServer::start().await;
    assert!(
        !is_session_param_fallback(&server, "SHOW MEMORY", "memory").await,
        "SHOW MEMORY must not be routed to the session-parameter fallback"
    );
}

// ── Evidence: the dispatch flaw is systemic, not specific to the issue
//    list. Each of these is dispatched in `ddl/router/admin.rs` but is
//    unreachable because the session-parameter fallback intercepts every
//    `SHOW ` prefix that isn't on the allowlist in `handler/sql_exec.rs`.

/// `SHOW SCHEDULES` is wired in `admin.rs` and must reach that handler.
#[tokio::test]
async fn show_schedules_is_not_session_param_fallback() {
    let server = TestServer::start().await;
    assert!(
        !is_session_param_fallback(&server, "SHOW SCHEDULES", "schedules").await,
        "SHOW SCHEDULES must reach its admin-router handler, not the \
         session-parameter fallback"
    );
}

/// `SHOW SEQUENCES` is wired in `admin.rs` and must reach that handler.
#[tokio::test]
async fn show_sequences_is_not_session_param_fallback() {
    let server = TestServer::start().await;
    assert!(
        !is_session_param_fallback(&server, "SHOW SEQUENCES", "sequences").await,
        "SHOW SEQUENCES must reach its admin-router handler, not the \
         session-parameter fallback"
    );
}

/// `SHOW ALERTS` is wired in `admin.rs` and must reach that handler.
#[tokio::test]
async fn show_alerts_is_not_session_param_fallback() {
    let server = TestServer::start().await;
    assert!(
        !is_session_param_fallback(&server, "SHOW ALERTS", "alerts").await,
        "SHOW ALERTS must reach its admin-router handler, not the \
         session-parameter fallback"
    );
}

/// `SHOW MATERIALIZED VIEWS` is wired in `admin.rs` and must reach
/// that handler.
#[tokio::test]
async fn show_materialized_views_is_not_session_param_fallback() {
    let server = TestServer::start().await;
    assert!(
        !is_session_param_fallback(&server, "SHOW MATERIALIZED VIEWS", "materialized views",).await,
        "SHOW MATERIALIZED VIEWS must reach its admin-router handler, \
         not the session-parameter fallback"
    );
}

/// `SHOW BLACKLIST` is wired in `admin.rs` and must reach that handler.
#[tokio::test]
async fn show_blacklist_is_not_session_param_fallback() {
    let server = TestServer::start().await;
    assert!(
        !is_session_param_fallback(&server, "SHOW BLACKLIST", "blacklist").await,
        "SHOW BLACKLIST must reach its admin-router handler, not the \
         session-parameter fallback"
    );
}

/// `SHOW ORGS` is wired in `admin.rs` and must reach that handler.
#[tokio::test]
async fn show_orgs_is_not_session_param_fallback() {
    let server = TestServer::start().await;
    assert!(
        !is_session_param_fallback(&server, "SHOW ORGS", "orgs").await,
        "SHOW ORGS must reach its admin-router handler, not the \
         session-parameter fallback"
    );
}

// ── Strict PG-runtime-parameter handler ──────────────────────────────

/// `SHOW <unknown-name>` for a name that is not a known PostgreSQL
/// runtime parameter, not set in the session, and not claimed by any
/// administrative router must return `42704`
/// (`unrecognized configuration parameter`) — never a silent empty row.
/// This guard prevents the original ghost-row bug from regressing for
/// arbitrary new SHOW commands added in the future.
#[tokio::test]
async fn show_unknown_parameter_returns_42704() {
    let server = TestServer::start().await;
    match server
        .client
        .simple_query("SHOW totally_made_up_parameter_xyz")
        .await
    {
        Ok(msgs) => panic!(
            "SHOW on an unknown parameter must error with 42704, got success: \
             {msgs:?}"
        ),
        Err(e) => {
            let code = e.code().map(|c| c.code().to_string()).unwrap_or_default();
            let msg = e
                .as_db_error()
                .map(|d| d.message().to_string())
                .unwrap_or_else(|| e.to_string());
            assert!(
                code == "42704" || msg.contains("unrecognized configuration"),
                "expected 42704 / unrecognized configuration parameter, got \
                 code={code:?} msg={msg:?}"
            );
        }
    }
}

/// Built-in PostgreSQL runtime parameters keep working: clients depend
/// on `SHOW server_version`, `SHOW server_encoding`, and `SHOW ALL` to
/// negotiate driver behaviour at startup.
#[tokio::test]
async fn show_builtin_pg_runtime_parameters_still_work() {
    let server = TestServer::start().await;

    let version = server
        .query_text("SHOW server_version")
        .await
        .expect("SHOW server_version must succeed");
    assert_eq!(version.len(), 1, "SHOW server_version must return one row");
    assert!(
        version[0].contains("NodeDB"),
        "SHOW server_version must report a NodeDB version string, got: {:?}",
        version[0]
    );

    let encoding = server
        .query_text("SHOW server_encoding")
        .await
        .expect("SHOW server_encoding must succeed");
    assert_eq!(encoding, vec!["UTF8".to_string()]);

    let all = server
        .query_rows("SHOW ALL")
        .await
        .expect("SHOW ALL must succeed");
    // SHOW ALL returns the session parameter map — may be empty on a
    // fresh connection. The assertion is that it does not error.
    let _ = all;
}

/// Values explicitly set via `SET <name>` in the current session must
/// remain readable via `SHOW <name>`, even if `<name>` is not on the
/// built-in PG-runtime-parameter allowlist. This preserves the
/// `SET foo = 'bar'; SHOW foo;` round-trip used by some clients for
/// session-scoped configuration.
#[tokio::test]
async fn show_session_set_parameter_round_trips() {
    let server = TestServer::start().await;
    server
        .exec("SET application_name = 'mae8_bootstrap'")
        .await
        .expect("SET application_name must succeed");
    let rows = server
        .query_text("SHOW application_name")
        .await
        .expect("SHOW application_name must succeed");
    assert_eq!(rows, vec!["mae8_bootstrap".to_string()]);
}

// ── SHOW SNAPSHOT ─────────────────────────────────────────────────────

/// `SHOW SNAPSHOT` must reach its handler (not the session-parameter
/// fallback) and return the WAL pin a reader captures before paging. The pin
/// must not move backwards across two reads.
#[tokio::test]
async fn show_snapshot_returns_the_wal_pin() {
    let server = TestServer::start().await;

    assert!(
        !is_session_param_fallback(&server, "SHOW SNAPSHOT", "snapshot").await,
        "SHOW SNAPSHOT must not fall through to the session-parameter fallback"
    );

    let rows = server
        .query_named_rows("SHOW SNAPSHOT")
        .await
        .expect("SHOW SNAPSHOT must not error");
    let first: std::collections::BTreeMap<String, String> = rows
        .into_iter()
        .map(|r| {
            (
                r.get("name").cloned().unwrap_or_default(),
                r.get("value").cloned().unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(
        first.get("snapshot_pin").map(String::as_str),
        Some("wal_lsn"),
        "the pin kind must be named: {first:?}"
    );
    let before: u64 = first
        .get("wal_next_lsn")
        .expect("wal_next_lsn row")
        .parse()
        .expect("wal_next_lsn is a decimal integer");

    // A write advances the WAL; the pin must reflect it and never regress.
    server
        .exec("CREATE COLLECTION show_snapshot_probe")
        .await
        .expect("seed write");
    let rows = server
        .query_named_rows("SHOW SNAPSHOT")
        .await
        .expect("SHOW SNAPSHOT after a write");
    let after_row = rows
        .iter()
        .find(|r| r.get("name").map(|n| n == "wal_next_lsn").unwrap_or(false))
        .expect("wal_next_lsn row after a write");
    let after: u64 = after_row["value"]
        .parse()
        .expect("wal_next_lsn stays a decimal integer");
    assert!(
        after >= before,
        "the pin must not regress: {before} -> {after}"
    );
}
