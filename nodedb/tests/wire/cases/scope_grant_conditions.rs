// SPDX-License-Identifier: BUSL-1.1

//! Conditional scope grants: `WHEN` / `REQUIRE` clauses parse, land in the
//! catalog, survive a restart, and `SHOW SCOPE GRANTS` renders them.
//!
//! Whether a conditioned grant *applies* to a given request is decided in
//! `RequestAuthScopeBuilder::build`, unit-tested at that choke point — no
//! protocol surface exposes one request's resolved scope set.

use crate::harness::TestServer;

/// The grant used throughout: a business-hours, office-network entitlement.
const CONDITIONAL_GRANT: &str = "GRANT SCOPE 'ops:all' TO USER 'user_42' \
     WHEN BETWEEN '09:00' AND '17:00' ON WEEKDAYS REQUIRE MFA \
     REQUIRE IP IN ('10.0.0.0/8')";

/// The rendered form `SHOW SCOPE GRANTS` must display for that grant.
const RENDERED: [&str; 3] = [
    "WHEN BETWEEN '09:00' AND '17:00' ON WEEKDAYS",
    "REQUIRE MFA",
    "REQUIRE IP IN ('10.0.0.0/8')",
];

async fn show_scope_grants(server: &TestServer) -> Vec<String> {
    server
        .query_text_joined("SHOW SCOPE GRANTS")
        .await
        .expect("SHOW SCOPE GRANTS")
}

/// `SHOW SCOPE GRANTS` on a second connection, each row's columns joined by
/// tab -- the second-connection counterpart of `TestServer::query_text_joined`,
/// which only runs on the server's own superuser client.
async fn show_scope_grants_as(client: &tokio_postgres::Client) -> Vec<String> {
    let messages = client
        .simple_query("SHOW SCOPE GRANTS")
        .await
        .expect("SHOW SCOPE GRANTS");
    let mut rows = Vec::new();
    for message in messages {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            let n = row.len();
            let mut joined = String::new();
            for i in 0..n {
                if i > 0 {
                    joined.push('\t');
                }
                joined.push_str(row.get(i).unwrap_or(""));
            }
            rows.push(joined);
        }
    }
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conditional_grant_is_shown_and_survives_a_restart() {
    let server = TestServer::start().await;

    server
        .exec("DEFINE SCOPE 'ops:all' AS READ ON widgets")
        .await
        .expect("DEFINE SCOPE");
    server.exec(CONDITIONAL_GRANT).await.expect("GRANT SCOPE");

    let rows = show_scope_grants(&server).await;
    for clause in RENDERED {
        assert!(
            rows.iter().any(|row| row.contains(clause)),
            "SHOW SCOPE GRANTS must display {clause}: {rows:?}"
        );
    }

    // The conditions live in the system catalog, so a reopened server must
    // still know them — a grant that came back unconditional would be a
    // silent privilege widening at every restart.
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let rows = show_scope_grants(&server).await;
    for clause in RENDERED {
        assert!(
            rows.iter().any(|row| row.contains(clause)),
            "the grant's conditions must survive a restart, missing {clause}: {rows:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unconditional_grant_is_shown_as_such() {
    let server = TestServer::start().await;

    server
        .exec("GRANT SCOPE 'basic:all' TO USER 'user_7'")
        .await
        .expect("GRANT SCOPE");

    let rows = show_scope_grants(&server).await;
    let row = rows
        .iter()
        .find(|row| row.contains("basic:all"))
        .expect("the grant must be listed");
    assert!(
        !row.contains("REQUIRE") && !row.contains("WHEN BETWEEN"),
        "an unconditional grant must not display conditions: {row}"
    );
}

/// A malformed condition is a syntax error, never a grant that quietly
/// applies unconditionally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_conditions_are_rejected() {
    let server = TestServer::start().await;

    server
        .expect_error(
            "GRANT SCOPE 'ops:all' TO USER 'user_42' REQUIRE IP IN ('10.0.0.0/64')",
            "not a valid IP address or CIDR range",
        )
        .await;
    server
        .expect_error(
            "GRANT SCOPE 'ops:all' TO USER 'user_42' WHEN BETWEEN '09:00' AND '09:00'",
            "never open",
        )
        .await;
    server
        .expect_error(
            "GRANT SCOPE 'ops:all' TO USER 'user_42' REQUIRE TELEPATHY",
            "expected MFA, IP, STEP_UP, or DEVICE_TRUST",
        )
        .await;

    let rows = show_scope_grants(&server).await;
    assert!(
        !rows.iter().any(|row| row.contains("ops:all")),
        "a rejected statement must not leave a grant behind: {rows:?}"
    );
}

/// GRANT SCOPE accepts `ORG`, `USER`, `ROLE` and `TEAM` as the grantee type.
/// A non-superuser must see a `ROLE`-type grant in their own tenant the same
/// as a `USER`-type one -- the tenant-visibility filter must resolve a
/// tenant for every grantee type GRANT SCOPE itself accepts, not only `org`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_role_grant_in_the_callers_own_tenant_is_shown_to_a_non_superuser() {
    let server = TestServer::start().await;
    const PASSWORD: &str = "scope-grant-role-secret-42";

    server
        .exec("CREATE ROLE sgr_role")
        .await
        .expect("CREATE ROLE");
    server
        .exec("GRANT SCOPE 'sgr:all' TO ROLE 'sgr_role'")
        .await
        .expect("GRANT SCOPE TO ROLE");
    server
        .exec(&format!("CREATE USER sgr_viewer PASSWORD '{PASSWORD}'"))
        .await
        .expect("CREATE USER");
    server
        .exec("GRANT ROLE readwrite TO sgr_viewer")
        .await
        .expect("GRANT ROLE readwrite");

    let (client, handle) = server
        .connect_as("sgr_viewer", PASSWORD)
        .await
        .expect("connect as sgr_viewer");
    let rows = show_scope_grants_as(&client).await;
    drop(client);
    handle.abort();

    assert!(
        rows.iter()
            .any(|row| row.contains("sgr:all") && row.contains("sgr_role")),
        "a role-type scope grant in the caller's own tenant must be visible to a \
         non-superuser, got: {rows:?}"
    );
}
