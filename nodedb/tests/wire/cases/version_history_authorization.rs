// SPDX-License-Identifier: BUSL-1.1

//! Authorization and policy handling for the version-history read family.
//!
//! `SELECT … AT VERSION` returns a document's merged state at a version and
//! `SELECT DIFF(…)` returns the oplog delta those states were built from —
//! both are stored row content, so both carry the collection's read grant, and
//! both now reach the Data Plane through the authorized door rather than the
//! system door reserved for work with no user behind it.
//!
//! `SHOW VERSIONS OF` is read straight from the catalog: checkpoint names,
//! their creators, and their timestamps describe a document in the collection,
//! so disclosing them takes the same grant the document does.
//!
//! None of the three carries a row a filter could be evaluated against, so all
//! three refuse while a read policy applies rather than returning a payload the
//! policy cannot narrow.

use crate::harness::TestServer;

const PASSWORD: &str = "version-authz-secret-7";

/// A collection with one document, plus a principal holding no grant on it.
async fn seed(server: &TestServer, collection: &str, stranger: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, owner) VALUES ('doc-1', 'alice')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed doc-1: {e}"));
    // The role is defined and granted nothing: a user may hold only a
    // defined role.
    server
        .exec("CREATE ROLE IF NOT EXISTS version_nobody")
        .await
        .unwrap_or_else(|e| panic!("create role version_nobody: {e}"));
    server
        .exec(&format!(
            "CREATE USER {stranger} PASSWORD '{PASSWORD}' ROLE version_nobody"
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
        Err(e) => Err(e.as_db_error().map_or_else(
            || (String::new(), e.to_string()),
            |db| (db.code().code().to_string(), db.message().to_string()),
        )),
    }
}

/// Assert `result` is the RBAC denial, not some later failure.
fn assert_denied(result: Result<usize, (String, String)>, what: &str) {
    match result {
        Err((sqlstate, message)) => {
            assert_eq!(
                sqlstate, "42501",
                "expected {what} to be denied for want of a read grant, got: {message}"
            );
            assert!(
                message.to_lowercase().contains("permission denied"),
                "expected a permission denial from {what}, got: {message}"
            );
        }
        Ok(rows) => panic!("ungranted principal ran {what} and saw {rows} row(s)"),
    }
}

/// Assert `result` is the policy refusal.
fn assert_refused(result: Result<usize, (String, String)>, what: &str) {
    match result {
        Err((sqlstate, message)) => assert_eq!(
            sqlstate, "0A000",
            "expected {what} to refuse under a read policy, got: {message}"
        ),
        Ok(rows) => panic!("{what} returned {rows} row(s) under a read policy"),
    }
}

/// Assert `result` did not fail for an authorization reason — the regression
/// guard for an authorized caller with no policies. The statement may still
/// fail on its own terms (no such checkpoint), but never on the gate.
fn assert_not_gated(result: Result<usize, (String, String)>, what: &str) {
    if let Err((sqlstate, message)) = result {
        assert!(
            sqlstate != "42501" && sqlstate != "0A000",
            "granted principal with no policy was gated out of {what}: {sqlstate} {message}"
        );
    }
}

/// A principal with no read grant learns nothing about a document's history.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn version_reads_without_a_read_grant_are_denied() {
    let server = TestServer::start().await;
    seed(&server, "vh_authz_grant", "vh_grant_stranger").await;

    assert_denied(
        run_as(
            &server,
            "vh_grant_stranger",
            "SHOW VERSIONS OF vh_authz_grant WHERE id = 'doc-1'",
        )
        .await,
        "SHOW VERSIONS OF",
    );
    assert_denied(
        run_as(
            &server,
            "vh_grant_stranger",
            "SELECT * FROM vh_authz_grant AT VERSION 'ckpt' WHERE id = 'doc-1'",
        )
        .await,
        "SELECT … AT VERSION",
    );
    assert_denied(
        run_as(
            &server,
            "vh_grant_stranger",
            "SELECT DIFF('vh_authz_grant', 'doc-1', 'ckpt_a', 'ckpt_b')",
        )
        .await,
        "SELECT DIFF(…)",
    );
}

/// A read policy refuses all three: each returns document content or metadata
/// about it through a payload with no row for the filter to narrow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn version_reads_refuse_while_a_read_policy_exists() {
    let server = TestServer::start().await;
    seed(&server, "vh_authz_rls", "vh_rls_user").await;
    server
        .exec("GRANT ROLE readwrite TO vh_rls_user")
        .await
        .expect("grant readwrite");
    server
        .exec(
            "CREATE RLS POLICY vh_owner ON vh_authz_rls FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create policy");

    assert_refused(
        run_as(
            &server,
            "vh_rls_user",
            "SHOW VERSIONS OF vh_authz_rls WHERE id = 'doc-1'",
        )
        .await,
        "SHOW VERSIONS OF",
    );
    assert_refused(
        run_as(
            &server,
            "vh_rls_user",
            "SELECT * FROM vh_authz_rls AT VERSION 'ckpt' WHERE id = 'doc-1'",
        )
        .await,
        "SELECT … AT VERSION",
    );
    assert_refused(
        run_as(
            &server,
            "vh_rls_user",
            "SELECT DIFF('vh_authz_rls', 'doc-1', 'ckpt_a', 'ckpt_b')",
        )
        .await,
        "SELECT DIFF(…)",
    );
}

/// A granted principal with no policy in place reaches the statements' own
/// behavior — the listing returns its (empty) rowset, and neither of the two
/// content reads is turned away by the gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn granted_principal_reaches_version_history() {
    let server = TestServer::start().await;
    seed(&server, "vh_authz_ok", "vh_ok_user").await;
    server
        .exec("GRANT ROLE readwrite TO vh_ok_user")
        .await
        .expect("grant readwrite");

    let listed = run_as(
        &server,
        "vh_ok_user",
        "SHOW VERSIONS OF vh_authz_ok WHERE id = 'doc-1'",
    )
    .await;
    assert_eq!(
        listed,
        Ok(0),
        "granted principal was refused an empty checkpoint listing"
    );

    assert_not_gated(
        run_as(
            &server,
            "vh_ok_user",
            "SELECT * FROM vh_authz_ok AT VERSION 'ckpt' WHERE id = 'doc-1'",
        )
        .await,
        "SELECT … AT VERSION",
    );
    assert_not_gated(
        run_as(
            &server,
            "vh_ok_user",
            "SELECT DIFF('vh_authz_ok', 'doc-1', 'ckpt_a', 'ckpt_b')",
        )
        .await,
        "SELECT DIFF(…)",
    );
}
