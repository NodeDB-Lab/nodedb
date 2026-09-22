// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage: an RLS write policy created inside a non-default
//! database must be enforced against the external CRDT write path
//! (`CRDT MERGE INTO`), not just the planner-driven DML path.
//!
//! Every `PhysicalPlan::Crdt(CrdtOp::Apply { collection, .. })` construction
//! site on the external write paths (SQL `crdt_apply()`, the HTTP CRDT
//! endpoint, native raw dispatch, `RESTORE`, `CRDT MERGE`, and sync delta
//! push) fed `ExternalCrdtPostImagePolicy` the bare, request-typed collection
//! name, while RLS write policies are stored keyed by
//! `db_qualified(database_id, collection)`. In `default` the two coincide, so
//! the bug was invisible there; in any other database the keys never
//! matched, `RlsPolicyStore::write_policies` returned empty, and the post-
//! image policy evaluated to "no policy, allow" — the CRDT write went
//! through unopposed.

use crate::harness::TestServer;

const PASSWORD: &str = "crdt-rls-scope-secret-1";
const ROLE: &str = "readwrite";

async fn query_ok(server: &TestServer, sql: &str) {
    server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nsql: {sql}"));
}

/// Create `user`, grant it write access to the role and to `database`.
async fn create_scoped_user(server: &TestServer, user: &str, database: &str) {
    query_ok(
        server,
        &format!("CREATE USER {user} WITH PASSWORD '{PASSWORD}' ROLE {ROLE}"),
    )
    .await;
    query_ok(
        server,
        &format!("GRANT ALL ON DATABASE {database} TO {user}"),
    )
    .await;
}

/// Run `sql` as `user` against `database`; returns the server SQLSTATE on
/// failure, or `Ok(())` on success.
async fn try_exec_as(
    server: &TestServer,
    user: &str,
    database: &str,
    sql: &str,
) -> Result<(), String> {
    let (client, handle) = server
        .connect_as_database(user, PASSWORD, database)
        .await
        .unwrap_or_else(|e| panic!("connect as {user} on {database}: {e}"));
    let result = client.simple_query(sql).await.map(|_| ()).map_err(|error| {
        error
            .as_db_error()
            .map(|db_error| db_error.code().code().to_string())
            .unwrap_or_else(|| error.to_string())
    });
    drop(client);
    handle.abort();
    result
}

/// The `title` of `id` on `collection`, read back as the superuser — who
/// holds no restricting policy, so this is the true stored state.
async fn title_of(server: &TestServer, collection: &str, id: &str) -> String {
    let rows = server
        .query_rows(&format!("SELECT title FROM {collection} WHERE id = '{id}'"))
        .await
        .unwrap_or_else(|e| panic!("read back {collection}/{id}: {e}"));
    rows.into_iter()
        .next()
        .and_then(|row| row.into_iter().next())
        .unwrap_or_else(|| panic!("{collection}/{id} not found"))
}

/// A write policy created against a CRDT collection living in a non-default
/// database must reject a `CRDT MERGE INTO` whose result the policy forbids,
/// exactly as it does for an ordinary planner-driven write.
///
/// This is the regression: pre-fix, `ExternalCrdtPostImagePolicy` was handed
/// the bare collection name while the RLS store held the policy under the
/// database-qualified key, so the lookup found nothing and the merge applied
/// unopposed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_merge_in_non_default_database_is_rls_enforced() {
    let (server, db) = TestServer::with_database("crdt_rls_scope_db").await;
    let user = "crdt_rls_scope_nondefault_user";

    query_ok(
        &server,
        "CREATE TABLE crdt_rls_notes (id TEXT PRIMARY KEY, title TEXT) WITH (crdt='true')",
    )
    .await;
    query_ok(
        &server,
        "INSERT INTO crdt_rls_notes (id, title) VALUES ('src', 'poisoned')",
    )
    .await;
    query_ok(
        &server,
        "INSERT INTO crdt_rls_notes (id, title) VALUES ('target', 'placeholder')",
    )
    .await;
    create_scoped_user(&server, user, &db).await;

    // A predicate that no real row ever satisfies: `id` is the primary key,
    // so it always holds the document's own id, never this sentinel. Every
    // CRDT write on this collection is therefore denied regardless of what
    // fields the merge actually produces — the assertion below does not
    // depend on Loro's cross-document merge conflict resolution.
    query_ok(
        &server,
        "CREATE RLS POLICY crdt_rls_notes_block ON crdt_rls_notes FOR WRITE \
         USING (id = 'sentinel_id_no_row_ever_has')",
    )
    .await;

    let result = try_exec_as(
        &server,
        user,
        &db,
        "CRDT MERGE INTO crdt_rls_notes FROM 'src' TO 'target'",
    )
    .await;

    let sqlstate = result.expect_err(
        "a write policy on a non-default-database CRDT collection must reject \
         a CRDT MERGE its predicate forbids",
    );
    // `CRDT MERGE`'s handler classifies every admission failure — an RLS
    // denial reaches the client as `42501`. Pre-fix the merge applied
    // silently and no error, of any code, reached the client, so the
    // substantive assertion stays "an error surfaces at all"; the code is
    // pinned because a denial is the one case a client can act on.
    assert_eq!(
        sqlstate, "42501",
        "expected the CRDT admission failure's SQLSTATE, got: {sqlstate}"
    );
    assert_eq!(
        title_of(&server, "crdt_rls_notes", "target").await,
        "placeholder",
        "a rejected CRDT merge in a non-default database must leave the \
         target document's stored state untouched"
    );
}

/// The same scenario in `default` must keep working — this is the path the
/// fix must not regress, since `db_qualified` is a no-op for `default` and
/// the pre-fix code happened to key both sides identically there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_merge_in_default_database_is_still_rls_enforced() {
    let server = TestServer::start().await;
    let user = "crdt_rls_scope_default_user";

    query_ok(
        &server,
        "CREATE TABLE crdt_rls_default_notes (id TEXT PRIMARY KEY, title TEXT) \
         WITH (crdt='true')",
    )
    .await;
    query_ok(
        &server,
        "INSERT INTO crdt_rls_default_notes (id, title) VALUES ('src', 'poisoned')",
    )
    .await;
    query_ok(
        &server,
        "INSERT INTO crdt_rls_default_notes (id, title) VALUES ('target', 'placeholder')",
    )
    .await;
    query_ok(
        &server,
        &format!("CREATE USER {user} WITH PASSWORD '{PASSWORD}' ROLE {ROLE}"),
    )
    .await;

    query_ok(
        &server,
        "CREATE RLS POLICY crdt_rls_default_notes_block ON crdt_rls_default_notes FOR WRITE \
         USING (id = 'sentinel_id_no_row_ever_has')",
    )
    .await;

    let result = try_exec_as(
        &server,
        user,
        "default",
        "CRDT MERGE INTO crdt_rls_default_notes FROM 'src' TO 'target'",
    )
    .await;

    let sqlstate = result.expect_err(
        "a write policy in the default database must still reject a CRDT \
         MERGE its predicate forbids",
    );
    assert_eq!(
        sqlstate, "42501",
        "expected the CRDT admission failure's SQLSTATE, got: {sqlstate}"
    );
    assert_eq!(
        title_of(&server, "crdt_rls_default_notes", "target").await,
        "placeholder",
        "a rejected CRDT merge in the default database must leave the target \
         document's stored state untouched"
    );
}
