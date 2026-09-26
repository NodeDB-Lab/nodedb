// SPDX-License-Identifier: BUSL-1.1

//! Authorization and row-level security for the reads that name an index
//! instead of a collection.
//!
//! `RANK`, `TOPK`, `RANGE` and `SORTED_COUNT` take a sorted-index name and
//! nothing else, yet every one of them returns keys drawn from the collection
//! the index was built over. `TREE_CHILDREN` names an edge label and returns
//! node ids from anywhere in the tenant. Neither shape reaches the planner
//! that would otherwise check the grant, so each resolves what it is reading
//! and gates on it: the owning collection for a sorted-index read, the whole
//! tenant for a walk that names no collection.
//!
//! None of these plans carries a filter slot — the reply is a rank, a count,
//! or a list of keys, never a row body — so a read policy makes the answer
//! unrepresentable rather than smaller, and the read fails closed.

use crate::harness::TestServer;

const PASSWORD: &str = "sidx-secret-42";

/// Create a principal that holds no grant on anything: a custom role confers
/// nothing without an explicit grant.
async fn create_stranger(server: &TestServer, user: &str) {
    // The role is defined and granted nothing: a user may hold only a
    // defined role.
    server
        .exec("CREATE ROLE IF NOT EXISTS sidx_nobody")
        .await
        .unwrap_or_else(|e| panic!("create role sidx_nobody: {e}"));
    server
        .exec(&format!(
            "CREATE USER {user} PASSWORD '{PASSWORD}' ROLE sidx_nobody"
        ))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
}

/// Create a principal that may read its own tenant's collections.
async fn create_reader(server: &TestServer, user: &str) {
    create_stranger(server, user).await;
    server
        .exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant readwrite to {user}: {e}"));
}

/// Run `sql` as `user`, returning the delivered row texts or the server's
/// refusal message.
async fn run_as(server: &TestServer, user: &str, sql: &str) -> Result<Vec<String>, String> {
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
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                    (0..row.len())
                        .filter_map(|i| row.get(i))
                        .collect::<Vec<_>>()
                        .join(" "),
                ),
                _ => None,
            })
            .collect()),
        // `tokio_postgres::Error`'s Display is just "db error"; the server's
        // message lives on the DbError payload.
        Err(e) => Err(e
            .as_db_error()
            .map(|db| db.message().to_string())
            .unwrap_or_else(|| e.to_string())),
    }
}

fn assert_permission_denied(what: &str, result: Result<Vec<String>, String>) {
    match result {
        Err(message) => assert!(
            message.to_lowercase().contains("permission denied"),
            "{what}: expected a permission denial, got: {message}"
        ),
        Ok(rows) => panic!("{what}: ungranted principal read {rows:?}"),
    }
}

fn assert_refused_by_policy(what: &str, result: Result<Vec<String>, String>) {
    match result {
        Err(message) => {
            let lowered = message.to_lowercase();
            assert!(
                lowered.contains("rls") || lowered.contains("polic"),
                "{what}: expected the refusal to name the policy, got: {message}"
            );
        }
        Ok(rows) => panic!("{what}: a policy-protected ordering was answered: {rows:?}"),
    }
}

/// A KV collection of scores with a sorted index over them.
async fn seed_board(server: &TestServer, collection: &str, index: &str, owner: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (id STRING PRIMARY KEY, score INT, owner STRING) \
             WITH (engine='kv')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {collection} {{ id: 'p1', score: 10, owner: '{owner}' }}"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed p1: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {collection} {{ id: 'p2', score: 20, owner: 'someone_else' }}"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed p2: {e}"));
    server
        .exec(&format!(
            "CREATE SORTED INDEX {index} ON {collection} (score DESC) KEY id"
        ))
        .await
        .unwrap_or_else(|e| panic!("create sorted index {index}: {e}"));
}

/// `TOPK` returns the collection's highest-scoring keys, so the caller needs a
/// read grant on the collection the index was built over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topk_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed_board(&server, "sidx_topk_deny", "sidx_topk_deny_idx", "owner_a").await;
    create_stranger(&server, "sidx_topk_stranger").await;

    let result = run_as(
        &server,
        "sidx_topk_stranger",
        "SELECT * FROM TOPK(sidx_topk_deny_idx, 5)",
    )
    .await;

    assert_permission_denied("TOPK", result);
}

/// `RANK` reports where one key sits among the collection's rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rank_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed_board(&server, "sidx_rank_deny", "sidx_rank_deny_idx", "owner_a").await;
    create_stranger(&server, "sidx_rank_stranger").await;

    let result = run_as(
        &server,
        "sidx_rank_stranger",
        "SELECT RANK(sidx_rank_deny_idx, 'p1')",
    )
    .await;

    assert_permission_denied("RANK", result);
}

/// `RANGE` returns every key whose score falls in the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed_board(&server, "sidx_range_deny", "sidx_range_deny_idx", "owner_a").await;
    create_stranger(&server, "sidx_range_stranger").await;

    let result = run_as(
        &server,
        "sidx_range_stranger",
        "SELECT * FROM RANGE(sidx_range_deny_idx, 0, 100)",
    )
    .await;

    assert_permission_denied("RANGE", result);
}

/// `SORTED_COUNT` reports how many of the collection's rows the index holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorted_count_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed_board(&server, "sidx_count_deny", "sidx_count_deny_idx", "owner_a").await;
    create_stranger(&server, "sidx_count_stranger").await;

    let result = run_as(
        &server,
        "sidx_count_stranger",
        "SELECT SORTED_COUNT(sidx_count_deny_idx)",
    )
    .await;

    assert_permission_denied("SORTED_COUNT", result);
}

/// The ordering is derived from every indexed row and carries no filter slot a
/// policy could be pushed into, so it fails closed rather than ranking rows the
/// caller may not read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topk_refuses_under_a_read_policy() {
    let server = TestServer::start().await;
    seed_board(
        &server,
        "sidx_topk_rls",
        "sidx_topk_rls_idx",
        "sidx_rls_reader",
    )
    .await;
    create_reader(&server, "sidx_rls_reader").await;
    server
        .exec(
            "CREATE RLS POLICY sidx_topk_owner ON sidx_topk_rls FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create policy");

    let result = run_as(
        &server,
        "sidx_rls_reader",
        "SELECT * FROM TOPK(sidx_topk_rls_idx, 5)",
    )
    .await;

    assert_refused_by_policy("TOPK", result);
}

/// …and so does the count, for the same reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorted_count_refuses_under_a_read_policy() {
    let server = TestServer::start().await;
    seed_board(
        &server,
        "sidx_count_rls",
        "sidx_count_rls_idx",
        "sidx_count_reader",
    )
    .await;
    create_reader(&server, "sidx_count_reader").await;
    server
        .exec(
            "CREATE RLS POLICY sidx_count_owner ON sidx_count_rls FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create policy");

    let result = run_as(
        &server,
        "sidx_count_reader",
        "SELECT SORTED_COUNT(sidx_count_rls_idx)",
    )
    .await;

    assert_refused_by_policy("SORTED_COUNT", result);
}

/// A principal that may read the collection and is under no policy reaches the
/// index rather than being refused — the gate does not regress the authorized
/// path.
///
/// This asserts reachability, not row content. `CREATE SORTED INDEX` does not
/// currently surface any rows through `TOPK` for rows inserted over the wire:
/// the backfill count it computes is discarded, and no test in this repository
/// has ever exercised registration and read end to end. That gap predates the
/// authorization gate added here — the gate only adds a refusal path, and the
/// dispatch below it is unchanged — so pinning row content in this file would
/// be asserting a separate, unfixed defect rather than the security property
/// this test owns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topk_reaches_an_authorized_caller() {
    let server = TestServer::start().await;
    seed_board(&server, "sidx_topk_ok", "sidx_topk_ok_idx", "owner_a").await;
    create_reader(&server, "sidx_topk_reader_ok").await;

    run_as(
        &server,
        "sidx_topk_reader_ok",
        "SELECT * FROM TOPK(sidx_topk_ok_idx, 5)",
    )
    .await
    .expect("granted principal was refused a collection it may read");
}

/// An index whose name resolves to no collection has nothing to authorize
/// against, so the read is refused rather than run unguarded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topk_on_an_unknown_index_is_refused() {
    let server = TestServer::start().await;
    create_reader(&server, "sidx_unknown_reader").await;

    let result = run_as(
        &server,
        "sidx_unknown_reader",
        "SELECT * FROM TOPK(sidx_no_such_index, 5)",
    )
    .await;

    match result {
        Err(message) => assert!(
            message.to_lowercase().contains("does not exist"),
            "expected an unresolved-index refusal, got: {message}"
        ),
        Ok(rows) => panic!("an unresolvable index answered: {rows:?}"),
    }
}

/// `DROP SORTED INDEX` removes the index's Data Plane state and its registry
/// row, so a principal that neither created it nor holds admin must be
/// refused — mirroring the owner-or-admin gate the generic `DROP INDEX`
/// teardown applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_sorted_index_without_ownership_is_denied() {
    let server = TestServer::start().await;
    seed_board(&server, "sidx_drop_deny", "sidx_drop_deny_idx", "owner_a").await;
    create_reader(&server, "sidx_drop_stranger").await;

    let result = run_as(
        &server,
        "sidx_drop_stranger",
        "DROP SORTED INDEX sidx_drop_deny_idx",
    )
    .await;

    assert_permission_denied("DROP SORTED INDEX", result);

    // The index must still be REGISTERED afterward: an ungated drop would have
    // removed it even though the caller was refused. A read against a dropped
    // index fails (the Data Plane answers `NotFound`, which surfaces as an
    // error), so reaching `Ok` here is what proves the drop did not happen.
    // The row CONTENT is deliberately not asserted — see
    // `topk_reaches_an_authorized_caller` for why.
    run_as(
        &server,
        "sidx_drop_stranger",
        "SELECT * FROM TOPK(sidx_drop_deny_idx, 5)",
    )
    .await
    .expect("index should still be registered after a denied drop");
}

/// The principal that created the index may still drop it — the gate does
/// not regress the authorized path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_sorted_index_by_owner_still_succeeds() {
    let server = TestServer::start().await;
    seed_board(&server, "sidx_drop_ok", "sidx_drop_ok_idx", "owner_a").await;

    // `seed_board` creates the index as the server's default admin
    // connection, so dropping it back through the same connection is the
    // owner (and admin) path.
    server
        .exec("DROP SORTED INDEX sidx_drop_ok_idx")
        .await
        .expect("owner/admin should be able to drop its own sorted index");

    let result = server.exec("SELECT SORTED_COUNT(sidx_drop_ok_idx)").await;
    assert!(
        result.is_err(),
        "sorted index should be gone after an authorized drop"
    );
}

/// Dropping a name that was never registered fails closed with the same
/// "not found" the read gates use, rather than proceeding ungated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_unknown_sorted_index_is_not_found() {
    let server = TestServer::start().await;

    let result = server
        .exec("DROP SORTED INDEX sidx_no_such_drop_index")
        .await;

    match result {
        Err(e) => {
            let message = e.to_string().to_lowercase();
            assert!(
                message.contains("does not exist"),
                "expected a not-found refusal, got: {message}"
            );
        }
        Ok(_) => panic!("dropping an unregistered sorted index should fail"),
    }
}

/// `TREE_CHILDREN` walks edges by label and can surface node ids from any
/// collection in the tenant, so a principal granted none of them is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tree_children_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION sidx_tree_deny")
        .await
        .expect("create collection");
    server
        .exec("INSERT INTO sidx_tree_deny { id: 'r1', parent: '' }")
        .await
        .expect("seed root");
    create_stranger(&server, "sidx_tree_stranger").await;

    let result = run_as(
        &server,
        "sidx_tree_stranger",
        "SELECT TREE_CHILDREN(sidx_tree_idx, 'r1')",
    )
    .await;

    assert_permission_denied("TREE_CHILDREN", result);
}

/// The walk returns node ids, which carry no row filter, so a read policy
/// anywhere on this identity refuses it rather than returning a descendant set
/// the policy cannot narrow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tree_children_refuses_under_a_read_policy() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION sidx_tree_rls")
        .await
        .expect("create collection");
    server
        .exec("INSERT INTO sidx_tree_rls { id: 'r1', owner: 'sidx_tree_reader' }")
        .await
        .expect("seed root");
    create_reader(&server, "sidx_tree_reader").await;
    server
        .exec(
            "CREATE RLS POLICY sidx_tree_owner ON sidx_tree_rls FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create policy");

    let result = run_as(
        &server,
        "sidx_tree_reader",
        "SELECT TREE_CHILDREN(sidx_tree_idx, 'r1')",
    )
    .await;

    assert_refused_by_policy("TREE_CHILDREN", result);
}
