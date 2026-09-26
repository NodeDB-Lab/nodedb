// SPDX-License-Identifier: BUSL-1.1

//! Authorization, row-level security, and column redaction for the SQL
//! functions that build their own physical plans.
//!
//! `TEMPORAL_LOOKUP`, `BALANCE_AS_OF`, `VERIFY_BALANCE`, `VERIFY_HASH_CHAIN`,
//! `CONVERT_CURRENCY_LOOKUP`, `ESTIMATE_COUNT`, `WEIGHTED_PICK` and `TREE_SUM`
//! take a collection name straight out of the caller's argument list and reach
//! the Data Plane through a hand-built plan, bypassing the planner that would
//! otherwise check the grant and inject the row filter. Each of them therefore
//! has to do it itself.
//!
//! The distinction these tests pin down is inject-versus-refuse: a function
//! whose plan carries a filter slot keeps working under a read policy and
//! returns fewer rows, while one whose plan carries no slot at all (a
//! statistics estimate) fails closed instead of answering over rows the policy
//! hides.
//!
//! `VERIFY_AUDIT_CHAIN` is the odd member of the family: it names no
//! collection at all, reading the node-wide audit log instead. Neither gate
//! applies to it, so it is checked here against the privilege the other
//! node-wide audit readers require.

use crate::harness::TestServer;

const PASSWORD: &str = "qfn-secret-71";
/// The role the harness superuser holds, which a redaction policy's `FOR ROLE`
/// binds against.
const SUPERUSER_ROLE: &str = "superuser";

/// Create a principal that holds no grant on anything.
///
/// `CREATE USER` defaults to ReadWrite and the `monitor` role still confers
/// `Permission::Read`, so neither is an unprivileged principal. A custom role
/// confers nothing without an explicit grant — that is what "no access to this
/// collection" actually looks like.
async fn create_stranger(server: &TestServer, user: &str) {
    // The role is defined and granted nothing: a user may hold only a
    // defined role.
    server
        .exec("CREATE ROLE IF NOT EXISTS qfn_nobody")
        .await
        .unwrap_or_else(|e| panic!("create role qfn_nobody: {e}"));
    server
        .exec(&format!(
            "CREATE USER {user} PASSWORD '{PASSWORD}' ROLE qfn_nobody"
        ))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
}

/// Create a principal that may read its own tenant's collections.
async fn create_reader(server: &TestServer, user: &str) {
    // The role is defined and granted nothing: a user may hold only a
    // defined role.
    server
        .exec("CREATE ROLE IF NOT EXISTS qfn_nobody")
        .await
        .unwrap_or_else(|e| panic!("create role qfn_nobody: {e}"));
    server
        .exec(&format!(
            "CREATE USER {user} PASSWORD '{PASSWORD}' ROLE qfn_nobody"
        ))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
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

/// Two versions of the same key, owned by different principals.
async fn seed_temporal(server: &TestServer, collection: &str, owner: &str) {
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {collection} {{ id: 'r1', pair: 'k1', ts: '2024-01-01', \
             owner: '{owner}', secret: 'mine' }}"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed r1: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {collection} {{ id: 'r2', pair: 'k1', ts: '2024-06-01', \
             owner: 'someone_else', secret: 'theirs' }}"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed r2: {e}"));
}

fn temporal_lookup_sql(collection: &str) -> String {
    format!("SELECT TEMPORAL_LOOKUP('{collection}', 'k1', '2024-12-31', 'pair', 'ts')")
}

/// A principal with no read grant on the named table gets no row from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn temporal_lookup_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed_temporal(&server, "qfn_tl_deny", "owner_a").await;
    create_stranger(&server, "qfn_tl_stranger").await;

    let result = run_as(
        &server,
        "qfn_tl_stranger",
        &temporal_lookup_sql("qfn_tl_deny"),
    )
    .await;

    assert_permission_denied("TEMPORAL_LOOKUP", result);
}

/// The plan carries a `filters` slot, so a read policy narrows the rows the
/// lookup considers instead of refusing the statement: the caller gets its own
/// version of the key, not the later one it may not read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn temporal_lookup_applies_the_read_policy() {
    let server = TestServer::start().await;
    seed_temporal(&server, "qfn_tl_rls", "qfn_tl_reader").await;
    create_reader(&server, "qfn_tl_reader").await;
    server
        .exec(
            "CREATE RLS POLICY qfn_tl_owner ON qfn_tl_rls FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create policy");

    let rows = run_as(&server, "qfn_tl_reader", &temporal_lookup_sql("qfn_tl_rls"))
        .await
        .expect("a read policy must narrow the lookup, not refuse it");
    let delivered = rows.join(" ");

    assert!(
        !delivered.contains("theirs"),
        "the policy-excluded version was returned: {delivered}"
    );
    assert!(
        delivered.contains("mine"),
        "the caller's own version should still be found: {delivered}"
    );
}

/// The matched row is returned verbatim, so a redaction rule on one of its
/// columns masks that column here exactly as it does on a plain SELECT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn temporal_lookup_masks_a_redacted_column() {
    let server = TestServer::start().await;
    seed_temporal(&server, "qfn_tl_mask", "owner_a").await;
    server
        .exec(&format!(
            "CREATE REDACTION POLICY qfn_tl_mask_secret ON qfn_tl_mask FOR ROLE \
             {SUPERUSER_ROLE} (secret MASK '***')"
        ))
        .await
        .expect("create redaction policy");

    let rows = server
        .query_text_joined(&temporal_lookup_sql("qfn_tl_mask"))
        .await
        .expect("lookup must still answer under a redaction rule");
    let delivered = rows.join(" ");

    assert!(
        delivered.contains("***"),
        "the ruled column must be masked: {delivered}"
    );
    assert!(
        !delivered.contains("theirs"),
        "the raw value must not survive masking: {delivered}"
    );
}

/// The happy path is unchanged for a principal that may read the table and is
/// under no policy: the latest version at or before the cutoff comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn temporal_lookup_still_answers_an_authorized_caller() {
    let server = TestServer::start().await;
    seed_temporal(&server, "qfn_tl_ok", "owner_a").await;
    create_reader(&server, "qfn_tl_reader_ok").await;

    let rows = run_as(
        &server,
        "qfn_tl_reader_ok",
        &temporal_lookup_sql("qfn_tl_ok"),
    )
    .await
    .expect("granted principal was refused its own collection");

    assert!(
        rows.join(" ").contains("theirs"),
        "the latest version at or before the cutoff should be returned: {rows:?}"
    );
}

/// `VERIFY_HASH_CHAIN` scans every document in the named collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_hash_chain_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed_temporal(&server, "qfn_hash_deny", "owner_a").await;
    create_stranger(&server, "qfn_hash_stranger").await;

    let result = run_as(
        &server,
        "qfn_hash_stranger",
        "SELECT VERIFY_HASH_CHAIN('qfn_hash_deny')",
    )
    .await;

    assert_permission_denied("VERIFY_HASH_CHAIN", result);
}

/// `BALANCE_AS_OF` point-gets the named collection before it consults the
/// catalog, so the denial lands whether or not a materialized sum exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn balance_as_of_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed_temporal(&server, "qfn_bal_deny", "owner_a").await;
    create_stranger(&server, "qfn_bal_stranger").await;

    let result = run_as(
        &server,
        "qfn_bal_stranger",
        "SELECT BALANCE_AS_OF('qfn_bal_deny', 'r1', 'secret', '1700000000')",
    )
    .await;

    assert_permission_denied("BALANCE_AS_OF", result);
}

/// `VERIFY_BALANCE` scans every row of the named collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_balance_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed_temporal(&server, "qfn_vbal_deny", "owner_a").await;
    create_stranger(&server, "qfn_vbal_stranger").await;

    let result = run_as(
        &server,
        "qfn_vbal_stranger",
        "SELECT VERIFY_BALANCE('qfn_vbal_deny', 'secret')",
    )
    .await;

    assert_permission_denied("VERIFY_BALANCE", result);
}

/// `CONVERT_CURRENCY_LOOKUP` scans the rate table it is handed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_currency_lookup_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed_temporal(&server, "qfn_ccy_deny", "owner_a").await;
    create_stranger(&server, "qfn_ccy_stranger").await;

    let result = run_as(
        &server,
        "qfn_ccy_stranger",
        "SELECT CONVERT_CURRENCY_LOOKUP('10', 'USD', 'EUR', 'qfn_ccy_deny', \
         '2024-12-31', 'pair', 'secret', 'ts', '2')",
    )
    .await;

    assert_permission_denied("CONVERT_CURRENCY_LOOKUP", result);
}

/// `ESTIMATE_COUNT` reports the cardinality of a column in the named
/// collection, which is a read of every row in it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn estimate_count_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed_temporal(&server, "qfn_est_deny", "owner_a").await;
    create_stranger(&server, "qfn_est_stranger").await;

    let result = run_as(
        &server,
        "qfn_est_stranger",
        "SELECT ESTIMATE_COUNT('qfn_est_deny', 'pair')",
    )
    .await;

    assert_permission_denied("ESTIMATE_COUNT", result);
}

/// An estimate is derived from statistics over every row and carries no filter
/// slot a policy could be pushed into, so it fails closed rather than reporting
/// a cardinality that counts rows the caller may not read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn estimate_count_refuses_under_a_read_policy() {
    let server = TestServer::start().await;
    seed_temporal(&server, "qfn_est_rls", "qfn_est_reader").await;
    create_reader(&server, "qfn_est_reader").await;
    server
        .exec(
            "CREATE RLS POLICY qfn_est_owner ON qfn_est_rls FOR READ \
             USING (owner = $auth.username)",
        )
        .await
        .expect("create policy");

    let result = run_as(
        &server,
        "qfn_est_reader",
        "SELECT ESTIMATE_COUNT('qfn_est_rls', 'pair')",
    )
    .await;

    match result {
        Err(message) => {
            let lowered = message.to_lowercase();
            assert!(
                lowered.contains("rls") || lowered.contains("polic"),
                "expected the refusal to name the policy, got: {message}"
            );
        }
        Ok(rows) => panic!("an estimate over a policy-protected collection was answered: {rows:?}"),
    }
}

/// `WEIGHTED_PICK` scans every entry of the KV collection it samples from.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn weighted_pick_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION qfn_pick_deny (id STRING PRIMARY KEY, weight INT) WITH (engine='kv')")
        .await
        .expect("create kv collection");
    server
        .exec("INSERT INTO qfn_pick_deny { id: 'a', weight: 1 }")
        .await
        .expect("seed pick row");
    create_stranger(&server, "qfn_pick_stranger").await;

    let result = run_as(
        &server,
        "qfn_pick_stranger",
        "SELECT * FROM WEIGHTED_PICK('qfn_pick_deny', weight => 'weight', count => 1)",
    )
    .await;

    assert_permission_denied("WEIGHTED_PICK", result);
}

/// `TREE_SUM` point-gets the documents it sums, so the named collection's read
/// grant is checked before any traversal runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tree_sum_without_a_read_grant_is_denied() {
    let server = TestServer::start().await;
    seed_temporal(&server, "qfn_tree_deny", "owner_a").await;
    create_stranger(&server, "qfn_tree_stranger").await;

    let result = run_as(
        &server,
        "qfn_tree_stranger",
        "SELECT TREE_SUM(amount, qfn_tree_idx, 'r1', 'qfn_tree_deny')",
    )
    .await;

    assert_permission_denied("TREE_SUM", result);
}

/// `VERIFY_AUDIT_CHAIN` names no collection — it reads the node-wide audit
/// log, whose entries belong to every tenant on the node. There is no grant to
/// check and no filter to inject, so it is gated the way the other node-wide
/// audit readers are: superuser only. The refusal must also land before the
/// log is read, so no part of the chain leaks through the error text.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_audit_chain_without_superuser_is_denied() {
    let server = TestServer::start().await;
    create_stranger(&server, "qfn_audit_stranger").await;

    let result = run_as(
        &server,
        "qfn_audit_stranger",
        "SELECT VERIFY_AUDIT_CHAIN(1, 100)",
    )
    .await;

    match result {
        Err(message) => {
            assert!(
                message.to_lowercase().contains("permission denied"),
                "expected a permission denial, got: {message}"
            );
            for leaked in ["last_hash", "entries_checked", "broken_at_seq"] {
                assert!(
                    !message.contains(leaked),
                    "the refusal disclosed chain state ({leaked}): {message}"
                );
            }
        }
        Ok(rows) => panic!("an unprivileged principal read the node-wide audit chain: {rows:?}"),
    }
}

/// A collection read grant is not the relevant privilege: the audit log is not
/// a collection, so a principal that may read its own tenant's data is still
/// refused a report covering every tenant's entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_audit_chain_is_denied_to_a_plain_reader() {
    let server = TestServer::start().await;
    create_reader(&server, "qfn_audit_reader").await;

    let result = run_as(
        &server,
        "qfn_audit_reader",
        "SELECT VERIFY_AUDIT_CHAIN(1, 100)",
    )
    .await;

    assert_permission_denied("VERIFY_AUDIT_CHAIN", result);
}

/// Regression guard: the gate must not shut out the principal the statement
/// was always meant for. The harness connection is superuser.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_audit_chain_still_answers_a_superuser() {
    let server = TestServer::start().await;

    let rows = server
        .query_text_joined("SELECT VERIFY_AUDIT_CHAIN(1, 100)")
        .await
        .expect("superuser was refused the audit chain");
    let delivered = rows.join(" ");

    assert!(
        delivered.contains("valid") && delivered.contains("entries_checked"),
        "the chain report should carry its verdict and entry count: {delivered}"
    );
}

/// `KV_INCR` reports the value it computed, so it is a read as well as a write
/// and needs both grants on the collection it names.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_incr_without_a_grant_is_denied() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION qfn_kv_deny (id STRING PRIMARY KEY, counter INT) WITH (engine='kv')",
        )
        .await
        .expect("create kv collection");
    create_stranger(&server, "qfn_kv_stranger").await;

    let result = run_as(
        &server,
        "qfn_kv_stranger",
        "SELECT KV_INCR('qfn_kv_deny', 'counter', 1)",
    )
    .await;

    assert_permission_denied("KV_INCR", result);
}
