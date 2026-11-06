// SPDX-License-Identifier: BUSL-1.1

//! End-to-end coverage for the column-redaction policy DDL.
//!
//! Column redaction was fully enforced on every delivery surface long before a
//! statement existed to create a policy, so these tests are the first proof the
//! feature reaches a user at all: create a policy over pgwire, read the
//! collection back, and see the column masked.

use nodedb_test_support::pgwire_harness::TestServer;

/// The role the harness superuser holds, which is what a policy's `FOR ROLE`
/// binds against.
///
/// Registry assertions below use the tenant-agnostic `list_all_flat()` rather
/// than a hardcoded tenant id: the harness serves a single tenant, so "how many
/// policies exist at all" is the question these tests actually mean, and a
/// wrong-tenant constant would make an emptiness assertion pass vacuously
/// instead of failing. Cross-tenant scoping is covered separately, by the
/// explicit `TENANT 4242` listing in `show_redaction_policies_lists_created_policies`.
const ROLE: &str = "superuser";

/// `CREATE REDACTION POLICY` masks the ruled column on a plain SELECT, and
/// `DROP REDACTION POLICY` puts it back in the clear.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_then_drop_redaction_policy_round_trips_masking() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION redact_users").await.unwrap();
    server
        .exec("INSERT INTO redact_users { id: 'u1', email: 'alice@example.com', name: 'Alice' }")
        .await
        .unwrap();

    // Before any policy the column is delivered in the clear.
    let rows = server
        .query_text_joined("SELECT * FROM redact_users WHERE id = 'u1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("alice@example.com"),
        "email should be clear before the policy: {rows:?}"
    );

    server
        .exec(&format!(
            "CREATE REDACTION POLICY mask_pii ON redact_users FOR ROLE {ROLE} \
             (email MASK '***@***.com')"
        ))
        .await
        .unwrap();

    let rows = server
        .query_text_joined("SELECT * FROM redact_users WHERE id = 'u1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("***@***.com"),
        "email must be masked once the policy exists: {rows:?}"
    );
    assert!(
        !rows[0].contains("alice@example.com"),
        "the raw email must not survive masking: {rows:?}"
    );
    assert!(
        rows[0].contains("Alice"),
        "unruled columns stay clear: {rows:?}"
    );

    server
        .exec(&format!(
            "DROP REDACTION POLICY ON redact_users FOR ROLE {ROLE}"
        ))
        .await
        .unwrap();

    let rows = server
        .query_text_joined("SELECT * FROM redact_users WHERE id = 'u1'")
        .await
        .unwrap();
    assert!(
        rows[0].contains("alice@example.com"),
        "dropping the policy must restore the clear value: {rows:?}"
    );
}

/// `IF NOT EXISTS` / `IF EXISTS` behave like their RLS counterparts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redaction_policy_existence_guards() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION redact_guard").await.unwrap();

    // Dropping an absent policy is an error, and a silent success with IF EXISTS.
    server
        .exec(&format!(
            "DROP REDACTION POLICY ON redact_guard FOR ROLE {ROLE}"
        ))
        .await
        .expect_err("dropping an absent policy must fail without IF EXISTS");
    server
        .exec(&format!(
            "DROP REDACTION POLICY IF EXISTS ON redact_guard FOR ROLE {ROLE}"
        ))
        .await
        .unwrap();

    server
        .exec(&format!(
            "CREATE REDACTION POLICY p ON redact_guard FOR ROLE {ROLE} (ssn HASH)"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE REDACTION POLICY p ON redact_guard FOR ROLE {ROLE} (ssn HASH)"
        ))
        .await
        .expect_err("a duplicate policy must fail without IF NOT EXISTS");
    server
        .exec(&format!(
            "CREATE REDACTION POLICY IF NOT EXISTS p ON redact_guard FOR ROLE {ROLE} (ssn HASH)"
        ))
        .await
        .unwrap();
}

/// `SHOW REDACTION POLICIES` lists what was created, scoped to the tenant, and
/// narrows to one collection with `ON`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_redaction_policies_lists_created_policies() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION redact_a").await.unwrap();
    server.exec("CREATE COLLECTION redact_b").await.unwrap();
    server
        .exec(&format!(
            "CREATE REDACTION POLICY pa ON redact_a FOR ROLE {ROLE} (email MASK '***', ssn HASH)"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE REDACTION POLICY pb ON redact_b FOR ROLE {ROLE} (notes NULL)"
        ))
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SHOW REDACTION POLICIES")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "both policies must be listed: {rows:?}");
    let pa = rows
        .iter()
        .find(|row| row.get("name").map(String::as_str) == Some("pa"))
        .expect("policy pa listed");
    assert_eq!(pa.get("collection").map(String::as_str), Some("redact_a"));
    assert_eq!(pa.get("for_role").map(String::as_str), Some(ROLE));
    assert_eq!(pa.get("fields").map(String::as_str), Some("email, ssn"));
    assert!(
        pa.get("modes")
            .is_some_and(|m| m.contains("MASK '***'") && m.contains("HASH")),
        "modes column must render both rules: {pa:?}"
    );

    let scoped = server
        .query_named_rows("SHOW REDACTION POLICIES ON redact_b")
        .await
        .unwrap();
    assert_eq!(scoped.len(), 1, "ON must scope the listing: {scoped:?}");
    assert_eq!(scoped[0].get("name").map(String::as_str), Some("pb"));

    // A different tenant sees none of them.
    let other_tenant = server
        .query_named_rows("SHOW REDACTION POLICIES TENANT 4242")
        .await
        .unwrap();
    assert!(
        other_tenant.is_empty(),
        "policies must not leak across tenants: {other_tenant:?}"
    );
}

/// `MERGE ... RETURNING` surfaces real target rows, so its response must go
/// through the same masking pass a SELECT does.
///
/// It must not be classified as an opaque execution result and forwarded to the
/// client undecoded — that is harmless only while a MERGE returns nothing but
/// an affected count. Both halves of that are load-bearing: the plan must be
/// recognised as row-returning, AND it must report its target collection, or
/// the masking pass finds no policy to key on and runs inert.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_returning_rows_are_redacted() {
    let server = TestServer::start().await;

    for name in ["redact_merge_tgt", "redact_merge_src"] {
        server
            .exec(&format!(
                "CREATE COLLECTION {name} (\
                     id TEXT PRIMARY KEY, email TEXT, name TEXT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .unwrap_or_else(|e| panic!("create {name}: {e}"));
    }
    server
        .exec(
            "INSERT INTO redact_merge_tgt (id, email, name) \
             VALUES ('u1', 'alice@example.com', 'Alice')",
        )
        .await
        .unwrap();
    for (id, email, name) in [
        ("u1", "alice.new@example.com", "Alice"),
        ("u2", "bob@example.com", "Bob"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO redact_merge_src (id, email, name) \
                 VALUES ('{id}', '{email}', '{name}')"
            ))
            .await
            .unwrap();
    }
    server
        .exec(&format!(
            "CREATE REDACTION POLICY mask_merge ON redact_merge_tgt FOR ROLE {ROLE} \
             (email MASK '***@***.com')"
        ))
        .await
        .unwrap();

    // One matched UPDATE (u1) and one NOT-MATCHED INSERT (u2).
    let rows = server
        .query_rows(
            "MERGE INTO redact_merge_tgt t USING redact_merge_src s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET email = s.email \
             WHEN NOT MATCHED THEN INSERT (id, email, name) VALUES (s.id, s.email, s.name) \
             RETURNING id, email, name",
        )
        .await
        .expect("MERGE RETURNING should succeed");

    assert_eq!(rows.len(), 2, "one updated + one inserted row: {rows:?}");
    for row in &rows {
        assert_eq!(row[1], "***@***.com", "email must be masked: {row:?}");
    }
    let joined = rows
        .iter()
        .map(|r| r.join(","))
        .collect::<Vec<_>>()
        .join(";");
    assert!(
        !joined.contains("@example.com"),
        "no raw address may survive masking: {joined}"
    );
    assert!(
        joined.contains("Alice") && joined.contains("Bob"),
        "unruled columns stay clear: {joined}"
    );
}

/// An array is refused: its cells are delivered through a fan-out that carries
/// no subscriber identity, so a policy naming an array attribute would be
/// accepted and then silently never applied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_redaction_policy_rejects_an_array_collection() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE ARRAY redact_cube \
             DIMS (x INT64 [0..100], y INT64 [0..100]) \
             ATTRS (secret STRING) \
             TILE_EXTENTS (10, 10) \
             CELL_ORDER HILBERT",
        )
        .await
        .unwrap();

    server
        .expect_error(
            &format!("CREATE REDACTION POLICY p ON redact_cube FOR ROLE {ROLE} (secret HASH)"),
            "column redaction does not cover array attributes",
        )
        .await;

    assert!(
        server.shared.redaction.list_all_flat().is_empty(),
        "the refused policy must not have been installed"
    );
}

/// The other order is refused too: a policy first, then an array of its name.
/// The array reads would run under a policy that cannot mask them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_array_rejects_a_name_a_redaction_policy_holds() {
    let server = TestServer::start().await;

    server
        .exec(&format!(
            "CREATE REDACTION POLICY p ON redact_later FOR ROLE {ROLE} (secret HASH)"
        ))
        .await
        .unwrap();

    server
        .expect_error(
            "CREATE ARRAY redact_later \
             DIMS (x INT64 [0..100], y INT64 [0..100]) \
             ATTRS (secret STRING) \
             TILE_EXTENTS (10, 10) \
             CELL_ORDER HILBERT",
            "column redaction does not cover array attributes",
        )
        .await;
}

/// Purging a collection removes its policies from both the catalog and the
/// in-memory store, so a later collection of the same name is not redacted by
/// a policy nobody re-created.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purging_a_collection_removes_its_redaction_policies() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION redact_gone").await.unwrap();
    server
        .exec(&format!(
            "CREATE REDACTION POLICY p ON redact_gone FOR ROLE {ROLE} (email MASK '***')"
        ))
        .await
        .unwrap();
    assert_eq!(server.shared.redaction.list_all_flat().len(), 1);
    assert_eq!(
        server
            .shared
            .credentials
            .catalog()
            .load_all_redaction_policies()
            .unwrap()
            .len(),
        1
    );

    server
        .exec("DROP COLLECTION redact_gone PURGE")
        .await
        .unwrap();

    assert!(
        server.shared.redaction.list_all_flat().is_empty(),
        "the in-memory registry must be swept on purge"
    );
    assert!(
        server
            .shared
            .credentials
            .catalog()
            .load_all_redaction_policies()
            .unwrap()
            .is_empty(),
        "the catalog rows must be swept on purge"
    );

    // A same-name collection created afterwards is not redacted.
    server.exec("CREATE COLLECTION redact_gone").await.unwrap();
    server
        .exec("INSERT INTO redact_gone { id: 'u1', email: 'bob@example.com' }")
        .await
        .unwrap();
    let rows = server
        .query_text_joined("SELECT * FROM redact_gone WHERE id = 'u1'")
        .await
        .unwrap();
    assert!(
        rows[0].contains("bob@example.com"),
        "an orphaned policy must not resurrect against a recreated collection: {rows:?}"
    );
}

/// A policy created mid-session refuses an aggregate the same session already
/// planned (and may have cached) over the now-redacted column.
///
/// The refusal verdict is a property of the live policy set, not of the
/// compiled plan, so it must be re-evaluated on every execution rather than
/// baked into a cached one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn policy_created_mid_session_refuses_a_previously_cached_aggregate() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION redact_agg").await.unwrap();
    server
        .exec("INSERT INTO redact_agg { id: 'u1', salary: 100 }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO redact_agg { id: 'u2', salary: 200 }")
        .await
        .unwrap();

    const AGGREGATE: &str = "SELECT MAX(salary) FROM redact_agg";

    // Run it twice so the statement is warm in the per-session plan cache.
    server.query_text(AGGREGATE).await.unwrap();
    server.query_text(AGGREGATE).await.unwrap();

    server
        .exec(&format!(
            "CREATE REDACTION POLICY p ON redact_agg FOR ROLE {ROLE} (salary HASH)"
        ))
        .await
        .unwrap();

    // Same SQL, same session: the cached plan must not slip past the refusal.
    let result = server.query_text(AGGREGATE).await;
    assert!(
        result.is_err(),
        "an aggregate over a redacted column must be refused even from cache: {result:?}"
    );

    // Dropping the policy makes the same statement runnable again.
    server
        .exec(&format!(
            "DROP REDACTION POLICY ON redact_agg FOR ROLE {ROLE}"
        ))
        .await
        .unwrap();
    server
        .query_text(AGGREGATE)
        .await
        .expect("the aggregate runs again once the policy is gone");
}

/// `INSERT ... RETURNING` surfaces real stored rows, so its response must go
/// through the same masking pass a SELECT does.
///
/// Both halves are load-bearing, exactly as for MERGE: the plan must classify
/// as row-returning, AND it must report its collection, or the masking pass
/// finds no policy to key on and ships the rows in the clear.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_rows_are_redacted() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION redact_insert (id TEXT PRIMARY KEY, email TEXT, name TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create redact_insert");
    server
        .exec(&format!(
            "CREATE REDACTION POLICY mask_insert ON redact_insert FOR ROLE {ROLE} \
             (email MASK '***@***.com')"
        ))
        .await
        .expect("create redaction policy");

    let rows = server
        .query_rows(
            "INSERT INTO redact_insert (id, email, name) \
             VALUES ('u1', 'alice@example.com', 'Alice') \
             RETURNING id, email, name",
        )
        .await
        .expect("INSERT RETURNING should succeed");

    assert_eq!(rows.len(), 1, "one inserted row: {rows:?}");
    assert_eq!(rows[0][1], "***@***.com", "email must be masked: {rows:?}");
    assert_eq!(rows[0][2], "Alice", "an unruled column must survive intact");

    // The masking is a display rule, not a write rule: storage holds the real
    // address.
    let stored = server
        .query_named_rows("SELECT email FROM redact_insert")
        .await
        .expect("read back");
    assert_eq!(stored.len(), 1, "one stored row: {stored:?}");
}

/// A KV `INSERT ... RETURNING` surfaces real stored rows, so its response goes
/// through the same masking pass a SELECT does.
///
/// Both halves are load-bearing here as everywhere: the plan must classify as
/// row-returning, AND it must report its collection — for KV that comes from
/// `KvOp::collection()`, so a variant missing from that list would leave the
/// masking pass with no policy to key on and ship the rows in the clear.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_insert_returning_rows_are_redacted() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION redact_kv (key TEXT PRIMARY KEY, email TEXT, name TEXT) \
             WITH (engine='kv')",
        )
        .await
        .expect("create redact_kv");
    server
        .exec(&format!(
            "CREATE REDACTION POLICY mask_kv ON redact_kv FOR ROLE {ROLE} \
             (email MASK '***@***.com')"
        ))
        .await
        .expect("create redaction policy");

    let rows = server
        .query_rows(
            "INSERT INTO redact_kv (key, email, name) \
             VALUES ('u1', 'alice@example.com', 'Alice') \
             RETURNING key, email, name",
        )
        .await
        .expect("KV INSERT RETURNING should succeed");

    assert_eq!(rows.len(), 1, "one inserted row: {rows:?}");
    assert_eq!(rows[0][1], "***@***.com", "email must be masked: {rows:?}");
    assert_eq!(rows[0][2], "Alice", "an unruled column must survive intact");
}
