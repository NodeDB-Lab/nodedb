// SPDX-License-Identifier: BUSL-1.1

//! Catalog objects that reference a collection in a named database must not
//! fail the boot sanity check.
//!
//! An RLS policy stores its collection database-qualified
//! (`"{database_id}/{name}"`), while the catalog keys the collection by its
//! bare name and database. The boot integrity check must resolve the policy's
//! collection in the policy's own database, or it reports a dangling reference
//! and every boot refuses to start. A change stream on the same collection
//! rides along, since its check is scoped the same way.

mod crash_harness;

use crash_harness::CrashHarness;

const DATABASE: &str = "named_refs_db";
const COLLECTION: &str = "named_refs_pat";

#[tokio::test(flavor = "multi_thread")]
async fn a_named_database_rls_policy_and_change_stream_survive_restart() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready();

    h.exec(&format!("CREATE DATABASE {DATABASE}")).await;
    h.exec_in(
        DATABASE,
        &format!("CREATE COLLECTION {COLLECTION} (id TEXT PRIMARY KEY, owner TEXT)"),
    )
    .await;
    h.exec_in(
        DATABASE,
        &format!("INSERT INTO {COLLECTION} (id, owner) VALUES ('r1', 'alice')"),
    )
    .await;
    h.exec_in(
        DATABASE,
        &format!(
            "CREATE RLS POLICY named_refs_owner ON {COLLECTION} FOR READ \
             USING (owner = 'alice')"
        ),
    )
    .await;
    h.exec_in(
        DATABASE,
        &format!("CREATE CHANGE STREAM named_refs_stream ON {COLLECTION}"),
    )
    .await;

    h.kill_9();
    // `reopen` panics with the server output when the boot fails, which is
    // how the dangling-reference sanity check showed up.
    h.reopen();

    let policies = h.query_col_idx_in(DATABASE, "SHOW RLS POLICIES", 0).await;
    assert!(
        policies.iter().any(|name| name == "named_refs_owner"),
        "the RLS policy is missing after the restart: {policies:?}"
    );
    let ids = h
        .query_col_idx_in(DATABASE, &format!("SELECT id FROM {COLLECTION}"), 0)
        .await;
    assert_eq!(
        ids,
        vec!["r1".to_string()],
        "the row is missing after the restart"
    );

    // A second restart boots from the state the first restart left behind.
    h.kill_9();
    h.reopen();
}
