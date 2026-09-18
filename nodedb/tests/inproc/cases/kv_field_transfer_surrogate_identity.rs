// SPDX-License-Identifier: BUSL-1.1

//! Regression test for cross-engine identity on the two remaining KV
//! value-computing write-back paths that previously hard-coded
//! `Surrogate::ZERO`: the field-atomic merge (`FieldSet`, reached via a KV
//! `UPDATE`) and the atomic transfer (`TRANSFER`, which moves a value between a
//! debit key and a credit key).
//!
//! Every row in every engine carries a stable global u32 surrogate allocated
//! from the WAL-durable, Raft-replicated counter and content-addressed on the
//! row's primary key. Cross-engine prefilter/join is a roaring-bitmap
//! intersection over those surrogates, so a KV row touched by these ops MUST
//! own a real, non-zero surrogate — the same one a normal insert of that key
//! would allocate. A transfer touches TWO rows (debit + credit), and each must
//! get ITS OWN distinct surrogate — collapsing both onto one identity would be
//! the very bug this guards against, one level over.
//!
//! As with the `Incr`/`Cas`/`GetSet` family, this is NOT observable through a
//! plain `SELECT ... WHERE key=X` (KV point-gets resolve by raw key bytes). The
//! authoritative observation is the persisted PK->surrogate map: the fix makes
//! the Control Plane allocate + persist a binding per touched key at plan
//! construction; the bug never called the assigner on these paths, so the
//! catalog map is missing the affected binding(s).

use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::{DatabaseId, Surrogate, TenantId};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_field_set_on_existing_key_persists_a_real_surrogate() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cf (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .expect("create kv collection");

    // SQL `UPDATE` only ever touches an existing row (an absent key is
    // `UPDATE 0`, never a create), so seed the row first.
    server
        .exec("INSERT INTO cf (key, n) VALUES ('fresh', 0)")
        .await
        .expect("seed key");

    // A KV `UPDATE` with a literal RHS on a PK-equality WHERE lowers to a
    // `FieldSet` (HSET-style read-modify-write) gated to the existing row.
    // Its write-back must carry the row's real persisted surrogate, not
    // `Surrogate::ZERO`.
    server
        .exec("UPDATE cf SET n = 5 WHERE key = 'fresh'")
        .await
        .expect("kv field-set on existing key");

    let catalog = server.shared.credentials.catalog();
    let bindings = catalog
        .scan_surrogates_for_collection(DatabaseId::DEFAULT, TenantId::new(1), "cf")
        .expect("scan persisted surrogate bindings for cf");

    assert_eq!(
        bindings.len(),
        1,
        "the field-atomic op must persist exactly one PK->surrogate binding for \
         the key (the bug allocated none and stored Surrogate::ZERO), \
         got: {bindings:?}"
    );
    assert!(
        bindings.iter().all(|(_, s)| *s != Surrogate::ZERO),
        "the field-atomic row must not be assigned the reserved zero surrogate, \
         got: {bindings:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_transfer_persists_two_distinct_surrogates() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION ct (key TEXT PRIMARY KEY, bal DOUBLE) WITH (engine='kv')")
        .await
        .expect("create kv collection");

    // The transfer requires the debit (source) row to exist, so insert it. That
    // insert persists the debit key's surrogate on its own. The credit (dest)
    // key is fresh — the transfer materializes it, and ITS surrogate must be
    // allocated on the transfer path.
    server
        .exec("INSERT INTO ct (key, bal) VALUES ('debit', 100)")
        .await
        .expect("insert debit row");

    server
        .exec("SELECT TRANSFER('ct', 'debit', 'credit', 'bal', 10)")
        .await
        .expect("transfer debit -> credit");

    let catalog = server.shared.credentials.catalog();
    let bindings = catalog
        .scan_surrogates_for_collection(DatabaseId::DEFAULT, TenantId::new(1), "ct")
        .expect("scan persisted surrogate bindings for ct");

    // Fix: two bindings (debit from the insert, credit from the transfer),
    // distinct and non-zero. Bug: the transfer never called the assigner for
    // the credit key, so only the debit binding exists (len == 1) and this
    // assertion fails.
    assert_eq!(
        bindings.len(),
        2,
        "the transfer must persist a distinct PK->surrogate binding for BOTH the \
         debit and credit keys (the bug allocated none for the transfer, leaving \
         only the debit key's insert-time binding), got: {bindings:?}"
    );
    assert!(
        bindings.iter().all(|(_, s)| *s != Surrogate::ZERO),
        "neither transferred row may be assigned the reserved zero surrogate, \
         got: {bindings:?}"
    );
    let distinct: std::collections::HashSet<Surrogate> = bindings.iter().map(|(_, s)| *s).collect();
    assert_eq!(
        distinct.len(),
        2,
        "the debit and credit rows must own DISTINCT surrogates — collapsing both \
         onto one identity is the exact cross-engine-join bug, one level over, \
         got: {bindings:?}"
    );
}
