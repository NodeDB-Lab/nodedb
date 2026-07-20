// SPDX-License-Identifier: BUSL-1.1

//! End-to-end contract: every parent-replicated `CREATE <TYPE>` DDL
//! issued through pgwire on a single-node server must persist BOTH
//! the primary row AND the matching `StoredOwner` row to the redb
//! system catalog.
//!
//! Counterpart of `catalog_integrity_apply.rs`. That file covers the
//! Raft-applier write path; this file covers the pgwire handler write
//! path. The single-node handler does not flow through the applier:
//! `propose_catalog_entry` returns `Ok(0)` when no metadata raft
//! group is installed (see `nodedb/src/control/metadata_proposer.rs`
//! line 124), and the handler falls back to writing the primary row
//! to redb directly. The applier's `owner::put_parent_owner` call is
//! the second half of that write — handlers that replicate only the
//! primary half leave redb with an orphan row, which the next clean
//! restart's `verify_redb_integrity` walker reports as an
//! `OrphanRow` divergence and aborts boot at the
//! `CatalogSanityCheck` phase.
//!
//! Each test issues a real `CREATE <TYPE>` via pgwire, then inspects
//! the catalog directly to assert both rows landed. A failing
//! `verify_redb_integrity` assertion is included as a regression
//! guard: silent owner-row omission is exactly the failure mode
//! exposed by the original report — a positive `owner_row_present`
//! check alone would not catch a future regression that wrote a
//! mis-keyed owner row (wrong tenant, wrong object_type) and would
//! still trip the integrity walker on the next restart.

mod catalog_integrity_helpers;
mod common;

use catalog_integrity_helpers::TENANT;
use common::pgwire_harness::TestServer;
use nodedb::control::cluster::recovery_check::integrity::verify_redb_integrity;
use nodedb::control::security::catalog::SystemCatalog;
use nodedb::control::security::catalog::auth_types::StoredOwner;

/// `TestServer::start()` bootstraps a single user with this name and
/// hands every connection an `AuthenticatedIdentity` with the same
/// `username`. Every CREATE issued in this file therefore writes
/// `Stored<T>.owner = "nodedb"`, and the matching `StoredOwner` row
/// must carry `owner_username = "nodedb"`. The shared
/// `owner_row_present` helper in `catalog_integrity_helpers` hardcodes
/// `"admin"` for the apply-path tests, so we ship a local
/// owner-finder keyed on the harness user instead.
const HARNESS_USER: &str = "nodedb";

/// Look up the owner row for `(object_type, TENANT, object_name)` in
/// redb. Returns the row if found, `None` otherwise. Owner-username
/// is checked separately so failure messages can distinguish
/// "no owner row at all" (the canonical orphan symptom) from
/// "wrong owner_username" (a future regression that wrote the row
/// with the bootstrap user instead of the creator).
fn owner_row_for(
    catalog: &SystemCatalog,
    object_type: &str,
    object_name: &str,
) -> Option<StoredOwner> {
    catalog.load_all_owners().unwrap().into_iter().find(|o| {
        o.object_type == object_type && o.tenant_id == TENANT && o.object_name == object_name
    })
}

/// Pull the live `SystemCatalog` out of a running `TestServer` so
/// tests can inspect redb directly. The harness uses the same
/// `CredentialStore` the handler writes through, so any orphan the
/// handler leaves is visible here.
fn catalog_of(server: &TestServer) -> &SystemCatalog {
    server.shared.credentials.catalog()
}

/// Common post-condition for every test in this file: after a CREATE,
/// the catalog contains the owner row AND `verify_redb_integrity`
/// reports zero violations.
///
/// Both halves matter. `owner_row_present` catches the canonical
/// orphan symptom (no `StoredOwner` for the new object). The
/// `verify_redb_integrity` sweep catches the *exact* failure mode the
/// startup `CatalogSanityCheck` runs on every clean restart — a
/// future regression that wrote a mis-keyed owner row (wrong
/// tenant_id, wrong object_type, wrong object_name) would silently
/// pass the positive check yet still brick the next boot.
fn assert_owner_persisted(catalog: &SystemCatalog, object_type: &str, object_name: &str) {
    let owner = owner_row_for(catalog, object_type, object_name).unwrap_or_else(|| {
        panic!(
            "CREATE {object_type} '{object_name}' via pgwire on a single-node server \
             must persist a matching StoredOwner row to redb — the handler's \
             `log_index == 0` direct-write path is responsible for the owner row \
             because no Raft applier runs in single-node mode"
        )
    });
    assert_eq!(
        owner.owner_username, HARNESS_USER,
        "the owner row for {object_type} '{object_name}' must record the user \
         who issued the CREATE ('{HARNESS_USER}'), not some hardcoded default — \
         a row with the wrong owner_username silently breaks permission lookups"
    );
    let violations = verify_redb_integrity(catalog);
    assert!(
        violations.is_empty(),
        "verify_redb_integrity must report zero violations after CREATE \
         {object_type} '{object_name}' — clean restart's CatalogSanityCheck \
         aborts boot on any OrphanRow. Got: {violations:?}"
    );
}

// ── 1. CREATE COLLECTION — the owner-persistence repro ───────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_collection_via_pgwire_persists_owner_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION orphan_repro TYPE document")
        .await
        .expect("CREATE COLLECTION should succeed");
    assert_owner_persisted(catalog_of(&server), "collection", "orphan_repro");
}

// ── 2. CREATE FUNCTION ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_function_via_pgwire_persists_owner_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE FUNCTION double_it(x INT) RETURNS INT AS SELECT x * 2")
        .await
        .expect("CREATE FUNCTION should succeed");
    assert_owner_persisted(catalog_of(&server), "function", "double_it");
}

// ── 3. CREATE PROCEDURE ──────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_procedure_via_pgwire_persists_owner_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE PROCEDURE noop() AS BEGIN DECLARE x INT := 0; END")
        .await
        .expect("CREATE PROCEDURE should succeed");
    assert_owner_persisted(catalog_of(&server), "procedure", "noop");
}

// ── 4. CREATE TRIGGER (requires a parent collection) ─────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_trigger_via_pgwire_persists_owner_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION trg_parent TYPE document")
        .await
        .expect("parent collection should create");
    server
        .exec(
            "CREATE TRIGGER trg_on_parent AFTER INSERT ON trg_parent FOR EACH ROW \
             BEGIN DECLARE noop INT := 0; END",
        )
        .await
        .expect("CREATE TRIGGER should succeed");
    let catalog = catalog_of(&server);
    // The parent collection's owner row must also be present —
    // it goes through the same buggy bypass path as the trigger.
    assert_owner_persisted(catalog, "collection", "trg_parent");
    assert_owner_persisted(catalog, "trigger", "trg_on_parent");
}

// ── 5. CREATE MATERIALIZED VIEW (requires a source collection) ──────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_materialized_view_via_pgwire_persists_owner_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION mv_src TYPE document")
        .await
        .expect("source collection should create");
    server
        .exec("CREATE MATERIALIZED VIEW mv_owned ON mv_src AS SELECT * FROM mv_src")
        .await
        .expect("CREATE MATERIALIZED VIEW should succeed");
    let catalog = catalog_of(&server);
    assert_owner_persisted(catalog, "collection", "mv_src");
    assert_owner_persisted(catalog, "materialized_view", "mv_owned");
    // The MV handler also creates a target collection via a nested
    // `propose_catalog_entry` + direct `put_collection` (see
    // `pgwire/ddl/materialized_view/create.rs` line ~136-144). That
    // nested write is on the same `log_index == 0` bypass branch
    // and must also persist an owner row.
    assert_owner_persisted(catalog, "collection", "mv_owned");
}

// ── 6. CREATE SEQUENCE ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_sequence_via_pgwire_persists_owner_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE SEQUENCE order_seq START 1 INCREMENT 1")
        .await
        .expect("CREATE SEQUENCE should succeed");
    assert_owner_persisted(catalog_of(&server), "sequence", "order_seq");
}

// ── 7. CREATE SCHEDULE ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_schedule_via_pgwire_persists_owner_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE SCHEDULE nightly_owned CRON '0 3 * * *' AS BEGIN RETURN 1; END")
        .await
        .expect("CREATE SCHEDULE should succeed");
    assert_owner_persisted(catalog_of(&server), "schedule", "nightly_owned");
}

// ── 8. CREATE CHANGE STREAM (requires a parent collection) ───────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_change_stream_via_pgwire_persists_owner_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION cs_parent TYPE document")
        .await
        .expect("parent collection should create");
    server
        .exec("CREATE CHANGE STREAM cs_owned ON cs_parent")
        .await
        .expect("CREATE CHANGE STREAM should succeed");
    let catalog = catalog_of(&server);
    assert_owner_persisted(catalog, "collection", "cs_parent");
    assert_owner_persisted(catalog, "change_stream", "cs_owned");
}

// ── 9. CREATE COLLECTION (... col SERIAL ...) nested sequence write ─────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_collection_with_serial_field_persists_sequence_owner_row() {
    // `CREATE COLLECTION ... (id SERIAL)` auto-creates a backing
    // sequence via a direct `catalog.put_sequence(...)` call inside
    // the CREATE COLLECTION handler (see
    // `pgwire/ddl/collection/create/handler.rs` line ~246). That
    // direct write is unconditional — it never goes through
    // `propose_catalog_entry` at all, so the orphan happens on every
    // deployment mode, not just single-node.
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION serial_owner FIELDS (id SERIAL, name TEXT)")
        .await
        .expect("CREATE COLLECTION ... (id SERIAL) should succeed");
    let catalog = catalog_of(&server);
    assert_owner_persisted(catalog, "collection", "serial_owner");
    // Auto-created sequence name pattern from the handler:
    // `format!("{name}_{field_name}_seq")`.
    assert_owner_persisted(catalog, "sequence", "serial_owner_id_seq");
}

// ── 10. Restart roundtrip — the boot-integrity wedge ─────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_create_collection_then_reopen_has_zero_integrity_violations() {
    // Mirrors the deterministic owner-persistence repro:
    //   1. Boot fresh server.
    //   2. `CREATE COLLECTION orphan_repro TYPE document` via pgwire.
    //   3. Graceful shutdown.
    //   4. Reopen the same data dir.
    //   5. `verify_redb_integrity` on the reopened catalog must
    //      report zero violations — this is exactly what the
    //      `CatalogSanityCheck` startup phase runs, and any
    //      `OrphanRow` here is what bricks boot in production.
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION orphan_repro TYPE document")
        .await
        .expect("CREATE COLLECTION should succeed");

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;

    let (reopened, _dir) = TestServer::open_on_path(dir).await;
    let catalog = catalog_of(&reopened);

    let violations = verify_redb_integrity(catalog);
    assert!(
        violations.is_empty(),
        "verify_redb_integrity on a freshly-reopened catalog must be \
         empty after a single CREATE COLLECTION + restart — the original \
         bug is that this returns `OrphanRow(collection)` \
         and the StartupSequencer transitions to Failed at \
         CatalogSanityCheck. Got: {violations:?}"
    );
    assert!(
        owner_row_for(catalog, "collection", "orphan_repro").is_some(),
        "the collection's owner row must survive a restart — its absence \
         is the symptom that triggers the OrphanRow divergence"
    );

    reopened.graceful_shutdown().await;
}

// ── ALTER bypass: same `log_index == 0` shape, must heal an absent owner ────
//
// Every ALTER handler in the parent-replicated family shares the
// same `if log_index == 0 { catalog.put_<type>(...) }` block as its
// CREATE peer. CREATE already proves the bypass forgets the owner
// row, but those tests don't isolate ALTER — a fix that patches only
// the CREATE handlers leaves ALTER bypasses still capable of
// silently perpetuating an orphan from any future cause (a
// half-applied snapshot, an operator catalog edit, a yet-unfixed
// CREATE path nobody remembered).
//
// Each ALTER test below forces the precondition the test is about:
// primary row present, owner row absent. `delete_owner` is
// idempotent — today the CREATE handler has already left the owner
// absent and the call is a no-op; once CREATE is fixed it actively
// deletes, producing the same state. Either way, the ALTER that
// follows must restore the owner row (the architectural fix routes
// every single-node bypass through `apply_to`, whose
// `put_parent_owner` companion write is the second half of the
// primary-row write).

/// Force the post-CREATE state to "primary present, owner absent",
/// regardless of whether the CREATE handler has been fixed yet.
fn force_orphan_state(catalog: &SystemCatalog, object_type: &str, object_name: &str) {
    catalog
        .delete_owner(object_type, 0, TENANT, object_name)
        .expect("delete_owner is idempotent");
    assert!(
        owner_row_for(catalog, object_type, object_name).is_none(),
        "test precondition: owner row for {object_type} '{object_name}' \
         must be absent before the ALTER under test"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_collection_add_column_via_pgwire_restores_missing_owner_row() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION add_col_target (id TEXT PRIMARY KEY, name TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION should succeed");
    force_orphan_state(catalog_of(&server), "collection", "add_col_target");
    server
        .exec("ALTER COLLECTION add_col_target ADD COLUMN score INT DEFAULT 0")
        .await
        .expect("ALTER ... ADD COLUMN should succeed");
    assert_owner_persisted(catalog_of(&server), "collection", "add_col_target");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_collection_drop_column_via_pgwire_restores_missing_owner_row() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION drop_col_target (id TEXT PRIMARY KEY, scratch INT DEFAULT 0) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION should succeed");
    force_orphan_state(catalog_of(&server), "collection", "drop_col_target");
    server
        .exec("ALTER COLLECTION drop_col_target DROP COLUMN scratch")
        .await
        .expect("ALTER ... DROP COLUMN should succeed");
    assert_owner_persisted(catalog_of(&server), "collection", "drop_col_target");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_collection_rename_column_via_pgwire_restores_missing_owner_row() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION rename_col_target (id TEXT PRIMARY KEY, name TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION should succeed");
    force_orphan_state(catalog_of(&server), "collection", "rename_col_target");
    server
        .exec("ALTER COLLECTION rename_col_target RENAME COLUMN name TO title")
        .await
        .expect("ALTER ... RENAME COLUMN should succeed");
    assert_owner_persisted(catalog_of(&server), "collection", "rename_col_target");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_sequence_via_pgwire_restores_missing_owner_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE SEQUENCE altered_seq START 1 INCREMENT 1")
        .await
        .expect("CREATE SEQUENCE should succeed");
    force_orphan_state(catalog_of(&server), "sequence", "altered_seq");
    // ALTER SEQUENCE FORMAT is the variant that ships a full
    // `PutSequence` record (RESTART ships `PutSequenceState` —
    // Exempt from the owner-row invariant). FORMAT exercises the
    // same `log_index == 0` bypass shape as CREATE SEQUENCE.
    server
        .exec("ALTER SEQUENCE altered_seq FORMAT 'SEQ-{SEQ:04}'")
        .await
        .expect("ALTER SEQUENCE FORMAT should succeed");
    assert_owner_persisted(catalog_of(&server), "sequence", "altered_seq");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_schedule_via_pgwire_restores_missing_owner_row() {
    // `pgwire/ddl/schedule/alter.rs:74-83` is the worst variant of
    // the bypass: it calls `catalog.put_schedule(...)` with no
    // `propose_catalog_entry` at all. The orphan-owner symptom is
    // identical, but the same handler also silently skips cluster
    // replication. The fix that routes through `apply_to` cures
    // both halves at once; here we only assert the owner-row half
    // because that is the issue's reported failure class.
    let server = TestServer::start().await;
    server
        .exec("CREATE SCHEDULE altered_sch CRON '0 3 * * *' AS BEGIN RETURN 1; END")
        .await
        .expect("CREATE SCHEDULE should succeed");
    force_orphan_state(catalog_of(&server), "schedule", "altered_sch");
    server
        .exec("ALTER SCHEDULE altered_sch SET CRON '0 0 * * *'")
        .await
        .expect("ALTER SCHEDULE SET CRON should succeed");
    assert_owner_persisted(catalog_of(&server), "schedule", "altered_sch");
}
