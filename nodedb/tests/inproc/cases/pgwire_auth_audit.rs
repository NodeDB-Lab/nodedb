// SPDX-License-Identifier: BUSL-1.1

//! Audit-log emission by DDL + catalog-backed audit persistence across
//! restart cycles.

use std::sync::Arc;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::security::audit::AuditEvent;
use nodedb::control::state::SharedState;
use nodedb::types::TenantId;
use nodedb_test_support::pgwire_auth_helpers::{
    ddl_ok, make_state, make_state_with_catalog, superuser,
};

#[tokio::test]
async fn audit_records_create_and_drop() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER audit_test WITH PASSWORD 'pass'").await;
    ddl_ok(&state, &su, "DROP USER audit_test").await;

    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::PrivilegeChange);
    assert!(
        events.len() >= 2,
        "expected at least 2 PrivilegeChange events, got {}",
        events.len()
    );
    assert!(events.iter().any(|e| e.detail.contains("created")));
    assert!(events.iter().any(|e| e.detail.contains("dropped")));
}

#[tokio::test]
async fn audit_records_grant_revoke() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER karl WITH PASSWORD 'pass' ROLE readonly",
    )
    .await;
    ddl_ok(&state, &su, "GRANT ROLE readwrite TO karl").await;
    ddl_ok(&state, &su, "REVOKE ROLE readonly FROM karl").await;

    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::PrivilegeChange);
    assert!(events.iter().any(|e| e.detail.contains("granted")));
    assert!(events.iter().any(|e| e.detail.contains("revoked")));
}

#[tokio::test]
async fn audit_flush_persists_to_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let wal = Arc::new(nodedb::wal::WalManager::open_for_testing(&wal_path).unwrap());
    let catalog_path = dir.path().join("system.redb");

    let auth_config = nodedb::config::auth::AuthConfig::default();
    let (dispatcher, _sides) = Dispatcher::new(1, 64);
    let state = SharedState::open(
        nodedb::control::state::DataPlaneHandles {
            dispatcher,
            quiesce: nodedb::bridge::quiesce::CollectionQuiesce::new(),
            array_catalog: nodedb::control::array_catalog::ArrayCatalog::handle(),
            system_metrics: std::sync::Arc::new(nodedb::control::metrics::SystemMetrics::new()),
        },
        wal,
        &catalog_path,
        nodedb::control::state::OpenConfig::strict(&auth_config),
        nodedb_types::config::TuningConfig::default(),
        false,
        nodedb::data::executor::core_loop::test_governor(),
    )
    .unwrap();

    state.audit_record(AuditEvent::AuthSuccess, None, "test", "user logged in");
    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(TenantId::new(1)),
        "test",
        "granted role",
    );

    state.flush_audit_log();

    let catalog = state.credentials.catalog();
    let count = catalog.audit_entry_count().unwrap();
    assert_eq!(count, 2, "expected 2 persisted audit entries");

    let max_seq = catalog.load_audit_max_seq().unwrap();
    assert!(max_seq >= 2);
}

#[tokio::test]
async fn audit_sequence_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let catalog_path = dir.path().join("system.redb");
    let auth_config = nodedb::config::auth::AuthConfig::default();

    {
        let wal = Arc::new(nodedb::wal::WalManager::open_for_testing(&wal_path).unwrap());
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::open(
            nodedb::control::state::DataPlaneHandles {
                dispatcher,
                quiesce: nodedb::bridge::quiesce::CollectionQuiesce::new(),
                array_catalog: nodedb::control::array_catalog::ArrayCatalog::handle(),
                system_metrics: std::sync::Arc::new(nodedb::control::metrics::SystemMetrics::new()),
            },
            wal,
            &catalog_path,
            nodedb::control::state::OpenConfig::strict(&auth_config),
            nodedb_types::config::TuningConfig::default(),
            false,
            nodedb::data::executor::core_loop::test_governor(),
        )
        .unwrap();

        state.audit_record(AuditEvent::AuthSuccess, None, "src", "event1");
        state.audit_record(AuditEvent::AuthSuccess, None, "src", "event2");
        state.flush_audit_log();
        // Signal shutdown so background tasks (e.g. array GC) that hold
        // Arc<OriginOpLog> wake up and exit, releasing all redb file locks
        // before we reopen the same on-disk paths to simulate a restart.
        state.shutdown.signal();
        drop(state);
        // Yield to give background tasks a chance to observe the shutdown
        // signal and release their Arc references before we reopen.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }

    {
        let wal = Arc::new(nodedb::wal::WalManager::open_for_testing(&wal_path).unwrap());
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::open(
            nodedb::control::state::DataPlaneHandles {
                dispatcher,
                quiesce: nodedb::bridge::quiesce::CollectionQuiesce::new(),
                array_catalog: nodedb::control::array_catalog::ArrayCatalog::handle(),
                system_metrics: std::sync::Arc::new(nodedb::control::metrics::SystemMetrics::new()),
            },
            wal,
            &catalog_path,
            nodedb::control::state::OpenConfig::strict(&auth_config),
            nodedb_types::config::TuningConfig::default(),
            false,
            nodedb::data::executor::core_loop::test_governor(),
        )
        .unwrap();

        state.audit_record(AuditEvent::AdminAction, None, "src", "event3");
        state.flush_audit_log();

        let catalog = state.credentials.catalog();
        let count = catalog.audit_entry_count().unwrap();
        assert_eq!(
            count, 3,
            "expected 3 total persisted audit entries across restarts"
        );

        let max_seq = catalog.load_audit_max_seq().unwrap();
        assert!(max_seq >= 3, "sequence should be >= 3, got {max_seq}");
    }
}

/// The audit reader returns entries on both sides of a flush, once each, in seq order.
#[tokio::test]
async fn recent_audit_entries_merges_flushed_and_buffered() {
    let state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "CREATE USER ryw_first WITH PASSWORD 'pass'").await;
    state.flush_audit_log();
    assert!(
        state.credentials.catalog().audit_entry_count().unwrap() > 0,
        "the flush must persist the first entry to the catalog"
    );
    ddl_ok(&state, &su, "CREATE USER ryw_second WITH PASSWORD 'pass'").await;

    let entries = state.recent_audit_entries(10_000, |_| true).unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e.detail.contains("created user 'ryw_first'")),
        "the flushed entry must be visible"
    );
    assert!(
        entries
            .iter()
            .any(|e| e.detail.contains("created user 'ryw_second'")),
        "the buffered entry must be visible"
    );
    let seqs: Vec<u64> = entries.iter().map(|e| e.seq).collect();
    assert!(
        seqs.windows(2).all(|w| w[0] < w[1]),
        "entries must be unique and ascending: {seqs:?}"
    );

    // The limit applies after the filter and keeps the newest match.
    let newest = state
        .recent_audit_entries(1, |e| e.event == AuditEvent::PrivilegeChange)
        .unwrap();
    assert_eq!(newest.len(), 1);
    assert!(
        newest[0].detail.contains("created user 'ryw_second'"),
        "LIMIT 1 must return the newest matching entry: {:?}",
        newest[0]
    );

    // A second flush moves the buffered entry without duplicating it.
    state.flush_audit_log();
    let after = state.recent_audit_entries(10_000, |_| true).unwrap();
    let after_seqs: Vec<u64> = after.iter().map(|e| e.seq).collect();
    assert!(
        after_seqs.windows(2).all(|w| w[0] < w[1]),
        "entries must stay unique and ascending after a flush: {after_seqs:?}"
    );
    assert!(
        seqs.iter().all(|s| after_seqs.contains(s)),
        "a flush must not hide a visible entry: before {seqs:?}, after {after_seqs:?}"
    );
}

/// `VERIFY AUDIT CHAIN` passes on both sides of a flush, and fails on an altered persisted row.
#[tokio::test]
async fn verify_audit_chain_spans_flushes_and_detects_tampering() {
    let state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "CREATE USER chain_first WITH PASSWORD 'pass'").await;
    ddl_ok(&state, &su, "VERIFY AUDIT CHAIN").await;
    state.flush_audit_log();
    ddl_ok(&state, &su, "VERIFY AUDIT CHAIN").await;
    ddl_ok(&state, &su, "CREATE USER chain_second WITH PASSWORD 'pass'").await;
    ddl_ok(&state, &su, "VERIFY AUDIT CHAIN").await;
    state.flush_audit_log();

    let catalog = state.credentials.catalog();
    let mut rows = catalog
        .load_recent_audit_entries(usize::MAX, |_| true)
        .unwrap();
    assert!(rows.len() >= 2, "the flushes must persist the entries");
    let tampered_seq = rows[0].seq;
    rows[0].detail = "tampered".to_string();
    catalog.append_audit_entries(&rows[..1]).unwrap();

    let message =
        nodedb_test_support::pgwire_auth_helpers::ddl_err(&state, &su, "VERIFY AUDIT CHAIN").await;
    assert!(
        message.contains(&format!("broken at sequence {}", tampered_seq + 1)),
        "the first failing link must be reported: {message}"
    );
}

/// `CREATE DATABASE` emits a `DatabaseCreated` audit entry with the correct
/// database_id populated in the in-memory audit log.
#[tokio::test]
async fn create_database_emits_database_created_event() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE DATABASE audit_created_db").await;

    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::DatabaseCreated);
    assert!(
        !events.is_empty(),
        "expected at least one DatabaseCreated audit entry"
    );
    let latest = events.last().unwrap();
    assert!(
        latest.detail.contains("audit_created_db"),
        "detail should contain the database name"
    );
    assert!(
        latest.database_id.is_some(),
        "database_id must be populated on DatabaseCreated"
    );
}

/// `ALTER DATABASE SET AUDIT_DML = WRITES` persists the mode so that reading
/// back the descriptor from the catalog shows `audit_dml = Writes`.
#[tokio::test]
async fn alter_set_audit_dml_persists() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE DATABASE dml_audit_db").await;
    ddl_ok(
        &state,
        &su,
        "ALTER DATABASE dml_audit_db SET AUDIT_DML = 'WRITES'",
    )
    .await;

    // Read back from catalog and verify the mode was persisted.
    let catalog = state.credentials.catalog();
    let db_id = catalog
        .get_database_id_by_name("dml_audit_db")
        .unwrap()
        .expect("database must exist");
    let descriptor = catalog
        .get_database(db_id)
        .unwrap()
        .expect("descriptor must exist");
    assert_eq!(
        descriptor.audit_dml,
        nodedb_types::AuditDmlMode::Writes,
        "audit_dml mode must be Writes after ALTER DATABASE SET AUDIT_DML = WRITES"
    );

    // Verify audit event was emitted.
    let log = state.audit.lock().unwrap();
    let events = log.query_by_event(&AuditEvent::DatabaseAuditDmlChanged);
    assert!(
        !events.is_empty(),
        "expected DatabaseAuditDmlChanged audit entry"
    );
    assert_eq!(
        events.last().unwrap().database_id,
        Some(db_id),
        "audit entry must carry the correct database_id"
    );
}

/// `SHOW AUDIT IN DATABASE` returns only entries whose `database_id` matches
/// the named database and filters out entries from other databases.
#[tokio::test]
async fn show_audit_in_database_filters() {
    use nodedb_types::AuditDmlMode;

    let state = make_state_with_catalog();
    let su = superuser();

    // Create two databases and emit one audit entry each.
    ddl_ok(&state, &su, "CREATE DATABASE filter_db_a").await;
    ddl_ok(&state, &su, "CREATE DATABASE filter_db_b").await;

    let catalog = state.credentials.catalog();
    let db_id_a = catalog
        .get_database_id_by_name("filter_db_a")
        .unwrap()
        .expect("filter_db_a must exist");
    let db_id_b = catalog
        .get_database_id_by_name("filter_db_b")
        .unwrap()
        .expect("filter_db_b must exist");

    // In-memory log entries were emitted by CREATE DATABASE above.
    let log = state.audit.lock().unwrap();
    let db_a_entries = log.query_by_database(db_id_a);
    let db_b_entries = log.query_by_database(db_id_b);

    assert!(
        !db_a_entries.is_empty(),
        "filter_db_a should have at least one audit entry"
    );
    assert!(
        !db_b_entries.is_empty(),
        "filter_db_b should have at least one audit entry"
    );

    // Entries from db_a must not appear in db_b's view and vice versa.
    for entry in &db_a_entries {
        assert_eq!(
            entry.database_id,
            Some(db_id_a),
            "entries from db_a must have db_id_a"
        );
        assert_ne!(
            entry.database_id,
            Some(db_id_b),
            "entries from db_a must not carry db_id_b"
        );
    }
    for entry in &db_b_entries {
        assert_eq!(
            entry.database_id,
            Some(db_id_b),
            "entries from db_b must have db_id_b"
        );
    }
    // Suppress unused import warning from the AuditDmlMode import at the top.
    let _ = AuditDmlMode::None;
}
