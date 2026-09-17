// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the catalog recovery sanity check pipeline.
//!
//! Each test builds a real `SharedState` backed by a tempdir `system.redb`,
//! plants a specific bad state by writing to the catalog while skipping the
//! in-memory registry update (simulating a load_from bug), and then calls
//! `verify_registries` directly. Assertions check for specific divergences.

use std::sync::Arc;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::cluster::recovery_check::registry_verify::verify_registries;
use nodedb::control::security::catalog::auth_types::{StoredApiKey, StoredBlacklistEntry};
use nodedb::control::security::catalog::trigger_types::{
    StoredTrigger, TriggerEvents, TriggerGranularity, TriggerTiming,
};
use nodedb::control::security::credential::store::CredentialStore;
use nodedb::control::state::SharedState;
use nodedb::wal::WalManager;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Build a SharedState with a real catalog-backed credential store.
/// Returns (shared, Arc<CredentialStore>) — the credential store Arc is kept
/// alive so `credentials.catalog()` remains valid for the duration of the test.
fn make_shared(data_dir: &std::path::Path) -> (Arc<SharedState>, Arc<CredentialStore>) {
    let wal_path = data_dir.join("test.wal");
    let catalog_path = data_dir.join("system.redb");

    let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let credentials = Arc::new(CredentialStore::open(&catalog_path).unwrap());
    let shared =
        SharedState::new_with_credentials(dispatcher, wal, Arc::clone(&credentials), false)
            .unwrap();
    (shared, credentials)
}

fn make_schedule_def(tenant_id: u64, name: &str) -> nodedb::event::scheduler::types::ScheduleDef {
    use nodedb::event::scheduler::types::{MissedPolicy, ScheduleDef, ScheduleScope};
    ScheduleDef {
        tenant_id,
        database_id: nodedb::types::DatabaseId::DEFAULT.as_u64(),
        name: name.to_string(),
        cron_expr: "*/5 * * * *".to_string(),
        body_sql: "SELECT 1".to_string(),
        scope: ScheduleScope::Normal,
        missed_policy: MissedPolicy::Skip,
        allow_overlap: true,
        enabled: true,
        target_collection: None,
        owner: "admin".to_string(),
        created_at: 0,
    }
}

fn make_alert_def(
    tenant_id: u64,
    name: &str,
    collection: &str,
) -> nodedb::event::alert::types::AlertDef {
    use nodedb::event::alert::types::{AlertCondition, AlertDef, CompareOp};
    AlertDef {
        database_id: 0,
        tenant_id,
        name: name.to_string(),
        collection: collection.to_string(),
        where_filter: None,
        condition: AlertCondition {
            agg_func: "avg".to_string(),
            column: "value".to_string(),
            op: CompareOp::Gt,
            threshold: 90.0,
        },
        group_by: vec![],
        window_ms: 60_000,
        fire_after: 1,
        recover_after: 1,
        severity: "warning".to_string(),
        notify_targets: vec![],
        enabled: true,
        owner: "admin".to_string(),
        created_at: 0,
    }
}

fn make_stream_def(tenant_id: u64, name: &str) -> nodedb::event::cdc::stream_def::ChangeStreamDef {
    use nodedb::event::cdc::stream_def::{
        ChangeStreamDef, OpFilter, RetentionConfig, StreamFormat,
    };
    ChangeStreamDef {
        database_id: nodedb::types::DatabaseId::new(7),
        tenant_id,
        name: name.to_string(),
        collection: "*".to_string(),
        op_filter: OpFilter::all(),
        format: StreamFormat::Json,
        retention: RetentionConfig::default(),
        compaction: Default::default(),
        webhook: Default::default(),
        late_data: Default::default(),
        kafka: Default::default(),
        owner: "admin".to_string(),
        created_at: 0,
        subscriber_roles: Vec::new(),
    }
}

fn make_consumer_group(
    tenant_id: u64,
    stream: &str,
    group: &str,
) -> nodedb::event::cdc::consumer_group::types::ConsumerGroupDef {
    use nodedb::event::cdc::consumer_group::types::ConsumerGroupDef;
    ConsumerGroupDef {
        database_id: nodedb::types::DatabaseId::new(7),
        tenant_id,
        name: group.to_string(),
        stream_name: stream.to_string(),
        owner: "admin".to_string(),
        created_at: 0,
    }
}

fn make_retention_policy(
    tenant_id: u64,
    name: &str,
    collection: &str,
) -> nodedb::engine::timeseries::retention_policy::types::RetentionPolicyDef {
    use nodedb::engine::timeseries::retention_policy::types::{RetentionPolicyDef, TierDef};
    RetentionPolicyDef {
        database_id: 0,
        tenant_id,
        name: name.to_string(),
        collection: collection.to_string(),
        tiers: vec![TierDef {
            tier_index: 0,
            resolution_ms: 0,
            aggregates: vec![],
            retain_ms: 86_400_000,
            archive: None,
        }],
        auto_tier: false,
        enabled: true,
        eval_interval_ms: RetentionPolicyDef::DEFAULT_EVAL_INTERVAL_MS,
        owner: "admin".to_string(),
        created_at: 0,
    }
}

fn make_mv_def(
    tenant_id: u64,
    name: &str,
    source_stream: &str,
) -> nodedb::event::streaming_mv::types::StreamingMvDef {
    use nodedb::event::streaming_mv::types::StreamingMvDef;
    StreamingMvDef {
        database_id: nodedb::types::DatabaseId::DEFAULT,
        tenant_id,
        name: name.to_string(),
        source_stream: source_stream.to_string(),
        group_by_columns: vec![],
        aggregates: vec![],
        filter_expr: None,
        owner: "admin".to_string(),
        created_at: 0,
    }
}

fn make_blacklist_entry(key: &str, kind: &str) -> StoredBlacklistEntry {
    StoredBlacklistEntry {
        key: key.to_string(),
        kind: kind.to_string(),
        reason: "test".to_string(),
        created_by: "admin".to_string(),
        created_at: 0,
        expires_at: 0,
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// A completely clean catalog passes all verifiers.
#[tokio::test]
async fn happy_path_clean_catalog_passes_all_verifiers() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    let result = verify_registries(&shared, catalog).unwrap();
    assert!(
        result.counts.is_empty(),
        "expected no divergences, got: {:?}",
        result.counts
    );
    assert!(result.all_repairs_ok);
    assert!(result.initial_divergences.is_empty());
}

/// RLS policy in redb but not in the in-memory store → MissingInRegistry.
#[tokio::test]
async fn rls_policy_orphan_refuses_startup() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    let stored = nodedb::control::security::catalog::rls::StoredRlsPolicy {
        tenant_id: 1,
        database_id: 0,
        collection: "orders".to_string(),
        display_collection: "orders".to_string(),
        name: "only_own_orders".to_string(),
        policy_type_tag: 0,
        predicate_text: String::new(),
        mode_tag: 0,
        on_deny_json: r#""Silent""#.to_string(),
        enabled: true,
        created_by: "admin".to_string(),
        created_at: 0,
    };
    catalog.put_rls_policy(&stored).unwrap();
    // Do NOT update shared.rls — simulate load_from bug.

    let result = verify_registries(&shared, catalog).unwrap();
    let rls_count = result
        .counts
        .get("rls_policies")
        .expect("rls_policies entry");
    assert!(rls_count.detected > 0, "expected rls_policies divergence");
}

/// Redaction policy in redb but not in the in-memory store → MissingInRegistry.
#[tokio::test]
async fn redaction_policy_orphan_refuses_startup() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    let stored = nodedb::control::security::catalog::redaction::StoredRedactionPolicy {
        tenant_id: 1,
        collection: "users".to_string(),
        display_collection: "users".to_string(),
        for_role: "support".to_string(),
        name: "mask_pii".to_string(),
        rules_json: "[]".to_string(),
    };
    catalog.put_redaction_policy(&stored).unwrap();
    // Do NOT update shared.redaction — simulate load_from bug.

    let result = verify_registries(&shared, catalog).unwrap();
    let redaction_count = result
        .counts
        .get("redaction_policies")
        .expect("redaction_policies entry");
    assert!(
        redaction_count.detected > 0,
        "expected redaction_policies divergence"
    );
}

/// Redaction policy value mismatch (rule count differs between redb and memory).
#[tokio::test]
async fn redaction_policy_value_mismatch_detected() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    let stored = nodedb::control::security::catalog::redaction::StoredRedactionPolicy {
        tenant_id: 1,
        collection: "users".to_string(),
        display_collection: "users".to_string(),
        for_role: "support".to_string(),
        name: "mask_pii".to_string(),
        rules_json: r#"[{"field":"email","mode":{"Mask":"***"}}]"#.to_string(),
    };
    catalog.put_redaction_policy(&stored).unwrap();

    // Insert into memory with zero rules — value mismatch (rule count differs).
    let mut policy = stored.to_runtime().unwrap();
    policy.rules.clear();
    shared.redaction.install_replicated_policy(policy);

    let result = verify_registries(&shared, catalog).unwrap();
    let redaction = result
        .counts
        .get("redaction_policies")
        .expect("redaction_policies");
    assert!(
        redaction.detected > 0,
        "expected redaction value mismatch detected"
    );
}

/// A policy whose rule list is the same length but redacts a field a different
/// way is the divergence that matters most — a weakened mask still "looks
/// right" by count. The verifier must compare rule content, not just arity.
#[tokio::test]
async fn redaction_policy_mode_change_detected_at_same_rule_count() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    let stored = nodedb::control::security::catalog::redaction::StoredRedactionPolicy {
        tenant_id: 1,
        collection: "users".to_string(),
        display_collection: "users".to_string(),
        for_role: "support".to_string(),
        name: "mask_pii".to_string(),
        rules_json: r#"[{"field":"email","mode":{"Mask":"***@***.com"}}]"#.to_string(),
    };
    catalog.put_redaction_policy(&stored).unwrap();

    // Same field, same rule count — only the mask payload differs.
    let mut policy = stored.to_runtime().unwrap();
    policy.rules[0].mode =
        nodedb::control::security::redaction::RedactionMode::Mask("*".to_string());
    shared.redaction.install_replicated_policy(policy);

    let result = verify_registries(&shared, catalog).unwrap();
    let redaction = result
        .counts
        .get("redaction_policies")
        .expect("redaction_policies");
    assert!(
        redaction.detected > 0,
        "expected a value mismatch when only the redaction mode differs"
    );
}

/// Blacklist entry in redb but not in memory → MissingInRegistry.
#[tokio::test]
async fn blacklist_ghost_refuses_startup() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    catalog
        .put_blacklist_entry(&make_blacklist_entry("user:evil_user", "user"))
        .unwrap();

    let result = verify_registries(&shared, catalog).unwrap();
    let bl = result.counts.get("blacklist").expect("blacklist entry");
    assert!(bl.detected > 0, "expected blacklist divergence");
}

/// Schedule in redb but not in memory → MissingInRegistry.
#[tokio::test]
async fn schedule_orphan_refuses_startup() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    catalog
        .put_schedule(&make_schedule_def(1, "nightly_cleanup"))
        .unwrap();

    let result = verify_registries(&shared, catalog).unwrap();
    let s = result.counts.get("schedules").expect("schedules entry");
    assert!(s.detected > 0, "expected schedules divergence");
}

/// Alert rule in redb but not in memory → MissingInRegistry.
#[tokio::test]
async fn alert_orphan_refuses_startup() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    catalog
        .put_alert_rule(&make_alert_def(1, "high_temp_alert", "sensors"))
        .unwrap();

    let result = verify_registries(&shared, catalog).unwrap();
    let a = result.counts.get("alert_rules").expect("alert_rules entry");
    assert!(a.detected > 0, "expected alert_rules divergence");
}

/// Streaming MV in redb but not in memory → MissingInRegistry.
#[tokio::test]
async fn mv_orphan_refuses_startup() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    catalog
        .put_streaming_mv(&make_mv_def(1, "orders_summary", "orders_stream"))
        .unwrap();

    let result = verify_registries(&shared, catalog).unwrap();
    let m = result
        .counts
        .get("streaming_mvs")
        .expect("streaming_mvs entry");
    assert!(m.detected > 0, "expected streaming_mvs divergence");
}

/// Change stream in redb but not in memory → MissingInRegistry.
#[tokio::test]
async fn change_stream_orphan_refuses_startup() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    catalog
        .put_change_stream(&make_stream_def(1, "orders_cdc"))
        .unwrap();

    let result = verify_registries(&shared, catalog).unwrap();
    let c = result
        .counts
        .get("change_streams")
        .expect("change_streams entry");
    assert!(c.detected > 0, "expected change_streams divergence");
}

/// Consumer group in redb but not in memory → MissingInRegistry.
#[tokio::test]
async fn consumer_group_orphan_refuses_startup() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    catalog
        .put_consumer_group(&make_consumer_group(1, "orders_cdc", "analytics_group"))
        .unwrap();

    let result = verify_registries(&shared, catalog).unwrap();
    let cg = result
        .counts
        .get("consumer_groups")
        .expect("consumer_groups entry");
    assert!(cg.detected > 0, "expected consumer_groups divergence");
}

/// Retention policy in redb but not in memory → MissingInRegistry.
#[tokio::test]
async fn retention_policy_orphan_refuses_startup() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    catalog
        .put_retention_policy(&make_retention_policy(1, "keep_90d", "metrics"))
        .unwrap();

    let result = verify_registries(&shared, catalog).unwrap();
    let r = result
        .counts
        .get("retention_policies")
        .expect("retention_policies entry");
    assert!(r.detected > 0, "expected retention_policies divergence");
}

/// User in redb but not loaded into memory → MissingInRegistry.
/// Simulates a load_from bug by using a CredentialStore::new() (in-memory only)
/// while the catalog was written by a separately-opened store.
#[tokio::test]
async fn credential_ghost_refuses_startup() {
    let dir = tempfile::tempdir().unwrap();
    let catalog_path = dir.path().join("system.redb");
    let wal_path = dir.path().join("test.wal");

    // Phase 1: Write a user to redb via a catalog-backed credential store.
    {
        let writer = CredentialStore::open(&catalog_path).unwrap();
        let cat = writer.catalog();
        let stored_user = nodedb::control::security::catalog::auth_types::StoredUser {
            user_id: 999,
            username: "ghost_user".to_string(),
            tenant_id: 1,
            password_hash: "argon2id$dummy".to_string(),
            scram_salt: vec![],
            scram_salted_password: vec![],
            roles: vec!["ReadOnly".to_string()],
            is_superuser: false,
            is_active: true,
            is_service_account: false,
            created_at: 0,
            updated_at: 0,
            password_expires_at: 0,
            must_change_password: false,
            password_changed_at: 0,
            default_database_id: 0,
            accessible_databases: vec![],
        };
        cat.put_user(&stored_user).unwrap();
        // writer and catalog dropped here — redb file is unlocked.
    }

    // Phase 2: Re-open with a catalog-backed store so we have the catalog,
    // but patch in an empty in-memory-only store as the credential store.
    // We do this by opening a second credential store backed by the same redb
    // (which now has the ghost user), but then replacing it in shared with an
    // empty store so memory doesn't know about the user.
    let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());
    let (dispatcher, _) = Dispatcher::new(1, 64);

    // Catalog-bearing store — for catalog access only.
    let catalog_store = Arc::new(CredentialStore::open(&catalog_path).unwrap());
    let catalog = catalog_store.catalog();

    // Memory-only store — no users loaded.
    let empty_creds = Arc::new(CredentialStore::new().expect("build credential store"));
    let shared = SharedState::new_with_credentials(dispatcher, wal, empty_creds, false).unwrap();

    let result = verify_registries(&shared, catalog).unwrap();
    let c = result.counts.get("credentials").expect("credentials entry");
    assert!(c.detected > 0, "expected credentials divergence");
}

/// RLS policy value mismatch (enabled flag differs between redb and memory).
#[tokio::test]
async fn rls_policy_value_mismatch_detected() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    let stored = nodedb::control::security::catalog::rls::StoredRlsPolicy {
        tenant_id: 1,
        database_id: 0,
        collection: "docs".to_string(),
        display_collection: "docs".to_string(),
        name: "read_own".to_string(),
        policy_type_tag: 0,
        predicate_text: String::new(),
        mode_tag: 0,
        on_deny_json: r#""Silent""#.to_string(),
        enabled: true,
        created_by: "admin".to_string(),
        created_at: 0,
    };
    catalog.put_rls_policy(&stored).unwrap();

    // Insert into memory with enabled=false — value mismatch.
    let mut policy = stored.to_runtime(catalog).unwrap();
    policy.enabled = false;
    shared.rls.install_replicated_policy(policy);

    let result = verify_registries(&shared, catalog).unwrap();
    let rls = result.counts.get("rls_policies").expect("rls_policies");
    assert!(rls.detected > 0, "expected rls value mismatch detected");
}

/// Re-prove that the triggers verifier still fires (existing verifier regression).
#[tokio::test]
async fn triggers_verifier_still_fires() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    let trigger = StoredTrigger {
        tenant_id: 1,
        database_id: nodedb::types::DatabaseId::DEFAULT,
        collection: "orders".to_string(),
        name: "send_email".to_string(),
        timing: TriggerTiming::After,
        events: TriggerEvents {
            on_insert: true,
            on_update: false,
            on_delete: false,
        },
        granularity: TriggerGranularity::Row,
        when_condition: None,
        body_sql: "BEGIN notify_email(); END".to_string(),
        priority: 0,
        enabled: true,
        execution_mode: Default::default(),
        security: Default::default(),
        batch_mode: Default::default(),
        owner: "admin".to_string(),
        created_at: 0,
        descriptor_version: 1,
        modification_hlc: Default::default(),
    };
    catalog.put_trigger(&trigger).unwrap();

    let result = verify_registries(&shared, catalog).unwrap();
    let t = result.counts.get("triggers").expect("triggers entry");
    assert!(t.detected > 0, "expected triggers divergence");
}

/// Re-prove that the api_keys verifier still fires (existing verifier regression).
#[tokio::test]
async fn api_keys_verifier_still_fires() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    let key = StoredApiKey {
        key_id: "test_key_id".to_string(),
        secret_hash: vec![0u8; 32],
        username: "admin".to_string(),
        user_id: 1,
        tenant_id: 1,
        expires_at: 0,
        is_revoked: false,
        created_at: 0,
        scope: vec![],
        accessible_databases: vec![],
    };
    catalog.put_api_key(&key).unwrap();

    let result = verify_registries(&shared, catalog).unwrap();
    let k = result.counts.get("api_keys").expect("api_keys entry");
    assert!(k.detected > 0, "expected api_keys divergence");
}

/// Repair cycle: verify detects divergence, repair runs automatically,
/// post-repair verify should show repaired count matches detected.
#[tokio::test]
async fn repair_cycle_succeeds_for_schedules() {
    let dir = tempfile::tempdir().unwrap();
    let (shared, creds) = make_shared(dir.path());
    let catalog = creds.catalog();

    catalog
        .put_schedule(&make_schedule_def(1, "hourly_job"))
        .unwrap();

    let pre = verify_registries(&shared, catalog).unwrap();
    let detected = pre.counts.get("schedules").map(|c| c.detected).unwrap_or(0);
    assert!(detected > 0, "expected initial divergence");
    assert!(
        pre.all_repairs_ok,
        "repair should have succeeded automatically"
    );

    // Re-verify after repair should show no divergences for schedules.
    let post = verify_registries(&shared, catalog).unwrap();
    let post_detected = post
        .counts
        .get("schedules")
        .map(|c| c.detected)
        .unwrap_or(0);
    assert_eq!(post_detected, 0, "after repair, schedule should be in sync");
}

/// Boot-load repopulation: a redaction policy written to the catalog before
/// a fresh `SharedState::open` must be present in the in-memory
/// `RedactionStore` immediately after boot — proving the
/// `load_all_redaction_policies` replay wired into `init_prod::bootstrap`
/// actually runs and installs policies rather than just compiling.
#[tokio::test]
async fn redaction_policy_survives_restart_via_boot_load() {
    use nodedb::config::auth::AuthConfig;
    use nodedb::control::security::catalog::redaction::StoredRedactionPolicy;
    use nodedb::control::security::credential::store::CredentialStore;

    let dir = tempfile::tempdir().unwrap();
    let catalog_path = dir.path().join("system.redb");
    let wal_path = dir.path().join("test.wal");

    // Phase 1: write a redaction policy directly to the catalog, then close
    // the credential store (redb only allows one writer handle at a time).
    {
        let credentials = CredentialStore::open(&catalog_path).unwrap();
        let stored = StoredRedactionPolicy {
            tenant_id: 1,
            collection: "users".to_string(),
            display_collection: "users".to_string(),
            for_role: "support".to_string(),
            name: "mask_pii".to_string(),
            rules_json: "[]".to_string(),
        };
        credentials.catalog().put_redaction_policy(&stored).unwrap();
    }

    // Phase 2: open a fresh `SharedState` against the same catalog path —
    // this runs `init_prod::bootstrap::run`, which must replay the policy
    // into `redaction_store` before `SharedState::open` returns.
    let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let shared = SharedState::open(
        nodedb::control::state::DataPlaneHandles {
            dispatcher,
            quiesce: nodedb::bridge::quiesce::CollectionQuiesce::new(),
            array_catalog: nodedb::control::array_catalog::ArrayCatalog::handle(),
            system_metrics: std::sync::Arc::new(nodedb::control::metrics::SystemMetrics::new()),
        },
        wal,
        &catalog_path,
        &AuthConfig::default(),
        Default::default(),
        false,
        nodedb::data::executor::core_loop::test_governor(),
    )
    .expect("shared_state reopen");

    let loaded = shared.redaction.list_all_flat();
    assert_eq!(loaded.len(), 1, "expected one redaction policy after boot");
    assert_eq!(loaded[0].name, "mask_pii");
    assert_eq!(loaded[0].collection, "users");
    assert_eq!(loaded[0].for_role, "support");
}
