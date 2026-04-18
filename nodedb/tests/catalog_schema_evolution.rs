//! Catalog schema evolution: a previously-written catalog row must decode
//! into a current-binary struct that has added, removed, reordered, or
//! renamed `#[serde(default)]` fields.
//!
//! Invariant: catalog types are stored with named-key MessagePack so that
//! additive evolution, field removal, reordering, and unknown-key
//! tolerance are all backward-compatible without a manual migration.
//!
//! Each test builds a slim shadow of the real catalog type (the shape a
//! prior binary would have written), serializes it with `zerompk`, then
//! decodes it into the current real type. Missing `#[serde(default)]`
//! fields must be filled with `Default::default()`.
//!
//! When this invariant is violated, `zerompk::from_msgpack` surfaces an
//! `Array length mismatch` (array-mode) or `KeyMissing` (strict-map)
//! error, which propagates through `list_collections` / `get_collection`
//! and similar calls as `storage error (catalog): deser …` — the exact
//! failure mode operators see after a routine additive schema edit.

use nodedb::control::security::catalog::procedure_types::{ProcedureParam, StoredProcedure};
use nodedb::control::security::catalog::trigger_types::{
    StoredTrigger, TriggerEvents, TriggerGranularity, TriggerTiming,
};
use nodedb::control::security::catalog::types::{CheckpointRecord, StoredCollection};
use nodedb::engine::timeseries::retention_policy::types::{RetentionPolicyDef, TierDef};
use nodedb::event::cdc::stream_def::{ChangeStreamDef, OpFilter, RetentionConfig, StreamFormat};
use nodedb::event::scheduler::types::{MissedPolicy, ScheduleDef, ScheduleScope};
use nodedb_types::Hlc;

// ── StoredCollection — the reported symptom (issue's direct repro) ────────

/// Previous-binary shape: every currently-defaulted field is absent.
/// Writer emits named-key map; reader must honour `#[serde(default)]`
/// for the missing keys.
#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct StoredCollectionPrev {
    tenant_id: u32,
    name: String,
    owner: String,
    created_at: u64,
    is_active: bool,
}

#[test]
fn stored_collection_missing_trailing_field_decodes_defaulted() {
    let prev = StoredCollectionPrev {
        tenant_id: 7,
        name: "orders".into(),
        owner: "alice".into(),
        created_at: 1_700_000_000,
        is_active: true,
    };
    let bytes = zerompk::to_msgpack_vec(&prev).expect("encode prev");

    let decoded: StoredCollection =
        zerompk::from_msgpack(&bytes).expect("decode into current StoredCollection");

    assert_eq!(decoded.tenant_id, 7);
    assert_eq!(decoded.name, "orders");
    assert_eq!(decoded.owner, "alice");
    assert!(decoded.is_active);
    // Every currently-defaulted field must be at its Default value.
    assert_eq!(decoded.descriptor_version, 0);
    assert_eq!(decoded.modification_hlc, Hlc::ZERO);
    assert!(decoded.fields.is_empty());
    assert!(decoded.field_defs.is_empty());
    assert!(decoded.event_defs.is_empty());
    assert!(decoded.timeseries_config.is_none());
    assert!(!decoded.append_only);
    assert!(!decoded.hash_chain);
    assert!(decoded.indexes.is_empty());
}

/// One-field-ago shape: mirrors the real struct but drops the newest
/// field (`indexes`). Matches the exact scenario in the bug report —
/// prior binary wrote N fields, current binary added the N+1-th.
#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct StoredCollectionOneFieldAgo {
    tenant_id: u32,
    name: String,
    owner: String,
    created_at: u64,
    is_active: bool,
    append_only: bool,
}

#[test]
fn stored_collection_one_field_ago_decodes() {
    let prev = StoredCollectionOneFieldAgo {
        tenant_id: 1,
        name: "t".into(),
        owner: "alice".into(),
        created_at: 1_700_000_000,
        is_active: true,
        append_only: false,
    };
    let bytes = zerompk::to_msgpack_vec(&prev).expect("encode");
    let decoded: StoredCollection = zerompk::from_msgpack(&bytes)
        .expect("decode old StoredCollection into new after additive field");
    assert!(decoded.indexes.is_empty());
}

/// Field reordering: a future binary writes the same fields in a
/// different order. Map-encoded catalog types must decode regardless
/// of key order.
#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct StoredCollectionReordered {
    is_active: bool,
    owner: String,
    name: String,
    created_at: u64,
    tenant_id: u32,
}

#[test]
fn stored_collection_reordered_fields_decode() {
    let prev = StoredCollectionReordered {
        is_active: true,
        owner: "alice".into(),
        name: "t".into(),
        created_at: 1,
        tenant_id: 42,
    };
    let bytes = zerompk::to_msgpack_vec(&prev).expect("encode");
    let decoded: StoredCollection = zerompk::from_msgpack(&bytes).expect("decode reordered keys");
    assert_eq!(decoded.tenant_id, 42);
    assert_eq!(decoded.name, "t");
}

/// A future binary adds a brand new field the current binary does not
/// know about. Unknown keys must be skipped, not rejected.
#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct StoredCollectionWithUnknownFutureField {
    tenant_id: u32,
    name: String,
    owner: String,
    created_at: u64,
    is_active: bool,
    /// Invented field a future version may add.
    future_tiering_policy: String,
}

#[test]
fn stored_collection_unknown_future_field_is_ignored() {
    let prev = StoredCollectionWithUnknownFutureField {
        tenant_id: 1,
        name: "t".into(),
        owner: "alice".into(),
        created_at: 1,
        is_active: true,
        future_tiering_policy: "hot-warm-cold".into(),
    };
    let bytes = zerompk::to_msgpack_vec(&prev).expect("encode");
    let decoded: StoredCollection =
        zerompk::from_msgpack(&bytes).expect("unknown future field must be ignored");
    assert_eq!(decoded.name, "t");
}

// ── Sibling catalog types: same invariant applies ─────────────────────────

#[derive(zerompk::ToMessagePack)]
struct CheckpointRecordPrev {
    tenant_id: u32,
    collection: String,
    doc_id: String,
    checkpoint_name: String,
    version_vector_json: String,
    created_by: String,
    created_at: u64,
}

#[test]
fn checkpoint_record_prev_shape_decodes() {
    // Today this is the full struct, but the spec must hold if a new
    // `#[serde(default)]` field is added tomorrow.
    let prev = CheckpointRecordPrev {
        tenant_id: 1,
        collection: "c".into(),
        doc_id: "d".into(),
        checkpoint_name: "cp".into(),
        version_vector_json: "{}".into(),
        created_by: "alice".into(),
        created_at: 0,
    };
    let bytes = zerompk::to_msgpack_vec(&prev).expect("encode");
    let _decoded: CheckpointRecord =
        zerompk::from_msgpack(&bytes).expect("decode CheckpointRecord");
}

#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct StoredTriggerPrev {
    tenant_id: u32,
    name: String,
    collection: String,
    timing: TriggerTiming,
    events: TriggerEvents,
    granularity: TriggerGranularity,
    body_sql: String,
    owner: String,
    created_at: u64,
}

#[test]
fn stored_trigger_missing_optional_fields_decodes() {
    let prev = StoredTriggerPrev {
        tenant_id: 1,
        name: "trg".into(),
        collection: "c".into(),
        timing: TriggerTiming::After,
        events: TriggerEvents {
            on_insert: true,
            on_update: false,
            on_delete: false,
        },
        granularity: TriggerGranularity::Row,
        body_sql: "SELECT 1".into(),
        owner: "alice".into(),
        created_at: 0,
    };
    let bytes = zerompk::to_msgpack_vec(&prev).expect("encode");
    let decoded: StoredTrigger =
        zerompk::from_msgpack(&bytes).expect("decode StoredTrigger with defaulted tail");
    assert!(decoded.enabled, "enabled defaults to true when missing");
    assert_eq!(decoded.descriptor_version, 0);
    assert_eq!(decoded.modification_hlc, Hlc::ZERO);
    assert!(decoded.when_condition.is_none());
}

#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct ChangeStreamDefPrev {
    tenant_id: u32,
    name: String,
    collection: String,
    op_filter: OpFilter,
    format: StreamFormat,
    retention: RetentionConfig,
    owner: String,
    created_at: u64,
}

#[test]
fn change_stream_def_missing_optional_fields_decodes() {
    let prev = ChangeStreamDefPrev {
        tenant_id: 1,
        name: "s".into(),
        collection: "*".into(),
        op_filter: OpFilter::default(),
        format: StreamFormat::default(),
        retention: RetentionConfig::default(),
        owner: "alice".into(),
        created_at: 0,
    };
    let bytes = zerompk::to_msgpack_vec(&prev).expect("encode");
    let _decoded: ChangeStreamDef =
        zerompk::from_msgpack(&bytes).expect("decode ChangeStreamDef with defaulted tail");
}

#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct ScheduleDefPrev {
    tenant_id: u32,
    name: String,
    cron_expr: String,
    body_sql: String,
    scope: ScheduleScope,
    missed_policy: MissedPolicy,
    allow_overlap: bool,
    enabled: bool,
    owner: String,
    created_at: u64,
}

#[test]
fn schedule_def_missing_target_collection_decodes() {
    let prev = ScheduleDefPrev {
        tenant_id: 1,
        name: "s".into(),
        cron_expr: "*/5 * * * *".into(),
        body_sql: "SELECT 1".into(),
        scope: ScheduleScope::Normal,
        missed_policy: MissedPolicy::Skip,
        allow_overlap: true,
        enabled: true,
        owner: "alice".into(),
        created_at: 0,
    };
    let bytes = zerompk::to_msgpack_vec(&prev).expect("encode");
    let decoded: ScheduleDef =
        zerompk::from_msgpack(&bytes).expect("decode ScheduleDef missing target_collection");
    assert!(decoded.target_collection.is_none());
}

#[derive(zerompk::ToMessagePack)]
struct RetentionPolicyDefPrev {
    tenant_id: u32,
    name: String,
    collection: String,
    tiers: Vec<TierDef>,
    auto_tier: bool,
    enabled: bool,
    eval_interval_ms: u64,
    owner: String,
    created_at: u64,
}

#[test]
fn retention_policy_def_prev_shape_decodes() {
    // RetentionPolicyDef currently has no trailing-default fields —
    // this test guards the invariant: the moment one is added, the
    // test still passes without a migration.
    let prev = RetentionPolicyDefPrev {
        tenant_id: 1,
        name: "p".into(),
        collection: "metrics".into(),
        tiers: Vec::new(),
        auto_tier: true,
        enabled: true,
        eval_interval_ms: 3_600_000,
        owner: "alice".into(),
        created_at: 0,
    };
    let bytes = zerompk::to_msgpack_vec(&prev).expect("encode");
    let _decoded: RetentionPolicyDef = zerompk::from_msgpack(&bytes)
        .expect("decode RetentionPolicyDef prev shape (once map-encoded)");
}

#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct StoredProcedurePrev {
    tenant_id: u32,
    name: String,
    parameters: Vec<ProcedureParam>,
    body_sql: String,
    owner: String,
    created_at: u64,
}

#[test]
fn stored_procedure_missing_trailing_defaults_decodes() {
    let prev = StoredProcedurePrev {
        tenant_id: 1,
        name: "p".into(),
        parameters: Vec::new(),
        body_sql: "BEGIN END".into(),
        owner: "alice".into(),
        created_at: 0,
    };
    let bytes = zerompk::to_msgpack_vec(&prev).expect("encode");
    let decoded: StoredProcedure =
        zerompk::from_msgpack(&bytes).expect("decode StoredProcedure with defaulted tail");
    assert_eq!(decoded.max_iterations, 1_000_000);
    assert_eq!(decoded.timeout_secs, 60);
    assert_eq!(decoded.descriptor_version, 0);
    assert_eq!(decoded.modification_hlc, Hlc::ZERO);
}
