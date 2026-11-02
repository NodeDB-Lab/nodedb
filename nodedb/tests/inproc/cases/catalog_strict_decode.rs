// SPDX-License-Identifier: BUSL-1.1

//! Catalog rows decode only in their exact current shape.
//!
//! Each test encodes a shadow of a real catalog type that lacks a field or
//! carries an extra one, then checks that the real type refuses it.

use nodedb::control::security::catalog::procedure_types::{ProcedureParam, StoredProcedure};
use nodedb::control::security::catalog::trigger_types::{
    StoredTrigger, TriggerEvents, TriggerGranularity, TriggerTiming,
};
use nodedb::control::security::catalog::types::StoredCollection;
use nodedb::engine::timeseries::retention_policy::types::{RetentionPolicyDef, TierDef};
use nodedb::event::cdc::stream_def::{ChangeStreamDef, OpFilter, RetentionConfig, StreamFormat};
use nodedb::event::scheduler::types::{MissedPolicy, ScheduleDef, ScheduleScope};

/// A `StoredCollection` row that carries only the identity fields.
#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct StoredCollectionMissingFields {
    tenant_id: u64,
    name: String,
    owner: String,
    created_at: u64,
    is_active: bool,
}

#[test]
fn stored_collection_missing_fields_is_refused() {
    let row = StoredCollectionMissingFields {
        tenant_id: 7,
        name: "orders".into(),
        owner: "alice".into(),
        created_at: 1_700_000_000,
        is_active: true,
    };
    let bytes = zerompk::to_msgpack_vec(&row).expect("encode");
    assert!(zerompk::from_msgpack::<StoredCollection>(&bytes).is_err());
}

/// A full `StoredCollection` row plus one key the type does not define.
#[test]
fn stored_collection_unknown_field_is_refused() {
    let collection = StoredCollection::new(1, "t", "alice");
    let bytes = zerompk::to_msgpack_vec(&collection).expect("encode");
    let value: zerompk::Value<'_> = zerompk::from_msgpack(&bytes).expect("decode as value");
    let zerompk::Value::Map(mut entries) = value else {
        panic!("StoredCollection must encode as a map");
    };
    entries.push((
        zerompk::Value::String("unknown_field".into()),
        zerompk::Value::String("x".into()),
    ));
    let extended = zerompk::to_msgpack_vec(&zerompk::Value::Map(entries)).expect("re-encode");
    assert!(zerompk::from_msgpack::<StoredCollection>(&extended).is_err());

    // The unmodified row decodes.
    let decoded: StoredCollection = zerompk::from_msgpack(&bytes).expect("decode current row");
    assert_eq!(decoded.name, "t");
}

#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct StoredTriggerMissingFields {
    tenant_id: u64,
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
fn stored_trigger_missing_fields_is_refused() {
    let row = StoredTriggerMissingFields {
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
    let bytes = zerompk::to_msgpack_vec(&row).expect("encode");
    assert!(zerompk::from_msgpack::<StoredTrigger>(&bytes).is_err());
}

#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct ChangeStreamDefMissingFields {
    tenant_id: u64,
    name: String,
    collection: String,
    op_filter: OpFilter,
    format: StreamFormat,
    retention: RetentionConfig,
    owner: String,
    created_at: u64,
}

#[test]
fn change_stream_def_missing_fields_is_refused() {
    let row = ChangeStreamDefMissingFields {
        tenant_id: 1,
        name: "s".into(),
        collection: "*".into(),
        op_filter: OpFilter::default(),
        format: StreamFormat::default(),
        retention: RetentionConfig::default(),
        owner: "alice".into(),
        created_at: 0,
    };
    let bytes = zerompk::to_msgpack_vec(&row).expect("encode");
    assert!(zerompk::from_msgpack::<ChangeStreamDef>(&bytes).is_err());
}

#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct ScheduleDefMissingFields {
    tenant_id: u64,
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
fn schedule_def_missing_fields_is_refused() {
    let row = ScheduleDefMissingFields {
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
    let bytes = zerompk::to_msgpack_vec(&row).expect("encode");
    assert!(zerompk::from_msgpack::<ScheduleDef>(&bytes).is_err());
}

#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct RetentionPolicyDefMissingDatabase {
    tenant_id: u64,
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
fn retention_policy_def_missing_database_is_refused() {
    let row = RetentionPolicyDefMissingDatabase {
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
    let bytes = zerompk::to_msgpack_vec(&row).expect("encode");
    assert!(zerompk::from_msgpack::<RetentionPolicyDef>(&bytes).is_err());
}

#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct StoredProcedureMissingFields {
    tenant_id: u64,
    name: String,
    parameters: Vec<ProcedureParam>,
    body_sql: String,
    owner: String,
    created_at: u64,
}

#[test]
fn stored_procedure_missing_fields_is_refused() {
    let row = StoredProcedureMissingFields {
        tenant_id: 1,
        name: "p".into(),
        parameters: Vec::new(),
        body_sql: "BEGIN END".into(),
        owner: "alice".into(),
        created_at: 0,
    };
    let bytes = zerompk::to_msgpack_vec(&row).expect("encode");
    assert!(zerompk::from_msgpack::<StoredProcedure>(&bytes).is_err());
}
