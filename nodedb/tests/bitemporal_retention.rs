//! Bitemporal retention: backfill preservation + version-lossless compaction.
//!
//! Retention on bitemporal timeseries keys off `max_system_ts` (when the
//! write happened) rather than `max_ts` (event time). A late-arriving
//! backfill with old event-time but current system-time must survive a
//! short event-time-based retention window.
//!
//! Plain columnar bitemporal preserves every version of a PK: consecutive
//! inserts for the same `id` append rather than tombstone the prior row.

use nodedb_types::timeseries::{
    PartitionInterval, PartitionMeta, PartitionState, TieredPartitionConfig,
};

use nodedb::engine::timeseries::partition_registry::PartitionRegistry;

fn test_config(retention_ms: u64) -> TieredPartitionConfig {
    let mut cfg = TieredPartitionConfig::origin_defaults();
    cfg.partition_by = PartitionInterval::Duration(86_400_000); // 1d
    cfg.merge_after_ms = 7 * 86_400_000;
    cfg.merge_count = 3;
    cfg.retention_period_ms = retention_ms;
    cfg
}

fn sealed_partition_meta(min_ts: i64, max_ts: i64, max_system_ts: i64) -> PartitionMeta {
    PartitionMeta {
        min_ts,
        max_ts,
        row_count: 10,
        size_bytes: 1024,
        schema_version: 1,
        state: PartitionState::Sealed,
        interval_ms: 86_400_000,
        last_flushed_wal_lsn: 0,
        column_stats: Default::default(),
        max_system_ts,
    }
}

/// A backdated partition (old `max_ts`, recent `max_system_ts`) is
/// expired under event-time retention but survives under bitemporal
/// (system-time) retention.
#[test]
fn bitemporal_retention_preserves_backfill() {
    let now_ms = 40 * 86_400_000i64;
    let backfill_event_ts = 5 * 86_400_000i64; // 35 days old (event time)
    let backfill_system_ts = now_ms - 60_000; // 1 minute ago (system time)
    let retention_ms: i64 = 30 * 86_400_000; // 30-day retention window

    let mut reg = PartitionRegistry::new(test_config(retention_ms as u64));

    // Seed a partition directly — simulates a flushed backfill batch.
    let start = backfill_event_ts;
    reg.get_or_create_partition(start);
    let meta = sealed_partition_meta(backfill_event_ts, backfill_event_ts, backfill_system_ts);
    reg.update_meta(start, meta);

    // Non-bitemporal axis: partition is older than retention → expired.
    let expired_event = reg.find_expired(now_ms, false);
    assert_eq!(
        expired_event,
        vec![start],
        "event-time retention should drop the backdated partition"
    );

    // Bitemporal axis: partition was *written* recently → not expired.
    let expired_system = reg.find_expired(now_ms, true);
    assert!(
        expired_system.is_empty(),
        "bitemporal retention must preserve backfilled partition; got {expired_system:?}"
    );
}

/// When `max_system_ts` is 0 (non-bitemporal partition or pre-upgrade
/// manifest), the bitemporal axis falls back to `max_ts` so existing
/// non-bitemporal retention continues to work.
#[test]
fn bitemporal_fallback_when_system_ts_zero() {
    let now_ms = 40 * 86_400_000i64;
    let retention_ms = 30 * 86_400_000i64;

    let mut reg = PartitionRegistry::new(test_config(retention_ms as u64));
    let start = 5 * 86_400_000i64;
    reg.get_or_create_partition(start);
    let meta = sealed_partition_meta(start, start, 0); // max_system_ts = 0
    reg.update_meta(start, meta);

    // Both axes should agree because max_system_ts = 0 triggers fallback.
    assert_eq!(reg.find_expired(now_ms, false), vec![start]);
    assert_eq!(reg.find_expired(now_ms, true), vec![start]);
}

/// Plain columnar bitemporal: two inserts with the same user PK but
/// different `_ts_system` stamps produce two memtable rows with no
/// delete bitmap entry — compaction sees both versions.
#[test]
fn bitemporal_columnar_insert_preserves_versions() {
    use nodedb_columnar::MutationEngine;
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    // Schema matches the Data Plane's `prepend_bitemporal_columns` shape:
    // three reserved bitemporal columns ahead of the user-provided ones.
    let schema = ColumnarSchema::new(vec![
        ColumnDef::required("_ts_system", ColumnType::Int64),
        ColumnDef::required("_ts_valid_from", ColumnType::Int64),
        ColumnDef::required("_ts_valid_until", ColumnType::Int64),
        ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
        ColumnDef::nullable("value", ColumnType::Float64),
    ])
    .unwrap();
    assert!(
        schema.is_bitemporal(),
        "schema should self-report bitemporal"
    );

    let mut engine = MutationEngine::new("bi_collection".into(), schema);

    // First version of user pk=42.
    engine
        .insert(&[
            Value::Integer(1_000),
            Value::Integer(i64::MIN),
            Value::Integer(i64::MAX),
            Value::Integer(42),
            Value::Float(1.0),
        ])
        .expect("insert v1");

    // Second version of the same pk — later system time, different value.
    let r2 = engine
        .insert(&[
            Value::Integer(2_000),
            Value::Integer(i64::MIN),
            Value::Integer(i64::MAX),
            Value::Integer(42),
            Value::Float(2.0),
        ])
        .expect("insert v2");

    // Non-bitemporal would emit a DeleteRows record for the prior version;
    // bitemporal should emit ONLY the InsertRow.
    assert_eq!(
        r2.wal_records.len(),
        1,
        "bitemporal insert must not tombstone prior version"
    );
    assert!(matches!(
        &r2.wal_records[0],
        nodedb_columnar::wal_record::ColumnarWalRecord::InsertRow { .. }
    ));

    // Both rows are live in the memtable — no delete bitmap entries.
    assert_eq!(engine.memtable().row_count(), 2);
    assert!(
        engine
            .delete_bitmap(0)
            .is_none_or(|bm| bm.deleted_count() == 0),
        "bitemporal insert must not create tombstones"
    );

    // PK index rebinds to latest version so current-state reads find v2.
    let pk = nodedb_columnar::pk_index::encode_pk(&Value::Integer(42));
    let loc = engine.pk_index().get(&pk).expect("pk indexed");
    // Second insert lands at memtable row index 1.
    assert_eq!(loc.row_index, 1);
}
