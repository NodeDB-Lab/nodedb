// SPDX-License-Identifier: BUSL-1.1

//! Startup replay of a large WAL recovers every row.

use std::time::Duration;

use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::types::*;
use nodedb::wal::manager::{NO_APPLY_KEY, WalManager};
use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};

use super::stack::ilp_payload;

/// Simulates server restart: WAL has many batches, startup replay must
/// recover all rows even if total data exceeds memtable capacity (64MB).
/// Replay must flush memtable when full, not silently drop excess rows.
#[test]
fn startup_replay_recovers_all_wal_data() {
    use nodedb_bridge::buffer::RingBuffer;

    let dir = tempfile::tempdir().unwrap();

    // Create WAL with 20 batches × 50K rows = 1M rows (~80MB+).
    let wal_path = dir.path().join("wal");
    std::fs::create_dir_all(&wal_path).unwrap();
    let wal = WalManager::open_for_testing(&wal_path).unwrap();
    let collection = "replay_test";
    let rows_per_batch = 4_000;
    let num_batches = 250;
    let total_rows = rows_per_batch * num_batches;

    for batch in 0..num_batches {
        let start_ts =
            1_700_000_000_000_000_000i64 + batch as i64 * rows_per_batch as i64 * 1_000_000;
        let payload = ilp_payload(collection, rows_per_batch, start_ts);
        let wal_payload = zerompk::to_msgpack_vec(&(collection.to_string(), payload)).unwrap();
        wal.appender(NO_APPLY_KEY)
            .with_event_source(nodedb::event::EventSource::User)
            .append_timeseries_batch(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                &wal_payload,
            )
            .unwrap();
    }
    wal.sync().unwrap();

    // Create a fresh CoreLoop (simulating restart).
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let (mut req_tx, req_rx) = RingBuffer::channel(64);
    let (resp_tx, mut resp_rx) = RingBuffer::channel(64);
    let mut core = CoreLoop::open(
        0,
        req_rx,
        resp_tx,
        &data_dir,
        std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
        nodedb::data::executor::core_loop::test_governor(),
    )
    .unwrap();

    // Replay WAL — simulating startup recovery.
    let records = wal.replay_from(nodedb_types::Lsn::new(0)).unwrap();
    assert_eq!(
        records.len(),
        num_batches,
        "WAL should have {num_batches} batches"
    );
    core.replay_timeseries_wal(&records, 1, &nodedb_wal::TombstoneSet::new());

    // Query via direct scan: COUNT(*) must see ALL rows.
    let scan_plan = PhysicalPlan::Timeseries(TimeseriesOp::Scan {
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            collection,
        ),
        time_range: (0, i64::MAX),
        projection: Vec::new(),
        limit: usize::MAX,
        filters: Vec::new(),
        sort_keys: Vec::new(),
        bucket_interval_ms: 0,
        group_by: Vec::new(),
        aggregates: vec![("count".into(), "*".into())],
        gap_fill: String::new(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        computed_columns: Vec::new(),
    });

    use nodedb::bridge::dispatch::BridgeRequest;
    use nodedb::bridge::envelope::{Priority, Request};

    req_tx
        .try_push(BridgeRequest::unfloored(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: nodedb::types::DatabaseId::DEFAULT,
            plan: scan_plan,
            deadline: std::time::Instant::now() + Duration::from_secs(10),
            priority: Priority::Normal,
            trace_id: nodedb_types::TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: nodedb::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            commit_hlc: None,
            entry_version: None,
            admission: nodedb::bridge::envelope::Admission::Exempt(
                nodedb::bridge::envelope::ExemptReason::Read,
            ),
        }))
        .unwrap();
    core.tick();
    let resp = resp_rx.try_pop().unwrap();
    let json_str =
        nodedb::data::executor::response_codec::decode_payload_to_json(&resp.inner.payload);
    let result: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap_or_default();
    let count = result
        .first()
        .and_then(|r| r["count(*)"].as_u64())
        .unwrap_or(0);

    assert_eq!(
        count, total_rows as u64,
        "startup replay must recover all {total_rows} rows, got {count}"
    );
}
