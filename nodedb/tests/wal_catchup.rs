//! Integration tests for WAL catch-up: verifies that timeseries records
//! written to WAL but not dispatched to the Data Plane become queryable
//! after the catch-up task re-dispatches them.

use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::bridge::envelope::PhysicalPlan;
use nodedb::bridge::physical_plan::TimeseriesOp;
use nodedb::control::state::SharedState;
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::types::*;
use nodedb::wal::manager::WalManager;

fn ilp_payload(collection: &str, count: usize, start_ts_ns: i64) -> Vec<u8> {
    let mut lines = String::new();
    let qtypes = ["A", "AAAA", "MX", "CNAME"];
    for i in 0..count {
        let ts_ns = start_ts_ns + i as i64 * 1_000_000;
        let qtype = qtypes[i % qtypes.len()];
        lines.push_str(&format!(
            "{collection},qtype={qtype} elapsed_ms={}.0 {ts_ns}\n",
            i % 1000
        ));
    }
    lines.into_bytes()
}

/// Spin up a full Control+Data Plane test stack.
/// Data Plane runs on a dedicated OS thread (matches production).
/// Response poller runs as a tokio task.
struct TestStack {
    shared: Arc<SharedState>,
    wal: Arc<WalManager>,
    _dir: tempfile::TempDir,
}

impl TestStack {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal");
        std::fs::create_dir_all(&wal_path).unwrap();
        let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());

        let (dispatcher, data_sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, Arc::clone(&wal));

        let data_side = data_sides.into_iter().next().unwrap();
        let core_dir = dir.path().join("data");
        std::fs::create_dir_all(&core_dir).unwrap();

        // Data Plane: dedicated OS thread with tick loop.
        std::thread::spawn(move || {
            let mut core = CoreLoop::open(
                0,
                data_side.request_rx,
                data_side.response_tx,
                &core_dir,
                std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            )
            .unwrap();
            loop {
                core.tick();
                std::thread::sleep(Duration::from_millis(1));
            }
        });

        // Response poller: routes Data Plane responses to waiting sessions.
        let shared_poller = Arc::clone(&shared);
        tokio::spawn(async move {
            loop {
                shared_poller.poll_and_route_responses();
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        Self {
            shared,
            wal,
            _dir: dir,
        }
    }

    async fn dispatch(&self, plan: PhysicalPlan, collection: &str) -> serde_json::Value {
        let resp = nodedb::control::server::dispatch_utils::dispatch_to_data_plane(
            &self.shared,
            TenantId::new(1),
            VShardId::from_collection(collection),
            plan,
            0,
        )
        .await
        .expect("dispatch failed");
        let json_str =
            nodedb::data::executor::response_codec::decode_payload_to_json(&resp.payload);
        serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null)
    }

    async fn query_count(&self, collection: &str) -> u64 {
        let resp = self
            .dispatch(
                PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                    collection: collection.to_string(),
                    time_range: (0, i64::MAX),
                    projection: Vec::new(),
                    limit: usize::MAX,
                    filters: Vec::new(),
                    bucket_interval_ms: 0,
                    group_by: Vec::new(),
                    aggregates: vec![("count".into(), "*".into())],
                    gap_fill: String::new(),
                    rls_filters: Vec::new(),
                    system_as_of_ms: None,
                    valid_at_ms: None,
                    computed_columns: Vec::new(),
                }),
                collection,
            )
            .await;
        resp.as_array()
            .and_then(|a| a.first())
            .and_then(|r| r["count(*)"].as_u64())
            .unwrap_or(0)
    }

    fn write_to_wal(&self, collection: &str, payload: Vec<u8>) {
        let wal_payload = zerompk::to_msgpack_vec(&(collection.to_string(), payload)).unwrap();
        self.wal
            .append_timeseries_batch(
                TenantId::new(1),
                VShardId::from_collection(collection),
                &wal_payload,
            )
            .unwrap();
        self.wal.sync().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// WAL records re-dispatched to the Data Plane become queryable.
///
/// Simulates: ILP batch WAL-acknowledged → SPSC dispatch failed →
/// catch-up reads WAL → re-dispatches → query sees the data.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_redispatch_makes_data_queryable() {
    let stack = TestStack::new();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let collection = "wal_test";

    // Write 500 rows directly to WAL (bypassing Data Plane dispatch).
    stack.write_to_wal(
        collection,
        ilp_payload(collection, 500, 1_700_000_000_000_000_000),
    );

    // Read WAL records and manually re-dispatch (simulating catch-up logic).
    let records = stack.wal.replay_from(nodedb_types::Lsn::new(0)).unwrap();
    assert_eq!(records.len(), 1, "WAL should have 1 timeseries batch");

    for record in &records {
        let (coll, payload): (String, Vec<u8>) =
            zerompk::from_msgpack::<(String, Vec<u8>)>(&record.payload).unwrap();
        let resp = stack
            .dispatch(
                PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                    collection: coll,
                    payload,
                    format: "ilp".to_string(),
                    wal_lsn: Some(record.header.lsn),
                    surrogates: Vec::new(),
                }),
                collection,
            )
            .await;
        assert_eq!(resp["accepted"].as_u64().unwrap(), 500);
    }

    // Query: all 500 rows should be visible.
    let count = stack.query_count(collection).await;
    assert_eq!(count, 500, "all WAL-redispatched rows should be queryable");
}

/// The catch-up background task automatically re-dispatches WAL records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catchup_task_dispatches_wal_records() {
    let stack = TestStack::new();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let collection = "catchup_auto";

    // Write 300 rows to WAL (not dispatched to Data Plane).
    stack.write_to_wal(
        collection,
        ilp_payload(collection, 300, 1_700_000_000_000_000_000),
    );

    // Start the catch-up task from LSN 0.
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    nodedb::control::wal_catchup::spawn_wal_catchup_task(
        Arc::clone(&stack.shared),
        nodedb_types::Lsn::new(0),
        shutdown_rx,
    );

    // Wait for at least one catch-up cycle (first fires at 500ms).
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Query: should see all 300 rows.
    let count = stack.query_count(collection).await;
    assert_eq!(
        count, 300,
        "catch-up task should automatically make WAL rows queryable"
    );
}

/// Simulates the real production failure:
/// - 5 ILP batches written to WAL (LSN 1-5)
/// - Only batches 1, 3, 5 dispatched to Data Plane (SPSC dropped 2, 4)
/// - Catch-up task runs and replays WAL from LSN 0
/// - All 5 batches must become visible (catch-up fills the gaps)
///
/// This is the exact scenario that caused 58%→24% visibility regression.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catchup_fills_gaps_from_spsc_drops() {
    let stack = TestStack::new();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let collection = "gap_test";
    let rows_per_batch = 100;

    // Write 5 batches to WAL (simulating ILP listener WAL append).
    for batch in 0..5 {
        let start_ts = 1_700_000_000_000_000_000i64 + batch * rows_per_batch as i64 * 1_000_000;
        stack.write_to_wal(
            collection,
            ilp_payload(collection, rows_per_batch, start_ts),
        );
    }

    // Simulate SPSC partial delivery: only dispatch batches 0, 2, 4 to Data Plane.
    // (Batches 1, 3 were "dropped" by SPSC — only in WAL.)
    let records = stack.wal.replay_from(nodedb_types::Lsn::new(0)).unwrap();
    assert_eq!(records.len(), 5, "WAL should have 5 batches");

    for (i, record) in records.iter().enumerate() {
        if i == 1 || i == 3 {
            continue; // simulate SPSC drop
        }
        let (coll, payload): (String, Vec<u8>) =
            zerompk::from_msgpack::<(String, Vec<u8>)>(&record.payload).unwrap();
        stack
            .dispatch(
                PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                    collection: coll,
                    payload,
                    format: "ilp".to_string(),
                    wal_lsn: Some(record.header.lsn),
                    surrogates: Vec::new(),
                }),
                collection,
            )
            .await;
    }

    // Verify: only 300 rows visible (3 dispatched batches × 100).
    let count_before = stack.query_count(collection).await;
    assert_eq!(count_before, 300, "only 3/5 batches dispatched");

    // Start catch-up task — should replay ALL WAL records.
    // The dedup must NOT skip batches 1,3 just because batch 4 has a higher LSN.
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    nodedb::control::wal_catchup::spawn_wal_catchup_task(
        Arc::clone(&stack.shared),
        nodedb_types::Lsn::new(0),
        shutdown_rx,
    );

    // Wait for catch-up to process all records.
    tokio::time::sleep(Duration::from_millis(3000)).await;

    // All 500 rows must be visible (300 from live + 200 from catch-up).
    // Note: batches 0,2,4 are re-dispatched by catch-up too, creating
    // duplicates. Total visible will be 500 (from catch-up) + 300 (from
    // live) = 800 with duplicates, or 500 if catch-up dedup works.
    // The key assertion: at LEAST 500 rows must be visible.
    let count_after = stack.query_count(collection).await;
    assert!(
        count_after >= 500,
        "catch-up must make all 5 batches visible, got {count_after} (expected >= 500)"
    );
}

/// Regression test: catch-up must NOT start from wal.next_lsn() because
/// that's PAST all existing records. It must start from LSN 0 so it can
/// replay records that were WAL'd but never dispatched to the Data Plane.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catchup_from_next_lsn_misses_all_records() {
    let stack = TestStack::new();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let collection = "lsn_bug";

    // Write 500 rows to WAL (not dispatched to Data Plane).
    stack.write_to_wal(
        collection,
        ilp_payload(collection, 500, 1_700_000_000_000_000_000),
    );

    // BAD: Start catch-up from next_lsn (past all records).
    // This is what main.rs USED to do — catch-up finds 0 records.
    let next_lsn = stack.wal.next_lsn();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    nodedb::control::wal_catchup::spawn_wal_catchup_task(
        Arc::clone(&stack.shared),
        next_lsn,
        shutdown_rx,
    );

    tokio::time::sleep(Duration::from_millis(3000)).await;

    let count_bad = stack.query_count(collection).await;
    // This SHOULD be 0 — catch-up started past all records.
    assert_eq!(
        count_bad, 0,
        "catch-up from next_lsn should find nothing (confirming the bug)"
    );
}

/// Same scenario but with the fix: catch-up starts from LSN 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catchup_from_lsn_zero_recovers_all_records() {
    let stack = TestStack::new();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let collection = "lsn_fix";

    // Write 500 rows to WAL (not dispatched to Data Plane).
    stack.write_to_wal(
        collection,
        ilp_payload(collection, 500, 1_700_000_000_000_000_000),
    );

    // GOOD: Start catch-up from LSN 0.
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    nodedb::control::wal_catchup::spawn_wal_catchup_task(
        Arc::clone(&stack.shared),
        nodedb_types::Lsn::new(0),
        shutdown_rx,
    );

    tokio::time::sleep(Duration::from_millis(3000)).await;

    let count_good = stack.query_count(collection).await;
    assert_eq!(
        count_good, 500,
        "catch-up from LSN 0 must recover all WAL records"
    );
}

/// Simulates the EXACT production scenario:
/// 1. Catch-up task starts from wal.next_lsn() (like main.rs)
/// 2. WAL records are written DURING the catch-up task's lifetime
/// 3. Some dispatches succeed, some are written to WAL only (simulating drops)
/// 4. After all writes complete, catch-up drains remaining WAL records
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_scenario_catchup_drains_wal_after_ingest() {
    let stack = TestStack::new();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let collection = "prod_test";

    // Start catch-up task from current WAL tip (empty WAL → LSN 1).
    let initial_lsn = stack.wal.next_lsn();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    nodedb::control::wal_catchup::spawn_wal_catchup_task(
        Arc::clone(&stack.shared),
        initial_lsn,
        shutdown_rx,
    );

    // Simulate ILP ingest: write 10 batches to WAL.
    // Dispatch only 6 of them (simulate 4 SPSC drops).
    let rows_per_batch = 200;
    for batch in 0..10 {
        let start_ts = 1_700_000_000_000_000_000i64 + batch * rows_per_batch as i64 * 1_000_000;
        let payload = ilp_payload(collection, rows_per_batch, start_ts);

        // Always write to WAL (like ILP listener does).
        stack.write_to_wal(collection, payload.clone());

        // Only dispatch some batches directly (simulate SPSC success).
        if batch % 5 < 3 {
            // 6 out of 10 batches dispatched directly.
            let (coll_str, raw_payload): (String, Vec<u8>) = {
                let wal_payload =
                    zerompk::to_msgpack_vec(&(collection.to_string(), payload)).unwrap();
                zerompk::from_msgpack::<(String, Vec<u8>)>(&wal_payload).unwrap()
            };
            stack
                .dispatch(
                    PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                        collection: coll_str,
                        payload: raw_payload,
                        format: "ilp".to_string(),
                        wal_lsn: None,
                        surrogates: Vec::new(),
                    }),
                    collection,
                )
                .await;
        }
    }

    // Immediately after ingest: should see at least 6*200 = 1200 rows.
    let count_during = stack.query_count(collection).await;
    assert!(
        count_during >= 1200,
        "should see at least 1200 rows from 6 dispatched batches, got {count_during}"
    );

    // Wait for catch-up to drain the remaining 4 WAL batches.
    // Catch-up runs every 100-2000ms, should complete within a few seconds.
    tokio::time::sleep(Duration::from_millis(5000)).await;

    let count_after = stack.query_count(collection).await;
    // All 10 batches should be visible (2000 rows), possibly with some
    // duplicates from catch-up re-dispatching the 6 already-dispatched ones.
    assert!(
        count_after >= 2000,
        "catch-up must drain all WAL batches, got {count_after} (expected >= 2000)"
    );
}

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
        wal.append_timeseries_batch(TenantId::new(1), VShardId::new(0), &wal_payload)
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
        collection: collection.to_string(),
        time_range: (0, i64::MAX),
        projection: Vec::new(),
        limit: usize::MAX,
        filters: Vec::new(),
        bucket_interval_ms: 0,
        group_by: Vec::new(),
        aggregates: vec![("count".into(), "*".into())],
        gap_fill: String::new(),
        rls_filters: Vec::new(),
        system_as_of_ms: None,
        valid_at_ms: None,
        computed_columns: Vec::new(),
    });

    use nodedb::bridge::dispatch::BridgeRequest;
    use nodedb::bridge::envelope::{Priority, Request};

    req_tx
        .try_push(BridgeRequest {
            inner: Request {
                request_id: RequestId::new(1),
                tenant_id: TenantId::new(1),
                vshard_id: VShardId::new(0),
                plan: scan_plan,
                deadline: std::time::Instant::now() + Duration::from_secs(10),
                priority: Priority::Normal,
                trace_id: 0,
                consistency: ReadConsistency::Strong,
                idempotency_key: None,
                event_source: nodedb::event::EventSource::User,
                user_roles: Vec::new(),
            },
        })
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
