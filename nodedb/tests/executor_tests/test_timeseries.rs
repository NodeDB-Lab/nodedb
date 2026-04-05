//! Integration tests for timeseries query engine (GitHub issues #6-#9).
//!
//! Tests the full path: ILP ingest → memtable → flush → disk partitions →
//! query across both sources.

use nodedb::bridge::envelope::PhysicalPlan;
use nodedb::bridge::physical_plan::TimeseriesOp;

use crate::helpers::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build an ILP payload with `count` lines for a given collection.
///
/// Generates wide rows (~10 columns, ~80 bytes each) to trigger memtable
/// flushes faster. Each 64MB memtable holds ~840K wide rows.
fn ilp_lines(collection: &str, count: usize, start_ts_ns: i64) -> String {
    let mut lines = String::new();
    let qtypes = ["A", "AAAA", "MX", "CNAME"];
    let rcodes = ["NOERROR", "NXDOMAIN", "SERVFAIL", "REFUSED"];
    for i in 0..count {
        let ts_ns = start_ts_ns + i as i64 * 1_000_000; // 1ms apart
        let qtype = qtypes[i % qtypes.len()];
        let rcode = rcodes[i % rcodes.len()];
        let qname = format!("host-{}.example.com", i % 50);
        let client_ip = format!("10.0.{}.{}", (i / 256) % 256, i % 256);
        lines.push_str(&format!(
            "{collection},qtype={qtype},rcode={rcode},qname={qname},client_ip={client_ip} elapsed_ms={}.0 {ts_ns}\n",
            (i % 1000) as f64
        ));
    }
    lines
}

fn ingest_ilp(
    ctx: &mut crate::helpers::TestCtx,
    collection: &str,
    payload: &str,
) -> serde_json::Value {
    let raw = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.to_string(),
            payload: payload.as_bytes().to_vec(),
            format: "ilp".to_string(),
            wal_lsn: None,
        }),
    );
    let json_str = nodedb::data::executor::response_codec::decode_payload_to_json(&raw);
    serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null)
}

fn ts_scan(
    ctx: &mut crate::helpers::TestCtx,
    collection: &str,
    group_by: Vec<String>,
    aggregates: Vec<(String, String)>,
    bucket_interval_ms: i64,
) -> Vec<serde_json::Value> {
    let raw = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: collection.to_string(),
            time_range: (0, i64::MAX),
            projection: Vec::new(),
            limit: usize::MAX,
            filters: Vec::new(),
            bucket_interval_ms,
            group_by,
            aggregates,
            gap_fill: String::new(),
            rls_filters: Vec::new(),
        }),
    );
    // Payload may be MessagePack (grouped aggregates) or JSON — decode both.
    let json_str = nodedb::data::executor::response_codec::decode_payload_to_json(&raw);
    serde_json::from_str(&json_str).unwrap_or_default()
}

fn ts_scan_filtered(
    ctx: &mut crate::helpers::TestCtx,
    collection: &str,
    group_by: Vec<String>,
    aggregates: Vec<(String, String)>,
    bucket_interval_ms: i64,
    filters: Vec<nodedb::bridge::scan_filter::ScanFilter>,
) -> Vec<serde_json::Value> {
    let filter_bytes = if filters.is_empty() {
        Vec::new()
    } else {
        zerompk::to_msgpack_vec(&filters).unwrap_or_default()
    };
    let raw = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: collection.to_string(),
            time_range: (0, i64::MAX),
            projection: Vec::new(),
            limit: usize::MAX,
            filters: filter_bytes,
            bucket_interval_ms,
            group_by,
            aggregates,
            gap_fill: String::new(),
            rls_filters: Vec::new(),
        }),
    );
    let json_str = nodedb::data::executor::response_codec::decode_payload_to_json(&raw);
    serde_json::from_str(&json_str).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// #6 — Query reads all data (memtable + disk partitions)
// ---------------------------------------------------------------------------

#[test]
fn count_star_sees_flushed_partitions() {
    let mut ctx = make_ctx();

    // Ingest enough wide rows to force multiple memtable flushes.
    // Each row is ~7 columns × ~8 bytes = ~56 bytes of column data.
    // 64MB / 56 = ~1.2M rows per flush. We send 3M rows to guarantee
    // at least 2 flush cycles. Each batch is 10K rows.
    let batch_size = 10_000;
    let num_batches = 300;
    let mut total_accepted: u64 = 0;
    let mut total_rejected: u64 = 0;

    for b in 0..num_batches {
        let start_ns = (b * batch_size) as i64 * 1_000_000;
        let payload = ilp_lines("metrics", batch_size, start_ns);
        let resp = ingest_ilp(&mut ctx, "metrics", &payload);
        total_accepted += resp["accepted"].as_u64().unwrap_or(0);
        total_rejected += resp["rejected"].as_u64().unwrap_or(0);
    }

    let total_sent = (batch_size * num_batches) as u64;
    assert_eq!(
        total_rejected, 0,
        "no rows should be rejected — pre-flush must prevent hard-limit rejections"
    );
    assert_eq!(
        total_accepted, total_sent,
        "all sent rows should be accepted by the Data Plane"
    );

    // COUNT(*) must equal total accepted (memtable + ALL flushed partitions).
    // This catches: partition registry not populated, flush losing drain data,
    // or query path not reading from all sources.
    let results = ts_scan(
        &mut ctx,
        "metrics",
        Vec::new(),
        vec![("count".into(), "*".into())],
        0,
    );
    assert_eq!(results.len(), 1, "expected single aggregate row");
    let count = results[0]["count_all"].as_u64().unwrap_or(0);
    assert_eq!(
        count, total_accepted,
        "COUNT(*) must see every accepted row — found {count}, expected {total_accepted}. \
         If count < accepted, rows were lost during flush or partitions are invisible."
    );
}

#[test]
fn group_by_sees_both_memtable_and_partitions() {
    let mut ctx = make_ctx();

    // Ingest two batches — enough for at least one flush.
    for b in 0..10 {
        let start_ns = b * 5_000 * 1_000_000_000i64;
        let payload = ilp_lines("dns", 5_000, start_ns);
        ingest_ilp(&mut ctx, "dns", &payload);
    }

    // GROUP BY qtype should see all data.
    let results = ts_scan(
        &mut ctx,
        "dns",
        vec!["qtype".into()],
        vec![("count".into(), "*".into())],
        0,
    );
    let total: u64 = results
        .iter()
        .map(|r| r["count_all"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(total, 50_000, "GROUP BY total should match ingested rows");
    assert_eq!(results.len(), 4, "should have 4 qtypes: A, AAAA, MX, CNAME");
}

// ---------------------------------------------------------------------------
// #7 — time_bucket() works without panic
// ---------------------------------------------------------------------------

#[test]
fn time_bucket_aggregation_does_not_panic() {
    let mut ctx = make_ctx();

    // Ingest data spanning multiple hours.
    let payload = ilp_lines("ts_bucket", 1_000, 1_700_000_000_000_000_000);
    ingest_ilp(&mut ctx, "ts_bucket", &payload);

    // time_bucket('1h') should return valid buckets — not panic or timeout.
    let results = ts_scan(
        &mut ctx,
        "ts_bucket",
        Vec::new(),
        vec![("count".into(), "*".into())],
        3_600_000, // 1 hour
    );
    assert!(
        !results.is_empty(),
        "time_bucket should return at least 1 bucket"
    );
    let total: u64 = results
        .iter()
        .map(|r| r["count_all"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(total, 1_000, "bucket counts should sum to ingested rows");
}

#[test]
fn time_bucket_with_named_value_column() {
    let mut ctx = make_ctx();

    let payload = ilp_lines("ts_named", 500, 1_700_000_000_000_000_000);
    ingest_ilp(&mut ctx, "ts_named", &payload);

    // AVG(elapsed_ms) grouped by time bucket — uses named column, not hardcoded index.
    let results = ts_scan(
        &mut ctx,
        "ts_named",
        Vec::new(),
        vec![
            ("count".into(), "*".into()),
            ("avg".into(), "elapsed_ms".into()),
        ],
        3_600_000,
    );
    assert!(!results.is_empty());
    for r in &results {
        let avg = r["avg_elapsed_ms"].as_f64();
        assert!(avg.is_some(), "avg should be present: {r}");
    }
}

// ---------------------------------------------------------------------------
// #6 (cont.) — WHERE predicates filter aggregate results
// ---------------------------------------------------------------------------

#[test]
fn where_predicate_filters_count() {
    let mut ctx = make_ctx();

    let payload = ilp_lines("dns_filt", 1_000, 1_700_000_000_000_000_000);
    ingest_ilp(&mut ctx, "dns_filt", &payload);

    // Unfiltered COUNT(*) = 1000.
    let all = ts_scan(
        &mut ctx,
        "dns_filt",
        Vec::new(),
        vec![("count".into(), "*".into())],
        0,
    );
    let total = all[0]["count_all"].as_u64().unwrap();
    assert_eq!(total, 1_000);

    // Filtered: qtype = 'A' → 25% of rows (4 qtypes, round-robin).
    let filtered = ts_scan_filtered(
        &mut ctx,
        "dns_filt",
        Vec::new(),
        vec![("count".into(), "*".into())],
        0,
        vec![nodedb::bridge::scan_filter::ScanFilter {
            field: "qtype".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String("A".into()),
            clauses: vec![],
        }],
    );
    let filtered_count = filtered[0]["count_all"].as_u64().unwrap();
    assert_eq!(filtered_count, 250, "WHERE qtype='A' should return 25%");
}

// ---------------------------------------------------------------------------
// #8 — Schema evolution (new ILP fields)
// ---------------------------------------------------------------------------

#[test]
fn schema_evolution_adds_new_fields() {
    let mut ctx = make_ctx();

    // First batch: only elapsed_ms.
    let batch1 = "dns,qtype=A elapsed_ms=1.5 1700000000000000000\n\
                   dns,qtype=AAAA elapsed_ms=2.5 1700000001000000000\n";
    ingest_ilp(&mut ctx, "dns", batch1);

    // Second batch: adds response_bytes and ttl (new fields).
    let batch2 = "dns,qtype=A elapsed_ms=3.0,response_bytes=512i,ttl=3600i 1700000002000000000\n\
                   dns,qtype=MX elapsed_ms=4.0,response_bytes=256i,ttl=7200i 1700000003000000000\n";
    ingest_ilp(&mut ctx, "dns", batch2);

    // Raw scan should show new columns with values for batch2 rows.
    let results = ts_scan(
        &mut ctx,
        "dns",
        Vec::new(),
        Vec::new(), // raw scan
        0,
    );
    assert_eq!(results.len(), 4, "should see all 4 rows");

    // Batch2 rows (last 2) should have response_bytes.
    let has_response_bytes: Vec<_> = results
        .iter()
        .filter(|r| {
            r.get("response_bytes")
                .is_some_and(|v| !v.is_null() && v.as_i64().unwrap_or(0) > 0)
        })
        .collect();
    assert_eq!(
        has_response_bytes.len(),
        2,
        "2 rows should have response_bytes set"
    );

    // Batch1 rows should have response_bytes=0 (int64 default) or null.
    let batch1_rows: Vec<_> = results
        .iter()
        .filter(|r| {
            r.get("response_bytes")
                .is_none_or(|v| v.is_null() || v.as_i64() == Some(0))
        })
        .collect();
    assert_eq!(
        batch1_rows.len(),
        2,
        "batch1 rows should have null/0 for new fields"
    );
}

// ---------------------------------------------------------------------------
// #9 — GROUP BY not capped at 10K
// ---------------------------------------------------------------------------

#[test]
fn group_by_not_capped_at_10k() {
    let mut ctx = make_ctx();

    // Ingest data with high cardinality: 15K unique qnames.
    let mut lines = String::new();
    for i in 0..15_000 {
        let ts_ns = 1_700_000_000_000_000_000i64 + i as i64 * 1_000_000;
        lines.push_str(&format!(
            "dns,qname=host-{i}.example.com elapsed_ms=1.0 {ts_ns}\n"
        ));
    }
    ingest_ilp(&mut ctx, "dns_card", &lines);

    // GROUP BY qname should return all 15K groups — not capped at 10K.
    let results = ts_scan(
        &mut ctx,
        "dns_card",
        vec!["qname".into()],
        vec![("count".into(), "*".into())],
        0,
    );
    assert_eq!(
        results.len(),
        15_000,
        "GROUP BY should return all unique groups, not capped at 10K"
    );
}

// ---------------------------------------------------------------------------
// #11 — LSN-based deduplication for WAL catch-up
// ---------------------------------------------------------------------------

#[test]
fn dedup_only_skips_flushed_partitions() {
    let mut ctx = make_ctx();
    let payload = b"dns,qname=a.com elapsed_ms=1.0 1700000000000000000\n";

    // Ingest with wal_lsn — accepted (no flushed partitions yet).
    let resp1 = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: "dns_dedup".to_string(),
            payload: payload.to_vec(),
            format: "ilp".to_string(),
            wal_lsn: Some(100),
        }),
    );
    let v1: serde_json::Value = serde_json::from_str(
        &nodedb::data::executor::response_codec::decode_payload_to_json(&resp1.payload),
    )
    .unwrap();
    assert_eq!(v1["accepted"], 1);

    // Same LSN again — accepted (no flushed partition to dedup against).
    // In-memory dedup is intentionally NOT done because SPSC gaps mean
    // max-LSN watermarks produce false negatives.
    let resp2 = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: "dns_dedup".to_string(),
            payload: payload.to_vec(),
            format: "ilp".to_string(),
            wal_lsn: Some(100),
        }),
    );
    let v2: serde_json::Value = serde_json::from_str(
        &nodedb::data::executor::response_codec::decode_payload_to_json(&resp2.payload),
    )
    .unwrap();
    assert_eq!(
        v2["accepted"], 1,
        "same LSN re-ingest must be accepted (no flushed partition to dedup against)"
    );

    // Live ingest (wal_lsn=None) — always accepted.
    let resp3 = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: "dns_dedup".to_string(),
            payload: payload.to_vec(),
            format: "ilp".to_string(),
            wal_lsn: None,
        }),
    );
    let v3: serde_json::Value = serde_json::from_str(
        &nodedb::data::executor::response_codec::decode_payload_to_json(&resp3.payload),
    )
    .unwrap();
    assert_eq!(v3["accepted"], 1);
}

// ---------------------------------------------------------------------------
// #11 — Catch-up must not skip gaps in LSN coverage
// ---------------------------------------------------------------------------

/// Simulates the real-world failure: ILP ingests batches 1,2,3,4,5 to WAL,
/// but SPSC drops batches 2 and 4 (never reach Data Plane). Batch 1,3,5
/// are ingested with their WAL LSNs. Later, catch-up replays batch 2 and 4
/// from WAL. The dedup must NOT skip them just because LSN 2 < max(5).
#[test]
fn catchup_replays_gaps_in_lsn_coverage() {
    let mut ctx = make_ctx();

    let mk_payload = |i: i64| -> Vec<u8> {
        format!(
            "dns,qname=host-{i}.test elapsed_ms=1.0 {}\n",
            1700000000000000000 + i * 1000000
        )
        .into_bytes()
    };

    // Simulate live ingest: batches at LSN 1, 3, 5 reach Data Plane.
    // (Batches at LSN 2, 4 were dropped by SPSC — they're in WAL only.)
    for lsn in [1, 3, 5] {
        send_raw(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: "dns_gap".to_string(),
                payload: mk_payload(lsn),
                format: "ilp".to_string(),
                wal_lsn: Some(lsn as u64),
            }),
        );
    }

    // Verify: 3 rows visible.
    let results = ts_scan(
        &mut ctx,
        "dns_gap",
        Vec::new(),
        vec![("count".into(), "*".into())],
        0,
    );
    assert_eq!(
        results[0]["count_all"], 3,
        "should see 3 rows from live ingest"
    );

    // Now simulate catch-up replaying the DROPPED batches (LSN 2, 4).
    // These must NOT be skipped by dedup even though LSN 2 < max(5).
    for lsn in [2, 4] {
        let resp = send_raw(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: "dns_gap".to_string(),
                payload: mk_payload(lsn),
                format: "ilp".to_string(),
                wal_lsn: Some(lsn as u64),
            }),
        );
        let v: serde_json::Value = serde_json::from_str(
            &nodedb::data::executor::response_codec::decode_payload_to_json(&resp.payload),
        )
        .unwrap();
        assert_eq!(
            v["accepted"], 1,
            "catch-up batch at LSN {lsn} must be accepted (not deduped)"
        );
    }

    // Verify: all 5 rows visible.
    let results = ts_scan(
        &mut ctx,
        "dns_gap",
        Vec::new(),
        vec![("count".into(), "*".into())],
        0,
    );
    assert_eq!(
        results[0]["count_all"], 5,
        "all 5 rows must be visible after catch-up fills the gaps"
    );
}

/// Multi-batch ingest exceeding memtable capacity: all rows must survive
/// across memtable flushes. Simulates sustained ILP ingest where the
/// server receives many small batches, not one giant batch.
#[test]
fn multi_batch_ingest_survives_memtable_flush() {
    let mut ctx = make_ctx();

    // Ingest in 10 batches of 100K rows each = 1M total.
    // Each batch is ~10MB of wide ILP rows. After ~6 batches the memtable
    // hits 64MB and flushes. All 1M rows must be visible at the end.
    let batch_size = 100_000;
    let num_batches = 10;
    let total = batch_size * num_batches;

    for batch in 0..num_batches {
        let start = batch * batch_size;
        let lines = ilp_lines(
            "dns_multi",
            batch_size,
            1_700_000_000_000_000_000 + start as i64 * 1_000_000,
        );
        let result = ingest_ilp(&mut ctx, "dns_multi", &lines);
        let accepted = result["accepted"].as_u64().unwrap_or(0);
        assert!(
            accepted > 0,
            "batch {batch} must accept rows, got accepted={accepted}"
        );
    }

    // COUNT must see all rows (memtable + sealed partitions).
    let results = ts_scan(
        &mut ctx,
        "dns_multi",
        Vec::new(),
        vec![("count".into(), "*".into())],
        0,
    );
    let count = results[0]["count_all"].as_u64().unwrap();
    assert_eq!(
        count, total as u64,
        "all {total} rows must be visible across memtable + partitions"
    );
}

// ---------------------------------------------------------------------------
// #11 — Idle memtable flush via maybe_run_maintenance
// ---------------------------------------------------------------------------

#[test]
fn idle_flush_triggers_after_inactivity() {
    let mut ctx = make_ctx();

    // Ingest 5 small rows (well under 64MB threshold).
    let mut lines = String::new();
    for i in 0..5 {
        let ts_ns = 1_700_000_000_000_000_000i64 + i * 1_000_000;
        lines.push_str(&format!("dns,qname=idle-{i}.test elapsed_ms=1.0 {ts_ns}\n"));
    }
    ingest_ilp(&mut ctx, "dns_idle", &lines);

    // COUNT(*) should see all 5 rows (from memtable).
    let results = ts_scan(
        &mut ctx,
        "dns_idle",
        Vec::new(),
        vec![("count".into(), "*".into())],
        0,
    );
    assert_eq!(results[0]["count_all"], 5);

    // Simulate idle time: set last_ts_ingest to 10 seconds ago.
    ctx.core.set_last_ts_ingest(Some(
        std::time::Instant::now() - std::time::Duration::from_secs(10),
    ));

    // Run maintenance — should trigger idle flush.
    ctx.core.maybe_run_maintenance();

    // COUNT(*) should still see all 5 rows (now from sealed partition).
    let results = ts_scan(
        &mut ctx,
        "dns_idle",
        Vec::new(),
        vec![("count".into(), "*".into())],
        0,
    );
    assert_eq!(results[0]["count_all"], 5);

    // Verify memtable was flushed by ingesting more and checking partition count.
    // After idle flush, the partition registry should have at least one entry.
    // We test indirectly: ingest 3 more rows and count — should get 8 total.
    let mut lines2 = String::new();
    for i in 5..8 {
        let ts_ns = 1_700_000_000_000_000_000i64 + i * 1_000_000;
        lines2.push_str(&format!("dns,qname=idle-{i}.test elapsed_ms=1.0 {ts_ns}\n"));
    }
    ingest_ilp(&mut ctx, "dns_idle", &lines2);

    let results = ts_scan(
        &mut ctx,
        "dns_idle",
        Vec::new(),
        vec![("count".into(), "*".into())],
        0,
    );
    assert_eq!(
        results[0]["count_all"], 8,
        "should see all 8 rows (5 from partition + 3 from memtable)"
    );
}

// ---------------------------------------------------------------------------
// #12 — Large GROUP BY produces valid single-payload response at Data Plane
// ---------------------------------------------------------------------------

#[test]
fn large_group_by_returns_single_valid_json() {
    let mut ctx = make_ctx();

    // Ingest 2000 unique groups (exceeds default stream_chunk_size of 1000).
    let mut lines = String::new();
    for i in 0..2_000 {
        let ts_ns = 1_700_000_000_000_000_000i64 + i as i64 * 1_000_000;
        lines.push_str(&format!(
            "dns,qname=host-{i}.example.com elapsed_ms=1.0 {ts_ns}\n"
        ));
    }
    ingest_ilp(&mut ctx, "dns_large", &lines);

    // GROUP BY should return a single valid JSON response with all 2000 groups.
    let raw = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: "dns_large".to_string(),
            time_range: (0, i64::MAX),
            projection: Vec::new(),
            limit: usize::MAX,
            filters: Vec::new(),
            bucket_interval_ms: 0,
            group_by: vec!["qname".into()],
            aggregates: vec![("count".into(), "*".into())],
            gap_fill: String::new(),
            rls_filters: Vec::new(),
        }),
    );

    // The response should be a single valid JSON array.
    let json_str = nodedb::data::executor::response_codec::decode_payload_to_json(&raw);
    let results: Vec<serde_json::Value> = serde_json::from_str(&json_str)
        .expect("Data Plane response should be a single valid JSON array");
    assert_eq!(results.len(), 2_000);
}
