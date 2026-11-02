// SPDX-License-Identifier: BUSL-1.1

//! Corruption in any of (columnar, FTS, Raft snapshot) segment files must
//! surface as `SegmentQuarantined` and appear in the debug enumeration endpoint.
//!
//! Each test writes a valid segment using the real segment writer, corrupts one
//! byte to flip the footer CRC, then drives the quarantine wrapper through two
//! open attempts. The first returns a plain engine error (first strike). The
//! second returns `SegmentQuarantined` (second strike).
//!
//! The final test checks the HTTP endpoint's JSON response shape. It fills a
//! registry with three entries and runs the same serialisation path as the
//! `/v1/cluster/debug/quarantined-segments` handler.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb::storage::quarantine::{
    CollectionScope, QuarantineRegistry, SegmentKey,
    engines::{
        ColumnarOrQuarantine, FtsOrQuarantine, FtsSegmentRef, RaftOrQuarantine,
        decode_snapshot_chunk_with_quarantine, open_segment_with_quarantine,
        validate_fts_segment_bytes,
    },
};
use nodedb_columnar::{ColumnarMemtable, SegmentWriter, writer::PROFILE_PLAIN};
use nodedb_mem::EngineId;
use nodedb_raft::snapshot_framing::{SnapshotEngineId, encode_snapshot_chunk};
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Build a single-column schema with one Int64 column.
fn minimal_columnar_schema() -> ColumnarSchema {
    ColumnarSchema::new(vec![ColumnDef::required("id", ColumnType::Int64)]).expect("valid schema")
}

/// Produce a valid sealed columnar segment with one row.
fn write_columnar_segment() -> Vec<u8> {
    let schema = minimal_columnar_schema();
    let mut mem = ColumnarMemtable::new(&schema);
    mem.append_row(&[Value::Integer(42)]).expect("append row");

    SegmentWriter::new(
        PROFILE_PLAIN,
        super::support::memory::test_scoped_memory(EngineId::Columnar),
    )
    .write_segment(&schema, mem.columns(), mem.row_count(), None)
    .expect("write segment")
}

/// Produce a valid FTS segment with one term.
fn write_fts_segment() -> Vec<u8> {
    use nodedb_fts::CompactPosting;
    use nodedb_types::Surrogate;

    let mut postings: HashMap<String, Vec<CompactPosting>> = HashMap::new();
    postings.insert(
        "hello".to_string(),
        vec![CompactPosting {
            doc_id: Surrogate(1),
            term_freq: 1,
            fieldnorm: 128,
            positions: vec![0],
        }],
    );
    nodedb_fts::lsm::segment::writer::flush_to_segment(postings).expect("flush fts segment")
}

/// Produce a valid framed Raft snapshot chunk.
fn write_raft_chunk() -> Vec<u8> {
    encode_snapshot_chunk(SnapshotEngineId::Columnar, b"snapshot-payload")
}

/// Corrupt the last byte of a segment buffer (flips the footer CRC).
fn corrupt_last_byte(mut bytes: Vec<u8>) -> Vec<u8> {
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    bytes
}

// ── columnar corruption ───────────────────────────────────────────────────────

#[test]
fn columnar_real_segment_corrupt_crc_quarantines_on_second_strike() {
    let reg = Arc::new(QuarantineRegistry::new());
    let corrupt = corrupt_last_byte(write_columnar_segment());
    let scope = CollectionScope::new(1, 7, "coll");

    // First open: CRC-class error → first strike → ColumnarError returned.
    let r1 = open_segment_with_quarantine(&reg, &corrupt, &scope, 1);
    assert!(
        matches!(r1, Err(ColumnarOrQuarantine::Columnar(_))),
        "first strike must return a ColumnarError"
    );

    // Second open: second strike → SegmentQuarantined.
    let r2 = open_segment_with_quarantine(&reg, &corrupt, &scope, 1);
    assert!(
        matches!(r2, Err(ColumnarOrQuarantine::Quarantined(_))),
        "second strike must return SegmentQuarantined"
    );

    let snap = reg.quarantined_snapshot();
    assert_eq!(snap.len(), 1, "exactly one segment must be quarantined");
    assert_eq!(
        snap[0].key,
        SegmentKey::Columnar {
            scope: scope.clone(),
            segment_id: 1,
        }
    );
    assert_eq!(snap[0].strikes, 2);
}

#[test]
fn columnar_good_segment_readable_after_quarantine_of_different_segment() {
    let reg = Arc::new(QuarantineRegistry::new());
    let good = write_columnar_segment();
    let corrupt = corrupt_last_byte(write_columnar_segment());
    let scope = CollectionScope::new(1, 7, "coll");

    // Quarantine the corrupt segment.
    open_segment_with_quarantine(&reg, &corrupt, &scope, 1).ok();
    open_segment_with_quarantine(&reg, &corrupt, &scope, 1).ok();

    // The good segment must still open successfully.
    let result = open_segment_with_quarantine(&reg, &good, &scope, 2);
    assert!(result.is_ok(), "good segment must remain readable");
}

/// Two tenants each own a collection named `orders` with flushed segment 1.
/// Tenant A's corrupt segment 1 is quarantined. Tenant B's segment 1 is a
/// different segment and must keep its own record.
#[test]
fn columnar_same_collection_and_segment_in_two_tenants_are_separate_records() {
    let reg = Arc::new(QuarantineRegistry::new());
    let corrupt = corrupt_last_byte(write_columnar_segment());
    let good = write_columnar_segment();
    let tenant_a = CollectionScope::new(1, 10, "orders");
    let tenant_b = CollectionScope::new(1, 11, "orders");

    open_segment_with_quarantine(&reg, &corrupt, &tenant_a, 1).ok();
    let quarantined = open_segment_with_quarantine(&reg, &corrupt, &tenant_a, 1);
    assert!(matches!(
        quarantined,
        Err(ColumnarOrQuarantine::Quarantined(_))
    ));

    // Tenant B's healthy segment 1 opens.
    assert!(open_segment_with_quarantine(&reg, &good, &tenant_b, 1).is_ok());

    // A corrupt read of tenant B's segment 1 is B's first strike, not A's
    // quarantine.
    let first_b = open_segment_with_quarantine(&reg, &corrupt, &tenant_b, 1);
    assert!(
        matches!(first_b, Err(ColumnarOrQuarantine::Columnar(_))),
        "tenant B's first failure must be its own first strike, got: {:?}",
        first_b.as_ref().err()
    );

    let snap = reg.quarantined_snapshot();
    assert_eq!(snap.len(), 1, "only tenant A's segment is quarantined");
    assert_eq!(snap[0].key.scope(), Some(&tenant_a));

    // A different database with the same tenant id, collection, and segment
    // id is also a separate record.
    let other_db = CollectionScope::new(2, 10, "orders");
    assert!(!reg.is_quarantined(&SegmentKey::Columnar {
        scope: other_db,
        segment_id: 1,
    }));
}

// ── FTS corruption ────────────────────────────────────────────────────────────

#[test]
fn fts_real_segment_corrupt_crc_quarantines_on_second_strike() {
    let reg = Arc::new(QuarantineRegistry::new());
    let corrupt = corrupt_last_byte(write_fts_segment());
    let scope = CollectionScope::new(1, 7, "ftscoll");
    let segment = FtsSegmentRef {
        scope: &scope,
        field: Some("body"),
        segment_id: "seg-fts-1",
    };

    // First open: CRC-class error → first strike → SegmentError returned.
    let r1 = validate_fts_segment_bytes(&reg, corrupt.clone(), &segment);
    assert!(
        matches!(r1, Err(FtsOrQuarantine::Segment(_))),
        "first strike must return SegmentError, got: {r1:?}"
    );

    // Second open: second strike → SegmentQuarantined.
    let r2 = validate_fts_segment_bytes(&reg, corrupt, &segment);
    assert!(
        matches!(r2, Err(FtsOrQuarantine::Quarantined(_))),
        "second strike must return SegmentQuarantined, got: {r2:?}"
    );

    let snap = reg.quarantined_snapshot();
    assert_eq!(snap.len(), 1, "exactly one segment must be quarantined");
    assert_eq!(
        snap[0].key,
        SegmentKey::Fts {
            scope: scope.clone(),
            field: Some("body".to_string()),
            segment_id: "seg-fts-1".to_string(),
        }
    );
    assert_eq!(snap[0].strikes, 2);
}

// ── Raft snapshot corruption ──────────────────────────────────────────────────

#[test]
fn raft_snapshot_corrupt_crc_quarantines_on_second_strike() {
    let reg = Arc::new(QuarantineRegistry::new());

    // Flip a bit in the CRC field (bytes 8–11 of the frame header).
    let mut corrupt = write_raft_chunk();
    corrupt[8] ^= 0xFF;

    // First decode: CRC mismatch → first strike → SnapshotFramingError returned.
    let r1 = decode_snapshot_chunk_with_quarantine(&reg, &corrupt, 1, 99);
    assert!(
        matches!(r1, Err(RaftOrQuarantine::Framing(_))),
        "first strike must return a SnapshotFramingError, got: {r1:?}"
    );

    // Second decode: second strike → SegmentQuarantined.
    let r2 = decode_snapshot_chunk_with_quarantine(&reg, &corrupt, 1, 99);
    assert!(
        matches!(r2, Err(RaftOrQuarantine::Quarantined(_))),
        "second strike must return SegmentQuarantined, got: {r2:?}"
    );

    let snap = reg.quarantined_snapshot();
    assert_eq!(snap.len(), 1, "exactly one snapshot must be quarantined");
    assert_eq!(
        snap[0].key,
        SegmentKey::Raft {
            group_id: 1,
            last_included_index: 99,
        }
    );
    assert_eq!(snap[0].key.segment_label(), "group=1:index=99");
    assert_eq!(snap[0].strikes, 2);
}

// ── HTTP endpoint response shape ──────────────────────────────────────────────

/// Mirror of the private response shape used by the
/// `/v1/cluster/debug/quarantined-segments` handler.
///
/// Matches the field names of `QuarantinedSegmentsResponse` in
/// `routes/cluster_debug/quarantined_segments.rs`. The test checks the
/// serialised JSON shape without a live HTTP server.
#[derive(serde::Serialize, serde::Deserialize)]
struct QuarantinedSegmentEntry {
    engine: String,
    database_id: Option<u64>,
    tenant_id: Option<u64>,
    collection: Option<String>,
    field: Option<String>,
    segment_id: String,
    quarantined_at_unix_ms: u64,
    last_error_summary: String,
    strikes: u32,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct QuarantinedSegmentsResponse {
    segments: Vec<QuarantinedSegmentEntry>,
}

/// Serialise a quarantine registry snapshot with the same `sonic_rs` path as
/// `ok_json` in `cluster_debug/guard.rs`. Parse it back and check that all
/// three engine entries are present with the expected shape.
#[tokio::test]
async fn http_endpoint_response_lists_all_three_quarantined_engines() {
    let reg = QuarantineRegistry::new();

    // Seed one quarantined entry per engine by driving two strikes each.
    for k in [
        SegmentKey::Columnar {
            scope: CollectionScope::new(1, 3, "coll-a"),
            segment_id: 4,
        },
        SegmentKey::Fts {
            scope: CollectionScope::new(1, 3, "coll-b"),
            field: None,
            segment_id: "seg-b".to_string(),
        },
        SegmentKey::Raft {
            group_id: 2,
            last_included_index: 1,
        },
    ] {
        reg.record_failure(k.clone(), "crc")
            .expect("first strike must be Ok");
        reg.record_failure(k, "crc")
            .expect_err("second strike must be Err");
    }

    assert_eq!(
        reg.quarantined_snapshot().len(),
        3,
        "all three engines must be quarantined before testing the response shape"
    );

    // Replicate the handler's response-building logic (sans auth gate).
    let mut entries: Vec<QuarantinedSegmentEntry> = reg
        .quarantined_snapshot()
        .into_iter()
        .map(|s| {
            let scope = s.key.scope();
            QuarantinedSegmentEntry {
                engine: s.key.engine().as_str().to_string(),
                database_id: scope.map(|sc| sc.database_id),
                tenant_id: scope.map(|sc| sc.tenant_id),
                collection: scope.map(|sc| sc.collection.clone()),
                field: s.key.field().map(str::to_string),
                segment_id: s.key.segment_label(),
                quarantined_at_unix_ms: s.quarantined_at_unix_ms,
                last_error_summary: s.last_error_summary,
                strikes: s.strikes,
            }
        })
        .collect();
    entries.sort_by(|a, b| a.engine.cmp(&b.engine));

    let response_body = sonic_rs::to_string(&QuarantinedSegmentsResponse { segments: entries })
        .expect("serialization must succeed");
    let parsed: QuarantinedSegmentsResponse =
        sonic_rs::from_str(&response_body).expect("response body must be valid JSON");

    assert_eq!(
        parsed.segments.len(),
        3,
        "response must contain exactly three quarantined segments"
    );
    let engines: Vec<&str> = parsed.segments.iter().map(|e| e.engine.as_str()).collect();
    assert_eq!(
        engines,
        vec!["columnar", "fts", "raft"],
        "engines must appear in sorted order"
    );

    for entry in &parsed.segments {
        assert_eq!(entry.strikes, 2, "every entry must have exactly 2 strikes");
        assert!(
            entry.quarantined_at_unix_ms > 0,
            "quarantine timestamp must be non-zero"
        );
        assert!(
            !entry.last_error_summary.is_empty(),
            "last_error_summary must be non-empty"
        );
    }

    let col = &parsed.segments[0];
    assert_eq!(col.database_id, Some(1));
    assert_eq!(col.tenant_id, Some(3));
    assert_eq!(col.collection.as_deref(), Some("coll-a"));
    assert_eq!(col.field, None);
    assert_eq!(col.segment_id, "4");

    let fts = &parsed.segments[1];
    assert_eq!(fts.collection.as_deref(), Some("coll-b"));
    assert_eq!(fts.segment_id, "seg-b");

    let raft = &parsed.segments[2];
    assert_eq!(raft.database_id, None);
    assert_eq!(raft.tenant_id, None);
    assert_eq!(raft.collection, None);
    assert_eq!(raft.segment_id, "group=2:index=1");
}
