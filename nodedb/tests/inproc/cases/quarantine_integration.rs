// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the corrupt-segment quarantine subsystem.
//!
//! Tests cover the registry's two-strike logic, re-detection after a restart,
//! per-engine open wrappers, and the HTTP/metrics snapshot surfaces.

use std::sync::Arc;

use nodedb::storage::quarantine::{
    CollectionScope, QuarantineCountKey, QuarantineEngine, QuarantineError, QuarantineRegistry,
    SegmentKey,
    engines::{ColumnarOrQuarantine, FtsOrQuarantine, FtsSegmentRef, VectorOrQuarantine},
};

fn columnar_key(collection: &str, segment_id: u64) -> SegmentKey {
    SegmentKey::Columnar {
        scope: CollectionScope::new(1, 1, collection),
        segment_id,
    }
}

// ── Registry: two-strike logic ───────────────────────────────────────────────

#[test]
fn quarantine_registry_first_strike_returns_ok() {
    let reg = QuarantineRegistry::new();
    assert!(reg.record_failure(columnar_key("c", 1), "crc").is_ok());
}

#[test]
fn quarantine_registry_second_strike_returns_quarantined() {
    let reg = QuarantineRegistry::new();
    let k = columnar_key("c", 2);
    reg.record_failure(k.clone(), "crc").unwrap();
    let err = reg.record_failure(k, "crc").unwrap_err();
    assert!(
        matches!(err, QuarantineError::SegmentQuarantined { .. }),
        "{err}"
    );
}

#[test]
fn quarantine_registry_success_resets_strikes() {
    let reg = QuarantineRegistry::new();
    let k = SegmentKey::Fts {
        scope: CollectionScope::new(1, 1, "c"),
        field: None,
        segment_id: "3".into(),
    };
    reg.record_failure(k.clone(), "crc").unwrap();
    reg.record_success(&k);
    // First failure after reset is strike 1 again — not quarantined.
    assert!(reg.record_failure(k, "crc").is_ok());
}

#[test]
fn quarantine_registry_quarantined_ignores_success_call() {
    let reg = QuarantineRegistry::new();
    let k = SegmentKey::Vector {
        scope: CollectionScope::new(1, 1, "c"),
        segment_id: "4".into(),
    };
    reg.record_failure(k.clone(), "crc").unwrap();
    reg.record_failure(k.clone(), "crc").unwrap_err();
    // success() must NOT lift the quarantine.
    reg.record_success(&k);
    let err = reg.record_failure(k, "crc").unwrap_err();
    assert!(
        matches!(err, QuarantineError::SegmentQuarantined { .. }),
        "{err}"
    );
}

#[test]
fn quarantine_registry_concurrent_second_strike_both_see_quarantined() {
    let reg = Arc::new(QuarantineRegistry::new());
    let k = SegmentKey::Raft {
        group_id: 3,
        last_included_index: 5,
    };
    // Pre-seed one strike so both threads compete for the second.
    reg.record_failure(k.clone(), "first").unwrap();

    let r1 = Arc::clone(&reg);
    let k1 = k.clone();
    let r2 = Arc::clone(&reg);
    let k2 = k.clone();
    let t1 = std::thread::spawn(move || r1.record_failure(k1, "second"));
    let t2 = std::thread::spawn(move || r2.record_failure(k2, "second"));

    assert!(t1.join().unwrap().is_err());
    assert!(t2.join().unwrap().is_err());
    // Exactly one entry in the registry.
    assert_eq!(reg.quarantined_snapshot().len(), 1);
}

// ── Restart: quarantine is re-derived from the corrupt bytes ─────────────────

/// Quarantine lives for one process run. A restart starts with an empty
/// registry. The corrupt bytes are unchanged, so two reads quarantine the
/// segment again under the same key.
#[test]
fn quarantine_restart_rederives_quarantine_from_corrupt_bytes() {
    use nodedb::storage::quarantine::engines::open_segment_with_quarantine;

    let scope = CollectionScope::new(1, 1, "coll");
    let before = Arc::new(QuarantineRegistry::new());
    open_segment_with_quarantine(&before, &[], &scope, 1).ok();
    assert!(matches!(
        open_segment_with_quarantine(&before, &[], &scope, 1),
        Err(ColumnarOrQuarantine::Quarantined(_))
    ));

    let after = Arc::new(QuarantineRegistry::new());
    assert!(after.quarantined_snapshot().is_empty());
    assert!(matches!(
        open_segment_with_quarantine(&after, &[], &scope, 1),
        Err(ColumnarOrQuarantine::Columnar(_))
    ));
    assert!(matches!(
        open_segment_with_quarantine(&after, &[], &scope, 1),
        Err(ColumnarOrQuarantine::Quarantined(_))
    ));
    assert_eq!(
        after.quarantined_snapshot()[0].key,
        before.quarantined_snapshot()[0].key
    );
}

// ── Columnar engine: corrupt CRC → quarantine ────────────────────────────────

#[test]
fn quarantine_columnar_corrupt_segment_two_strike_quarantine() {
    use nodedb::storage::quarantine::engines::open_segment_with_quarantine;

    // Empty bytes → TruncatedSegment (a CRC-class error), easiest way to
    // trigger the quarantine path without building a full columnar segment.
    let reg = Arc::new(QuarantineRegistry::new());
    let scope = CollectionScope::new(1, 1, "coll");

    assert!(
        matches!(
            open_segment_with_quarantine(&reg, &[], &scope, 1),
            Err(ColumnarOrQuarantine::Columnar(_))
        ),
        "first strike must return ColumnarError"
    );

    assert!(
        matches!(
            open_segment_with_quarantine(&reg, &[], &scope, 1),
            Err(ColumnarOrQuarantine::Quarantined(_))
        ),
        "second strike must return SegmentQuarantined"
    );

    let snap = reg.quarantined_snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].key, columnar_key("coll", 1));
}

#[test]
fn quarantine_columnar_other_segments_still_readable_after_one_quarantine() {
    use nodedb::storage::quarantine::engines::open_segment_with_quarantine;

    let reg = Arc::new(QuarantineRegistry::new());
    let scope = CollectionScope::new(1, 1, "coll");

    // Quarantine segment 1.
    assert!(matches!(
        open_segment_with_quarantine(&reg, &[], &scope, 1),
        Err(ColumnarOrQuarantine::Columnar(_))
    ));
    assert!(matches!(
        open_segment_with_quarantine(&reg, &[], &scope, 1),
        Err(ColumnarOrQuarantine::Quarantined(_))
    ));

    // Segment 2 with the same bad bytes is still at first strike.
    assert!(
        matches!(
            open_segment_with_quarantine(&reg, &[], &scope, 2),
            Err(ColumnarOrQuarantine::Columnar(_))
        ),
        "segment 2 must be first-strike ColumnarError, not quarantined"
    );
}

// ── FTS engine: corrupt CRC → quarantine ─────────────────────────────────────

#[test]
fn quarantine_fts_corrupt_segment_two_strike_quarantine() {
    use nodedb::storage::quarantine::engines::validate_fts_segment_bytes;

    let reg = Arc::new(QuarantineRegistry::new());
    let scope = CollectionScope::new(1, 1, "ftscoll");
    let segment = FtsSegmentRef {
        scope: &scope,
        field: None,
        segment_id: "s1",
    };

    // Empty → Truncated.
    assert!(matches!(
        validate_fts_segment_bytes(&reg, vec![], &segment),
        Err(FtsOrQuarantine::Segment(_))
    ));

    assert!(
        matches!(
            validate_fts_segment_bytes(&reg, vec![], &segment),
            Err(FtsOrQuarantine::Quarantined(_))
        ),
        "second strike must return Quarantined"
    );

    assert_eq!(reg.quarantined_snapshot().len(), 1);
}

// ── Vector engine: corrupt CRC on file → quarantine, file untouched ─────────

#[test]
fn quarantine_vector_corrupt_crc_quarantines_and_keeps_file() {
    use nodedb::storage::quarantine::engines::{
        VectorQuarantineOpen, open_vector_segment_with_quarantine,
    };
    use nodedb_mem::EngineId;
    use nodedb_vector::error::VectorError;
    use nodedb_vector::mmap_segment::{MmapVectorSegment, VectorSegmentDropPolicy};

    let tmp = tempfile::tempdir().unwrap();
    let good_path = tmp.path().join("good.seg");
    let corrupt_path = tmp.path().join("corrupt.seg");
    let memory = super::support::memory::test_scoped_memory(EngineId::Vector);
    let scope = CollectionScope::new(1, 1, "vecdb");

    let v = vec![1.0f32, 2.0, 3.0];
    MmapVectorSegment::create(tmp.path(), "good.seg", 3, &[&v], &memory).unwrap();

    // Corrupt the CRC in the footer (FOOTER_SIZE=46, CRC at bytes [34..38] from footer start).
    let mut bytes = std::fs::read(&good_path).unwrap();
    let footer_start = bytes.len() - 46;
    bytes[footer_start + 34] ^= 0xFF;
    std::fs::write(&corrupt_path, &bytes).unwrap();

    let reg = Arc::new(QuarantineRegistry::new());
    let policy = VectorSegmentDropPolicy::keep_resident();
    let open = |path: &std::path::Path, segment_id: &str| {
        open_vector_segment_with_quarantine(VectorQuarantineOpen {
            registry: &reg,
            path,
            policy,
            scope: &scope,
            segment_id,
            memory: &memory,
        })
    };

    assert!(
        matches!(
            open(&corrupt_path, "s1"),
            Err(VectorOrQuarantine::Vector(VectorError::SegmentIo(_)))
        ),
        "first strike must be a SegmentIo VectorError"
    );
    assert!(
        matches!(
            open(&corrupt_path, "s1"),
            Err(VectorOrQuarantine::Quarantined(_))
        ),
        "second strike must be Quarantined"
    );

    // The segment file keeps its name.
    assert!(
        corrupt_path.exists(),
        "the corrupt file must not be renamed"
    );

    assert!(
        open(&good_path, "s2").is_ok(),
        "good segment must remain readable"
    );
}

// ── HTTP snapshot surfaces ────────────────────────────────────────────────────

#[test]
fn quarantine_http_snapshot_empty_before_any_quarantine() {
    let reg = QuarantineRegistry::new();
    assert!(reg.quarantined_snapshot().is_empty());
}

#[test]
fn quarantine_http_snapshot_returns_entry_after_second_strike() {
    let reg = QuarantineRegistry::new();
    let k = columnar_key("httpcoll", 99);
    reg.record_failure(k.clone(), "crc").unwrap();
    reg.record_failure(k.clone(), "crc").unwrap_err();

    let snap = reg.quarantined_snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].key, k);
    assert_eq!(snap[0].key.engine(), QuarantineEngine::Columnar);
    assert_eq!(snap[0].key.segment_label(), "99");
    assert_eq!(snap[0].strikes, 2);
}

// ── Prometheus metrics surface ────────────────────────────────────────────────

#[test]
fn quarantine_metrics_active_counts_reflect_state() {
    let reg = QuarantineRegistry::new();
    let scope = CollectionScope::new(4, 9, "mc");

    let k1 = SegmentKey::Columnar {
        scope: scope.clone(),
        segment_id: 1,
    };
    let k2 = SegmentKey::Fts {
        scope: scope.clone(),
        field: Some("body".into()),
        segment_id: "s1".into(),
    };
    // Quarantine both.
    reg.record_failure(k1.clone(), "crc").unwrap();
    reg.record_failure(k1, "crc").unwrap_err();
    reg.record_failure(k2.clone(), "crc").unwrap();
    reg.record_failure(k2, "crc").unwrap_err();

    let active = reg.active_counts();
    let count = |engine| {
        *active
            .get(&QuarantineCountKey {
                engine,
                scope: Some(scope.clone()),
            })
            .unwrap_or(&0)
    };
    assert_eq!(count(QuarantineEngine::Columnar), 1);
    assert_eq!(count(QuarantineEngine::Fts), 1);

    let mut out = String::new();
    nodedb::control::metrics::SystemMetrics::prometheus_segment_quarantine_active(
        &mut out, &active,
    );
    assert!(out.contains("nodedb_segments_quarantined_active"), "{out}");
    assert!(
        out.contains(r#"engine="columnar",database_id="4",tenant_id="9",collection="mc""#),
        "{out}"
    );
    assert!(out.contains("nodedb_segments_quarantined_total"), "{out}");
}
