// SPDX-License-Identifier: BUSL-1.1

// Note: bypasses WebSocket transport and live cluster; exercises
// coord-to-shard routing logic and op-log isolation directly.
//
// The shard routing here is a stand-in formula. Origin routes a cell by
// `array_vshard_for_tile` of its tile key. This test file:
//
//   1. Verifies that the stub shard-routing formula (tile_id → shard_id) is
//      deterministic, consistent, and partitions coords correctly.
//   2. Verifies that ops placed in one shard's InMemoryOpLog do not appear in
//      another shard's log — exercising the routing isolation property that
//      real routing maintains.
//   3. Exercises the AckVector + collapse_below interaction across two
//      independent shard logs, confirming that each shard GCs independently.

use nodedb_array::sync::ack::AckVector;
use nodedb_array::sync::gc::{MockSnapshotSink, collapse_below};
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::{ArrayOp, ArrayOpHeader, ArrayOpKind};
use nodedb_array::sync::op_log::{InMemoryOpLog, OpLog};
use nodedb_array::sync::snapshot::{CoordRange, TileSnapshot};
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_test_support::array_sync::{hlc, rep};

// ── Stub routing ──────────────────────────────────────────────────────────────

/// Minimal coord-to-shard mapping.
/// `shard = (tile_x % num_shards)` where `tile_x = coord_x / tile_extent`.
/// Origin routes by `nodedb_types::array_route::array_vshard_for_tile`.
fn shard_id(coord_x: i64, tile_extent: i64, num_shards: u32) -> u32 {
    let tile = coord_x / tile_extent;
    (tile.unsigned_abs() % num_shards as u64) as u32
}

fn put_op(array: &str, coord_x: i64, ms: u64) -> ArrayOp {
    ArrayOp {
        header: ArrayOpHeader {
            array: array.into(),
            hlc: hlc(ms, 1),
            schema_hlc: hlc(1, 1),
            valid_from_ms: 0,
            valid_until_ms: -1,
            system_from_ms: ms as i64,
        },
        kind: ArrayOpKind::Put,
        coord: vec![CoordValue::Int64(coord_x)],
        attrs: Some(vec![CellValue::Null]),
    }
}

fn dummy_snap(array: &str, frontier: Hlc) -> TileSnapshot {
    TileSnapshot {
        array: array.into(),
        coord_range: CoordRange {
            lo: vec![CoordValue::Int64(0)],
            hi: vec![CoordValue::Int64(100)],
        },
        tile_blob: vec![0xCC; 4],
        snapshot_hlc: frontier,
        schema_hlc: hlc(1, 1),
    }
}

// ── Routing property tests ────────────────────────────────────────────────────

#[test]
fn routing_is_deterministic() {
    // Same coord always maps to the same shard.
    assert_eq!(shard_id(5, 10, 4), shard_id(5, 10, 4));
    assert_eq!(shard_id(55, 10, 4), shard_id(55, 10, 4));
    assert_eq!(shard_id(95, 10, 4), shard_id(95, 10, 4));
}

#[test]
fn coords_in_same_tile_route_to_same_shard() {
    // Coords 0–9 all belong to tile 0 → same shard.
    let s0 = shard_id(0, 10, 4);
    for x in 1..10i64 {
        assert_eq!(
            shard_id(x, 10, 4),
            s0,
            "coord {x} must share shard with coord 0 (same tile)"
        );
    }
}

#[test]
fn adjacent_tiles_may_map_to_different_shards() {
    // With 4 shards and tile_extent=10: tile 0 → shard 0, tile 1 → shard 1, etc.
    let shard_tile0 = shard_id(5, 10, 4); // tile 0
    let shard_tile1 = shard_id(15, 10, 4); // tile 1
    // They differ only when tile % num_shards differs.
    // With 4 shards: tile 0 % 4 = 0, tile 1 % 4 = 1 → different.
    assert_ne!(
        shard_tile0, shard_tile1,
        "adjacent tiles must route to different shards"
    );
}

// ── Op-log isolation tests ────────────────────────────────────────────────────

/// Ops routed to shard 0 appear only in shard 0's log.
/// Ops routed to shard 1 appear only in shard 1's log.
#[test]
fn per_shard_logs_are_isolated() {
    let log0 = InMemoryOpLog::new();
    let log1 = InMemoryOpLog::new();

    // Route ops to the correct shard log.
    let op_tile0 = put_op("arr", 5, 100); // tile 0 → shard 0
    let op_tile1 = put_op("arr", 15, 200); // tile 1 → shard 1

    let s_op0 = shard_id(5, 10, 2);
    let s_op1 = shard_id(15, 10, 2);
    assert_ne!(s_op0, s_op1, "must route to different shards");

    if s_op0 == 0 {
        log0.append(&op_tile0).expect("append to shard0");
        log1.append(&op_tile1).expect("append to shard1");
    } else {
        log1.append(&op_tile0).expect("append to shard1");
        log0.append(&op_tile1).expect("append to shard0");
    }

    assert_eq!(log0.len().expect("len"), 1, "shard0 must have exactly 1 op");
    assert_eq!(log1.len().expect("len"), 1, "shard1 must have exactly 1 op");

    // Verify shard0 does NOT contain shard1's op.
    let shard0_ops: Vec<_> = log0
        .scan_from(Hlc::ZERO)
        .expect("scan")
        .collect::<Result<_, _>>()
        .expect("collect");
    let shard1_ops: Vec<_> = log1
        .scan_from(Hlc::ZERO)
        .expect("scan")
        .collect::<Result<_, _>>()
        .expect("collect");

    // The op in shard0 must not have the same coord as the one in shard1.
    let coord0 = match shard0_ops[0].coord.first() {
        Some(CoordValue::Int64(x)) => *x,
        _ => panic!("unexpected coord type"),
    };
    let coord1 = match shard1_ops[0].coord.first() {
        Some(CoordValue::Int64(x)) => *x,
        _ => panic!("unexpected coord type"),
    };
    assert_ne!(coord0, coord1, "shards must hold different coords");
}

// ── Cross-shard GC independence ───────────────────────────────────────────────

/// Each shard runs GC independently based on its own AckVector.
/// Shard 0 GCs below hlc=50; shard 1 has no acks yet (no-op).
#[test]
fn per_shard_gc_is_independent() {
    let log0 = InMemoryOpLog::new();
    let log1 = InMemoryOpLog::new();

    // 3 ops in shard0, 2 ops in shard1.
    for ms in [10u64, 20, 30] {
        log0.append(&put_op("arr", (ms / 10) as i64, ms))
            .expect("shard0 append");
    }
    for ms in [100u64, 200] {
        log1.append(&put_op("arr", (ms / 10) as i64, ms))
            .expect("shard1 append");
    }

    // Shard 0 receives an ack at frontier=25; shard 1 has no acks.
    let mut acks0 = AckVector::new();
    acks0.record(rep(1), hlc(25, 1));
    let acks1 = AckVector::new(); // empty → no-op GC

    let sink0 = MockSnapshotSink::new();
    let sink1 = MockSnapshotSink::new();

    let report0 = collapse_below(&log0, &acks0, &sink0, |a, f| Ok(Some(dummy_snap(a, f))))
        .expect("shard0 gc");

    let report1 = collapse_below(&log1, &acks1, &sink1, |a, f| Ok(Some(dummy_snap(a, f))))
        .expect("shard1 gc");

    // Shard 0: ops at ms=10 and ms=20 are < 25 → dropped; ms=30 survives.
    assert_eq!(report0.ops_dropped, 2, "shard0 must drop 2 ops");
    assert_eq!(log0.len().expect("len"), 1, "shard0 retains ms=30");

    // Shard 1: no acks → no GC.
    assert_eq!(report1.ops_dropped, 0, "shard1 must drop 0 ops (no acks)");
    assert_eq!(log1.len().expect("len"), 2, "shard1 log unchanged");
}

/// Cross-shard: ops for different arrays on different shards each get
/// their own snapshot on GC, and the snapshots are independent.
#[test]
fn multi_shard_gc_produces_independent_snapshots() {
    let log_a = InMemoryOpLog::new();
    let log_b = InMemoryOpLog::new();

    log_a.append(&put_op("alpha", 0, 10)).expect("alpha");
    log_b.append(&put_op("beta", 20, 20)).expect("beta");

    let mut acks = AckVector::new();
    acks.record(rep(1), hlc(100, 1));

    let sink_a = MockSnapshotSink::new();
    let sink_b = MockSnapshotSink::new();

    collapse_below(&log_a, &acks, &sink_a, |a, f| Ok(Some(dummy_snap(a, f)))).expect("alpha gc");
    collapse_below(&log_b, &acks, &sink_b, |a, f| Ok(Some(dummy_snap(a, f)))).expect("beta gc");

    assert_eq!(sink_a.snapshot_count(), 1);
    assert_eq!(sink_b.snapshot_count(), 1);

    let snaps_a = sink_a.into_snapshots();
    let snaps_b = sink_b.into_snapshots();
    assert_eq!(snaps_a[0].array, "alpha");
    assert_eq!(snaps_b[0].array, "beta");
}

/// HLC total order: two ops from different replicas at the same physical ms
/// are ordered by replica_id, confirming that the cross-shard merge stream
/// will be deterministic.
#[test]
fn cross_replica_hlc_tiebreak_is_deterministic() {
    let h_rep1 = hlc(1000, 1);
    let h_rep2 = hlc(1000, 2);

    // Both have physical_ms = 1000; tiebreak by replica_id.
    assert_ne!(h_rep1, h_rep2, "different replicas produce different HLCs");
    // Total order is well-defined (either rep1 < rep2 or rep2 < rep1).
    assert!(h_rep1 < h_rep2 || h_rep2 < h_rep1);
    // Stable across calls.
    assert_eq!(h_rep1.cmp(&h_rep2), h_rep1.cmp(&h_rep2));
}
