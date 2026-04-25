//! Bitemporal recovery & idempotency.
//!
//! Two correctness properties verified here:
//!
//! 1. **Crash mid-bitemporal-write** — redb is ACID, so committed
//!    versions survive across a process restart. The bitemporal
//!    contract additionally requires that the Ceiling resolver pick
//!    the correct prior version even when a later version was lost
//!    (i.e. never committed). We simulate the crash by writing v1+v2,
//!    dropping the store, reopening from the same path, and asserting
//!    Ceiling at any cutoff returns the latest *committed* version —
//!    never a torn read, never a missing prior.
//!
//! 2. **Follower replay idempotency** — every versioned key is fully
//!    determined by `(collection, src, label, dst, system_from_ms)`,
//!    where `system_from_ms` is stamped on the leader and propagated
//!    in the Raft payload. Replaying the same logical write stream on
//!    a follower must produce a state that is functionally identical
//!    on both the forward and reverse indexes. We model this by
//!    writing the same sequence to two independent stores and asserting
//!    `neighbors_out_as_of` / `neighbors_in_as_of` agree at a sweep
//!    of system-time cutoffs.

use nodedb::engine::graph::edge_store::EdgeStore;
use nodedb::engine::graph::edge_store::temporal::EdgeRef;
use nodedb_types::TenantId;

const T: TenantId = TenantId::new(1);
const COLL: &str = "users";

fn e<'a>(src: &'a str, label: &'a str, dst: &'a str) -> EdgeRef<'a> {
    EdgeRef::new(T, COLL, src, label, dst)
}

/// Functional state snapshot at one system-time cutoff: outbound and
/// inbound neighbor sets per node, sorted for stable comparison.
fn snapshot_at(store: &EdgeStore, nodes: &[&str], cutoff: i64) -> Vec<(String, String, Vec<u8>)> {
    let mut out = Vec::new();
    for n in nodes {
        let outs = store
            .neighbors_out_as_of(T, COLL, n, None, Some(cutoff), None)
            .unwrap();
        for ed in outs {
            out.push((
                format!("out:{}->{}/{}", ed.src_id, ed.dst_id, ed.label),
                format!("from {n}"),
                ed.properties,
            ));
        }
        let ins = store
            .neighbors_in_as_of(T, COLL, n, None, Some(cutoff), None)
            .unwrap();
        for ed in ins {
            out.push((
                format!("in:{}->{}/{}", ed.src_id, ed.dst_id, ed.label),
                format!("to {n}"),
                ed.properties,
            ));
        }
    }
    out.sort();
    out
}

#[test]
fn ceiling_resolves_correctly_after_crash_mid_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph.redb");

    // Phase 1: write v1 @ 100 and v2 @ 200, then "crash" (drop the
    // handle without ever attempting v3). redb commits each
    // put_edge_versioned independently, so v1 and v2 survive.
    {
        let store = EdgeStore::open(&path).unwrap();
        store
            .put_edge_versioned(e("a", "L", "b"), b"v1", 100, 100, i64::MAX)
            .unwrap();
        store
            .put_edge_versioned(e("a", "L", "b"), b"v2", 200, 200, i64::MAX)
            .unwrap();
    }

    // Phase 2: reopen and confirm Ceiling resolves the right version
    // at every interesting cutoff. Crucially, the cutoff *past* the
    // last committed version (1_000) must still yield v2 — the resolver
    // doesn't depend on any out-of-band "latest" pointer.
    let store = EdgeStore::open(&path).unwrap();
    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 99, None)
            .unwrap(),
        None,
        "no version at or before 99",
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 100, None)
            .unwrap(),
        Some(b"v1".to_vec()),
        "v1 visible at its own stamp",
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 150, None)
            .unwrap(),
        Some(b"v1".to_vec()),
        "v1 visible between v1 and v2 stamps",
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 200, None)
            .unwrap(),
        Some(b"v2".to_vec()),
        "v2 visible at its own stamp",
    );
    assert_eq!(
        store
            .ceiling_resolve_edge(e("a", "L", "b"), 1_000, None)
            .unwrap(),
        Some(b"v2".to_vec()),
        "Ceiling past the last committed version still returns v2 — no torn read",
    );

    // Reverse index symmetry survives reopen.
    let ins = store
        .neighbors_in_as_of(T, COLL, "b", None, Some(1_000), None)
        .unwrap();
    assert_eq!(ins.len(), 1, "v2 inbound to b survives reopen");
    assert_eq!(ins[0].properties, b"v2");
}

#[test]
fn follower_replay_yields_identical_state() {
    // Model: leader write stream, replayed on two independent followers.
    // Each "follower" is a fresh EdgeStore. The same payload (identical
    // system_from_ms, valid_from_ms, valid_until_ms) must produce
    // functionally identical state on both forward and reverse indexes.
    //
    // Mix of operations exercises every key-shape branch: Insert,
    // Update (same base, later system_from), soft-delete, GDPR erase,
    // and a second-edge write to ensure scans don't bleed across bases.
    type Write = Box<dyn Fn(&EdgeStore)>;
    let writes: Vec<Write> = vec![
        Box::new(|s| {
            s.put_edge_versioned(e("a", "L", "b"), b"v1", 100, 100, i64::MAX)
                .unwrap();
        }),
        Box::new(|s| {
            s.put_edge_versioned(e("a", "L", "b"), b"v2", 200, 150, i64::MAX)
                .unwrap();
        }),
        Box::new(|s| {
            s.put_edge_versioned(e("c", "M", "d"), b"v1", 250, 0, 1_000)
                .unwrap();
        }),
        Box::new(|s| {
            s.soft_delete_edge(e("a", "L", "b"), 300).unwrap();
        }),
        Box::new(|s| {
            s.gdpr_erase_edge(e("c", "M", "d"), 400).unwrap();
        }),
    ];

    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let s1 = EdgeStore::open(&dir1.path().join("graph.redb")).unwrap();
    let s2 = EdgeStore::open(&dir2.path().join("graph.redb")).unwrap();

    for w in &writes {
        w(&s1);
        w(&s2);
    }

    let nodes = ["a", "b", "c", "d"];
    // Sweep cutoffs spanning before-first, between-versions, after-erase.
    for cutoff in [50, 150, 200, 250, 300, 400, 1_000] {
        let snap1 = snapshot_at(&s1, &nodes, cutoff);
        let snap2 = snapshot_at(&s2, &nodes, cutoff);
        assert_eq!(
            snap1, snap2,
            "follower replay diverged at cutoff {cutoff}: \
             versioned keys must be fully determined by payload-stamped ordinals",
        );
    }

    // Replay-twice idempotency: applying the same write stream a
    // second time on s1 must yield identical state at every cutoff.
    let before: Vec<_> = [50, 150, 200, 250, 300, 400, 1_000]
        .iter()
        .map(|&c| snapshot_at(&s1, &nodes, c))
        .collect();
    for w in &writes {
        w(&s1);
    }
    let after: Vec<_> = [50, 150, 200, 250, 300, 400, 1_000]
        .iter()
        .map(|&c| snapshot_at(&s1, &nodes, c))
        .collect();
    assert_eq!(
        before, after,
        "double-apply of the same write stream is a no-op — \
         keys are deterministic, values overwrite the same cells",
    );
}
