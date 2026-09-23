// SPDX-License-Identifier: BUSL-1.1

//! Counter maintenance helpers: `increment_for_insert` and `decrement_for_delete`.
//!
//! Both functions accept a `&redb::WriteTransaction` opened by the caller and
//! operate fully within that transaction — no second transaction is opened.
//! Atomicity is preserved: either the EDGES write and the GRAPH_STATS update
//! both commit, or neither does.
//!
//! ## Prior-live probe
//!
//! Before deciding whether to increment (insert path) or decrement (delete
//! path), each function probes the EDGES table inside the same write transaction
//! to determine whether a prior live version of the same base edge exists. The
//! `Table<'txn, K, V>` type returned by `write_txn.open_table()` implements
//! `ReadableTable`, so `.range()` and `.get()` are available with the
//! transaction's consistent snapshot.

use redb::{ReadableTable, WriteTransaction};

use crate::engine::graph::edge_store::store::{EDGES, redb_err};
use crate::engine::graph::edge_store::temporal::keys::{
    edge_version_prefix, is_sentinel, parse_versioned_edge_key,
};

use super::table::{GRAPH_STATS, LabelRow, NodeRow, SummaryRow, label_key, node_key, summary_key};

/// Identity of a base edge `(tid, collection, src, label, dst)` under a
/// database, used to probe/maintain the GRAPH_STATS counters for it.
#[derive(Debug, Clone, Copy)]
pub struct EdgeStatsKey<'a> {
    pub db: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub label: &'a str,
    pub src: &'a str,
    pub dst: &'a str,
}

// ── Insert path ───────────────────────────────────────────────────────────────

/// Called from `put_edge_versioned` inside the same `WriteTransaction`.
///
/// If no prior live version exists for `(tid, collection, src, label, dst)`,
/// increments: `summary.edge_count`, `label[label].count` (creating the row and
/// bumping `summary.distinct_label_count` if new), and `node[src].refcount` +
/// `node[dst].refcount` (bumping `summary.distinct_node_count` per new node).
///
/// If a prior live version exists this is an update — counters are unchanged.
///
/// Returns whether the counters changed.
pub fn increment_for_insert(
    write_txn: &WriteTransaction,
    key: EdgeStatsKey<'_>,
    current_system_from: i64,
) -> crate::Result<bool> {
    if prior_live_exists(write_txn, key, current_system_from)? {
        return Ok(false);
    }
    increment_counts(write_txn, key)?;
    Ok(true)
}

/// Count one more live edge for `key`: the summary, its label and both
/// endpoints.
pub fn increment_counts(write_txn: &WriteTransaction, key: EdgeStatsKey<'_>) -> crate::Result<()> {
    let EdgeStatsKey {
        db,
        tid,
        collection,
        label,
        src,
        dst,
    } = key;
    let mut stats = write_txn
        .open_table(GRAPH_STATS)
        .map_err(|e| redb_err("open graph_stats (increment)", e))?;

    // --- summary row ---
    let skey = summary_key(collection);
    let mut summary = read_summary(&stats, db, tid, &skey)?;

    summary.edge_count = summary.edge_count.saturating_add(1);

    // --- label row ---
    let lkey = label_key(collection, label);
    let lrow = read_label_opt(&stats, db, tid, &lkey)?;
    let new_label_count = match lrow {
        None => {
            summary.distinct_label_count = summary.distinct_label_count.saturating_add(1);
            1u64
        }
        Some(r) => r.count.saturating_add(1),
    };
    stats
        .insert(
            (db, tid, lkey.as_str()),
            LabelRow {
                count: new_label_count,
            }
            .encode()?
            .as_slice(),
        )
        .map_err(|e| redb_err("insert label row", e))?;

    // --- node rows (src and dst; handle self-loop: only one row) ---
    increment_node(&mut stats, db, tid, collection, src, &mut summary)?;
    if src != dst {
        increment_node(&mut stats, db, tid, collection, dst, &mut summary)?;
    }

    // Write summary last so all increments are already done.
    stats
        .insert((db, tid, skey.as_str()), summary.encode()?.as_slice())
        .map_err(|e| redb_err("insert summary row", e))?;

    Ok(())
}

// ── Delete path ───────────────────────────────────────────────────────────────

/// Called from `write_sentinel` inside the same `WriteTransaction`.
///
/// If the immediately-preceding version was live (non-sentinel), decrements:
/// `summary.edge_count`, `label[label].count` (deleting the row and decrementing
/// `summary.distinct_label_count` when count reaches zero), and
/// `node[src].refcount` + `node[dst].refcount` (deleting the node row and
/// decrementing `summary.distinct_node_count` when refcount reaches zero).
///
/// If there was no prior live version, this is a no-op for the counters.
///
/// Returns whether the counters changed.
pub fn decrement_for_delete(
    write_txn: &WriteTransaction,
    key: EdgeStatsKey<'_>,
    sentinel_system_from: i64,
) -> crate::Result<bool> {
    if !prior_live_exists(write_txn, key, sentinel_system_from)? {
        return Ok(false);
    }
    decrement_counts(write_txn, key)?;
    Ok(true)
}

/// Count one live edge fewer for `key`. The exact inverse of
/// [`increment_counts`].
pub fn decrement_counts(write_txn: &WriteTransaction, key: EdgeStatsKey<'_>) -> crate::Result<()> {
    let EdgeStatsKey {
        db,
        tid,
        collection,
        label,
        src,
        dst,
    } = key;
    let mut stats = write_txn
        .open_table(GRAPH_STATS)
        .map_err(|e| redb_err("open graph_stats (decrement)", e))?;

    let skey = summary_key(collection);
    let mut summary = read_summary(&stats, db, tid, &skey)?;

    summary.edge_count = summary.edge_count.saturating_sub(1);

    // --- label row ---
    let lkey = label_key(collection, label);
    let lrow = read_label_opt(&stats, db, tid, &lkey)?;
    if let Some(r) = lrow {
        let new_count = r.count.saturating_sub(1);
        if new_count == 0 {
            stats
                .remove((db, tid, lkey.as_str()))
                .map_err(|e| redb_err("remove label row", e))?;
            summary.distinct_label_count = summary.distinct_label_count.saturating_sub(1);
        } else {
            stats
                .insert(
                    (db, tid, lkey.as_str()),
                    LabelRow { count: new_count }.encode()?.as_slice(),
                )
                .map_err(|e| redb_err("update label row", e))?;
        }
    }

    // --- node rows ---
    decrement_node(&mut stats, db, tid, collection, src, &mut summary)?;
    if src != dst {
        decrement_node(&mut stats, db, tid, collection, dst, &mut summary)?;
    }

    stats
        .insert((db, tid, skey.as_str()), summary.encode()?.as_slice())
        .map_err(|e| redb_err("update summary row", e))?;

    Ok(())
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Returns `true` when a live (non-sentinel) version of the base edge exists
/// in EDGES at any `system_from` strictly less than `exclude_system_from`,
/// using the same `WriteTransaction`'s consistent view.
fn prior_live_exists(
    write_txn: &WriteTransaction,
    key: EdgeStatsKey<'_>,
    exclude_system_from: i64,
) -> crate::Result<bool> {
    let EdgeStatsKey {
        db,
        tid,
        collection,
        label,
        src,
        dst,
    } = key;
    let prefix = edge_version_prefix(collection, src, label, dst);
    let edges = write_txn
        .open_table(EDGES)
        .map_err(|e| redb_err("open edges (prior_live probe)", e))?;

    let range = edges
        .range((db, tid, prefix.as_str())..)
        .map_err(|e| redb_err("prior_live range", e))?;

    for entry in range {
        let (k, v) = entry.map_err(|e| redb_err("prior_live iter", e))?;
        let (kd, kt, composite) = k.value();
        if kd != db || kt != tid || !composite.starts_with(&prefix) {
            break;
        }
        let Some((_c, _s, _l, _d, sys)) = parse_versioned_edge_key(composite) else {
            continue;
        };
        if sys >= exclude_system_from {
            // This is the key we're about to write (or a later one) — skip.
            continue;
        }
        // Found a version before this write — is it live?
        if !is_sentinel(v.value()) {
            return Ok(true);
        }
        // Sentinel at sys < exclude → prior state was already deleted.
        return Ok(false);
    }
    Ok(false)
}

fn read_summary(
    stats: &redb::Table<'_, (u64, u64, &str), &[u8]>,
    db: u64,
    tid: u64,
    skey: &str,
) -> crate::Result<SummaryRow> {
    match stats
        .get((db, tid, skey))
        .map_err(|e| redb_err("read summary row", e))?
    {
        Some(g) => SummaryRow::decode(g.value()),
        None => Ok(SummaryRow::zero()),
    }
}

fn read_label_opt(
    stats: &redb::Table<'_, (u64, u64, &str), &[u8]>,
    db: u64,
    tid: u64,
    lkey: &str,
) -> crate::Result<Option<LabelRow>> {
    match stats
        .get((db, tid, lkey))
        .map_err(|e| redb_err("read label row", e))?
    {
        Some(g) => Ok(Some(LabelRow::decode(g.value())?)),
        None => Ok(None),
    }
}

fn increment_node(
    stats: &mut redb::Table<'_, (u64, u64, &str), &[u8]>,
    db: u64,
    tid: u64,
    collection: &str,
    node_id: &str,
    summary: &mut SummaryRow,
) -> crate::Result<()> {
    let nkey = node_key(collection, node_id);
    let existing_bytes: Option<Vec<u8>> = {
        let guard = stats
            .get((db, tid, nkey.as_str()))
            .map_err(|e| redb_err("read node row", e))?;
        guard.map(|g| g.value().to_vec())
    };
    let new_refcount = match existing_bytes {
        None => {
            summary.distinct_node_count = summary.distinct_node_count.saturating_add(1);
            1u32
        }
        Some(b) => NodeRow::decode(&b)?.refcount.saturating_add(1),
    };
    stats
        .insert(
            (db, tid, nkey.as_str()),
            NodeRow {
                refcount: new_refcount,
            }
            .encode()?
            .as_slice(),
        )
        .map_err(|e| redb_err("insert node row", e))?;
    Ok(())
}

fn decrement_node(
    stats: &mut redb::Table<'_, (u64, u64, &str), &[u8]>,
    db: u64,
    tid: u64,
    collection: &str,
    node_id: &str,
    summary: &mut SummaryRow,
) -> crate::Result<()> {
    let nkey = node_key(collection, node_id);
    let existing_bytes: Option<Vec<u8>> = {
        let guard = stats
            .get((db, tid, nkey.as_str()))
            .map_err(|e| redb_err("read node row", e))?;
        guard.map(|g| g.value().to_vec())
    };
    if let Some(b) = existing_bytes {
        let current = NodeRow::decode(&b)?.refcount;
        if current <= 1 {
            stats
                .remove((db, tid, nkey.as_str()))
                .map_err(|e| redb_err("remove node row", e))?;
            summary.distinct_node_count = summary.distinct_node_count.saturating_sub(1);
        } else {
            stats
                .insert(
                    (db, tid, nkey.as_str()),
                    NodeRow {
                        refcount: current - 1,
                    }
                    .encode()?
                    .as_slice(),
                )
                .map_err(|e| redb_err("update node row", e))?;
        }
    }
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, TenantId};

    use crate::engine::graph::edge_store::EdgeStore;
    use crate::engine::graph::edge_store::stats::table::CollectionStats;
    use crate::engine::graph::edge_store::temporal::keys::versioned_edge_key;
    use crate::engine::graph::edge_store::temporal::payload::EdgeValuePayload;

    const DB: DatabaseId = DatabaseId::DEFAULT;
    const T1: TenantId = TenantId::new(1);
    const T2: TenantId = TenantId::new(2);
    const COLL: &str = "people";

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        (store, dir)
    }

    fn put(store: &EdgeStore, src: &str, label: &str, dst: &str, sys: i64) {
        store
            .put_edge_versioned(
                crate::engine::graph::edge_store::temporal::keys::EdgeRef::new(
                    DB, T1, COLL, src, label, dst,
                ),
                b"props",
                sys,
                0,
                i64::MAX,
            )
            .unwrap();
    }

    fn delete(store: &EdgeStore, src: &str, label: &str, dst: &str, sys: i64) {
        store
            .soft_delete_edge(
                crate::engine::graph::edge_store::temporal::keys::EdgeRef::new(
                    DB, T1, COLL, src, label, dst,
                ),
                sys,
            )
            .unwrap();
    }

    fn stats(store: &EdgeStore) -> CollectionStats {
        store.collection_stats(DB.as_u64(), T1, COLL, None).unwrap()
    }

    // 1. Single put → counters show 1 edge / 2 nodes / 1 label.
    #[test]
    fn single_put_increments_counters() {
        let (store, _dir) = make_store();
        put(&store, "a", "L", "b", 100);
        let s = stats(&store);
        assert_eq!(s.edge_count, 1);
        assert_eq!(s.distinct_node_count, 2);
        assert_eq!(s.distinct_label_count, 1);
        assert_eq!(s.labels, vec![("L".to_string(), 1)]);
    }

    // 2. Two puts of the same edge at different system_from → no double-count.
    #[test]
    fn update_same_edge_no_double_count() {
        let (store, _dir) = make_store();
        put(&store, "a", "L", "b", 100);
        put(&store, "a", "L", "b", 200);
        let s = stats(&store);
        assert_eq!(s.edge_count, 1);
        assert_eq!(s.distinct_node_count, 2);
        assert_eq!(s.distinct_label_count, 1);
    }

    // 3. Two distinct edges, same label → 2 edges / 4 nodes / 1 label.
    #[test]
    fn two_edges_same_label() {
        let (store, _dir) = make_store();
        put(&store, "a", "L", "b", 100);
        put(&store, "c", "L", "d", 110);
        let s = stats(&store);
        assert_eq!(s.edge_count, 2);
        assert_eq!(s.distinct_node_count, 4);
        assert_eq!(s.distinct_label_count, 1);
        assert_eq!(s.labels, vec![("L".to_string(), 2)]);
    }

    // 4. Two distinct edges, same nodes, different labels → 2 edges / 2 nodes / 2 labels.
    #[test]
    fn two_edges_same_nodes_different_labels() {
        let (store, _dir) = make_store();
        put(&store, "a", "L1", "b", 100);
        put(&store, "a", "L2", "b", 110);
        let s = stats(&store);
        assert_eq!(s.edge_count, 2);
        assert_eq!(s.distinct_node_count, 2);
        assert_eq!(s.distinct_label_count, 2);
        let mut labels = s.labels.clone();
        labels.sort();
        assert_eq!(labels, vec![("L1".to_string(), 1), ("L2".to_string(), 1)]);
    }

    // 5. Self-loop (src == dst) → 1 edge / 1 node / 1 label.
    #[test]
    fn self_loop_counts_one_node() {
        let (store, _dir) = make_store();
        put(&store, "a", "L", "a", 100);
        let s = stats(&store);
        assert_eq!(s.edge_count, 1);
        assert_eq!(s.distinct_node_count, 1);
        assert_eq!(s.distinct_label_count, 1);
    }

    // 6. Put then soft_delete → 0/0/0/[].
    #[test]
    fn put_then_delete_zeroes_counters() {
        let (store, _dir) = make_store();
        put(&store, "a", "L", "b", 100);
        delete(&store, "a", "L", "b", 200);
        let s = stats(&store);
        assert_eq!(s.edge_count, 0);
        assert_eq!(s.distinct_node_count, 0);
        assert_eq!(s.distinct_label_count, 0);
        assert!(s.labels.is_empty());
    }

    // 7. soft_delete without prior live → counters unchanged (still zero).
    #[test]
    fn delete_without_prior_live_is_noop() {
        let (store, _dir) = make_store();
        delete(&store, "x", "L", "y", 100);
        let s = stats(&store);
        assert_eq!(s.edge_count, 0);
        assert_eq!(s.distinct_node_count, 0);
        assert_eq!(s.distinct_label_count, 0);
    }

    // 8. Cold-start rebuild: write via put_edge_raw (bypass counter), then
    //    collection_stats triggers rebuild and returns correct counts.
    //    Second call reads from cached summary row.
    #[test]
    fn cold_start_rebuild() {
        let (store, _dir) = make_store();

        // Insert directly, bypassing the counter hooks.
        let key = versioned_edge_key(COLL, "a", "L", "b", 100).unwrap();
        let payload = EdgeValuePayload::new(0, i64::MAX, b"p".to_vec())
            .encode()
            .unwrap();
        store.put_edge_raw(DB.as_u64(), T1, &key, &payload).unwrap();

        // First call: no summary row → triggers rebuild.
        let s1 = store.collection_stats(DB.as_u64(), T1, COLL, None).unwrap();
        assert_eq!(s1.edge_count, 1);
        assert_eq!(s1.distinct_node_count, 2);
        assert_eq!(s1.distinct_label_count, 1);

        // Second call: reads from cached summary (still correct).
        let s2 = store.collection_stats(DB.as_u64(), T1, COLL, None).unwrap();
        assert_eq!(s2, s1);
    }

    // 9. Multi-tenant isolation.
    #[test]
    fn multi_tenant_isolation() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(
                crate::engine::graph::edge_store::temporal::keys::EdgeRef::new(
                    DB, T1, COLL, "a", "L", "b",
                ),
                b"t1",
                100,
                0,
                i64::MAX,
            )
            .unwrap();
        store
            .put_edge_versioned(
                crate::engine::graph::edge_store::temporal::keys::EdgeRef::new(
                    DB, T2, COLL, "x", "M", "y",
                ),
                b"t2",
                100,
                0,
                i64::MAX,
            )
            .unwrap();

        let s1 = store.collection_stats(DB.as_u64(), T1, COLL, None).unwrap();
        let s2 = store.collection_stats(DB.as_u64(), T2, COLL, None).unwrap();

        assert_eq!(s1.edge_count, 1);
        assert_eq!(s1.labels, vec![("L".to_string(), 1)]);

        assert_eq!(s2.edge_count, 1);
        assert_eq!(s2.labels, vec![("M".to_string(), 1)]);
    }

    // 10. tenant_stats returns one entry per collection, sorted deterministically.
    #[test]
    fn tenant_stats_one_entry_per_collection() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(
                crate::engine::graph::edge_store::temporal::keys::EdgeRef::new(
                    DB, T1, "alpha", "a", "L", "b",
                ),
                b"p",
                100,
                0,
                i64::MAX,
            )
            .unwrap();
        store
            .put_edge_versioned(
                crate::engine::graph::edge_store::temporal::keys::EdgeRef::new(
                    DB, T1, "beta", "c", "L", "d",
                ),
                b"p",
                100,
                0,
                i64::MAX,
            )
            .unwrap();

        let mut all = store.tenant_stats(DB.as_u64(), T1, None).unwrap();
        all.sort_by(|a, b| a.collection.cmp(&b.collection));
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].collection, "alpha");
        assert_eq!(all[1].collection, "beta");
        assert_eq!(all[0].edge_count, 1);
        assert_eq!(all[1].edge_count, 1);
    }
}
