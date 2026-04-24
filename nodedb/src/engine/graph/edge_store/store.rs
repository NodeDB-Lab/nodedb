use std::path::Path;
use std::sync::Arc;

use nodedb_types::TenantId;
use redb::{Database, ReadableTable, TableDefinition};

use super::temporal::{
    EdgeValuePayload, edge_from_versioned_entry, is_sentinel, parse_versioned_edge_key,
    versioned_edge_key,
};

/// Edge table: composite key `(tid, "collection\x00src\x00label\x00dst\x00{system_from:020}")` → value.
///
/// The tenant id is a first-class key component (not a lexical prefix).
/// Collection is the first segment of the composite string so edges in one
/// collection share a `"{collection}\x00"` prefix — enabling O(|collection|)
/// range drops in `purge_collection` without a schema-level third tuple component.
///
/// Value is either an [`EdgeValuePayload`] (zerompk fixarray-3) or a single-byte
/// sentinel (`TOMBSTONE_SENTINEL`, `GDPR_ERASURE_SENTINEL`).
pub(super) const EDGES: TableDefinition<(u32, &str), &[u8]> = TableDefinition::new("edges");

/// Reverse edge index: same versioned key shape as `EDGES` but with
/// `dst`/`src` swapped. Value is empty for live edges, or a sentinel for
/// soft-deleted / erased edges (symmetry with forward).
pub(super) const REVERSE_EDGES: TableDefinition<(u32, &str), &[u8]> =
    TableDefinition::new("reverse_edges");

pub(super) fn redb_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "graph".into(),
        detail: format!("{ctx}: {e}"),
    }
}

// Re-export shared Direction from nodedb-types.
pub use nodedb_types::graph::Direction;

/// Decoded edge record yielded by [`EdgeStore::scan_all_edges_decoded`]:
/// `(tenant, collection, src, label, dst, properties)`. Current-state only —
/// tombstoned and GDPR-erased edges are filtered out, and only the latest
/// non-sentinel version per base key is yielded.
pub type EdgeRecord = (TenantId, String, String, String, String, Vec<u8>);

/// A single edge with its properties.
#[derive(Debug, Clone)]
pub struct Edge {
    pub collection: String,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    pub properties: Vec<u8>,
}

/// redb-backed edge storage for the Knowledge Graph engine.
///
/// Keys are `(TenantId, versioned_composite_key)` tuples — tenant routing
/// is structural, not lexical. Each Data Plane core owns its own
/// `EdgeStore` instance; no cross-core sharing.
pub struct EdgeStore {
    pub(super) db: Arc<Database>,
}

impl EdgeStore {
    /// Open or create the edge store database at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(path).map_err(|e| redb_err("open", e))?;

        let write_txn = db.begin_write().map_err(|e| redb_err("begin_write", e))?;
        {
            let _ = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            let _ = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse_edges", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Soft-delete every edge incident on `node` (as either src or dst) in
    /// the caller's tenant, across all collections. Emits a tombstone
    /// version at `system_from` for each distinct base edge that has a
    /// live (non-sentinel) latest version.
    pub fn delete_edges_for_node(
        &self,
        tid: TenantId,
        node: &str,
        system_from: i64,
    ) -> crate::Result<()> {
        // Snapshot all live bases touching `node`. Done in a read txn first
        // so the write txn can call soft_delete_edge without nested locks.
        let bases = self.live_bases_touching_node(tid, node)?;
        for (collection, src, label, dst) in bases {
            self.soft_delete_edge(tid, &collection, &src, &label, &dst, system_from)?;
        }
        Ok(())
    }

    /// Enumerate `(collection, src, label, dst)` tuples for every base edge
    /// in this tenant whose latest version touches `node` as src or dst and
    /// is not a sentinel.
    fn live_bases_touching_node(
        &self,
        tid: TenantId,
        node: &str,
    ) -> crate::Result<Vec<(String, String, String, String)>> {
        use std::collections::HashMap;
        let t = tid.as_u32();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        // Map base-key-tuple → (latest_system_from, is_sentinel).
        let mut latest: HashMap<(String, String, String, String), (i64, bool)> = HashMap::new();
        let range = table
            .range((t, "")..(t + 1, ""))
            .map_err(|e| redb_err("iter", e))?;
        for entry in range {
            let (k, v) = entry.map_err(|e| redb_err("iter entry", e))?;
            let composite = k.value().1;
            let Some((coll, src, label, dst, sys)) = parse_versioned_edge_key(composite) else {
                continue;
            };
            if src != node && dst != node {
                continue;
            }
            let base = (
                coll.to_string(),
                src.to_string(),
                label.to_string(),
                dst.to_string(),
            );
            let is_sent = is_sentinel(v.value());
            latest
                .entry(base)
                .and_modify(|(cur, cur_sent)| {
                    if sys > *cur {
                        *cur = sys;
                        *cur_sent = is_sent;
                    }
                })
                .or_insert((sys, is_sent));
        }
        Ok(latest
            .into_iter()
            .filter_map(|(base, (_sys, is_sent))| if is_sent { None } else { Some(base) })
            .collect())
    }

    /// Scan every base forward edge belonging to a tenant in current-state,
    /// returning `(composite_key, properties)` pairs. The composite key is
    /// the **versioned** form — callers that only want the base key should
    /// use `scan_all_edges_decoded` or re-parse via `parse_versioned_edge_key`.
    ///
    /// Used by snapshot export, which preserves versioned keys verbatim.
    pub fn scan_edges_for_tenant(&self, tid: TenantId) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let t = tid.as_u32();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        let mut results = Vec::new();
        let range = table
            .range((t, "")..(t + 1, ""))
            .map_err(|e| redb_err("edge range", e))?;
        for entry in range {
            let entry = entry.map_err(|e| redb_err("edge entry", e))?;
            results.push((entry.0.value().1.to_string(), entry.1.value().to_vec()));
        }
        Ok(results)
    }

    /// Scan every forward edge across all tenants in current-state,
    /// yielding `(TenantId, collection, src, label, dst, properties)`.
    /// Used by CSR rebuild — tombstoned and erased versions are skipped,
    /// and only the Ceiling resolution at `system_as_of` is returned per
    /// base. `None` means "current state" (ordinal = `i64::MAX`).
    pub fn scan_all_edges_decoded(
        &self,
        system_as_of: Option<i64>,
    ) -> crate::Result<Vec<EdgeRecord>> {
        use std::collections::HashMap;
        let cutoff = system_as_of.unwrap_or(i64::MAX);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        // Latest non-sentinel version per (tid, base).
        let mut latest: HashMap<(u32, String, String, String, String), (i64, Vec<u8>)> =
            HashMap::new();
        let mut tombstoned: std::collections::HashSet<(u32, String, String, String, String)> =
            std::collections::HashSet::new();

        for entry in table.iter().map_err(|e| redb_err("iter", e))? {
            let (k, v) = entry.map_err(|e| redb_err("iter entry", e))?;
            let (t, composite) = k.value();
            let Some((coll, src, label, dst, sys)) = parse_versioned_edge_key(composite) else {
                continue;
            };
            if sys > cutoff {
                continue;
            }
            let base = (
                t,
                coll.to_string(),
                src.to_string(),
                label.to_string(),
                dst.to_string(),
            );
            let bytes = v.value();
            if is_sentinel(bytes) {
                // Later tombstone shadows any earlier live version — track it
                // and drop any accumulated value for this base.
                match latest.get(&base) {
                    Some((cur_sys, _)) if *cur_sys > sys => {}
                    _ => {
                        latest.remove(&base);
                        tombstoned.insert(base);
                    }
                }
                continue;
            }
            // Live version — only install if it's newer than the current
            // entry AND no later tombstone exists.
            if tombstoned.contains(&base) {
                continue;
            }
            match latest.get(&base) {
                Some((cur_sys, _)) if *cur_sys >= sys => {}
                _ => match EdgeValuePayload::decode(bytes) {
                    Ok(payload) => {
                        latest.insert(base, (sys, payload.properties));
                    }
                    Err(_) => continue,
                },
            }
        }

        let mut out = Vec::with_capacity(latest.len());
        for ((t, coll, src, label, dst), (_sys, props)) in latest {
            out.push((TenantId::new(t), coll, src, label, dst, props));
        }
        Ok(out)
    }

    /// Insert a raw edge record (for snapshot restore). Takes the tenant +
    /// versioned composite key + raw value bytes (payload or sentinel).
    ///
    /// The composite key must already include the `{system_from:020}` suffix.
    /// Reverse-index mirror is maintained with the same suffix.
    pub fn put_edge_raw(
        &self,
        tid: TenantId,
        composite_key: &str,
        value: &[u8],
    ) -> crate::Result<()> {
        let t = tid.as_u32();
        let (collection, src, label, dst, sys) = parse_versioned_edge_key(composite_key)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("put_edge_raw: malformed versioned key {composite_key:?}"),
            })?;
        let rev_key = versioned_edge_key(collection, dst, label, src, sys)?;
        // Reverse value mirrors forward: sentinels for tombstones, empty for live.
        let rev_value: &[u8] = if is_sentinel(value) { value } else { &[] };

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            edges
                .insert((t, composite_key), value)
                .map_err(|e| redb_err("insert edge", e))?;
            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            rev_t
                .insert((t, rev_key.as_str()), rev_value)
                .map_err(|e| redb_err("insert reverse", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit edge", e))?;
        Ok(())
    }

    /// Get an edge's current-state properties under the caller's tenant.
    /// Returns `None` if no live version exists.
    pub fn get_edge(
        &self,
        tid: TenantId,
        collection: &str,
        src: &str,
        label: &str,
        dst: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        self.ceiling_resolve_edge(tid, collection, src, label, dst, i64::MAX, None)
    }

    /// Scan forward edges under a tenant with a base-key prefix, applying
    /// current-state semantics: yields exactly one `Edge` per base that has
    /// a live latest version. Used by `neighbors_out` and friends.
    ///
    /// The prefix must be a base-key prefix (no `system_from` suffix) —
    /// typically `{collection}\x00{src}\x00` or
    /// `{collection}\x00{src}\x00{label}\x00`.
    pub(super) fn scan_edges_with_prefix<F>(
        &self,
        tid: TenantId,
        prefix: &str,
        mut make_edge: F,
    ) -> crate::Result<Vec<Edge>>
    where
        F: FnMut(&str, &str, &str, &str) -> Edge,
    {
        use std::collections::HashMap;
        let t = tid.as_u32();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        // Group versions by base key → (latest_sys, latest_value).
        let mut latest: HashMap<(String, String, String, String), (i64, Vec<u8>)> = HashMap::new();

        let range = table
            .range((t, prefix)..)
            .map_err(|e| redb_err("range", e))?;

        for entry in range {
            let (key, val) = entry.map_err(|e| redb_err("iter", e))?;
            let (kt, composite) = key.value();
            if kt != t || !composite.starts_with(prefix) {
                break;
            }
            let Some((coll, src, label, dst, sys)) = parse_versioned_edge_key(composite) else {
                continue;
            };
            let base = (
                coll.to_string(),
                src.to_string(),
                label.to_string(),
                dst.to_string(),
            );
            let bytes = val.value().to_vec();
            latest
                .entry(base)
                .and_modify(|(cur_sys, cur_val)| {
                    if sys > *cur_sys {
                        *cur_sys = sys;
                        *cur_val = bytes.clone();
                    }
                })
                .or_insert((sys, bytes));
        }

        let mut edges = Vec::with_capacity(latest.len());
        for ((coll, src, label, dst), (_sys, bytes)) in latest {
            if is_sentinel(&bytes) {
                continue;
            }
            let Ok(payload) = EdgeValuePayload::decode(&bytes) else {
                continue;
            };
            let mut edge = make_edge(&coll, &src, &label, &dst);
            edge.properties = payload.properties;
            edges.push(edge);
        }
        Ok(edges)
    }
}

// Silence the unused import for `edge_from_versioned_entry` until callers
// in future tiers pick it up (filter/planner integration).
#[allow(dead_code)]
fn _keep_edge_from_versioned_entry_referenced() {
    let _ = edge_from_versioned_entry;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::edge_store::temporal::{
        EdgeValuePayload, SYSTEM_TIME_WIDTH, TOMBSTONE_SENTINEL,
    };
    use nodedb_types::OrdinalClock;

    const T: TenantId = TenantId::new(1);
    const COLL: &str = "people";

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        (store, dir)
    }

    fn put(
        store: &EdgeStore,
        clock: &OrdinalClock,
        src: &str,
        label: &str,
        dst: &str,
        props: &[u8],
    ) {
        let ord = clock.next_ordinal();
        store
            .put_edge_versioned(T, COLL, src, label, dst, props, ord, ord, i64::MAX)
            .unwrap();
    }

    fn soft_delete(store: &EdgeStore, clock: &OrdinalClock, src: &str, label: &str, dst: &str) {
        let ord = clock.next_ordinal();
        store
            .soft_delete_edge(T, COLL, src, label, dst, ord)
            .unwrap();
    }

    #[test]
    fn put_and_get_edge_current_state() {
        let (store, _dir) = make_store();
        let clock = OrdinalClock::new();
        put(&store, &clock, "alice", "KNOWS", "bob", b"props");

        let result = store.get_edge(T, COLL, "alice", "KNOWS", "bob").unwrap();
        assert_eq!(result, Some(b"props".to_vec()));
    }

    #[test]
    fn get_nonexistent_edge_returns_none() {
        let (store, _dir) = make_store();
        let result = store.get_edge(T, COLL, "alice", "KNOWS", "bob").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn soft_deleted_edge_is_invisible_to_current_state() {
        let (store, _dir) = make_store();
        let clock = OrdinalClock::new();
        put(&store, &clock, "alice", "KNOWS", "bob", b"v1");
        soft_delete(&store, &clock, "alice", "KNOWS", "bob");

        assert!(
            store
                .get_edge(T, COLL, "alice", "KNOWS", "bob")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn put_after_soft_delete_resurrects_edge() {
        let (store, _dir) = make_store();
        let clock = OrdinalClock::new();
        put(&store, &clock, "alice", "KNOWS", "bob", b"v1");
        soft_delete(&store, &clock, "alice", "KNOWS", "bob");
        put(&store, &clock, "alice", "KNOWS", "bob", b"v2");

        assert_eq!(
            store.get_edge(T, COLL, "alice", "KNOWS", "bob").unwrap(),
            Some(b"v2".to_vec())
        );
    }

    #[test]
    fn scan_with_prefix_yields_current_state_only() {
        let (store, _dir) = make_store();
        let clock = OrdinalClock::new();
        put(&store, &clock, "a", "L", "b", b"v1");
        put(&store, &clock, "a", "L", "b", b"v2");
        put(&store, &clock, "a", "L", "c", b"x");
        soft_delete(&store, &clock, "a", "L", "c");

        let edges = store
            .scan_edges_with_prefix(T, &format!("{COLL}\x00a\x00L\x00"), |c, s, l, d| Edge {
                collection: c.to_string(),
                src_id: s.to_string(),
                label: l.to_string(),
                dst_id: d.to_string(),
                properties: Vec::new(),
            })
            .unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].dst_id, "b");
        assert_eq!(edges[0].properties, b"v2");
    }

    #[test]
    fn scan_all_edges_decoded_current_state() {
        let (store, _dir) = make_store();
        let clock = OrdinalClock::new();
        put(&store, &clock, "a", "L", "b", b"v1");
        put(&store, &clock, "a", "L", "b", b"v2");
        put(&store, &clock, "c", "L", "d", b"live");
        soft_delete(&store, &clock, "c", "L", "d");

        let out = store.scan_all_edges_decoded(None).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].5, b"v2".to_vec());
    }

    #[test]
    fn scan_all_edges_decoded_as_of_earlier_ordinal() {
        let (store, _dir) = make_store();
        // Hand-assign ordinals for deterministic AS OF boundaries.
        store
            .put_edge_versioned(T, COLL, "a", "L", "b", b"v1", 100, 100, i64::MAX)
            .unwrap();
        store
            .put_edge_versioned(T, COLL, "a", "L", "b", b"v2", 200, 200, i64::MAX)
            .unwrap();
        store.soft_delete_edge(T, COLL, "a", "L", "b", 300).unwrap();

        let at_150 = store.scan_all_edges_decoded(Some(150)).unwrap();
        assert_eq!(at_150.len(), 1);
        assert_eq!(at_150[0].5, b"v1".to_vec());

        let at_250 = store.scan_all_edges_decoded(Some(250)).unwrap();
        assert_eq!(at_250[0].5, b"v2".to_vec());

        let at_350 = store.scan_all_edges_decoded(Some(350)).unwrap();
        assert!(
            at_350.is_empty(),
            "tombstoned at 300 — must be empty at 350"
        );
    }

    #[test]
    fn delete_edges_for_node_soft_deletes_all_incident() {
        let (store, _dir) = make_store();
        let clock = OrdinalClock::new();
        put(&store, &clock, "alice", "KNOWS", "bob", b"1");
        put(&store, &clock, "alice", "KNOWS", "carol", b"2");
        put(&store, &clock, "dave", "KNOWS", "alice", b"3");
        put(&store, &clock, "eve", "KNOWS", "frank", b"4");

        let purge_ord = clock.next_ordinal();
        store.delete_edges_for_node(T, "alice", purge_ord).unwrap();

        assert!(
            store
                .get_edge(T, COLL, "alice", "KNOWS", "bob")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_edge(T, COLL, "alice", "KNOWS", "carol")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_edge(T, COLL, "dave", "KNOWS", "alice")
                .unwrap()
                .is_none()
        );
        // Unrelated edge stays.
        assert_eq!(
            store.get_edge(T, COLL, "eve", "KNOWS", "frank").unwrap(),
            Some(b"4".to_vec())
        );
    }

    #[test]
    fn tenants_are_isolated() {
        let (store, _dir) = make_store();
        let t1 = TenantId::new(1);
        let t2 = TenantId::new(2);
        store
            .put_edge_versioned(t1, COLL, "a", "L", "b", b"t1", 10, 10, i64::MAX)
            .unwrap();
        store
            .put_edge_versioned(t2, COLL, "a", "L", "b", b"t2", 20, 20, i64::MAX)
            .unwrap();
        assert_eq!(
            store.get_edge(t1, COLL, "a", "L", "b").unwrap(),
            Some(b"t1".to_vec())
        );
        assert_eq!(
            store.get_edge(t2, COLL, "a", "L", "b").unwrap(),
            Some(b"t2".to_vec())
        );
    }

    #[test]
    fn put_edge_raw_preserves_versioned_key_and_mirrors_reverse() {
        let (store, _dir) = make_store();
        let key = versioned_edge_key(COLL, "a", "L", "b", 123).unwrap();
        let payload = EdgeValuePayload::new(0, i64::MAX, b"hello".to_vec())
            .encode()
            .unwrap();
        store.put_edge_raw(T, &key, &payload).unwrap();
        assert_eq!(
            store.get_edge(T, COLL, "a", "L", "b").unwrap(),
            Some(b"hello".to_vec())
        );

        // Tombstone via put_edge_raw mirrors sentinel to reverse too.
        let tkey = versioned_edge_key(COLL, "a", "L", "b", 200).unwrap();
        store.put_edge_raw(T, &tkey, TOMBSTONE_SENTINEL).unwrap();
        assert!(store.get_edge(T, COLL, "a", "L", "b").unwrap().is_none());
    }

    #[test]
    fn put_edge_raw_rejects_unversioned_key() {
        let (store, _dir) = make_store();
        let err = store.put_edge_raw(T, "c\x00a\x00L\x00b", b"x").unwrap_err();
        match err {
            crate::Error::BadRequest { detail } => {
                assert!(detail.contains("malformed versioned key"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    // Sanity: SYSTEM_TIME_WIDTH hasn't drifted.
    #[test]
    fn key_suffix_width_constant_matches() {
        assert_eq!(SYSTEM_TIME_WIDTH, 20);
    }
}
