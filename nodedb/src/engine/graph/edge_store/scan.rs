// SPDX-License-Identifier: BUSL-1.1

//! Current-state scan + Ceiling-backed lookups and raw insert (snapshot restore).

use std::collections::HashMap;

use nodedb_types::{DatabaseId, TenantId};
use redb::{ReadableDatabase, ReadableTable};

use super::store::{
    BaseKey, EDGES, Edge, EdgeRecord, EdgeStore, REVERSE_EDGES, TenantBaseKey, redb_err,
};
use super::temporal::visibility::{
    EDGE_APPLIED, EDGE_CUTS, cut_key, parse_cut_key, read_visibility,
};
use super::temporal::{
    EdgeRef, EdgeValuePayload, is_sentinel, parse_versioned_edge_key, versioned_edge_key,
};

/// A tenant's edge records in one database, as a snapshot captures them.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TenantEdgeScan {
    /// Every forward version no TRUNCATE hides from a current read, as
    /// `(versioned key, raw value)`. A backup carries these alone.
    pub visible: Vec<(String, Vec<u8>)>,
    /// Every forward version a TRUNCATE hides from a current read. A read
    /// as of an earlier system time can still reach one, so a Raft snapshot
    /// carries them with the cuts and applied ordinals.
    pub hidden: Vec<(String, Vec<u8>)>,
    /// Every cut, as `(collection, ordinal)`.
    pub cuts: Vec<(String, i64)>,
    /// Every applied ordinal, under its version's forward versioned key.
    pub applied: Vec<(String, i64)>,
}

impl EdgeStore {
    /// Scan every forward record, cut and applied ordinal belonging to a
    /// tenant. Used by snapshot export.
    pub fn scan_edges_for_tenant(&self, db: u64, tid: TenantId) -> crate::Result<TenantEdgeScan> {
        let t = tid.as_u64();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;
        let mut visibility = read_visibility(&read_txn)?;

        let mut scan = TenantEdgeScan::default();
        let range = table
            .range((db, t, "")..(db, t + 1, ""))
            .map_err(|e| redb_err("edge range", e))?;
        for entry in range {
            let (key, value) = entry.map_err(|e| redb_err("edge entry", e))?;
            let composite = key.value().2;
            let record = (composite.to_string(), value.value().to_vec());
            let hidden = match parse_versioned_edge_key(composite) {
                Some((coll, _, _, _, sys)) => {
                    visibility.hidden(db, t, coll, composite, sys, i64::MAX)?
                }
                None => false,
            };
            if hidden {
                scan.hidden.push(record);
            } else {
                scan.visible.push(record);
            }
        }
        for entry in visibility
            .cuts
            .range((db, t, "")..(db, t + 1, ""))
            .map_err(|e| redb_err("edge cut range", e))?
        {
            let (key, _) = entry.map_err(|e| redb_err("edge cut entry", e))?;
            if let Some((collection, cut)) = parse_cut_key(key.value().2) {
                scan.cuts.push((collection.to_string(), cut));
            }
        }
        for entry in visibility
            .applied
            .range((db, t, "")..(db, t + 1, ""))
            .map_err(|e| redb_err("edge applied range", e))?
        {
            let (key, applied) = entry.map_err(|e| redb_err("edge applied entry", e))?;
            scan.applied
                .push((key.value().2.to_string(), applied.value()));
        }
        Ok(scan)
    }

    /// Scan every forward edge across all tenants in current-state,
    /// yielding `(TenantId, collection, src, label, dst, properties)`.
    /// Tombstoned and erased versions are skipped; only the Ceiling resolution
    /// at `system_as_of` is returned per base. `None` means "current state"
    /// (ordinal = `i64::MAX`).
    pub fn scan_all_edges_decoded(
        &self,
        system_as_of: Option<i64>,
    ) -> crate::Result<Vec<EdgeRecord>> {
        let cutoff = system_as_of.unwrap_or(i64::MAX);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;
        let mut visibility = read_visibility(&read_txn)?;

        // Keys sort by base, then by system time ascending, so the last
        // visible version seen for a base is its newest.
        let mut latest: HashMap<TenantBaseKey, Vec<u8>> = HashMap::new();
        for entry in table.iter().map_err(|e| redb_err("iter", e))? {
            let (k, v) = entry.map_err(|e| redb_err("iter entry", e))?;
            let (db, t, composite) = k.value();
            let Some((coll, src, label, dst, sys)) = parse_versioned_edge_key(composite) else {
                continue;
            };
            if !visibility.visible_at(db, t, coll, composite, sys, cutoff)? {
                continue;
            }
            let base: TenantBaseKey = (
                db,
                t,
                coll.to_string(),
                src.to_string(),
                label.to_string(),
                dst.to_string(),
            );
            latest.insert(base, v.value().to_vec());
        }

        let mut out = Vec::with_capacity(latest.len());
        for ((db, t, coll, src, label, dst), bytes) in latest {
            if is_sentinel(&bytes) {
                continue;
            }
            out.push((
                DatabaseId::new(db),
                TenantId::new(t),
                coll,
                src,
                label,
                dst,
                EdgeValuePayload::decode(&bytes)?.properties,
            ));
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
        db: u64,
        tid: TenantId,
        composite_key: &str,
        value: &[u8],
    ) -> crate::Result<()> {
        let t = tid.as_u64();
        let (collection, src, label, dst, sys) = parse_versioned_edge_key(composite_key)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("put_edge_raw: malformed versioned key {composite_key:?}"),
            })?;
        let rev_key = versioned_edge_key(collection, dst, label, src, sys)?;
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
                .insert((db, t, composite_key), value)
                .map_err(|e| redb_err("insert edge", e))?;
            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            rev_t
                .insert((db, t, rev_key.as_str()), rev_value)
                .map_err(|e| redb_err("insert reverse", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit edge", e))?;
        Ok(())
    }

    /// Insert a raw cut of `collection` at `cut` (for snapshot restore). The
    /// snapshot's versions install with [`Self::put_edge_raw`], and the cuts
    /// and applied ordinals with them, so every read resolves as it did on
    /// the snapshot's source.
    pub fn put_edge_cut_raw(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
        cut: i64,
    ) -> crate::Result<()> {
        let key = cut_key(collection, cut)?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut cuts = write_txn
                .open_table(EDGE_CUTS)
                .map_err(|e| redb_err("open edge_cuts", e))?;
            cuts.insert((db, tid.as_u64(), key.as_str()), ())
                .map_err(|e| redb_err("insert edge cut", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit edge cut", e))
    }

    /// Insert a raw applied ordinal for the version under `composite_key`
    /// (for snapshot restore). See [`Self::put_edge_cut_raw`].
    pub fn put_edge_applied_raw(
        &self,
        db: u64,
        tid: TenantId,
        composite_key: &str,
        applied: i64,
    ) -> crate::Result<()> {
        if parse_versioned_edge_key(composite_key).is_none() {
            return Err(crate::Error::BadRequest {
                detail: format!("put_edge_applied_raw: malformed versioned key {composite_key:?}"),
            });
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut table = write_txn
                .open_table(EDGE_APPLIED)
                .map_err(|e| redb_err("open edge_applied", e))?;
            table
                .insert((db, tid.as_u64(), composite_key), applied)
                .map_err(|e| redb_err("insert edge applied ordinal", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit edge applied ordinal", e))
    }

    /// Get an edge's current-state properties. `None` if no live version.
    pub fn get_edge(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
        src: &str,
        label: &str,
        dst: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        self.ceiling_resolve_edge(
            EdgeRef::new(DatabaseId::new(db), tid, collection, src, label, dst),
            i64::MAX,
            None,
        )
    }

    /// Scan forward edges under a tenant with a base-key prefix, applying
    /// current-state semantics: yields exactly one `Edge` per base that has
    /// a live latest version. Used by `neighbors_out` and friends.
    pub(super) fn scan_edges_with_prefix<F>(
        &self,
        db: u64,
        tid: TenantId,
        prefix: &str,
        mut make_edge: F,
    ) -> crate::Result<Vec<Edge>>
    where
        F: FnMut(&str, &str, &str, &str) -> Edge,
    {
        let t = tid.as_u64();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;
        let mut visibility = read_visibility(&read_txn)?;

        let mut latest: HashMap<BaseKey, (i64, Vec<u8>)> = HashMap::new();

        let range = table
            .range((db, t, prefix)..)
            .map_err(|e| redb_err("range", e))?;

        for entry in range {
            let (key, val) = entry.map_err(|e| redb_err("iter", e))?;
            let (kd, kt, composite) = key.value();
            if kd != db || kt != t || !composite.starts_with(prefix) {
                break;
            }
            let Some((coll, src, label, dst, sys)) = parse_versioned_edge_key(composite) else {
                continue;
            };
            if !visibility.visible_at(db, t, coll, composite, sys, i64::MAX)? {
                continue;
            }
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
            let mut edge = make_edge(&coll, &src, &label, &dst);
            edge.properties = EdgeValuePayload::decode(&bytes)?.properties;
            edges.push(edge);
        }
        Ok(edges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::edge_store::temporal::TOMBSTONE_SENTINEL;
    use nodedb_types::OrdinalClock;

    const T: TenantId = TenantId::new(1);
    const DB: DatabaseId = DatabaseId::DEFAULT;
    const D: u64 = 0;
    const COLL: &str = "people";

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        (store, dir)
    }

    fn e<'a>(src: &'a str, label: &'a str, dst: &'a str) -> EdgeRef<'a> {
        EdgeRef::new(DB, T, COLL, src, label, dst)
    }

    fn put(store: &EdgeStore, clock: &OrdinalClock, src: &str, label: &str, dst: &str, p: &[u8]) {
        let ord = clock.next_ordinal();
        store
            .put_edge_versioned(e(src, label, dst), p, ord, ord, i64::MAX)
            .unwrap();
    }

    fn soft_delete(store: &EdgeStore, clock: &OrdinalClock, src: &str, label: &str, dst: &str) {
        let ord = clock.next_ordinal();
        store.soft_delete_edge(e(src, label, dst), ord).unwrap();
    }

    #[test]
    fn put_and_get_edge_current_state() {
        let (store, _dir) = make_store();
        let clock = OrdinalClock::new();
        put(&store, &clock, "alice", "KNOWS", "bob", b"props");

        let result = store.get_edge(D, T, COLL, "alice", "KNOWS", "bob").unwrap();
        assert_eq!(result, Some(b"props".to_vec()));
    }

    #[test]
    fn get_nonexistent_edge_returns_none() {
        let (store, _dir) = make_store();
        let result = store.get_edge(D, T, COLL, "alice", "KNOWS", "bob").unwrap();
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
                .get_edge(D, T, COLL, "alice", "KNOWS", "bob")
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
            store.get_edge(D, T, COLL, "alice", "KNOWS", "bob").unwrap(),
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
            .scan_edges_with_prefix(D, T, &format!("{COLL}\x00a\x00L\x00"), |c, s, l, d| Edge {
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
        assert_eq!(out[0].6, b"v2".to_vec());
    }

    #[test]
    fn scan_all_edges_decoded_as_of_earlier_ordinal() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(e("a", "L", "b"), b"v1", 100, 100, i64::MAX)
            .unwrap();
        store
            .put_edge_versioned(e("a", "L", "b"), b"v2", 200, 200, i64::MAX)
            .unwrap();
        store.soft_delete_edge(e("a", "L", "b"), 300).unwrap();

        let at_150 = store.scan_all_edges_decoded(Some(150)).unwrap();
        assert_eq!(at_150.len(), 1);
        assert_eq!(at_150[0].6, b"v1".to_vec());

        let at_250 = store.scan_all_edges_decoded(Some(250)).unwrap();
        assert_eq!(at_250[0].6, b"v2".to_vec());

        let at_350 = store.scan_all_edges_decoded(Some(350)).unwrap();
        assert!(
            at_350.is_empty(),
            "tombstoned at 300 — must be empty at 350"
        );
    }

    #[test]
    fn tenants_are_isolated() {
        let (store, _dir) = make_store();
        let t1 = TenantId::new(1);
        let t2 = TenantId::new(2);
        store
            .put_edge_versioned(
                EdgeRef::new(DB, t1, COLL, "a", "L", "b"),
                b"t1",
                10,
                10,
                i64::MAX,
            )
            .unwrap();
        store
            .put_edge_versioned(
                EdgeRef::new(DB, t2, COLL, "a", "L", "b"),
                b"t2",
                20,
                20,
                i64::MAX,
            )
            .unwrap();
        assert_eq!(
            store.get_edge(D, t1, COLL, "a", "L", "b").unwrap(),
            Some(b"t1".to_vec())
        );
        assert_eq!(
            store.get_edge(D, t2, COLL, "a", "L", "b").unwrap(),
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
        store.put_edge_raw(D, T, &key, &payload).unwrap();
        assert_eq!(
            store.get_edge(D, T, COLL, "a", "L", "b").unwrap(),
            Some(b"hello".to_vec())
        );

        let tkey = versioned_edge_key(COLL, "a", "L", "b", 200).unwrap();
        store.put_edge_raw(D, T, &tkey, TOMBSTONE_SENTINEL).unwrap();
        assert!(store.get_edge(D, T, COLL, "a", "L", "b").unwrap().is_none());
    }

    #[test]
    fn put_edge_raw_rejects_unversioned_key() {
        let (store, _dir) = make_store();
        let err = store
            .put_edge_raw(D, T, "c\x00a\x00L\x00b", b"x")
            .unwrap_err();
        match err {
            crate::Error::BadRequest { detail } => {
                assert!(detail.contains("malformed versioned key"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
