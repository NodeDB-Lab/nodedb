// SPDX-License-Identifier: BUSL-1.1

//! Ceiling resolver — reverse range scan with optional valid-time filter.

use super::keys::{
    EdgeRef, edge_version_prefix, is_gdpr_erasure, is_tombstone, parse_versioned_edge_key,
    versioned_edge_key,
};
use super::payload::EdgeValuePayload;
use super::visibility::{ReadVisibility, StoreKey, read_visibility, visible_version};
use crate::engine::graph::edge_store::store::{EDGES, EdgeStore, redb_err};
use redb::{ReadOnlyTable, ReadableDatabase, ReadableTable};

/// Current-state edge properties under one read transaction, for many
/// lookups. A traversal hop opens one and reads every crossed edge through it.
pub struct EdgePropertyReader {
    edges: ReadOnlyTable<StoreKey, &'static [u8]>,
    visibility: ReadVisibility,
}

impl EdgeStore {
    /// Open a reader that resolves current-state properties for many edges.
    pub fn property_reader(&self) -> crate::Result<EdgePropertyReader> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let edges = txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;
        let visibility = read_visibility(&txn)?;
        Ok(EdgePropertyReader { edges, visibility })
    }
}

impl EdgePropertyReader {
    /// The properties of `edge`'s current version, or `None` when it has no
    /// live version. A TRUNCATE-hidden version is skipped. Valid time is
    /// not consulted.
    pub fn current(&mut self, edge: EdgeRef<'_>) -> crate::Result<Option<Vec<u8>>> {
        match visible_version(&self.edges, &mut self.visibility, &edge, i64::MAX)? {
            Some((_, bytes)) if !is_tombstone(&bytes) && !is_gdpr_erasure(&bytes) => {
                Ok(Some(EdgeValuePayload::decode(&bytes)?.properties))
            }
            _ => Ok(None),
        }
    }
}

impl EdgeStore {
    /// Resolve the Ceiling: the latest version of
    /// `(collection, src, label, dst)` whose `system_from ≤ system_as_of`.
    ///
    /// Returns `Ok(None)` if no version exists at or before the cutoff, or if
    /// the latest qualifying version is a tombstone/GDPR erasure.
    ///
    /// When `valid_at_ms` is supplied, the resolved version must also satisfy
    /// `valid_from_ms ≤ valid_at_ms < valid_until_ms`; otherwise the method
    /// continues scanning to earlier system-time versions.
    pub fn ceiling_resolve_edge(
        &self,
        edge: EdgeRef<'_>,
        system_as_of: i64,
        valid_at_ms: Option<i64>,
    ) -> crate::Result<Option<Vec<u8>>> {
        if system_as_of < 0 {
            return Err(crate::Error::BadRequest {
                detail: format!("ceiling_resolve_edge: negative system_as_of={system_as_of}"),
            });
        }
        let prefix = edge_version_prefix(edge.collection, edge.src, edge.label, edge.dst);
        let upper = versioned_edge_key(
            edge.collection,
            edge.src,
            edge.label,
            edge.dst,
            system_as_of,
        )?;
        let d = edge.db.as_u64();
        let t = edge.tid.as_u64();

        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;
        let mut visibility = read_visibility(&read_txn)?;

        // Inclusive upper — the exact key at system_as_of is a valid ceiling.
        let range = table
            .range((d, t, prefix.as_str())..=(d, t, upper.as_str()))
            .map_err(|e| redb_err("ceiling range", e))?;

        // Walk newest-first by reversing the iterator.
        for entry in range.rev() {
            let (k, v) = entry.map_err(|e| redb_err("ceiling iter", e))?;
            let (kd, kt, composite) = k.value();
            if kd != d || kt != t || !composite.starts_with(&prefix) {
                break;
            }
            let Some((_, _, _, _, sys)) = parse_versioned_edge_key(composite) else {
                continue;
            };
            // A version a TRUNCATE hides is skipped: the edge resolves to its
            // newest version the cut leaves visible.
            if visibility.hidden(d, t, edge.collection, composite, sys, system_as_of)? {
                continue;
            }
            let bytes = v.value();
            if is_tombstone(bytes) || is_gdpr_erasure(bytes) {
                return Ok(None);
            }
            let payload = EdgeValuePayload::decode(bytes)?;
            match valid_at_ms {
                Some(vt) if !(payload.valid_from_ms <= vt && vt < payload.valid_until_ms) => {
                    // This system-time version didn't assert the fact at `vt` —
                    // scan older versions.
                    continue;
                }
                _ => return Ok(Some(payload.properties)),
            }
        }
        Ok(None)
    }

    /// The `system_from` of the newest stored version of
    /// `(collection, src, label, dst)`, tombstones included, or `None` when
    /// the edge has no version.
    pub fn latest_version_ordinal(&self, edge: EdgeRef<'_>) -> crate::Result<Option<i64>> {
        let prefix = edge_version_prefix(edge.collection, edge.src, edge.label, edge.dst);
        let upper = versioned_edge_key(edge.collection, edge.src, edge.label, edge.dst, i64::MAX)?;
        let d = edge.db.as_u64();
        let t = edge.tid.as_u64();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;
        let mut range = table
            .range((d, t, prefix.as_str())..=(d, t, upper.as_str()))
            .map_err(|e| redb_err("latest version range", e))?;
        match range.next_back() {
            Some(entry) => {
                let (k, _) = entry.map_err(|e| redb_err("latest version iter", e))?;
                let (_, _, composite) = k.value();
                Ok(parse_versioned_edge_key(composite).map(|(_, _, _, _, sys)| sys))
            }
            None => Ok(None),
        }
    }

    /// The `system_from` of the newest stored version of
    /// `(collection, src, label, dst)` that another write applied: a version
    /// applied at `applied` is skipped, tombstones included. `None` when no
    /// other write stored a version.
    ///
    /// A Calvin transaction writes an edge on both endpoint homes. When one
    /// core holds both, the other home's version of the same transaction can
    /// be stored before this home resolves. Skipping it makes the answer the
    /// same on every home and replica: the versions of every earlier writer.
    pub fn latest_version_ordinal_of_other_writes(
        &self,
        edge: EdgeRef<'_>,
        applied: i64,
    ) -> crate::Result<Option<i64>> {
        let prefix = edge_version_prefix(edge.collection, edge.src, edge.label, edge.dst);
        let upper = versioned_edge_key(edge.collection, edge.src, edge.label, edge.dst, i64::MAX)?;
        let d = edge.db.as_u64();
        let t = edge.tid.as_u64();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;
        let visibility = read_visibility(&read_txn)?;
        let range = table
            .range((d, t, prefix.as_str())..=(d, t, upper.as_str()))
            .map_err(|e| redb_err("latest version range", e))?;
        for entry in range.rev() {
            let (k, _) = entry.map_err(|e| redb_err("latest version iter", e))?;
            let composite = k.value().2;
            let Some((_, _, _, _, sys)) = parse_versioned_edge_key(composite) else {
                continue;
            };
            if visibility.applied_at(d, t, composite, sys)? != applied {
                return Ok(Some(sys));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, TenantId};

    use super::*;

    const T: TenantId = TenantId::new(1);
    const DB: DatabaseId = DatabaseId::DEFAULT;
    const COLL: &str = "people";

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        (store, dir)
    }

    fn e<'a>(src: &'a str, label: &'a str, dst: &'a str) -> EdgeRef<'a> {
        EdgeRef::new(DB, T, COLL, src, label, dst)
    }

    /// The reader's answer for `edge` alongside `get_edge`'s, each from a
    /// fresh read transaction.
    fn both(store: &EdgeStore, src: &str, dst: &str) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        let read = store
            .property_reader()
            .unwrap()
            .current(e(src, "L", dst))
            .unwrap();
        let get = store.get_edge(DB.as_u64(), T, COLL, src, "L", dst).unwrap();
        (read, get)
    }

    #[test]
    fn property_reader_matches_get_edge_across_versions_and_sentinels() {
        let (store, _dir) = make_store();
        assert_eq!(both(&store, "a", "b"), (None, None));

        store
            .put_edge_versioned(e("a", "L", "b"), b"v1", 100, 100, i64::MAX)
            .unwrap();
        assert_eq!(
            both(&store, "a", "b"),
            (Some(b"v1".to_vec()), Some(b"v1".to_vec()))
        );

        store
            .put_edge_versioned(e("a", "L", "b"), b"v2", 200, 200, i64::MAX)
            .unwrap();
        assert_eq!(
            both(&store, "a", "b"),
            (Some(b"v2".to_vec()), Some(b"v2".to_vec()))
        );

        store.soft_delete_edge(e("a", "L", "b"), 300).unwrap();
        assert_eq!(both(&store, "a", "b"), (None, None));

        store
            .put_edge_versioned(e("a", "L", "b"), b"v3", 400, 400, i64::MAX)
            .unwrap();
        assert_eq!(
            both(&store, "a", "b"),
            (Some(b"v3".to_vec()), Some(b"v3".to_vec()))
        );

        store.gdpr_erase_edge(e("a", "L", "b"), 500).unwrap();
        assert_eq!(both(&store, "a", "b"), (None, None));
    }

    #[test]
    fn property_reader_skips_a_truncate_hidden_version() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(e("a", "L", "c"), b"old", 100, 100, i64::MAX)
            .unwrap();
        store.install_edge_cut(DB, T, COLL, 150).unwrap();
        assert_eq!(both(&store, "a", "c"), (None, None));

        store
            .put_edge_versioned(e("a", "L", "c"), b"new", 200, 200, i64::MAX)
            .unwrap();
        assert_eq!(
            both(&store, "a", "c"),
            (Some(b"new".to_vec()), Some(b"new".to_vec()))
        );
    }

    #[test]
    fn one_property_reader_serves_many_edges() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(e("a", "L", "b"), b"ab", 100, 100, i64::MAX)
            .unwrap();
        store
            .put_edge_versioned(e("a", "L", "c"), b"ac", 110, 110, i64::MAX)
            .unwrap();
        let mut reader = store.property_reader().unwrap();
        assert_eq!(
            reader.current(e("a", "L", "b")).unwrap(),
            Some(b"ab".to_vec())
        );
        assert_eq!(
            reader.current(e("a", "L", "c")).unwrap(),
            Some(b"ac".to_vec())
        );
        assert_eq!(reader.current(e("a", "L", "z")).unwrap(), None);
    }

    #[test]
    fn latest_version_ordinal_reads_the_newest_version_of_one_edge() {
        let (store, _dir) = make_store();
        assert_eq!(
            store.latest_version_ordinal(e("a", "L", "b")).unwrap(),
            None
        );
        for sys in [100, 300, 200] {
            store
                .put_edge_versioned(e("a", "L", "b"), b"v", sys, sys, i64::MAX)
                .unwrap();
        }
        store
            .put_edge_versioned(e("a", "L", "bb"), b"v", 900, 900, i64::MAX)
            .unwrap();
        store
            .put_edge_versioned(e("a", "L", "c"), b"v", 40, 40, i64::MAX)
            .unwrap();
        store.soft_delete_edge(e("a", "L", "c"), 50).unwrap();
        assert_eq!(
            store.latest_version_ordinal(e("a", "L", "b")).unwrap(),
            Some(300)
        );
        assert_eq!(
            store.latest_version_ordinal(e("a", "L", "c")).unwrap(),
            Some(50),
            "a tombstone is a version"
        );
    }

    #[test]
    fn put_and_ceiling_resolves_latest_at_cutoff() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(e("a", "L", "b"), b"v1", 100, 100, i64::MAX)
            .unwrap();
        store
            .put_edge_versioned(e("a", "L", "b"), b"v2", 200, 200, i64::MAX)
            .unwrap();
        store
            .put_edge_versioned(e("a", "L", "b"), b"v3", 300, 300, i64::MAX)
            .unwrap();

        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 99, None)
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 100, None)
                .unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 250, None)
                .unwrap(),
            Some(b"v2".to_vec())
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 1_000, None)
                .unwrap(),
            Some(b"v3".to_vec())
        );
    }

    #[test]
    fn valid_time_filter_skips_nonmatching_versions() {
        let (store, _dir) = make_store();
        // v1: valid_time [0, 100)
        store
            .put_edge_versioned(e("a", "L", "b"), b"v1", 10, 0, 100)
            .unwrap();
        // v2: valid_time [200, 300)  — disjoint hole between 100 and 200
        store
            .put_edge_versioned(e("a", "L", "b"), b"v2", 20, 200, 300)
            .unwrap();

        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 1_000, Some(150))
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 1_000, Some(50))
                .unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 1_000, Some(250))
                .unwrap(),
            Some(b"v2".to_vec())
        );
    }
}
