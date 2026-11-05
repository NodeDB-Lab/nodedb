// SPDX-License-Identifier: BUSL-1.1

//! History purge of the edge store below a system-time horizon.
//!
//! A read at or above horizon `H` resolves each edge to its newest version
//! at or below the read's system time that no TRUNCATE cut hides. The purge
//! removes every version no such read reaches:
//!
//! - a version at or below `H` that a cut hides at `H`: a later cut hides
//!   it too;
//! - a version older than the version visible at `H`, applied no later
//!   than it: a cut that hides the visible version hides it too;
//! - the version visible at `H` when it is a tombstone and no version
//!   follows it: the edge then reads as absent at every such cut.
//!
//! Versions above `H` stay. A TRUNCATE cut at or below `H` that a newer cut
//! at or below `H` shadows goes as well.

use super::keys::{is_tombstone, parse_versioned_edge_key, versioned_edge_key};
use super::visibility::{EDGE_APPLIED, EDGE_CUTS, ReadVisibility, cut_key, read_visibility};
use crate::engine::graph::edge_store::home_bytes::EdgeStorageDelta;
use crate::engine::graph::edge_store::storage_bytes::{
    EDGE_COUNTER, remove_applied_row, remove_edge_row,
};
use crate::engine::graph::edge_store::store::{EDGES, EdgeStore, REVERSE_EDGES, redb_err};
use nodedb_types::TenantId;
use nodedb_types::temporal::ms_to_ordinal_upper;
use redb::{ReadableDatabase, ReadableTable};

/// What one purge of a collection removed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EdgePurgeCount {
    /// Edge versions removed, each with its reverse entry.
    pub versions: usize,
    /// Shadowed TRUNCATE cuts removed.
    pub cuts: usize,
}

impl EdgeStore {
    /// The `audit_retention` purge of `(tid, collection)`: the purge at the
    /// last ordinal of millisecond `cutoff_system_ms`
    /// ([`Self::purge_edge_versions_below`]). Returns the versions removed.
    pub fn purge_superseded_versions(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> crate::Result<usize> {
        let horizon = ms_to_ordinal_upper(cutoff_system_ms);
        Ok(self
            .purge_edge_versions_below(db, tid, collection, horizon)?
            .versions)
    }

    /// Remove every edge version and TRUNCATE cut of `(tid, collection)`
    /// that no read at or above `horizon` reaches.
    ///
    /// Deletes from `EDGES`, `REVERSE_EDGES` and `EDGE_APPLIED` inside one
    /// write transaction, so the indexes never diverge mid-purge.
    pub fn purge_edge_versions_below(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
        horizon: i64,
    ) -> crate::Result<EdgePurgeCount> {
        let t = tid.as_u64();
        let (victims, cuts) = self.collect_purge_victims(db, t, collection, horizon)?;
        if victims.is_empty() && cuts.is_empty() {
            return Ok(EdgePurgeCount::default());
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write purge", e))?;
        let mut delta = EdgeStorageDelta::default();
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges purge", e))?;
            let mut rev = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse purge", e))?;
            let mut applied = write_txn
                .open_table(EDGE_APPLIED)
                .map_err(|e| redb_err("open edge_applied purge", e))?;
            for v in &victims {
                let fwd = (db, t, v.fwd_key.as_str());
                remove_edge_row(&mut edges, fwd, &mut delta, "remove edge")?;
                let rev_key = (db, t, v.rev_key.as_str());
                remove_edge_row(&mut rev, rev_key, &mut delta, "remove reverse")?;
                let what = "remove edge applied ordinal";
                remove_applied_row(&mut applied, fwd, &mut delta, what)?;
            }
            let mut cut_rows = write_txn
                .open_table(EDGE_CUTS)
                .map_err(|e| redb_err("open edge_cuts purge", e))?;
            for cut in &cuts {
                let key = cut_key(collection, *cut)?;
                cut_rows
                    .remove((db, t, key.as_str()))
                    .map_err(|e| redb_err("remove edge cut", e))?;
            }
        }
        delta.apply(&write_txn, EDGE_COUNTER)?;
        write_txn
            .commit()
            .map_err(|e| redb_err("commit purge", e))?;
        Ok(EdgePurgeCount {
            versions: victims.len(),
            cuts: cuts.len(),
        })
    }

    /// The versions and the cuts the purge at `horizon` removes.
    fn collect_purge_victims(
        &self,
        db: u64,
        t: u64,
        collection: &str,
        horizon: i64,
    ) -> crate::Result<(Vec<Victim>, Vec<i64>)> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read purge", e))?;
        let edges = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges read purge", e))?;
        let mut visibility = read_visibility(&read_txn)?;

        // Versioned keys sort by base, then by system time ascending.
        let prefix = format!("{collection}\x00");
        let range = edges
            .range((db, t, prefix.as_str())..)
            .map_err(|e| redb_err("range purge", e))?;

        let mut victims: Vec<Victim> = Vec::new();
        let mut group: Option<BaseVersions> = None;
        for entry in range {
            let (k, v) = entry.map_err(|e| redb_err("entry purge", e))?;
            let (kd, kt, key_str) = k.value();
            if kd != db || kt != t || !key_str.starts_with(&prefix) {
                break;
            }
            let Some((_, src, label, dst, system_from)) = parse_versioned_edge_key(key_str) else {
                continue;
            };
            let version = StoredVersion {
                system_from,
                tombstone: is_tombstone(v.value()),
                fwd_key: key_str.to_string(),
            };
            let base = Base {
                src: src.to_string(),
                label: label.to_string(),
                dst: dst.to_string(),
            };
            match group.as_mut() {
                Some(current) if current.base == base => current.versions.push(version),
                _ => {
                    if let Some(done) = group.take() {
                        done.push_victims(
                            &mut visibility,
                            (db, t, collection),
                            horizon,
                            &mut victims,
                        )?;
                    }
                    group = Some(BaseVersions {
                        base,
                        versions: vec![version],
                    });
                }
            }
        }
        if let Some(done) = group {
            done.push_victims(&mut visibility, (db, t, collection), horizon, &mut victims)?;
        }
        let cuts = visibility.cuts_shadowed_at(db, t, collection, horizon)?;
        Ok((victims, cuts))
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Base {
    src: String,
    label: String,
    dst: String,
}

/// One stored version of a base.
struct StoredVersion {
    system_from: i64,
    tombstone: bool,
    fwd_key: String,
}

/// Every version of one base, ascending by `system_from`.
struct BaseVersions {
    base: Base,
    versions: Vec<StoredVersion>,
}

impl BaseVersions {
    /// Push the versions no read at or above `horizon` reaches.
    fn push_victims(
        self,
        visibility: &mut ReadVisibility,
        (db, t, collection): (u64, u64, &str),
        horizon: i64,
        victims: &mut Vec<Victim>,
    ) -> crate::Result<()> {
        let at_or_below = self
            .versions
            .partition_point(|version| version.system_from <= horizon);
        let mut hidden = Vec::with_capacity(at_or_below);
        for version in &self.versions[..at_or_below] {
            hidden.push(visibility.hidden(
                db,
                t,
                collection,
                &version.fwd_key,
                version.system_from,
                horizon,
            )?);
        }
        let visible = hidden.iter().rposition(|is_hidden| !is_hidden);
        let visible_applied = match visible {
            Some(index) => {
                let version = &self.versions[index];
                Some(visibility.applied_at(db, t, &version.fwd_key, version.system_from)?)
            }
            None => None,
        };
        let nothing_after = at_or_below == self.versions.len();
        for (index, version) in self.versions[..at_or_below].iter().enumerate() {
            let goes = match (visible, visible_applied) {
                _ if hidden[index] => true,
                (Some(visible), Some(applied)) if index < visible => {
                    visibility.applied_at(db, t, &version.fwd_key, version.system_from)? <= applied
                }
                (Some(visible), _) if index == visible => version.tombstone && nothing_after,
                _ => false,
            };
            if goes {
                victims.push(Victim::new(collection, &self.base, version.system_from)?);
            }
        }
        Ok(())
    }
}

struct Victim {
    fwd_key: String,
    rev_key: String,
}

impl Victim {
    fn new(collection: &str, base: &Base, system_from: i64) -> crate::Result<Self> {
        let fwd_key =
            versioned_edge_key(collection, &base.src, &base.label, &base.dst, system_from)?;
        let rev_key =
            versioned_edge_key(collection, &base.dst, &base.label, &base.src, system_from)?;
        Ok(Self { fwd_key, rev_key })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::edge_store::temporal::keys::EdgeRef;
    use nodedb_types::DatabaseId;

    const DB: DatabaseId = DatabaseId::DEFAULT;
    const D: u64 = 0;

    fn fresh_store() -> (tempfile::TempDir, EdgeStore) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.redb");
        let store = EdgeStore::open(&path).unwrap();
        (dir, store)
    }

    fn put(store: &EdgeStore, edge: EdgeRef<'_>, system_from: i64) {
        store
            .put_edge_versioned(edge, b"v1", system_from, 0, i64::MAX)
            .unwrap();
    }

    fn count_edges(store: &EdgeStore, tid: TenantId, collection: &str) -> usize {
        let read_txn = store.db.begin_read().unwrap();
        let edges = read_txn.open_table(EDGES).unwrap();
        let low = (D, tid.as_u64(), "");
        let high = (D, tid.as_u64() + 1, "");
        edges
            .range(low..high)
            .unwrap()
            .filter_map(|r| {
                let (k, _) = r.ok()?;
                let (_d, _t, s) = k.value();
                let (c, ..) = parse_versioned_edge_key(s)?;
                (c == collection).then_some(())
            })
            .count()
    }

    fn delete(store: &EdgeStore, edge: EdgeRef<'_>, system_from: i64) {
        store.soft_delete_edge(edge, system_from).unwrap();
    }

    /// The version visible at the horizon stays; the one it superseded goes.
    #[test]
    fn purge_keeps_the_version_visible_at_the_horizon() {
        let (_dir, store) = fresh_store();
        let tid = TenantId::new(1);
        let edge = EdgeRef::new(DB, tid, "c", "a", "L", "b");
        for ms in [100i64, 200, 300] {
            put(&store, edge, ms_to_ordinal_upper(ms));
        }
        assert_eq!(count_edges(&store, tid, "c"), 3);

        let at_150 = store.purge_superseded_versions(D, tid, "c", 150).unwrap();
        assert_eq!(at_150, 0, "v@100 is the version a read at 150 sees");

        let at_250 = store.purge_superseded_versions(D, tid, "c", 250).unwrap();
        assert_eq!(at_250, 1, "v@100 is superseded by v@200 below the horizon");
        assert_eq!(count_edges(&store, tid, "c"), 2);
    }

    #[test]
    fn purge_never_deletes_the_latest_live_version() {
        let (_dir, store) = fresh_store();
        let tid = TenantId::new(1);
        let edge = EdgeRef::new(DB, tid, "c", "a", "L", "b");
        put(&store, edge, ms_to_ordinal_upper(100));

        let purged = store
            .purge_superseded_versions(D, tid, "c", 10_000)
            .unwrap();
        assert_eq!(purged, 0);
        assert_eq!(count_edges(&store, tid, "c"), 1);
    }

    /// A tombstone at or below the horizon with nothing after it removes the
    /// edge entirely. One followed by a later version stays.
    #[test]
    fn purge_removes_an_edge_deleted_below_the_horizon() {
        let (_dir, store) = fresh_store();
        let tid = TenantId::new(1);
        let gone = EdgeRef::new(DB, tid, "c", "a", "L", "b");
        put(&store, gone, ms_to_ordinal_upper(100));
        delete(&store, gone, ms_to_ordinal_upper(200));
        let back = EdgeRef::new(DB, tid, "c", "x", "L", "y");
        put(&store, back, ms_to_ordinal_upper(100));
        delete(&store, back, ms_to_ordinal_upper(200));
        put(&store, back, ms_to_ordinal_upper(400));

        let purged = store
            .purge_edge_versions_below(D, tid, "c", ms_to_ordinal_upper(300))
            .unwrap();
        assert_eq!(purged.versions, 3, "a->b whole, x->y's first version");
        assert_eq!(
            count_edges(&store, tid, "c"),
            2,
            "x->y keeps its tombstone and head"
        );
    }

    #[test]
    fn purge_is_per_tenant_collection_scoped() {
        let (_dir, store) = fresh_store();
        let tid = TenantId::new(1);
        let other_tenant = TenantId::new(2);
        let e1 = EdgeRef::new(DB, tid, "c", "a", "L", "b");
        let e_other_tenant = EdgeRef::new(DB, other_tenant, "c", "a", "L", "b");
        let e_other_coll = EdgeRef::new(DB, tid, "d", "a", "L", "b");

        for ms in [100i64, 200, 300] {
            put(&store, e1, ms_to_ordinal_upper(ms));
            put(&store, e_other_tenant, ms_to_ordinal_upper(ms));
            put(&store, e_other_coll, ms_to_ordinal_upper(ms));
        }

        let purged = store.purge_superseded_versions(D, tid, "c", 250).unwrap();
        assert_eq!(purged, 1);
        assert_eq!(count_edges(&store, tid, "c"), 2);
        assert_eq!(count_edges(&store, tid, "d"), 3);
        assert_eq!(count_edges(&store, other_tenant, "c"), 3);
    }

    #[test]
    fn purge_is_idempotent() {
        let (_dir, store) = fresh_store();
        let tid = TenantId::new(1);
        let edge = EdgeRef::new(DB, tid, "c", "a", "L", "b");
        for ms in [100i64, 200, 300] {
            put(&store, edge, ms_to_ordinal_upper(ms));
        }
        let first = store.purge_superseded_versions(D, tid, "c", 250).unwrap();
        let second = store.purge_superseded_versions(D, tid, "c", 250).unwrap();
        assert_eq!(first, 1);
        assert_eq!(second, 0);
    }

    /// A TRUNCATE cut shadowed by a newer cut at or below the horizon goes.
    #[test]
    fn purge_removes_shadowed_cuts() {
        let (_dir, store) = fresh_store();
        let tid = TenantId::new(1);
        store.install_edge_cut(DB, tid, "c", 1_000).unwrap();
        store.install_edge_cut(DB, tid, "c", 2_000).unwrap();
        store.install_edge_cut(DB, tid, "c", 9_000).unwrap();
        let purged = store.purge_edge_versions_below(D, tid, "c", 5_000).unwrap();
        assert_eq!(purged.cuts, 1, "only the cut at 1000 is shadowed at 5000");
        let again = store.purge_edge_versions_below(D, tid, "c", 5_000).unwrap();
        assert_eq!(again.cuts, 0);
    }
}
