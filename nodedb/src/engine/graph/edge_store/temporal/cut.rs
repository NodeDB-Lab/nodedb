// SPDX-License-Identifier: BUSL-1.1

//! Install and remove a TRUNCATE's cut of an edge collection.
//!
//! A cut writes no edge version. It records the TRUNCATE's ordinal, and
//! every read then hides the versions of the collection applied below it
//! (see [`super::visibility`]). The install reports each edge whose current
//! state the cut changed, so the caller brings the CSR along.
//!
//! The persistent counters cannot follow a cut edge by edge: a store holds
//! both homes' copies of an edge, and only the source home's copy was
//! counted. A cut that changes any edge therefore marks the collection's
//! summary for an exact scan, as a store without ownership-aware counters
//! is read.

use std::collections::BTreeMap;

use nodedb_types::{DatabaseId, TenantId};
use redb::{ReadableTable, WriteTransaction};

use super::keys::parse_versioned_edge_key;
use super::visibility::{WriteVisibility, cut_key, write_visibility};
use super::write::live_properties;
use crate::engine::graph::edge_store::stats::table::{GRAPH_STATS, SummaryRow, summary_key};
use crate::engine::graph::edge_store::store::{EDGES, EdgeStore, redb_err};

/// One edge whose current state a cut changed.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct EdgeFlip {
    pub src: String,
    pub label: String,
    pub dst: String,
    /// The properties the edge resolved to before the cut, `None` when it
    /// was not live.
    pub before: Option<Vec<u8>>,
    /// The properties the edge resolves to after the cut, `None` when it is
    /// not live.
    pub after: Option<Vec<u8>>,
}

/// What one cut install changed.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct EdgeCutInstall {
    pub collection: String,
    pub cut: i64,
    /// Whether this install recorded the cut. Installing a recorded cut
    /// again changes nothing.
    pub inserted: bool,
    /// The summary row the install replaced, `None` when it replaced none.
    pub prior_summary: Option<Vec<u8>>,
    /// Every edge whose current state the cut changed.
    pub flips: Vec<EdgeFlip>,
}

/// The versions of one edge, ascending, as `(system_from, applied, bytes)`.
type Versions = Vec<(i64, i64, Vec<u8>)>;

impl EdgeStore {
    /// Record the cut of `collection` at `cut`, the ordinal of the TRUNCATE's
    /// Calvin transaction, in one transaction.
    ///
    /// Every share of one TRUNCATE on this store records the same cut, so an
    /// install of a cut already recorded changes nothing.
    pub fn install_edge_cut(
        &self,
        db: DatabaseId,
        tid: TenantId,
        collection: &str,
        cut: i64,
    ) -> crate::Result<EdgeCutInstall> {
        let key = cut_key(collection, cut)?;
        let d = db.as_u64();
        let t = tid.as_u64();
        let mut install = EdgeCutInstall {
            collection: collection.to_string(),
            cut,
            inserted: false,
            prior_summary: None,
            flips: Vec::new(),
        };
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut visibility = write_visibility(&write_txn)?;
            let recorded = visibility
                .cuts
                .get((d, t, key.as_str()))
                .map_err(|e| redb_err("read edge cut", e))?
                .is_some();
            if !recorded {
                let newest = visibility.cut_at(d, t, collection, i64::MAX)?;
                // A newer cut already hides everything this one hides, so
                // only a cut above every recorded one changes the current
                // state.
                if newest.is_none_or(|newest| newest < cut) {
                    install.flips =
                        cut_flips(&write_txn, &visibility, db, tid, collection, newest, cut)?;
                }
                visibility
                    .cuts
                    .insert((d, t, key.as_str()), ())
                    .map_err(|e| redb_err("insert edge cut", e))?;
                install.inserted = true;
            }
        }
        if !install.inserted {
            write_txn
                .abort()
                .map_err(|e| redb_err("abort edge cut", e))?;
            return Ok(install);
        }
        if !install.flips.is_empty() {
            install.prior_summary = mark_summary_for_scan(&write_txn, d, t, collection)?;
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit edge cut", e))?;
        Ok(install)
    }

    /// Remove what `install` recorded, for a rollback: the cut, and the
    /// summary row it replaced.
    pub fn remove_edge_cut(
        &self,
        db: DatabaseId,
        tid: TenantId,
        install: &EdgeCutInstall,
    ) -> crate::Result<()> {
        if !install.inserted {
            return Ok(());
        }
        let key = cut_key(&install.collection, install.cut)?;
        let d = db.as_u64();
        let t = tid.as_u64();
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut visibility = write_visibility(&write_txn)?;
            visibility
                .cuts
                .remove((d, t, key.as_str()))
                .map_err(|e| redb_err("remove edge cut", e))?;
            if let Some(prior) = &install.prior_summary {
                let summary = summary_key(&install.collection);
                let mut stats = write_txn
                    .open_table(GRAPH_STATS)
                    .map_err(|e| redb_err("open graph_stats", e))?;
                stats
                    .insert((d, t, summary.as_str()), prior.as_slice())
                    .map_err(|e| redb_err("restore graph summary", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit edge cut removal", e))
    }
}

/// The edges of `collection` whose current state changes when the newest
/// cut moves from `newest` to `cut`.
fn cut_flips(
    write_txn: &WriteTransaction,
    visibility: &WriteVisibility<'_>,
    db: DatabaseId,
    tid: TenantId,
    collection: &str,
    newest: Option<i64>,
    cut: i64,
) -> crate::Result<Vec<EdgeFlip>> {
    let d = db.as_u64();
    let t = tid.as_u64();
    let prefix = format!("{collection}\x00");
    let edges = write_txn
        .open_table(EDGES)
        .map_err(|e| redb_err("open edges", e))?;
    let range = edges
        .range((d, t, prefix.as_str())..)
        .map_err(|e| redb_err("edge cut range", e))?;
    let mut bases: BTreeMap<(String, String, String), Versions> = BTreeMap::new();
    for entry in range {
        let (key, value) = entry.map_err(|e| redb_err("edge cut iter", e))?;
        let (kd, kt, composite) = key.value();
        if kd != d || kt != t || !composite.starts_with(&prefix) {
            break;
        }
        let Some((_, src, label, dst, system_from)) = parse_versioned_edge_key(composite) else {
            continue;
        };
        let applied = visibility.applied_at(d, t, composite, system_from)?;
        bases
            .entry((src.to_string(), label.to_string(), dst.to_string()))
            .or_default()
            .push((system_from, applied, value.value().to_vec()));
    }

    let visible_under = |versions: &Versions, cut: Option<i64>| {
        versions
            .iter()
            .rev()
            .find(|(_, applied, _)| cut.is_none_or(|cut| *applied >= cut))
            .map(|(system_from, _, bytes)| (*system_from, bytes.clone()))
    };
    let mut flips = Vec::new();
    for ((src, label, dst), versions) in bases {
        let before = live_properties(visible_under(&versions, newest))?;
        let after = live_properties(visible_under(&versions, Some(cut)))?;
        if before != after {
            flips.push(EdgeFlip {
                src,
                label,
                dst,
                before,
                after,
            });
        }
    }
    Ok(flips)
}

/// Mark the summary row of `collection` for an exact scan, and return the
/// row it replaced. A collection with no summary row is scanned already.
fn mark_summary_for_scan(
    write_txn: &WriteTransaction,
    db: u64,
    tid: u64,
    collection: &str,
) -> crate::Result<Option<Vec<u8>>> {
    let key = summary_key(collection);
    let mut stats = write_txn
        .open_table(GRAPH_STATS)
        .map_err(|e| redb_err("open graph_stats", e))?;
    let prior = stats
        .get((db, tid, key.as_str()))
        .map_err(|e| redb_err("read graph summary", e))?
        .map(|row| row.value().to_vec());
    let Some(prior) = prior else {
        return Ok(None);
    };
    let mut summary = SummaryRow::decode(&prior)?;
    if summary.ownership_version == 0 {
        return Ok(None);
    }
    summary.ownership_version = 0;
    stats
        .insert((db, tid, key.as_str()), summary.encode()?.as_slice())
        .map_err(|e| redb_err("mark graph summary for scan", e))?;
    Ok(Some(prior))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::edge_store::temporal::keys::EdgeRef;
    use crate::engine::graph::edge_store::temporal::write::VersionStamp;

    const T: TenantId = TenantId::new(1);
    const DB: DatabaseId = DatabaseId::DEFAULT;
    const D: u64 = 0;
    const COLL: &str = "g";

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = EdgeStore::open(&dir.path().join("graph.redb")).expect("open edge store");
        (store, dir)
    }

    fn e<'a>(src: &'a str, dst: &'a str) -> EdgeRef<'a> {
        EdgeRef::new(DB, T, COLL, src, "L", dst)
    }

    fn put(store: &EdgeStore, src: &str, dst: &str, props: &[u8], stamp: VersionStamp) {
        store
            .put_edge_version_recorded(e(src, dst), props, stamp, 0, i64::MAX, true)
            .expect("put edge version");
    }

    fn read(store: &EdgeStore, src: &str, dst: &str, as_of: i64) -> Option<Vec<u8>> {
        store
            .ceiling_resolve_edge(e(src, dst), as_of, None)
            .expect("resolve edge")
    }

    fn live_bases(store: &EdgeStore) -> Vec<(String, String, Vec<u8>)> {
        let mut live: Vec<(String, String, Vec<u8>)> = store
            .scan_all_edges_decoded(None)
            .expect("scan edges")
            .into_iter()
            .map(|(_, _, _, src, _, dst, props)| (src, dst, props))
            .collect();
        live.sort();
        live
    }

    /// A cut hides every version applied below it. A version stamped above
    /// it stays, and a read as of a time before the cut still sees history.
    #[test]
    fn a_cut_hides_versions_applied_below_it() {
        let (store, _dir) = make_store();
        put(&store, "a", "b", b"old", VersionStamp::at(100));
        put(&store, "c", "d", b"later", VersionStamp::at(300));
        let install = store.install_edge_cut(DB, T, COLL, 200).expect("cut");
        assert!(install.inserted);
        assert_eq!(
            install.flips,
            vec![EdgeFlip {
                src: "a".into(),
                label: "L".into(),
                dst: "b".into(),
                before: Some(b"old".to_vec()),
                after: None,
            }]
        );
        assert_eq!(read(&store, "a", "b", i64::MAX), None);
        assert_eq!(read(&store, "a", "b", 150), Some(b"old".to_vec()));
        assert_eq!(read(&store, "c", "d", i64::MAX), Some(b"later".to_vec()));
        assert_eq!(
            store
                .neighbors_in(D, T, COLL, "b", None)
                .expect("inbound")
                .len(),
            0
        );
        let stats = store
            .collection_stats(D, T, COLL, None)
            .expect("collection stats");
        assert_eq!(stats.edge_count, 1, "the counters follow the cut");
    }

    /// A restored version keeps its historical system time and is applied
    /// at the restore's ordinal. A TRUNCATE sequenced before the restore
    /// leaves it visible, and one sequenced after it hides it.
    #[test]
    fn a_restored_version_survives_an_earlier_cut_and_not_a_later_one() {
        let (store, _dir) = make_store();
        put(&store, "a", "b", b"live", VersionStamp::at(100));
        store.install_edge_cut(DB, T, COLL, 500).expect("cut");
        let restored = VersionStamp::applied_at(40, Some(600)).expect("stamp");
        put(&store, "a", "b", b"restored", restored);
        assert_eq!(read(&store, "a", "b", i64::MAX), Some(b"restored".to_vec()));
        assert_eq!(read(&store, "a", "b", 50), Some(b"restored".to_vec()));
        assert_eq!(read(&store, "a", "b", 150), Some(b"live".to_vec()));

        store.install_edge_cut(DB, T, COLL, 700).expect("later cut");
        assert_eq!(read(&store, "a", "b", i64::MAX), None);
        assert_eq!(read(&store, "a", "b", 650), Some(b"restored".to_vec()));
    }

    /// The same versions and cuts resolve alike whatever order the store
    /// applied them in: a late version from before the cut is hidden on
    /// arrival, and one from after it is kept.
    #[test]
    fn the_result_is_independent_of_apply_order() {
        let (cut_first, _a) = make_store();
        cut_first.install_edge_cut(DB, T, COLL, 200).expect("cut");
        put(&cut_first, "a", "b", b"early", VersionStamp::at(150));
        put(&cut_first, "c", "d", b"late", VersionStamp::at(250));
        put(
            &cut_first,
            "e",
            "f",
            b"restored",
            VersionStamp::applied_at(10, Some(260)).expect("stamp"),
        );

        let (cut_last, _b) = make_store();
        put(&cut_last, "c", "d", b"late", VersionStamp::at(250));
        put(
            &cut_last,
            "e",
            "f",
            b"restored",
            VersionStamp::applied_at(10, Some(260)).expect("stamp"),
        );
        put(&cut_last, "a", "b", b"early", VersionStamp::at(150));
        cut_last.install_edge_cut(DB, T, COLL, 200).expect("cut");

        assert_eq!(live_bases(&cut_first), live_bases(&cut_last));
        assert_eq!(
            live_bases(&cut_first),
            vec![
                ("c".into(), "d".into(), b"late".to_vec()),
                ("e".into(), "f".into(), b"restored".to_vec()),
            ]
        );
        for store in [&cut_first, &cut_last] {
            let stats = store
                .collection_stats(D, T, COLL, None)
                .expect("collection stats");
            assert_eq!(stats.edge_count, 2);
        }
    }

    /// A late write below the cut reports the edge as it resolves: not live.
    #[test]
    fn a_version_the_cut_hides_leaves_the_edge_as_it_was() {
        let (store, _dir) = make_store();
        store.install_edge_cut(DB, T, COLL, 200).expect("cut");
        let written = store
            .put_edge_version_recorded(
                e("a", "b"),
                b"early",
                VersionStamp::at(150),
                0,
                i64::MAX,
                true,
            )
            .expect("put");
        assert_eq!(written.current, None);
        assert_eq!(written.counted, None);
    }

    /// Every share of one TRUNCATE installs the same cut: the second install
    /// changes nothing, and removing it leaves the first in place.
    #[test]
    fn a_second_install_of_one_cut_changes_nothing() {
        let (store, _dir) = make_store();
        put(&store, "a", "b", b"v", VersionStamp::at(100));
        let first = store.install_edge_cut(DB, T, COLL, 200).expect("cut");
        let second = store.install_edge_cut(DB, T, COLL, 200).expect("cut again");
        assert!(!second.inserted);
        assert!(second.flips.is_empty());
        store
            .remove_edge_cut(DB, T, &second)
            .expect("remove second");
        assert_eq!(read(&store, "a", "b", i64::MAX), None);

        store.remove_edge_cut(DB, T, &first).expect("remove first");
        assert_eq!(read(&store, "a", "b", i64::MAX), Some(b"v".to_vec()));
        let stats = store
            .collection_stats(D, T, COLL, None)
            .expect("collection stats");
        assert_eq!(stats.edge_count, 1);
        assert!(!stats.exact_scan, "the summary row is back");
    }

    /// A snapshot scan carries the hidden versions, the cuts and the applied
    /// ordinals apart from the visible versions, and installing all of them
    /// reproduces every read.
    #[test]
    fn a_snapshot_scan_reproduces_every_read() {
        let (store, _dir) = make_store();
        put(&store, "a", "b", b"old", VersionStamp::at(100));
        put(
            &store,
            "c",
            "d",
            b"restored",
            VersionStamp::applied_at(50, Some(300)).expect("stamp"),
        );
        store.install_edge_cut(DB, T, COLL, 200).expect("cut");
        let scan = store.scan_edges_for_tenant(D, T).expect("scan");
        assert_eq!(scan.visible.len(), 1);
        assert_eq!(scan.hidden.len(), 1);
        assert_eq!(scan.cuts, vec![(COLL.to_string(), 200)]);
        assert_eq!(scan.applied.len(), 1);

        let (copy, _copy_dir) = make_store();
        for (key, value) in scan.visible.iter().chain(&scan.hidden) {
            copy.put_edge_raw(D, T, key, value).expect("raw edge");
        }
        for (collection, cut) in &scan.cuts {
            copy.put_edge_cut_raw(D, T, collection, *cut)
                .expect("raw cut");
        }
        for (key, applied) in &scan.applied {
            copy.put_edge_applied_raw(D, T, key, *applied)
                .expect("raw applied");
        }
        assert_eq!(live_bases(&copy), live_bases(&store));
        for as_of in [150, 250, i64::MAX] {
            assert_eq!(read(&copy, "a", "b", as_of), read(&store, "a", "b", as_of));
            assert_eq!(read(&copy, "c", "d", as_of), read(&store, "c", "d", as_of));
        }
    }

    /// An audit purge keeps the version a cut leaves current, even when a
    /// newer, hidden version exists.
    #[test]
    fn an_audit_purge_keeps_the_current_version_under_a_cut() {
        let (store, _dir) = make_store();
        put(
            &store,
            "a",
            "b",
            b"restored",
            VersionStamp::applied_at(1_000, Some(9_000)).expect("stamp"),
        );
        put(&store, "a", "b", b"hidden", VersionStamp::at(2_000));
        store.install_edge_cut(DB, T, COLL, 5_000).expect("cut");
        assert_eq!(read(&store, "a", "b", i64::MAX), Some(b"restored".to_vec()));
        // One millisecond is an ordinal cutoff above both versions.
        let purged = store
            .purge_superseded_versions(D, T, COLL, 1)
            .expect("purge");
        assert_eq!(purged, 1, "only the hidden version goes");
        assert_eq!(read(&store, "a", "b", i64::MAX), Some(b"restored".to_vec()));
    }
}
