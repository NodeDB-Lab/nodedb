// SPDX-License-Identifier: BUSL-1.1

//! The stored-bytes counter of the edge store file.
//!
//! The counted tables are the per-version edge tables, keyed by
//! `(database_id, tenant_id, "{collection}\x00…")`:
//!
//! - `edges`: every forward edge version, tombstones and erasures included;
//! - `reverse_edges`: the reverse mirror of each forward version;
//! - `edge_applied`: the applied ordinal of a version stamped at another
//!   ordinal.
//!
//! Each row counts under the edge collection its key leads with and the
//! endpoint homes of its edge (see [`super::home_bytes`]). A key counts 16 bytes for
//! `(database_id, tenant_id)` plus the UTF-8 length of its composite. An
//! `edges` or `reverse_edges` value counts its length, an `edge_applied`
//! value 8.
//!
//! Summary and marker tables do not count: `graph_stats` holds counters and
//! `edge_cuts` holds TRUNCATE markers. Node-scoped tables carry no
//! collection and do not count: `node_surrogates`, `node_label_versions`
//! and `node_label_lsn`.

use redb::{AccessGuard, KeyRange, Table};

use super::home_bytes::{
    EdgeHomes, EdgeStorageBytesRow, EdgeStorageDelta, edge_storage_bytes_snapshot,
    retire_edge_storage_bytes,
};
use super::store::{EdgeStore, redb_err};
use crate::engine::storage_bytes::DroppedScope;
use crate::types::VShardId;

/// The edge store file's name in counter errors and reports.
pub(super) const EDGE_COUNTER: &str = "graph";

/// Key bytes of `(database_id, tenant_id)`.
const SCOPE_BYTES: usize = 16;

/// A key of a counted edge table.
pub(super) type EdgeKey<'a> = (u64, u64, &'a str);

/// The edge collection a counted composite key leads with, and the
/// endpoint homes of its edge.
///
/// A forward key leads `collection, src, label, dst` and a reverse key
/// `collection, dst, label, src`. The home pair is the same either way.
pub(super) fn edge_scope_of(composite: &str) -> crate::Result<(&str, EdgeHomes)> {
    let mut parts = composite.splitn(5, '\x00');
    let collection = parts.next();
    let first = parts.next();
    let _label = parts.next();
    let second = parts.next();
    match (collection, first, second) {
        (Some(collection), Some(first), Some(second)) => {
            let home = |node: &str| VShardId::from_key(node.as_bytes()).as_u32();
            Ok((collection, EdgeHomes::of(home(first), home(second))))
        }
        _ => Err(crate::Error::Storage {
            engine: EDGE_COUNTER.into(),
            detail: format!("edge key names no collection and endpoints: {composite:?}"),
        }),
    }
}

/// The value bytes a counted edge table's row holds.
pub(super) trait StoredLen: redb::Value + Sized + 'static {
    fn stored_len(guard: &AccessGuard<'_, Self>) -> usize;
}

impl StoredLen for &'static [u8] {
    fn stored_len(guard: &AccessGuard<'_, Self>) -> usize {
        guard.value().len()
    }
}

impl StoredLen for i64 {
    fn stored_len(_: &AccessGuard<'_, Self>) -> usize {
        std::mem::size_of::<i64>()
    }
}

/// Record in `delta` a row of `key` holding `value_len` value bytes, as
/// stored when `stored`, as removed otherwise.
pub(super) fn record_row(
    delta: &mut EdgeStorageDelta,
    (db, tid, composite): EdgeKey<'_>,
    value_len: usize,
    stored: bool,
) -> crate::Result<()> {
    let (collection, homes) = edge_scope_of(composite)?;
    let key_len = SCOPE_BYTES + composite.len();
    delta.row((db, tid, collection), homes, key_len, value_len, stored);
    Ok(())
}

/// Insert `value` under `key` into `edges` or `reverse_edges`, record the
/// bytes it adds in `delta`, and return `keep` of the value the key held.
fn insert_edge_with<T>(
    table: &mut Table<'_, (u64, u64, &'static str), &'static [u8]>,
    key: EdgeKey<'_>,
    value: &[u8],
    delta: &mut EdgeStorageDelta,
    what: &'static str,
    keep: impl FnOnce(&[u8]) -> T,
) -> crate::Result<Option<T>> {
    let prior = table
        .insert(key, value)
        .map_err(|e| redb_err(what, e))?
        .map(|old| (old.value().len(), keep(old.value())));
    let (db, tid, composite) = key;
    let (collection, homes) = edge_scope_of(composite)?;
    let key_len = SCOPE_BYTES + composite.len();
    let scope = (db, tid, collection);
    match prior {
        Some((old_len, kept)) => {
            delta.row(scope, homes, key_len, old_len, false);
            delta.row(scope, homes, key_len, value.len(), true);
            Ok(Some(kept))
        }
        None => {
            delta.row(scope, homes, key_len, value.len(), true);
            Ok(None)
        }
    }
}

/// Insert `value` under `key` into `edges` or `reverse_edges`, and record
/// the bytes it adds in `delta`.
pub(super) fn insert_edge_row(
    table: &mut Table<'_, (u64, u64, &'static str), &'static [u8]>,
    key: EdgeKey<'_>,
    value: &[u8],
    delta: &mut EdgeStorageDelta,
    what: &'static str,
) -> crate::Result<()> {
    insert_edge_with(table, key, value, delta, what, |_| ()).map(drop)
}

/// [`insert_edge_row`], returning the value the key held before.
pub(super) fn replace_edge_row(
    table: &mut Table<'_, (u64, u64, &'static str), &'static [u8]>,
    key: EdgeKey<'_>,
    value: &[u8],
    delta: &mut EdgeStorageDelta,
    what: &'static str,
) -> crate::Result<Option<Vec<u8>>> {
    insert_edge_with(table, key, value, delta, what, <[u8]>::to_vec)
}

/// Remove `key` from `edges` or `reverse_edges`, record the bytes it held in
/// `delta`, and return `keep` of the value it held.
fn remove_edge_with<T>(
    table: &mut Table<'_, (u64, u64, &'static str), &'static [u8]>,
    key: EdgeKey<'_>,
    delta: &mut EdgeStorageDelta,
    what: &'static str,
    keep: impl FnOnce(&[u8]) -> T,
) -> crate::Result<Option<T>> {
    let prior = table
        .remove(key)
        .map_err(|e| redb_err(what, e))?
        .map(|old| (old.value().len(), keep(old.value())));
    match prior {
        Some((old_len, kept)) => {
            record_row(delta, key, old_len, false)?;
            Ok(Some(kept))
        }
        None => Ok(None),
    }
}

/// Remove `key` from `edges` or `reverse_edges`, and record the bytes it
/// held in `delta`.
pub(super) fn remove_edge_row(
    table: &mut Table<'_, (u64, u64, &'static str), &'static [u8]>,
    key: EdgeKey<'_>,
    delta: &mut EdgeStorageDelta,
    what: &'static str,
) -> crate::Result<()> {
    remove_edge_with(table, key, delta, what, |_| ()).map(drop)
}

/// [`remove_edge_row`], returning the value the key held.
pub(super) fn take_edge_row(
    table: &mut Table<'_, (u64, u64, &'static str), &'static [u8]>,
    key: EdgeKey<'_>,
    delta: &mut EdgeStorageDelta,
    what: &'static str,
) -> crate::Result<Option<Vec<u8>>> {
    remove_edge_with(table, key, delta, what, <[u8]>::to_vec)
}

/// Insert `applied` under `key` into `edge_applied`, and record the bytes
/// it adds in `delta`. Returns the ordinal the key held before.
pub(super) fn insert_applied_row(
    table: &mut Table<'_, (u64, u64, &'static str), i64>,
    key: EdgeKey<'_>,
    applied: i64,
    delta: &mut EdgeStorageDelta,
    what: &'static str,
) -> crate::Result<Option<i64>> {
    let prior = table
        .insert(key, applied)
        .map_err(|e| redb_err(what, e))?
        .map(|old| old.value());
    if prior.is_none() {
        record_row(delta, key, std::mem::size_of::<i64>(), true)?;
    }
    Ok(prior)
}

/// Remove `key` from `edge_applied`, and record the bytes it held in
/// `delta`. Returns the ordinal it held.
pub(super) fn remove_applied_row(
    table: &mut Table<'_, (u64, u64, &'static str), i64>,
    key: EdgeKey<'_>,
    delta: &mut EdgeStorageDelta,
    what: &'static str,
) -> crate::Result<Option<i64>> {
    let prior = table
        .remove(key)
        .map_err(|e| redb_err(what, e))?
        .map(|old| old.value());
    if prior.is_some() {
        record_row(delta, key, std::mem::size_of::<i64>(), false)?;
    }
    Ok(prior)
}

/// A [`drain_counted`] filter that takes every row of its range.
pub(super) fn every_row(_: EdgeKey<'_>) -> bool {
    true
}

/// Remove every entry of a counted table inside `range` that `takes`
/// accepts, recording the bytes each held in `delta`. Returns how many went.
///
/// One pass: `extract_from_if` yields each entry and removes it as it is
/// read. `what` names the drained table in the error of a purge that fails
/// partway.
pub(super) fn drain_counted<V: StoredLen>(
    table: &mut Table<'_, (u64, u64, &'static str), V>,
    range: impl KeyRange<(u64, u64, &'static str)>,
    mut takes: impl FnMut(EdgeKey<'_>) -> bool,
    delta: &mut EdgeStorageDelta,
    what: &'static str,
) -> crate::Result<usize> {
    let mut removed = 0usize;
    let drained = table
        .extract_from_if(range, |key, _| takes(key))
        .map_err(|e| redb_err(what, e))?;
    for entry in drained {
        let (key, value) = entry.map_err(|e| redb_err(what, e))?;
        record_row(delta, key.value(), V::stored_len(&value), false)?;
        removed += 1;
    }
    Ok(removed)
}

impl EdgeStore {
    /// Every edge collection's stored bytes in this file's counted tables,
    /// one row per collection and endpoint home pair holding bytes. Cost is
    /// the number of counter rows.
    pub fn storage_bytes_snapshot(&self) -> crate::Result<Vec<EdgeStorageBytesRow>> {
        edge_storage_bytes_snapshot(&self.db, EDGE_COUNTER)
    }
}

/// Remove the counter rows of `scope` inside `txn`, once its counted edge
/// rows are all dropped in the same transaction.
pub(super) fn retire_edge_bytes(
    txn: &redb::WriteTransaction,
    scope: DroppedScope<'_>,
) -> crate::Result<usize> {
    retire_edge_storage_bytes(txn, EDGE_COUNTER, scope)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet};
    use std::path::Path;

    use nodedb_types::id::VShardId;
    use nodedb_types::{DatabaseId, TenantId};
    use redb::{ReadableDatabase, ReadableTable};

    use super::*;
    use crate::engine::graph::edge_store::store::{EDGES, REVERSE_EDGES};
    use crate::engine::graph::edge_store::temporal::visibility::EDGE_APPLIED;
    use crate::engine::graph::edge_store::temporal::{EdgeRef, VersionStamp, versioned_edge_key};

    /// Stored bytes per `(database, tenant, collection, low home, high home)`.
    type Totals = BTreeMap<(u64, u64, String, u32, u32), u64>;

    /// The scan's own home rule: a forward or applied key names `src` then
    /// `dst`, a reverse key `dst` then `src`.
    fn add(totals: &mut Totals, (d, t, composite): EdgeKey<'_>, value_len: usize) {
        let parts: Vec<&str> = composite.split('\x00').collect();
        let home = |node: &str| VShardId::from_key(node.as_bytes()).as_u32();
        let (a, b) = (home(parts[1]), home(parts[3]));
        let key = (d, t, parts[0].to_owned(), a.min(b), a.max(b));
        *totals.entry(key).or_default() += (16 + composite.len() + value_len) as u64;
    }

    /// Totals summed over home pairs, per `(database, tenant, collection)`.
    fn per_collection(totals: &Totals) -> BTreeMap<(u64, u64, String), u64> {
        let mut out = BTreeMap::new();
        for ((d, t, c, _, _), bytes) in totals {
            *out.entry((*d, *t, c.clone())).or_default() += bytes;
        }
        out
    }

    /// The stored bytes of every counted table, by a full scan.
    fn scan(store: &EdgeStore) -> Totals {
        let txn = store.db().begin_read().unwrap();
        let mut totals = Totals::new();
        for def in [EDGES, REVERSE_EDGES] {
            for entry in txn.open_table(def).unwrap().iter().unwrap() {
                let (k, v) = entry.unwrap();
                add(&mut totals, k.value(), v.value().len());
            }
        }
        for entry in txn.open_table(EDGE_APPLIED).unwrap().iter().unwrap() {
            let (k, _) = entry.unwrap();
            add(&mut totals, k.value(), 8);
        }
        totals
    }

    fn counted(store: &EdgeStore) -> Totals {
        store
            .storage_bytes_snapshot()
            .unwrap()
            .into_iter()
            .map(|row| {
                let key = (
                    row.database_id.as_u64(),
                    row.tenant_id.as_u64(),
                    row.collection,
                    row.homes.low,
                    row.homes.high,
                );
                (key, row.bytes)
            })
            .collect()
    }

    fn assert_exact(store: &EdgeStore) {
        assert_eq!(counted(store), scan(store));
    }

    fn open(dir: &Path) -> EdgeStore {
        EdgeStore::open(&dir.join("graph.redb")).unwrap()
    }

    fn edge<'a>(db: u64, tid: u64, coll: &'a str, src: &'a str, dst: &'a str) -> EdgeRef<'a> {
        EdgeRef::new(DatabaseId::new(db), TenantId::new(tid), coll, src, "L", dst)
    }

    #[test]
    fn a_key_names_its_collection_and_endpoint_homes() {
        let home = |node: &str| VShardId::from_key(node.as_bytes()).as_u32();
        let expected = EdgeHomes::of(home("a"), home("b"));
        assert_eq!(
            edge_scope_of("follows\x00a\x00L\x00b\x0001").unwrap(),
            ("follows", expected)
        );
        // A reverse key names the endpoints the other way round.
        assert_eq!(
            edge_scope_of("follows\x00b\x00L\x00a\x0001").unwrap(),
            ("follows", expected)
        );
        assert!(edge_scope_of("follows").is_err());
        assert!(edge_scope_of("follows\x00a\x00L").is_err());
    }

    /// An edge counts under its two endpoint homes, never under its
    /// collection's home.
    #[test]
    fn an_edge_counts_under_its_endpoint_homes() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        let (src, dst) = two_homes();
        store
            .put_edge_versioned(edge(0, 1, "follows", &src, &dst), b"p", 10, 0, i64::MAX)
            .unwrap();
        let home = |node: &str| VShardId::from_key(node.as_bytes()).as_u32();
        let homes = EdgeHomes::of(home(&src), home(&dst));
        let rows = store.storage_bytes_snapshot().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].homes, homes);
        assert_eq!(rows[0].homes.iter().count(), 2);
        assert_exact(&store);
    }

    /// Two node names whose key vShards differ.
    fn two_homes() -> (String, String) {
        let home = |node: &str| VShardId::from_key(node.as_bytes());
        let src = "n0".to_owned();
        let dst = (1..)
            .map(|i| format!("n{i}"))
            .find(|n| home(n) != home(&src))
            .unwrap();
        (src, dst)
    }

    #[test]
    fn edge_writes_and_purges_match_a_scan() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        let e = edge(0, 1, "follows", "a", "b");
        store
            .put_edge_versioned(e, b"props", 10, 0, i64::MAX)
            .unwrap();
        assert_exact(&store);
        assert!(per_collection(&counted(&store))[&(0, 1, "follows".to_owned())] > 0);
        // A second apply of one version overwrites its key.
        store
            .put_edge_versioned(e, b"longer props", 10, 0, i64::MAX)
            .unwrap();
        assert_exact(&store);
        store.soft_delete_edge(e, 20).unwrap();
        assert_exact(&store);
        store.gdpr_erase_edge(e, 30).unwrap();
        assert_exact(&store);

        // A version applied at another ordinal stores an applied row.
        let stamp = VersionStamp {
            system_from: 40,
            applied: 35,
        };
        let written = store
            .put_edge_version_recorded(e, b"p", stamp, 0, i64::MAX, true)
            .unwrap();
        assert_exact(&store);
        store.remove_edge_version(e, &written).unwrap();
        assert_exact(&store);

        store
            .purge_edge_versions_below(0, TenantId::new(1), "follows", 25)
            .unwrap();
        assert_exact(&store);
        store
            .purge_collection(0, TenantId::new(1), "follows")
            .unwrap();
        assert_exact(&store);
        assert!(counted(&store).is_empty());
    }

    #[test]
    fn raw_restores_and_homed_purges_match_a_scan() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        let tid = TenantId::new(1);
        let key = versioned_edge_key("g", "a", "L", "b", 5).unwrap();
        store.put_edge_raw(0, tid, &key, b"raw").unwrap();
        store.put_edge_applied_raw(0, tid, &key, 3).unwrap();
        store.put_edge_applied_raw(0, tid, &key, 4).unwrap();
        assert_exact(&store);
        let every: HashSet<u32> = (0..VShardId::COUNT).collect();
        store.purge_homed(&every).unwrap();
        assert_exact(&store);
        assert!(counted(&store).is_empty());
    }

    #[test]
    fn the_counter_survives_reopening_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let before = {
            let store = open(dir.path());
            store
                .put_edge_versioned(edge(0, 1, "follows", "a", "b"), b"p", 10, 0, i64::MAX)
                .unwrap();
            counted(&store)
        };
        assert!(!before.is_empty());
        let store = open(dir.path());
        assert_eq!(counted(&store), before);
        assert_exact(&store);
    }

    #[test]
    fn tenants_databases_and_collections_stay_separate() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        for (d, t, c) in [(0, 1, "x"), (0, 2, "x"), (1, 1, "x"), (0, 1, "y")] {
            store
                .put_edge_versioned(edge(d, t, c, "a", "b"), b"p", 10, 0, i64::MAX)
                .unwrap();
        }
        let rows = per_collection(&counted(&store));
        assert_eq!(rows.len(), 4);
        assert_exact(&store);
        store.purge_tenant(0, TenantId::new(1)).unwrap();
        let after = per_collection(&counted(&store));
        assert_eq!(after.len(), 2);
        assert_eq!(
            after[&(0, 2, "x".to_owned())],
            rows[&(0, 2, "x".to_owned())]
        );
        assert_eq!(
            after[&(1, 1, "x".to_owned())],
            rows[&(1, 1, "x".to_owned())]
        );
        assert_exact(&store);
    }

    /// A small deterministic generator, so a failing sequence replays.
    struct Lcg(u64);

    impl Lcg {
        fn below(&mut self, n: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (self.0 >> 33) % n
        }
    }

    #[test]
    fn a_random_sequence_keeps_the_counter_equal_to_a_scan() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        let nodes = ["a", "b", "c"];
        let mut rng = Lcg(0xed6e);
        let mut sys = 1i64;
        for _ in 0..300 {
            sys += 1;
            let tenant = 1 + rng.below(2);
            let coll = ["x", "y"][rng.below(2) as usize];
            let src = nodes[rng.below(3) as usize];
            let dst = nodes[rng.below(3) as usize];
            let e = edge(0, tenant, coll, src, dst);
            match rng.below(6) {
                0 | 1 => {
                    let props = vec![b'p'; rng.below(30) as usize];
                    store
                        .put_edge_versioned(e, &props, sys, 0, i64::MAX)
                        .unwrap();
                }
                2 => store.soft_delete_edge(e, sys).unwrap(),
                3 => {
                    let stamp = VersionStamp {
                        system_from: sys,
                        applied: sys - 1,
                    };
                    let written = store
                        .put_edge_version_recorded(e, b"p", stamp, 0, i64::MAX, true)
                        .unwrap();
                    if rng.below(2) == 0 {
                        store.remove_edge_version(e, &written).unwrap();
                    }
                }
                4 => {
                    store
                        .purge_edge_versions_below(0, TenantId::new(tenant), coll, sys / 2)
                        .unwrap();
                }
                _ => {
                    if rng.below(4) == 0 {
                        store
                            .purge_collection(0, TenantId::new(tenant), coll)
                            .unwrap();
                    } else {
                        store.gdpr_erase_edge(e, sys).unwrap();
                    }
                }
            }
            assert_exact(&store);
        }
    }
}
