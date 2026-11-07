// SPDX-License-Identifier: BUSL-1.1

//! KV reads at a [`SystemTimeScope`], and the purge of ended versions.
//!
//! `Current` reads take the unchanged current path: one probe per point
//! read. `AsOf` and `Pinned` read the version visible at the scope's
//! ordinal. `AllVersions` lists every version of every row.

use nodedb_types::{Surrogate, SystemTimeScope, ordinal_to_ms};

use super::KvEngine;
use crate::engine::kv::engine_helpers::table_key;
use crate::engine::kv::hash_table::VersionedRow;

/// One version of one KV row, owned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvVersionRow {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub surrogate: Surrogate,
    /// Ordinal of the write that set this version.
    pub sys_from: i64,
    /// Ordinal of the write or delete that ended it, `None` for the live one.
    pub sys_to: Option<i64>,
    /// Absolute expiry instant in ms the version holds, `0` for none.
    pub expire_at_ms: u64,
}

impl From<VersionedRow<'_>> for KvVersionRow {
    fn from(row: VersionedRow<'_>) -> Self {
        Self {
            key: row.key.to_vec(),
            value: row.value.to_vec(),
            surrogate: row.surrogate,
            sys_from: row.sys_from,
            sys_to: row.sys_to,
            expire_at_ms: row.expire_at_ms,
        }
    }
}

/// Parameters for [`KvEngine::scan_at`].
#[derive(Debug, Clone, Copy)]
pub struct KvVersionedScan<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    /// The 8-byte big-endian position of the next row, empty to start.
    pub cursor: &'a [u8],
    pub count: usize,
    /// Wall time a `Current` scan judges expiry against.
    pub now_ms: u64,
    pub match_pattern: Option<&'a str>,
    /// `Some(c)` hides rows whose surrogate is above `c`.
    pub surrogate_ceiling: Option<u32>,
    pub system_time: SystemTimeScope,
}

/// The wall-clock millisecond a read at ordinal `at` judges expiry against.
fn wall_ms_at(at: i64) -> u64 {
    u64::try_from(ordinal_to_ms(at)).unwrap_or(0)
}

impl KvEngine {
    /// GET at `scope`: the value and surrogate of `key` visible at the
    /// scope's cut. `Current` and `AllVersions` read the newest version.
    pub fn get_at(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        scope: SystemTimeScope,
        now_ms: u64,
    ) -> Option<(Vec<u8>, Surrogate)> {
        match scope.as_of_ordinal() {
            None => self.get_with_surrogate(database_id, tenant_id, collection, key, now_ms),
            Some(at) => {
                let tkey = table_key(database_id, tenant_id, collection);
                self.tables
                    .get(&tkey)?
                    .get_at(key, at, wall_ms_at(at))
                    .map(|row| (row.value.to_vec(), row.surrogate))
            }
        }
    }

    /// The version of `key` a read at `scope` sees, with its stamps.
    /// `Current` and `AllVersions` read the live row, expiry judged at
    /// `now_ms`. `None` when the read sees no row.
    pub fn version_of_key(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        scope: SystemTimeScope,
        now_ms: u64,
    ) -> Option<KvVersionRow> {
        let (at, wall_ms) = match scope.as_of_ordinal() {
            Some(at) => (at, wall_ms_at(at)),
            None => (i64::MAX, now_ms),
        };
        self.tables
            .get(&table_key(database_id, tenant_id, collection))?
            .get_at(key, at, wall_ms)
            .map(KvVersionRow::from)
    }

    /// SCAN at `params.system_time`. A point-in-time scope answers every row
    /// visible at the cut. `AllVersions` answers every version of every row,
    /// oldest first per key. `Current` answers every live row. Returns the
    /// page and the cursor of the next page, empty when the scan is done.
    pub fn scan_at(&self, params: KvVersionedScan<'_>) -> (Vec<KvVersionRow>, Vec<u8>) {
        let KvVersionedScan {
            database_id,
            tenant_id,
            collection,
            cursor,
            count,
            now_ms,
            match_pattern,
            surrogate_ceiling,
            system_time,
        } = params;
        let tkey = table_key(database_id, tenant_id, collection);
        let Some(table) = self.tables.get(&tkey) else {
            return (Vec::new(), Vec::new());
        };
        let rows = match system_time {
            SystemTimeScope::AllVersions => table.all_versions(match_pattern),
            SystemTimeScope::Current => table.rows_at(i64::MAX, now_ms, match_pattern),
            SystemTimeScope::AsOf(_)
            | SystemTimeScope::Pinned { .. }
            | SystemTimeScope::TxnSnapshot { .. } => {
                let at = system_time.as_of_ordinal().unwrap_or(i64::MAX);
                table.rows_at(at, wall_ms_at(at), match_pattern)
            }
        };
        let start = <[u8; 8]>::try_from(cursor)
            .map(u64::from_be_bytes)
            .map_or(0, |pos| usize::try_from(pos).unwrap_or(usize::MAX))
            .min(rows.len());
        let visible = |row: &VersionedRow<'_>| {
            surrogate_ceiling.is_none_or(|ceiling| row.surrogate.as_u32() <= ceiling)
        };
        let mut page = Vec::with_capacity(count.min(rows.len() - start));
        let mut next = rows.len();
        for (pos, row) in rows.iter().enumerate().skip(start) {
            if page.len() >= count {
                next = pos;
                break;
            }
            if visible(row) {
                page.push(KvVersionRow::from(*row));
            }
        }
        let next_cursor = if next < rows.len() {
            (next as u64).to_be_bytes().to_vec()
        } else {
            Vec::new()
        };
        (page, next_cursor)
    }

    /// Every `(key, value)` row of `collection` a read at ordinal `at` sees,
    /// expiry judged at the cut's wall time. The rows a sorted index or an
    /// aggregate at the cut works over.
    pub fn collection_rows_at(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        at: i64,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let tkey = table_key(database_id, tenant_id, collection);
        self.tables.get(&tkey).map_or_else(Vec::new, |table| {
            table
                .rows_at(at, wall_ms_at(at), None)
                .into_iter()
                .map(|row| (row.key.to_vec(), row.value.to_vec()))
                .collect()
        })
    }

    /// Whether this core holds a table for `collection`: a KV collection
    /// with at least one stored version, live or ended.
    pub fn has_collection(&self, database_id: u64, tenant_id: u64, collection: &str) -> bool {
        self.tables
            .contains_key(&table_key(database_id, tenant_id, collection))
    }

    /// The wall-clock millisecond a read at ordinal `at` judges expiry
    /// against.
    pub fn cut_wall_ms(at: i64) -> u64 {
        wall_ms_at(at)
    }

    /// Remove every ended version of `(database_id, tenant_id, collection)`
    /// whose successor or delete starts at or below `horizon`. The version
    /// visible at `horizon` stays, live or ended. A key deleted at or below
    /// `horizon` leaves entirely. Returns the number of versions removed.
    pub fn purge_kv_versions_below(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        horizon: i64,
    ) -> usize {
        let tkey = table_key(database_id, tenant_id, collection);
        self.purge_table_versions_below(tkey, horizon)
    }

    /// [`Self::purge_kv_versions_below`] over every table on this core, each at
    /// `horizon_of` its database.
    pub fn purge_all_kv_versions_below(&mut self, horizon_of: impl Fn(u64) -> i64) -> usize {
        let tables: Vec<u64> = self.tables.keys().copied().collect();
        tables
            .into_iter()
            .map(|tkey| {
                // Every table registers its identity before it exists. A table with no
                // database has no horizon to judge it by, so it keeps its versions.
                let Some(database_id) = self.hash_to_database.get(&tkey).copied() else {
                    return 0;
                };
                self.purge_table_versions_below(tkey, horizon_of(database_id.as_u64()))
            })
            .sum()
    }

    fn purge_table_versions_below(&mut self, tkey: u64, horizon: i64) -> usize {
        let Some(table) = self.tables.get_mut(&tkey) else {
            return 0;
        };
        let removed = table.purge_versions_below(horizon);
        if removed > 0 {
            self.recharge_table(tkey);
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::kv::KvPutParams;
    use crate::engine::kv::test_support::row_surrogate;

    fn engine() -> KvEngine {
        KvEngine::new(1_000, 16, 0.75, 4, 64, 1_000, 1_024)
    }

    /// Put `key = value` in one apply scope fixed at `ordinal`.
    fn put_at(e: &mut KvEngine, key: &[u8], value: &[u8], ordinal: i64) {
        let guard = e.open_write_scope(Some(ordinal));
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            key,
            value,
            ttl_ms: 0,
            now_ms: 1_000,
            surrogate: row_surrogate(key),
        })
        .expect("a bound row writes");
        e.close_write_scope(guard);
    }

    fn delete_at(e: &mut KvEngine, key: &[u8], ordinal: i64) {
        let guard = e.open_write_scope(Some(ordinal));
        e.delete(0, 1, "c", &[key.to_vec()], 1_000);
        e.close_write_scope(guard);
    }

    fn pinned(ordinal: i64) -> SystemTimeScope {
        SystemTimeScope::Pinned {
            ordinal,
            cut_marker: 0,
        }
    }

    fn get(e: &KvEngine, key: &[u8], scope: SystemTimeScope) -> Option<Vec<u8>> {
        e.get_at(0, 1, "c", key, scope, 1_000).map(|(v, _)| v)
    }

    fn scan_keys(e: &KvEngine, scope: SystemTimeScope) -> Vec<Vec<u8>> {
        let (rows, next) = e.scan_at(KvVersionedScan {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            cursor: &[],
            count: usize::MAX,
            now_ms: 1_000,
            match_pattern: None,
            surrogate_ceiling: None,
            system_time: scope,
        });
        assert!(next.is_empty());
        let mut keys: Vec<Vec<u8>> = rows.into_iter().map(|r| r.key).collect();
        keys.sort();
        keys
    }

    /// A key's version at a cut carries the ordinal that set it.
    #[test]
    fn a_key_version_carries_the_ordinal_that_set_it() {
        let mut e = engine();
        put_at(&mut e, b"k", b"v1", 100);
        put_at(&mut e, b"k", b"v2", 200);
        let stamp = |scope| {
            e.version_of_key(0, 1, "c", b"k", scope, 1_000)
                .map(|row| (row.value, row.sys_from, row.expire_at_ms))
        };
        assert_eq!(stamp(pinned(150)), Some((b"v1".to_vec(), 100, 0)));
        assert_eq!(
            stamp(SystemTimeScope::Current),
            Some((b"v2".to_vec(), 200, 0))
        );
        assert_eq!(stamp(pinned(50)), None);
    }

    #[test]
    fn reads_at_each_cut_across_put_overwrite_delete_reinsert() {
        let mut e = engine();
        put_at(&mut e, b"k", b"v1", 100);
        put_at(&mut e, b"k", b"v2", 200);
        delete_at(&mut e, b"k", 300);
        put_at(&mut e, b"k", b"v3", 400);

        assert_eq!(get(&e, b"k", pinned(99)), None);
        assert_eq!(get(&e, b"k", pinned(150)), Some(b"v1".to_vec()));
        assert_eq!(get(&e, b"k", pinned(250)), Some(b"v2".to_vec()));
        assert_eq!(get(&e, b"k", pinned(350)), None);
        assert_eq!(get(&e, b"k", pinned(450)), Some(b"v3".to_vec()));
        assert_eq!(
            get(&e, b"k", SystemTimeScope::Current),
            Some(b"v3".to_vec())
        );
        assert_eq!(e.get(0, 1, "c", b"k", 1_000), Some(b"v3".to_vec()));
    }

    #[test]
    fn scan_at_a_cut_sees_rows_deleted_after_it() {
        let mut e = engine();
        put_at(&mut e, b"a", b"1", 100);
        put_at(&mut e, b"b", b"2", 100);
        delete_at(&mut e, b"b", 200);
        put_at(&mut e, b"c", b"3", 300);
        assert_eq!(
            scan_keys(&e, pinned(150)),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(scan_keys(&e, pinned(250)), vec![b"a".to_vec()]);
        assert_eq!(
            scan_keys(&e, SystemTimeScope::Current),
            vec![b"a".to_vec(), b"c".to_vec()]
        );
        assert_eq!(scan_keys(&e, SystemTimeScope::AllVersions).len(), 3);
    }

    #[test]
    fn a_scan_pages_by_cursor() {
        let mut e = engine();
        for i in 0..5u8 {
            put_at(&mut e, &[i], b"v", 100);
        }
        let mut cursor = Vec::new();
        let mut seen = 0;
        loop {
            let (rows, next) = e.scan_at(KvVersionedScan {
                database_id: 0,
                tenant_id: 1,
                collection: "c",
                cursor: &cursor,
                count: 2,
                now_ms: 1_000,
                match_pattern: None,
                surrogate_ceiling: None,
                system_time: pinned(100),
            });
            seen += rows.len();
            if next.is_empty() {
                break;
            }
            cursor = next;
        }
        assert_eq!(seen, 5);
    }

    #[test]
    fn an_expiry_reap_ends_the_version_and_keeps_it_for_earlier_cuts() {
        let mut e = engine();
        let guard = e.open_write_scope(Some(100));
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            key: b"s",
            value: b"live",
            ttl_ms: 500,
            now_ms: 1_000,
            surrogate: row_surrogate(b"s"),
        })
        .expect("a bound row writes");
        e.close_write_scope(guard);

        let reaped = e.tick_expiry(10_000);
        assert_eq!(reaped.len(), 1);
        assert_eq!(get(&e, b"s", SystemTimeScope::Current), None);
        assert_eq!(
            get(&e, b"s", pinned(150)),
            Some(b"live".to_vec()),
            "a cut before the reap reads the value its wall time had not expired"
        );
    }

    /// Every version of `key`, oldest first, as `(sys_from, sys_to)`.
    fn intervals(e: &KvEngine, key: &[u8]) -> Vec<(i64, Option<i64>)> {
        let (rows, _) = e.scan_at(KvVersionedScan {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            cursor: &[],
            count: usize::MAX,
            now_ms: 1_000,
            match_pattern: None,
            surrogate_ceiling: None,
            system_time: SystemTimeScope::AllVersions,
        });
        let mut out: Vec<(i64, Option<i64>)> = rows
            .into_iter()
            .filter(|row| row.key == key)
            .map(|row| (row.sys_from, row.sys_to))
            .collect();
        out.sort();
        out
    }

    fn put_with_ttl(e: &mut KvEngine, ordinal: i64) {
        let guard = e.open_write_scope(Some(ordinal));
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            key: b"s",
            value: b"live",
            ttl_ms: 500,
            now_ms: 1_000,
            surrogate: row_surrogate(b"s"),
        })
        .expect("a bound row writes");
        e.close_write_scope(guard);
    }

    /// A reap ends the version at the last ordinal of its expiry millisecond,
    /// whenever the reap runs, so two replicas that reap at different times
    /// hold the same versions.
    #[test]
    fn an_expiry_reap_ends_the_version_at_its_expiry_instant() {
        let expiry = nodedb_types::ms_to_ordinal_upper(1_500);
        for reap_at in [1_500, 10_000, 99_000] {
            let mut e = engine();
            put_with_ttl(&mut e, 100);
            assert_eq!(e.tick_expiry(reap_at).len(), 1);
            assert_eq!(intervals(&e, b"s"), vec![(100, Some(expiry))]);
        }
    }

    /// A reap whose expiry instant sits at or below the version's own
    /// ordinal ends it one above that ordinal.
    #[test]
    fn an_expiry_reap_rises_above_the_versions_ordinal() {
        let mut e = engine();
        let ahead = nodedb_types::ms_to_ordinal_upper(9_000);
        put_with_ttl(&mut e, ahead);
        assert_eq!(e.tick_expiry(10_000).len(), 1);
        assert_eq!(intervals(&e, b"s"), vec![(ahead, Some(ahead + 1))]);
    }

    /// A key a delete ended takes a new version that starts at the delete,
    /// even when the write's ordinal sits below it, so its versions never
    /// overlap.
    #[test]
    fn a_reinsert_below_a_delete_starts_at_it() {
        let mut e = engine();
        put_at(&mut e, b"k", b"v1", 100);
        delete_at(&mut e, b"k", 300);
        put_at(&mut e, b"k", b"v2", 200);
        assert_eq!(intervals(&e, b"k"), vec![(100, Some(300)), (300, None)]);
        assert_eq!(get(&e, b"k", pinned(299)), Some(b"v1".to_vec()));
        assert_eq!(get(&e, b"k", pinned(300)), Some(b"v2".to_vec()));
    }

    #[test]
    fn purge_keeps_the_version_visible_at_the_horizon() {
        let mut e = engine();
        put_at(&mut e, b"k", b"v1", 100);
        put_at(&mut e, b"k", b"v2", 200);
        put_at(&mut e, b"k", b"v3", 300);
        put_at(&mut e, b"gone", b"x", 100);
        delete_at(&mut e, b"gone", 150);

        assert_eq!(e.purge_kv_versions_below(0, 1, "c", 250), 2);
        assert_eq!(
            get(&e, b"k", pinned(250)),
            Some(b"v2".to_vec()),
            "visible at the horizon"
        );
        assert_eq!(
            get(&e, b"k", pinned(150)),
            None,
            "below the horizon is gone"
        );
        assert_eq!(
            get(&e, b"gone", pinned(120)),
            None,
            "a key deleted below the horizon leaves"
        );
        assert_eq!(
            get(&e, b"k", SystemTimeScope::Current),
            Some(b"v3".to_vec())
        );
    }
}
