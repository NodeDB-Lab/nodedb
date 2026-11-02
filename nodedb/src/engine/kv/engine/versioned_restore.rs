// SPDX-License-Identifier: BUSL-1.1

//! Reinstating a KV row's versions exactly as a checkpoint recorded them.

use super::KvEngine;
use super::history_merge::KvHistoryOverlap;
use super::write_scope::KvWriteScope;
use crate::engine::kv::engine_helpers::table_key;
use crate::engine::kv::hash_table::KvSnapshotTable;
use crate::engine::kv::history::HistoryVersion;
use crate::engine::kv::{KvPutParams, UnboundKvWrite};

impl KvEngine {
    /// The tenant-snapshot form of `collection`'s table: every live row at
    /// its ordinal and every ended version. `None` when no table exists.
    pub fn export_snapshot(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> Option<KvSnapshotTable> {
        self.tables
            .get(&table_key(database_id, tenant_id, collection))
            .map(|table| table.export_snapshot())
    }

    /// Install a live row whose value was set at ordinal `sys_from`, with
    /// the absolute expiry `expire_at_ms`, through the write path so expiry
    /// and indexes stay maintained. The ordinal folds into the clock.
    ///
    /// A key that holds a version or a delete above `sys_from` keeps its
    /// current state: the row installs as an ended version, from `sys_from`
    /// to the next stored version or delete, and the version before it ends
    /// at `sys_from`.
    pub fn restore_live_row(
        &mut self,
        params: KvPutParams<'_>,
        expire_at_ms: u64,
        sys_from: i64,
    ) -> Result<(), UnboundKvWrite> {
        UnboundKvWrite::check(params.collection, params.surrogate)?;
        self.observe_ordinal(sys_from);
        let tkey = table_key(params.database_id, params.tenant_id, params.collection);
        let next = self
            .tables
            .get(&tkey)
            .and_then(|table| table.next_version_above(params.key, sys_from));
        if let Some(sys_to) = next {
            self.table_for_write_or_create(
                tkey,
                params.database_id,
                params.tenant_id,
                params.collection,
            )
            .splice_history(
                params.key,
                HistoryVersion {
                    sys_from,
                    sys_to,
                    value: params.value.to_vec(),
                    expire_at_ms,
                    surrogate: params.surrogate,
                },
            );
            self.recharge_table(tkey);
            return Ok(());
        }
        // The recorded ordinal stamps the row whatever scope is open.
        let prior = self.write_scope;
        self.write_scope = KvWriteScope::Fixed(sys_from);
        self.observe_ordinal(sys_from);
        let written = self.put_with_absolute_expiry(params, expire_at_ms);
        self.write_scope = prior;
        written.map(|_| ())
    }

    /// Install the ended versions of `key` a checkpoint or a restore
    /// recorded, by the merge rule of [`KvEngine::merge_key_history`]: a
    /// version the key already stores installs once, and an overlap refuses
    /// the whole set with nothing written. The collection's table is created
    /// when it holds no live row. Every installed ordinal folds into the
    /// clock.
    pub fn restore_key_history(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        versions: Vec<HistoryVersion>,
    ) -> Result<(), KvHistoryOverlap> {
        let versions = self.merge_key_history(database_id, tenant_id, collection, key, versions)?;
        let Some(newest) = versions.iter().map(|v| v.sys_to).max() else {
            return Ok(());
        };
        self.observe_ordinal(newest);
        let tkey = table_key(database_id, tenant_id, collection);
        self.table_for_write_or_create(tkey, database_id, tenant_id, collection)
            .restore_history(key, versions);
        self.recharge_table(tkey);
        Ok(())
    }

    /// Every ended version of `key`, oldest first. Empty when the table or
    /// the key holds none.
    pub fn key_history(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
    ) -> Vec<HistoryVersion> {
        self.tables
            .get(&table_key(database_id, tenant_id, collection))
            .map(|table| table.history().versions_of(key).to_vec())
            .unwrap_or_default()
    }

    /// The newest ordinal a version or a delete of `key` names. `None` when
    /// the table or the key holds neither.
    pub fn key_newest_ordinal(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
    ) -> Option<i64> {
        self.tables
            .get(&table_key(database_id, tenant_id, collection))?
            .newest_ordinal_of(key)
    }

    /// End the live row of `key` at ordinal `sys_to`, as a delete stamped
    /// there. The ordinal folds into the clock. Returns whether a live row
    /// ended.
    pub fn end_live_row_at(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        sys_to: i64,
    ) -> bool {
        // The given ordinal stamps the delete whatever scope is open.
        let prior = self.write_scope;
        self.write_scope = KvWriteScope::Fixed(sys_to);
        self.observe_ordinal(sys_to);
        let ended = self.delete(database_id, tenant_id, collection, &[key.to_vec()], 0);
        self.write_scope = prior;
        ended > 0
    }

    /// Make `versions` the whole ended history of `key`, as a rollback puts
    /// back the history a restore found.
    pub fn replace_key_history(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        versions: Vec<HistoryVersion>,
    ) {
        let tkey = table_key(database_id, tenant_id, collection);
        if versions.is_empty() && !self.tables.contains_key(&tkey) {
            return;
        }
        self.table_for_write_or_create(tkey, database_id, tenant_id, collection)
            .replace_history(key, versions);
        self.recharge_table(tkey);
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{Surrogate, SystemTimeScope};

    use super::*;

    const NOW: u64 = 1_000_000;

    fn engine() -> KvEngine {
        KvEngine::new(NOW, 16, 0.75, 4, 64, 1000, 1024)
    }

    fn restore(e: &mut KvEngine, value: &[u8], sys_from: i64) {
        e.restore_live_row(
            KvPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "c",
                key: b"k",
                value,
                ttl_ms: 0,
                now_ms: NOW,
                surrogate: Surrogate::new(3),
            },
            0,
            sys_from,
        )
        .expect("restore");
    }

    fn at(e: &KvEngine, ordinal: i64) -> Option<Vec<u8>> {
        e.get_at(
            0,
            1,
            "c",
            b"k",
            SystemTimeScope::Pinned {
                ordinal,
                cut_marker: 0,
            },
            NOW,
        )
        .map(|(value, _)| value)
    }

    /// A live row restored below the key's newest version lands as history
    /// between its neighbours. The current row stays.
    #[test]
    fn a_row_below_the_newest_version_installs_as_history() {
        let mut e = engine();
        restore(&mut e, b"a", 100);
        restore(&mut e, b"c", 300);
        restore(&mut e, b"b", 200);
        assert_eq!(
            e.get(0, 1, "c", b"k", NOW).as_deref(),
            Some(&b"c"[..]),
            "the current row stays"
        );
        assert_eq!(at(&e, 150).as_deref(), Some(&b"a"[..]));
        assert_eq!(at(&e, 250).as_deref(), Some(&b"b"[..]));
        assert_eq!(at(&e, 300).as_deref(), Some(&b"c"[..]));
        // A second restore of the same version changes nothing.
        restore(&mut e, b"b", 200);
        assert_eq!(e.key_history(0, 1, "c", b"k").len(), 2);
    }

    /// The newest ordinal names the live row's start, then the delete that
    /// ends it. The delete stamps the given ordinal and keeps the row's
    /// history.
    #[test]
    fn a_live_row_ends_at_the_given_ordinal() {
        let mut e = engine();
        assert_eq!(e.key_newest_ordinal(0, 1, "c", b"k"), None);
        restore(&mut e, b"a", 100);
        assert_eq!(e.key_newest_ordinal(0, 1, "c", b"k"), Some(100));
        assert!(e.end_live_row_at(0, 1, "c", b"k", 400));
        assert_eq!(e.key_newest_ordinal(0, 1, "c", b"k"), Some(400));
        assert_eq!(e.get(0, 1, "c", b"k", NOW), None);
        assert_eq!(at(&e, 399).as_deref(), Some(&b"a"[..]));
        assert_eq!(at(&e, 400), None);
        assert!(
            !e.end_live_row_at(0, 1, "c", b"k", 500),
            "no live row is left to end"
        );
    }
}
