// SPDX-License-Identifier: BUSL-1.1

//! Per-table write epoch: the chokepoint the Data Plane aggregate result
//! cache reads to detect a KV write since a cached result was computed.
//!
//! `bump_write_epoch` is the only place the counter advances. Every mutating
//! KV path reaches it through [`KvEngine::table_mut_for_write`] (fetch an
//! existing table) or [`KvEngine::table_for_write_or_create`] (fetch-or-create
//! one), or calls it directly for a remove-shaped mutation (truncate, purge,
//! rename) that touches `tables` without going through either accessor.
//! Read-only paths (get, scan, exists, ttl reads) never call any of the three
//! and so never bump.

use super::super::engine_helpers::table_key;
use super::super::hash_table::KvHashTable;
use super::KvEngine;

impl KvEngine {
    /// Current write epoch for `(database_id, tenant_id, collection)`.
    ///
    /// `0` for a table never written on this core — including a collection
    /// that isn't KV at all, since its hash never appears in `write_epochs`.
    /// A cache entry stamped `kv_epoch: 0` for a non-KV collection therefore
    /// always matches here and is never evicted by this check.
    pub fn write_epoch(&self, database_id: u64, tenant_id: u64, collection: &str) -> u64 {
        let tkey = table_key(database_id, tenant_id, collection);
        self.write_epochs.get(&tkey).copied().unwrap_or(0)
    }

    /// Advance `tkey`'s write epoch by one. The single bump site.
    pub(in crate::engine::kv) fn bump_write_epoch(&mut self, tkey: u64) {
        *self.write_epochs.entry(tkey).or_insert(0) += 1;
    }

    /// Fetch `tkey`'s table for a write, bumping its epoch first. `None` if
    /// the table doesn't exist yet — the caller decides whether "nothing to
    /// mutate" is a no-op. A caller that also borrows `indexes`, `expiry`,
    /// or `sorted_indexes` while the table is live calls `bump_write_epoch`
    /// and `tables.get_mut` itself: the method-returned borrow would cover
    /// the whole engine.
    pub(in crate::engine::kv) fn table_mut_for_write(
        &mut self,
        tkey: u64,
    ) -> Option<&mut KvHashTable> {
        self.bump_write_epoch(tkey);
        self.tables.get_mut(&tkey)
    }

    /// Fetch-or-create `tkey`'s table for a write, bumping its epoch and
    /// registering the reverse-lookup maps on first creation.
    pub(in crate::engine::kv) fn table_for_write_or_create(
        &mut self,
        tkey: u64,
        tenant_id: u64,
        collection: &str,
    ) -> &mut KvHashTable {
        self.bump_write_epoch(tkey);
        self.hash_to_tenant.entry(tkey).or_insert(tenant_id);
        self.hash_to_collection
            .entry(tkey)
            .or_insert_with(|| collection.to_string());
        let default_capacity = self.default_capacity;
        let load_factor_threshold = self.load_factor_threshold;
        let rehash_batch_size = self.rehash_batch_size;
        let inline_threshold = self.inline_threshold;
        self.tables.entry(tkey).or_insert_with(|| {
            KvHashTable::new(
                default_capacity,
                load_factor_threshold,
                rehash_batch_size,
                inline_threshold,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::engine::kv::KvPutParams;

    fn make_engine() -> KvEngine {
        KvEngine::new(1000, 16, 0.75, 4, 64, 1000, 1024)
    }

    fn put(e: &mut KvEngine, collection: &str, key: &[u8]) {
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection,
            key,
            value: b"v",
            ttl_ms: 0,
            now_ms: 1000,
            surrogate: Surrogate::ZERO,
        });
    }

    #[test]
    fn put_bumps_epoch() {
        let mut e = make_engine();
        assert_eq!(e.write_epoch(0, 1, "c"), 0);
        put(&mut e, "c", b"k");
        assert_eq!(e.write_epoch(0, 1, "c"), 1);
        put(&mut e, "c", b"k");
        assert_eq!(e.write_epoch(0, 1, "c"), 2);
    }

    #[test]
    fn get_does_not_bump_epoch() {
        let mut e = make_engine();
        put(&mut e, "c", b"k");
        let epoch = e.write_epoch(0, 1, "c");
        let _ = e.get(0, 1, "c", b"k", 1000);
        assert_eq!(e.write_epoch(0, 1, "c"), epoch, "a read must not bump");
    }

    #[test]
    fn truncate_bumps_epoch() {
        let mut e = make_engine();
        put(&mut e, "c", b"k");
        let before = e.write_epoch(0, 1, "c");
        e.truncate(0, 1, "c");
        assert!(
            e.write_epoch(0, 1, "c") > before,
            "truncate must bump the epoch"
        );
    }

    #[test]
    fn per_table_isolation() {
        let mut e = make_engine();
        put(&mut e, "a", b"k");
        assert_eq!(e.write_epoch(0, 1, "a"), 1);
        assert_eq!(e.write_epoch(0, 1, "b"), 0, "an untouched table stays at 0");
    }
}
