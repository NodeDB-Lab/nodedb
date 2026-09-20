//! Collection- and tenant-level teardown for the KV engine.

use super::KvEngine;
use crate::engine::kv::engine_helpers::expiry_prefix;

impl KvEngine {
    /// Remove the hash table and indexes for a single `(tenant_id, collection)`.
    ///
    /// Returns `1` if the table existed and was removed, `0` otherwise.
    /// Idempotent — safe to re-run after partial completion.
    pub fn purge_collection(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> usize {
        let tkey = crate::engine::kv::engine_helpers::table_key(database_id, tenant_id, collection);
        let mut removed = 0;
        // Bump before removal so a cached aggregate result computed before the
        // purge is a miss, whether or not the table actually existed.
        self.bump_write_epoch(tkey);
        if self.tables.remove(&tkey).is_some() {
            removed += 1;
        }
        self.indexes.remove(&tkey);
        self.hash_to_tenant.remove(&tkey);
        self.hash_to_collection.remove(&tkey);
        self.sorted_indexes
            .purge_collection(database_id, tenant_id, collection);

        // Eagerly drop pending TTL-wheel entries for this collection.
        // Stale entries would otherwise no-op at fire time (the table
        // they reference is gone), but they still consume reap budget
        // per tick — for a large collection with many TTLs, that's
        // wasted work until every scheduled time has passed.
        let prefix = expiry_prefix(database_id, tenant_id, collection).into_bytes();
        let wheel_removed = self.expiry.purge_prefix(&prefix);
        if wheel_removed > 0 {
            tracing::debug!(
                tenant_id,
                collection,
                wheel_removed,
                "kv: dropped expiry-wheel entries for purged collection"
            );
        }

        removed
    }

    /// Remove all hash tables and indexes belonging to a specific tenant.
    ///
    /// Uses the `hash_to_tenant` reverse map to identify which tables belong
    /// to the tenant. Returns the number of tables removed.
    pub fn purge_tenant(&mut self, tenant_id: u64) -> usize {
        let keys_to_remove: Vec<u64> = self
            .hash_to_tenant
            .iter()
            .filter(|(_, tid)| **tid == tenant_id)
            .map(|(hash, _)| *hash)
            .collect();

        let removed = keys_to_remove.len();
        for key in &keys_to_remove {
            self.bump_write_epoch(*key);
            self.tables.remove(key);
            self.indexes.remove(key);
            self.hash_to_tenant.remove(key);
            self.hash_to_collection.remove(key);
        }
        removed
    }
}
