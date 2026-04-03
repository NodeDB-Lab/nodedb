//! KvEngine methods for sorted index lifecycle and query.
//!
//! Extends `KvEngine` with:
//! - `register_sorted_index()` / `drop_sorted_index()` — DDL
//! - `sorted_index_on_put()` / `sorted_index_on_delete()` — auto-maintenance
//! - `sorted_index_rank()` / `sorted_index_top_k()` / etc. — query

use super::engine::KvEngine;
use super::engine_helpers::table_key;
use super::sorted_index::manager::SortedIndexDef;

impl KvEngine {
    /// Register a new sorted index with backfill from existing KV data.
    ///
    /// Scans the hash table for all entries, extracts sort key columns,
    /// and populates the order-statistic tree. Returns backfill count.
    pub fn register_sorted_index(
        &mut self,
        tenant_id: u32,
        collection: &str,
        def: SortedIndexDef,
    ) -> u32 {
        let tkey = table_key(tenant_id, collection);
        let now_ms = super::current_ms();

        // Collect existing entries from the hash table for backfill.
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self
            .tables
            .get(&tkey)
            .map(|t| {
                let (entries, _) = t.scan(0, usize::MAX, now_ms, None);
                entries
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .collect()
            })
            .unwrap_or_default();

        self.sorted_indexes
            .register(tenant_id, def, entries.into_iter())
    }

    /// Drop a sorted index. Returns `true` if it existed.
    pub fn drop_sorted_index(&mut self, tenant_id: u32, index_name: &str) -> bool {
        self.sorted_indexes.drop(tenant_id, index_name)
    }

    /// Called after a KV PUT to maintain sorted indexes on this collection.
    ///
    /// `field_values` are the extracted field name/value pairs from the new value.
    pub fn sorted_index_on_put(
        &mut self,
        tenant_id: u32,
        collection: &str,
        primary_key: &[u8],
        field_values: &[(String, Vec<u8>)],
    ) {
        let tkey = table_key(tenant_id, collection);
        self.sorted_indexes.on_put(tkey, primary_key, field_values);
    }

    /// Called after a KV DELETE to maintain sorted indexes on this collection.
    pub fn sorted_index_on_delete(&mut self, tenant_id: u32, collection: &str, primary_key: &[u8]) {
        let tkey = table_key(tenant_id, collection);
        self.sorted_indexes.on_delete(tkey, primary_key);
    }

    /// Check if any sorted indexes exist for this tenant/collection.
    pub fn has_sorted_indexes(&self, tenant_id: u32, collection: &str) -> bool {
        let tkey = table_key(tenant_id, collection);
        self.sorted_indexes.has_indexes(tkey)
    }

    // ── Query methods ──────────────────────────────────────────────────

    pub fn sorted_index_rank(
        &self,
        tenant_id: u32,
        index_name: &str,
        primary_key: &[u8],
        now_ms: u64,
    ) -> Option<u32> {
        self.sorted_indexes
            .rank(tenant_id, index_name, primary_key, now_ms)
    }

    pub fn sorted_index_top_k(
        &self,
        tenant_id: u32,
        index_name: &str,
        k: u32,
        now_ms: u64,
    ) -> Option<Vec<(u32, Vec<u8>)>> {
        self.sorted_indexes.top_k(tenant_id, index_name, k, now_ms)
    }

    pub fn sorted_index_range(
        &self,
        tenant_id: u32,
        index_name: &str,
        score_min: Option<&[u8]>,
        score_max: Option<&[u8]>,
        now_ms: u64,
    ) -> Option<Vec<(u32, Vec<u8>)>> {
        self.sorted_indexes
            .range(tenant_id, index_name, score_min, score_max, now_ms)
    }

    pub fn sorted_index_count(&self, tenant_id: u32, index_name: &str, now_ms: u64) -> Option<u32> {
        self.sorted_indexes.count(tenant_id, index_name, now_ms)
    }

    pub fn sorted_index_score(
        &self,
        tenant_id: u32,
        index_name: &str,
        primary_key: &[u8],
    ) -> Option<Vec<u8>> {
        self.sorted_indexes
            .score(tenant_id, index_name, primary_key)
    }

    pub fn sorted_index_def(&self, tenant_id: u32, index_name: &str) -> Option<&SortedIndexDef> {
        self.sorted_indexes.get_def(tenant_id, index_name)
    }
}
