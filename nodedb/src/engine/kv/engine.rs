//! KvEngine: per-core KV engine owning hash tables and expiry wheel.
//!
//! `!Send` — owned by a single TPC core. Each collection gets its own
//! hash table; the expiry wheel is shared across all collections on
//! this core (one wheel tick processes all collections).

use std::collections::HashMap;

use super::engine_helpers::{expiry_key, extract_all_field_values_from_msgpack, table_key};
use super::entry::NO_EXPIRY;
use super::expiry_wheel::ExpiryWheel;
use super::hash_table::KvHashTable;
use super::index::KvIndexSet;

/// Result of a KV SCAN operation: `(entries, next_cursor_bytes)`.
///
/// Each entry is `(key_bytes, value_bytes)`. `next_cursor` is empty
/// when the scan is complete, otherwise an opaque cursor for continuation.
pub type ScanResult = (Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>);

/// Per-core KV engine.
///
/// Owns a hash table per collection and a shared expiry wheel.
/// Dispatched from the Data Plane executor via `PhysicalPlan::Kv(KvOp)`.
pub struct KvEngine {
    /// Per-collection hash tables. Key: "{tenant_id}:{collection}".
    pub(crate) tables: HashMap<u64, KvHashTable>,
    /// Per-collection secondary index sets. Key: "{tenant_id}:{collection}".
    pub(crate) indexes: HashMap<u64, KvIndexSet>,
    /// Reverse mapping: hash → tenant_id. Enables tenant purge without
    /// reversing the FxHash. Maintained in sync with `tables`.
    pub(crate) hash_to_tenant: HashMap<u64, u32>,
    /// Reverse mapping: hash → collection name. Enables snapshot export
    /// to include human-readable collection names (FxHash is not reversible).
    pub(crate) hash_to_collection: HashMap<u64, String>,
    /// Shared expiry wheel across all collections on this core.
    pub(super) expiry: ExpiryWheel,
    /// Default tuning parameters for new collections.
    pub(super) default_capacity: usize,
    pub(super) load_factor_threshold: f32,
    pub(super) rehash_batch_size: usize,
    pub(super) inline_threshold: usize,
    /// Memory budget in bytes (0 = unlimited). When total_mem_usage() exceeds
    /// this, new PUTs are rejected with a retriable error.
    memory_budget_bytes: usize,
    /// Sorted index manager: order-statistic trees for leaderboard-style queries.
    pub(super) sorted_indexes: super::sorted_index::SortedIndexManager,
}

impl KvEngine {
    /// Create a new KV engine with the given tuning parameters.
    pub fn new(
        now_ms: u64,
        default_capacity: usize,
        load_factor_threshold: f32,
        rehash_batch_size: usize,
        inline_threshold: usize,
        expiry_tick_ms: u64,
        expiry_reap_budget: usize,
    ) -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
            hash_to_tenant: HashMap::new(),
            hash_to_collection: HashMap::new(),
            expiry: ExpiryWheel::new(now_ms, expiry_tick_ms, expiry_reap_budget),
            default_capacity,
            load_factor_threshold,
            rehash_batch_size,
            inline_threshold,
            memory_budget_bytes: 0, // 0 = unlimited (set via set_memory_budget).
            sorted_indexes: super::sorted_index::SortedIndexManager::new(),
        }
    }

    /// Create a KV engine from `KvTuning` config.
    pub fn from_tuning(now_ms: u64, tuning: &nodedb_types::config::tuning::KvTuning) -> Self {
        Self::new(
            now_ms,
            tuning.default_capacity,
            tuning.rehash_load_factor,
            tuning.rehash_batch_size,
            tuning.default_inline_threshold,
            tuning.expiry_tick_ms,
            tuning.expiry_reap_budget,
        )
    }

    /// Set the memory budget in bytes. 0 = unlimited.
    pub fn set_memory_budget(&mut self, budget_bytes: usize) {
        self.memory_budget_bytes = budget_bytes;
    }

    /// Check if the memory budget is exceeded.
    ///
    /// Returns `true` if the budget is set and current usage exceeds it.
    /// Used by PUT handlers to reject new writes with a retriable error.
    pub fn is_over_budget(&self) -> bool {
        self.memory_budget_bytes > 0 && self.total_mem_usage() > self.memory_budget_bytes
    }

    /// Remove all hash tables and indexes belonging to a specific tenant.
    ///
    /// Uses the `hash_to_tenant` reverse map to identify which tables belong
    /// to the tenant. Returns the number of tables removed.
    pub fn purge_tenant(&mut self, tenant_id: u32) -> usize {
        let keys_to_remove: Vec<u64> = self
            .hash_to_tenant
            .iter()
            .filter(|(_, tid)| **tid == tenant_id)
            .map(|(hash, _)| *hash)
            .collect();

        let removed = keys_to_remove.len();
        for key in &keys_to_remove {
            self.tables.remove(key);
            self.indexes.remove(key);
            self.hash_to_tenant.remove(key);
            self.hash_to_collection.remove(key);
        }
        removed
    }

    // -----------------------------------------------------------------------
    // Core operations
    // -----------------------------------------------------------------------

    /// GET: O(1) hash table lookup. Returns None if not found or expired.
    pub fn get(
        &self,
        tenant_id: u32,
        collection: &str,
        key: &[u8],
        now_ms: u64,
    ) -> Option<Vec<u8>> {
        let tkey = table_key(tenant_id, collection);
        self.tables.get(&tkey)?.get(key, now_ms).map(|v| v.to_vec())
    }

    /// GET TTL: Returns the remaining TTL in milliseconds for a key.
    ///
    /// - `None` — key does not exist (or is expired)
    /// - `Some(-1)` — key exists but has no TTL (persistent)
    /// - `Some(remaining_ms)` — key exists and expires in `remaining_ms` milliseconds
    pub fn get_ttl_ms(
        &self,
        tenant_id: u32,
        collection: &str,
        key: &[u8],
        now_ms: u64,
    ) -> Option<i64> {
        let tkey = table_key(tenant_id, collection);
        let table = self.tables.get(&tkey)?;

        // First check the key exists and isn't expired.
        table.get(key, now_ms)?;

        // Now get the metadata for TTL info.
        let meta = table.get_entry_meta(key)?;
        if !meta.has_ttl {
            Some(-1)
        } else {
            let remaining = meta.expire_at_ms.saturating_sub(now_ms);
            Some(remaining as i64)
        }
    }

    /// PUT: insert or update. Returns old value if overwritten.
    ///
    /// If `ttl_ms > 0`, schedules expiry. If the key already had a TTL,
    /// the old expiry is cancelled and replaced.
    pub fn put(
        &mut self,
        tenant_id: u32,
        collection: &str,
        key: &[u8],
        value: &[u8],
        ttl_ms: u64,
        now_ms: u64,
    ) -> Option<Vec<u8>> {
        let expire_at = if ttl_ms > 0 {
            now_ms + ttl_ms
        } else {
            NO_EXPIRY
        };

        let tkey = table_key(tenant_id, collection);

        // Single-pass: check indexes + get old entry meta in one HashMap lookup.
        let has_indexes = self.indexes.get(&tkey).is_some_and(|idx| !idx.is_empty());
        let old_expire = self
            .tables
            .get(&tkey)
            .and_then(|t| t.get_entry_meta(key))
            .and_then(|m| {
                if m.has_ttl {
                    Some(m.expire_at_ms)
                } else {
                    None
                }
            });

        // Cancel old expiry (before mutating the table).
        if let Some(old_ms) = old_expire {
            let composite = expiry_key(tenant_id, collection, key);
            self.expiry.cancel(&composite, old_ms);
        }

        // Insert/update. Use get_mut (no clone) for existing tables,
        // entry (clones tkey) only for first-time table creation.
        let table = if let Some(t) = self.tables.get_mut(&tkey) {
            t
        } else {
            self.hash_to_tenant.entry(tkey).or_insert(tenant_id);
            self.hash_to_collection
                .entry(tkey)
                .or_insert_with(|| collection.to_string());
            self.tables.entry(tkey).or_insert_with(|| {
                KvHashTable::new(
                    self.default_capacity,
                    self.load_factor_threshold,
                    self.rehash_batch_size,
                    self.inline_threshold,
                )
            })
        };
        let old = table.put(key, value, expire_at);

        // Schedule new expiry.
        if expire_at != NO_EXPIRY {
            let composite = expiry_key(tenant_id, collection, key);
            self.expiry.insert(composite, expire_at);
        }

        // Secondary index maintenance (zero-index fast path: skip entirely).
        let has_sorted = self.sorted_indexes.has_indexes(tkey);
        if has_indexes || has_sorted {
            let new_value_bytes: Vec<u8> = self
                .tables
                .get(&tkey)
                .and_then(|t| t.get(key, now_ms))
                .map(|v| v.to_vec())
                .unwrap_or_default();
            let new_fields = extract_all_field_values_from_msgpack(&new_value_bytes);
            let old_fields = old
                .as_ref()
                .map(|v| extract_all_field_values_from_msgpack(v));

            if has_indexes {
                let new_refs: Vec<(&str, &[u8])> = new_fields
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_slice()))
                    .collect();
                let old_refs: Option<Vec<(&str, &[u8])>> = old_fields
                    .as_ref()
                    .map(|f| f.iter().map(|(k, v)| (k.as_str(), v.as_slice())).collect());

                if let Some(idx_set) = self.indexes.get_mut(&tkey) {
                    idx_set.on_put(key, &new_refs, old_refs.as_deref());
                }
            }

            if has_sorted {
                self.sorted_indexes.on_put(tkey, key, &new_fields);
            }
        }

        old
    }

    /// DELETE: remove key(s). Returns count of keys actually deleted.
    pub fn delete(
        &mut self,
        tenant_id: u32,
        collection: &str,
        keys: &[Vec<u8>],
        now_ms: u64,
    ) -> usize {
        let tkey = table_key(tenant_id, collection);
        let table = match self.tables.get_mut(&tkey) {
            Some(t) => t,
            None => return 0,
        };

        let mut count = 0;
        let has_indexes = self.indexes.get(&tkey).is_some_and(|s| !s.is_empty());
        let has_sorted = self.sorted_indexes.has_indexes(tkey);

        for key in keys {
            // Cancel expiry if the key had one.
            if let Some(meta) = table.get_entry_meta(key)
                && meta.has_ttl
            {
                let composite = expiry_key(tenant_id, collection, key);
                self.expiry.cancel(&composite, meta.expire_at_ms);
            }

            // Extract field values before deletion (for index cleanup).
            let old_fields = if has_indexes {
                table
                    .get(key, now_ms)
                    .map(extract_all_field_values_from_msgpack)
            } else {
                None
            };

            if table.delete(key, now_ms) {
                count += 1;

                // Clean up secondary indexes.
                if let Some(fields) = &old_fields
                    && let Some(idx_set) = self.indexes.get_mut(&tkey)
                {
                    let refs: Vec<(&str, &[u8])> = fields
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_slice()))
                        .collect();
                    idx_set.on_delete(key, &refs);
                }

                // Clean up sorted indexes.
                if has_sorted {
                    self.sorted_indexes.on_delete(tkey, key);
                }
            }
        }
        count
    }

    /// EXPIRE: set or update TTL on an existing key.
    /// Returns true if the key was found and TTL was set.
    pub fn expire(
        &mut self,
        tenant_id: u32,
        collection: &str,
        key: &[u8],
        ttl_ms: u64,
        now_ms: u64,
    ) -> bool {
        let tkey = table_key(tenant_id, collection);
        let table = match self.tables.get_mut(&tkey) {
            Some(t) => t,
            None => return false,
        };

        // Cancel old expiry.
        if let Some(meta) = table.get_entry_meta(key)
            && meta.has_ttl
        {
            let composite = expiry_key(tenant_id, collection, key);
            self.expiry.cancel(&composite, meta.expire_at_ms);
        }

        let expire_at = now_ms + ttl_ms;
        if table.set_expire(key, expire_at) {
            let composite = expiry_key(tenant_id, collection, key);
            self.expiry.insert(composite, expire_at);
            true
        } else {
            false
        }
    }

    /// PERSIST: remove TTL from a key. Returns true if the key was found.
    pub fn persist(&mut self, tenant_id: u32, collection: &str, key: &[u8]) -> bool {
        let tkey = table_key(tenant_id, collection);
        let table = match self.tables.get_mut(&tkey) {
            Some(t) => t,
            None => return false,
        };

        if let Some(meta) = table.get_entry_meta(key)
            && meta.has_ttl
        {
            let composite = expiry_key(tenant_id, collection, key);
            self.expiry.cancel(&composite, meta.expire_at_ms);
        }

        table.persist(key)
    }

    /// BATCH GET: fetch multiple keys. Returns values in order (None for missing).
    pub fn batch_get(
        &self,
        tenant_id: u32,
        collection: &str,
        keys: &[Vec<u8>],
        now_ms: u64,
    ) -> Vec<Option<Vec<u8>>> {
        keys.iter()
            .map(|k| self.get(tenant_id, collection, k, now_ms))
            .collect()
    }

    /// BATCH PUT: insert/update multiple pairs. Returns count of new keys.
    pub fn batch_put(
        &mut self,
        tenant_id: u32,
        collection: &str,
        entries: &[(Vec<u8>, Vec<u8>)],
        ttl_ms: u64,
        now_ms: u64,
    ) -> usize {
        let mut new_count = 0;
        for (key, value) in entries {
            if self
                .put(tenant_id, collection, key, value, ttl_ms, now_ms)
                .is_none()
            {
                new_count += 1;
            }
        }
        new_count
    }

    /// SCAN: cursor-based iteration with optional key pattern matching and
    /// index-accelerated predicate pushdown.
    ///
    /// If `filter_field` and `filter_value` are provided AND a secondary index
    /// exists for that field, the scan uses the index to narrow candidates
    /// (O(log n) + O(k) where k = matching keys) instead of full table scan.
    ///
    /// Returns `(entries, next_cursor_bytes)`. `next_cursor_bytes` is empty
    /// when the scan is complete. Each entry is `(key, value)`.
    #[allow(clippy::too_many_arguments)]
    pub fn scan(
        &self,
        tenant_id: u32,
        collection: &str,
        cursor: &[u8],
        count: usize,
        now_ms: u64,
        match_pattern: Option<&str>,
        filter_field: Option<&str>,
        filter_value: Option<&[u8]>,
    ) -> ScanResult {
        let tkey = table_key(tenant_id, collection);
        let table = match self.tables.get(&tkey) {
            Some(t) => t,
            None => return (Vec::new(), Vec::new()),
        };

        // Index-accelerated path: if we have an equality filter and an index, use it.
        // Also checks composite indexes for prefix matches.
        if let Some(field) = filter_field
            && let Some(value) = filter_value
            && let Some(idx_set) = self.indexes.get(&tkey)
        {
            // Try single-field index first.
            let candidate_keys = if idx_set.get_index(field).is_some() {
                idx_set.lookup_eq(field, value)
            } else if let Some(ci) = idx_set.find_composite_with_prefix(field) {
                // Composite index prefix match: use leading field.
                ci.lookup_prefix(&[value])
            } else {
                Vec::new() // No index available — will fall through to full scan.
            };

            if !candidate_keys.is_empty() {
                let mut results = Vec::with_capacity(count.min(candidate_keys.len()));

                for pk in candidate_keys {
                    if results.len() >= count {
                        break;
                    }
                    if let Some(val) = table.get(pk, now_ms)
                        && (match_pattern.is_none()
                            || super::scan::matches_pattern_pub(pk, match_pattern))
                    {
                        results.push((pk.to_vec(), val.to_vec()));
                    }
                }

                return (results, Vec::new());
            }
        }

        // Full scan fallback: iterate hash table slots.
        let cursor_idx = if cursor.len() >= 4 {
            u32::from_be_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]) as usize
        } else {
            0
        };

        let (entries, next_cursor_idx) = table.scan(cursor_idx, count, now_ms, match_pattern);

        let owned: Vec<(Vec<u8>, Vec<u8>)> = entries
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();

        let next_cursor = if next_cursor_idx == 0 {
            Vec::new()
        } else {
            (next_cursor_idx as u32).to_be_bytes().to_vec()
        };

        (owned, next_cursor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> u64 {
        1_000_000
    }

    fn make_engine() -> KvEngine {
        KvEngine::new(now(), 16, 0.75, 4, 64, 1000, 1024)
    }

    #[test]
    fn basic_get_put_delete() {
        let mut e = make_engine();
        let n = now();

        assert!(e.get(1, "cache", b"k1", n).is_none());

        e.put(1, "cache", b"k1", b"v1", 0, n);
        assert_eq!(e.get(1, "cache", b"k1", n).unwrap(), b"v1");

        e.put(1, "cache", b"k1", b"v2", 0, n);
        assert_eq!(e.get(1, "cache", b"k1", n).unwrap(), b"v2");

        assert_eq!(e.delete(1, "cache", &[b"k1".to_vec()], n), 1);
        assert!(e.get(1, "cache", b"k1", n).is_none());
    }

    #[test]
    fn ttl_expiry_via_tick() {
        let mut e = make_engine();
        let n = now();

        // Put with 5-second TTL.
        e.put(1, "sess", b"s1", b"data", 5000, n);
        assert!(e.get(1, "sess", b"s1", n).is_some());

        // Still alive at t+4999.
        assert!(e.get(1, "sess", b"s1", n + 4999).is_some());

        // Expired at t+5000 (lazy fallback).
        assert!(e.get(1, "sess", b"s1", n + 5000).is_none());

        // Tick reaps it.
        let reaped = e.tick_expiry(n + 5000);
        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].collection, "sess");
        assert_eq!(reaped[0].key, b"s1");
        assert_eq!(e.total_entries(), 0);
    }

    #[test]
    fn persist_removes_ttl() {
        let mut e = make_engine();
        let n = now();

        e.put(1, "cache", b"k", b"v", 3000, n);
        assert!(e.persist(1, "cache", b"k"));

        // Should never expire now.
        assert!(e.get(1, "cache", b"k", n + 100_000).is_some());
    }

    #[test]
    fn expire_sets_ttl() {
        let mut e = make_engine();
        let n = now();

        e.put(1, "cache", b"k", b"v", 0, n);
        assert!(e.get(1, "cache", b"k", n + 100_000).is_some()); // No TTL.

        assert!(e.expire(1, "cache", b"k", 2000, n));
        assert!(e.get(1, "cache", b"k", n + 1999).is_some());
        assert!(e.get(1, "cache", b"k", n + 2000).is_none()); // Expired.
    }

    #[test]
    fn batch_get_and_put() {
        let mut e = make_engine();
        let n = now();

        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..5u8).map(|i| (vec![i], vec![i * 10])).collect();
        let new_count = e.batch_put(1, "c", &entries, 0, n);
        assert_eq!(new_count, 5);

        let keys: Vec<Vec<u8>> = (0..7u8).map(|i| vec![i]).collect();
        let results = e.batch_get(1, "c", &keys, n);
        assert_eq!(results.len(), 7);
        assert_eq!(results[0], Some(vec![0]));
        assert_eq!(results[4], Some(vec![40]));
        assert!(results[5].is_none()); // Key 5 doesn't exist.
        assert!(results[6].is_none());
    }

    #[test]
    fn tenant_isolation() {
        let mut e = make_engine();
        let n = now();

        e.put(1, "c", b"k", b"t1", 0, n);
        e.put(2, "c", b"k", b"t2", 0, n);

        assert_eq!(e.get(1, "c", b"k", n).unwrap(), b"t1");
        assert_eq!(e.get(2, "c", b"k", n).unwrap(), b"t2");
    }

    #[test]
    fn stats() {
        let mut e = make_engine();
        let n = now();

        assert_eq!(e.total_entries(), 0);

        for i in 0..10u32 {
            e.put(1, "c", &i.to_be_bytes(), &[0; 32], 0, n);
        }
        assert_eq!(e.total_entries(), 10);
        assert_eq!(e.collection_len(1, "c"), 10);
        assert!(e.total_mem_usage() > 0);
    }

    /// Helper: create a MessagePack-encoded JSON object value.
    fn mp_obj(fields: &[(&str, &str)]) -> Vec<u8> {
        let obj: serde_json::Map<String, serde_json::Value> = fields
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        nodedb_types::json_to_msgpack(&serde_json::Value::Object(obj)).unwrap()
    }

    #[test]
    fn register_index_and_lookup() {
        let mut e = make_engine();
        let n = now();

        // Insert some entries before creating the index.
        e.put(
            1,
            "sessions",
            b"s1",
            &mp_obj(&[("region", "us-east"), ("status", "active")]),
            0,
            n,
        );
        e.put(
            1,
            "sessions",
            b"s2",
            &mp_obj(&[("region", "us-east"), ("status", "inactive")]),
            0,
            n,
        );
        e.put(
            1,
            "sessions",
            b"s3",
            &mp_obj(&[("region", "eu-west"), ("status", "active")]),
            0,
            n,
        );

        // Create index with backfill.
        let backfilled = e.register_index(1, "sessions", "region", 0, true, n);
        assert_eq!(backfilled, 3);

        // Lookup by indexed field.
        let us_east = e.index_lookup_eq(1, "sessions", "region", b"us-east");
        assert_eq!(us_east.len(), 2);
        assert!(us_east.contains(&b"s1".to_vec()));
        assert!(us_east.contains(&b"s2".to_vec()));

        let eu_west = e.index_lookup_eq(1, "sessions", "region", b"eu-west");
        assert_eq!(eu_west.len(), 1);
    }

    #[test]
    fn index_maintained_on_put() {
        let mut e = make_engine();
        let n = now();

        // Create index first (no backfill needed — empty collection).
        e.register_index(1, "c", "status", 0, false, n);

        // Insert.
        e.put(1, "c", b"k1", &mp_obj(&[("status", "active")]), 0, n);
        assert_eq!(e.index_lookup_eq(1, "c", "status", b"active").len(), 1);

        // Update: status changes.
        e.put(1, "c", b"k1", &mp_obj(&[("status", "inactive")]), 0, n);
        assert!(e.index_lookup_eq(1, "c", "status", b"active").is_empty());
        assert_eq!(e.index_lookup_eq(1, "c", "status", b"inactive").len(), 1);
    }

    #[test]
    fn index_cleaned_on_delete() {
        let mut e = make_engine();
        let n = now();

        e.register_index(1, "c", "region", 0, false, n);
        e.put(1, "c", b"k1", &mp_obj(&[("region", "us")]), 0, n);
        e.put(1, "c", b"k2", &mp_obj(&[("region", "us")]), 0, n);

        assert_eq!(e.index_lookup_eq(1, "c", "region", b"us").len(), 2);

        e.delete(1, "c", &[b"k1".to_vec()], n);
        assert_eq!(e.index_lookup_eq(1, "c", "region", b"us").len(), 1);
    }

    #[test]
    fn zero_index_fast_path() {
        let mut e = make_engine();
        let n = now();

        // No indexes — PUT should work without index overhead.
        assert!(!e.has_indexes(1, "c"));
        e.put(1, "c", b"k", b"raw_value", 0, n);
        assert!(e.get(1, "c", b"k", n).is_some());
        assert_eq!(e.write_amp_ratio(1, "c"), 0.0);
    }

    #[test]
    fn drop_index_clears_entries() {
        let mut e = make_engine();
        let n = now();

        e.register_index(1, "c", "status", 0, false, n);
        e.put(1, "c", b"k1", &mp_obj(&[("status", "active")]), 0, n);
        assert_eq!(e.index_count(1, "c"), 1);

        let dropped = e.drop_index(1, "c", "status");
        assert_eq!(dropped, 1);
        assert_eq!(e.index_count(1, "c"), 0);
        assert!(e.index_lookup_eq(1, "c", "status", b"active").is_empty());
    }

    #[test]
    fn write_amp_tracking() {
        let mut e = make_engine();
        let n = now();

        e.register_index(1, "c", "a", 0, false, n);
        e.register_index(1, "c", "b", 1, false, n);

        for i in 0..10u32 {
            let k = format!("k{i}");
            e.put(
                1,
                "c",
                k.as_bytes(),
                &mp_obj(&[("a", "x"), ("b", "y")]),
                0,
                n,
            );
        }

        // 10 PUTs, 2 indexes each = write amp ratio of 2.0.
        let ratio = e.write_amp_ratio(1, "c");
        assert!((ratio - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn raw_put_timing() {
        let mut e = make_engine();
        let n = now();
        let keys: Vec<Vec<u8>> = (0..10_000u32).map(|i| i.to_be_bytes().to_vec()).collect();
        let value = [0u8; 64];

        // Warmup: insert all keys once.
        for key in &keys {
            e.put(1, "b", key, &value, 0, n);
        }

        // Timed: 100K updates (keys already exist).
        let iters = 100_000u64;
        let start = std::time::Instant::now();
        for i in 0..iters {
            let key = &keys[(i as usize) % 10_000];
            e.put(1, "b", key, &value, 0, n);
        }
        let elapsed = start.elapsed();
        let ns_per_op = elapsed.as_nanos() / iters as u128;
        // 691 ns/op measured — well under document's 12μs.
        assert!(ns_per_op < 5_000, "PUT too slow: {ns_per_op} ns/op");
    }
}
