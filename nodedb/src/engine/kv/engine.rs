//! KvEngine: per-core KV engine owning hash tables and expiry wheel.
//!
//! `!Send` — owned by a single TPC core. Each collection gets its own
//! hash table; the expiry wheel is shared across all collections on
//! this core (one wheel tick processes all collections).

use std::collections::HashMap;

use super::entry::NO_EXPIRY;
use super::expiry_wheel::ExpiryWheel;
use super::hash_table::KvHashTable;

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
    tables: HashMap<String, KvHashTable>,
    /// Shared expiry wheel across all collections on this core.
    expiry: ExpiryWheel,
    /// Default tuning parameters for new collections.
    default_capacity: usize,
    load_factor_threshold: f32,
    rehash_batch_size: usize,
    inline_threshold: usize,
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
            expiry: ExpiryWheel::new(now_ms, expiry_tick_ms, expiry_reap_budget),
            default_capacity,
            load_factor_threshold,
            rehash_batch_size,
            inline_threshold,
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

    /// Get or create the hash table for a collection.
    fn table(&mut self, tenant_id: u32, collection: &str) -> &mut KvHashTable {
        let key = table_key(tenant_id, collection);
        self.tables.entry(key).or_insert_with(|| {
            KvHashTable::new(
                self.default_capacity,
                self.load_factor_threshold,
                self.rehash_batch_size,
                self.inline_threshold,
            )
        })
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

    /// PUT: insert or update. Returns old value if overwritten.
    ///
    /// If `ttl_ms > 0`, schedules expiry. If the key already had a TTL,
    /// the old expiry is cancelled and replaced.
    pub fn put(
        &mut self,
        tenant_id: u32,
        collection: &str,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Option<Vec<u8>> {
        let expire_at = if ttl_ms > 0 {
            now_ms + ttl_ms
        } else {
            NO_EXPIRY
        };

        let tkey = table_key(tenant_id, collection);

        // Cancel old expiry if the key existed with a TTL.
        if let Some(table) = self.tables.get(&tkey)
            && let Some(meta) = table.get_entry_meta(&key)
            && meta.has_ttl
        {
            let composite = expiry_key(tenant_id, collection, &key);
            self.expiry.cancel(&composite, meta.expire_at_ms);
        }

        let table = self.table(tenant_id, collection);
        let old = table.put(key.clone(), value, expire_at);

        // Schedule new expiry.
        if expire_at != NO_EXPIRY {
            let composite = expiry_key(tenant_id, collection, &key);
            self.expiry.insert(composite, expire_at);
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
        for key in keys {
            // Cancel expiry if the key had one.
            if let Some(meta) = table.get_entry_meta(key)
                && meta.has_ttl
            {
                let composite = expiry_key(tenant_id, collection, key);
                self.expiry.cancel(&composite, meta.expire_at_ms);
            }

            if table.delete(key, now_ms) {
                count += 1;
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
                .put(
                    tenant_id,
                    collection,
                    key.clone(),
                    value.clone(),
                    ttl_ms,
                    now_ms,
                )
                .is_none()
            {
                new_count += 1;
            }
        }
        new_count
    }

    /// SCAN: cursor-based iteration with optional key pattern matching.
    ///
    /// Returns `(entries, next_cursor_bytes)`. `next_cursor_bytes` is empty
    /// when the scan is complete. Each entry is `(key, value)`.
    pub fn scan(
        &self,
        tenant_id: u32,
        collection: &str,
        cursor: &[u8],
        count: usize,
        now_ms: u64,
        match_pattern: Option<&str>,
    ) -> ScanResult {
        let tkey = table_key(tenant_id, collection);
        let table = match self.tables.get(&tkey) {
            Some(t) => t,
            None => return (Vec::new(), Vec::new()),
        };

        // Decode cursor: 4 bytes big-endian u32 slot index, or 0 for start.
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
            Vec::new() // Scan complete.
        } else {
            (next_cursor_idx as u32).to_be_bytes().to_vec()
        };

        (owned, next_cursor)
    }

    // -----------------------------------------------------------------------
    // Expiry wheel tick — called from the TPC event loop
    // -----------------------------------------------------------------------

    /// Advance the expiry wheel and reap expired keys.
    ///
    /// Call this from the TPC core's event loop at the configured tick interval.
    /// Returns the number of keys reaped.
    pub fn tick_expiry(&mut self, now_ms: u64) -> usize {
        let batch = self.expiry.tick(now_ms);
        let mut reaped = 0;

        for (composite_key, expire_at_ms) in &batch.expired {
            if let Some((tid, collection, key)) = parse_expiry_key(composite_key)
                && let Some(table) = self.tables.get_mut(&table_key(tid, &collection))
                && table.reap_expired(&key, *expire_at_ms)
            {
                reaped += 1;
            }
        }

        reaped
    }

    /// Number of entries tracked in the expiry wheel.
    pub fn expiry_queue_depth(&self) -> usize {
        self.expiry.len()
    }

    /// Number of deferred expirations (backlog gauge).
    pub fn expiry_backlog(&self) -> usize {
        self.expiry.backlog()
    }

    // -----------------------------------------------------------------------
    // Stats
    // -----------------------------------------------------------------------

    /// Total number of entries across all collections.
    pub fn total_entries(&self) -> usize {
        self.tables.values().map(|t| t.len()).sum()
    }

    /// Total approximate memory usage across all collections.
    pub fn total_mem_usage(&self) -> usize {
        self.tables.values().map(|t| t.mem_usage()).sum()
    }

    /// Entry count for a specific collection.
    pub fn collection_len(&self, tenant_id: u32, collection: &str) -> usize {
        let tkey = table_key(tenant_id, collection);
        self.tables.get(&tkey).map(|t| t.len()).unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Key encoding helpers
// ---------------------------------------------------------------------------

/// Construct the per-collection hash table key: "{tenant_id}:{collection}".
fn table_key(tenant_id: u32, collection: &str) -> String {
    format!("{tenant_id}:{collection}")
}

/// Construct a composite key for the expiry wheel: "{tenant_id}:{collection}\0{key_bytes}".
/// The null byte separator is safe because collection names can't contain null.
fn expiry_key(tenant_id: u32, collection: &str, key: &[u8]) -> Vec<u8> {
    let prefix = format!("{tenant_id}:{collection}\0");
    let mut composite = prefix.into_bytes();
    composite.extend_from_slice(key);
    composite
}

/// Parse a composite expiry key back into (tenant_id, collection, key_bytes).
fn parse_expiry_key(composite: &[u8]) -> Option<(u32, String, Vec<u8>)> {
    let null_pos = composite.iter().position(|&b| b == 0)?;
    let prefix = std::str::from_utf8(&composite[..null_pos]).ok()?;
    let colon_pos = prefix.find(':')?;
    let tenant_id: u32 = prefix[..colon_pos].parse().ok()?;
    let collection = prefix[colon_pos + 1..].to_string();
    let key = composite[null_pos + 1..].to_vec();
    Some((tenant_id, collection, key))
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

        e.put(1, "cache", b"k1".to_vec(), b"v1".to_vec(), 0, n);
        assert_eq!(e.get(1, "cache", b"k1", n).unwrap(), b"v1");

        e.put(1, "cache", b"k1".to_vec(), b"v2".to_vec(), 0, n);
        assert_eq!(e.get(1, "cache", b"k1", n).unwrap(), b"v2");

        assert_eq!(e.delete(1, "cache", &[b"k1".to_vec()], n), 1);
        assert!(e.get(1, "cache", b"k1", n).is_none());
    }

    #[test]
    fn ttl_expiry_via_tick() {
        let mut e = make_engine();
        let n = now();

        // Put with 5-second TTL.
        e.put(1, "sess", b"s1".to_vec(), b"data".to_vec(), 5000, n);
        assert!(e.get(1, "sess", b"s1", n).is_some());

        // Still alive at t+4999.
        assert!(e.get(1, "sess", b"s1", n + 4999).is_some());

        // Expired at t+5000 (lazy fallback).
        assert!(e.get(1, "sess", b"s1", n + 5000).is_none());

        // Tick reaps it.
        let reaped = e.tick_expiry(n + 5000);
        assert_eq!(reaped, 1);
        assert_eq!(e.total_entries(), 0);
    }

    #[test]
    fn persist_removes_ttl() {
        let mut e = make_engine();
        let n = now();

        e.put(1, "cache", b"k".to_vec(), b"v".to_vec(), 3000, n);
        assert!(e.persist(1, "cache", b"k"));

        // Should never expire now.
        assert!(e.get(1, "cache", b"k", n + 100_000).is_some());
    }

    #[test]
    fn expire_sets_ttl() {
        let mut e = make_engine();
        let n = now();

        e.put(1, "cache", b"k".to_vec(), b"v".to_vec(), 0, n);
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

        e.put(1, "c", b"k".to_vec(), b"t1".to_vec(), 0, n);
        e.put(2, "c", b"k".to_vec(), b"t2".to_vec(), 0, n);

        assert_eq!(e.get(1, "c", b"k", n).unwrap(), b"t1");
        assert_eq!(e.get(2, "c", b"k", n).unwrap(), b"t2");
    }

    #[test]
    fn stats() {
        let mut e = make_engine();
        let n = now();

        assert_eq!(e.total_entries(), 0);

        for i in 0..10u32 {
            e.put(1, "c", i.to_be_bytes().to_vec(), vec![0; 32], 0, n);
        }
        assert_eq!(e.total_entries(), 10);
        assert_eq!(e.collection_len(1, "c"), 10);
        assert!(e.total_mem_usage() > 0);
    }
}
