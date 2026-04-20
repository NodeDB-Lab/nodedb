//! KV engine expiry, truncate, and observability stats.
//!
//! Methods on [`super::engine::KvEngine`] for expiry wheel management,
//! collection truncation, and comprehensive stats snapshots.

use super::engine::KvEngine;
use super::engine_helpers::{parse_expiry_key, table_key};

/// A key that was reaped by the expiry wheel.
///
/// Returned by [`KvEngine::tick_expiry`] so the caller can produce WAL
/// tombstones and CDC/keyspace notification events.
#[derive(Debug, Clone)]
pub struct ExpiredKey {
    pub tenant_id: u32,
    pub collection: String,
    pub key: Vec<u8>,
}

/// Observability snapshot for the KV engine on a single TPC core.
///
/// Produced by [`KvEngine::stats`]. Written to the telemetry ring
/// for the Control Plane to expose via HTTP metrics endpoint.
#[derive(Debug, Clone, Default)]
pub struct KvStats {
    /// Total key count across all collections.
    pub total_entries: usize,
    /// Approximate total memory usage in bytes.
    pub total_mem_bytes: usize,
    /// Number of active KV collections on this core.
    pub collection_count: usize,
    /// Highest load factor across all hash tables (triggers rehash at threshold).
    pub max_load_factor: f32,
    /// Whether any hash table is currently in incremental rehash.
    pub is_rehashing: bool,
    /// Total secondary index entries across all collections.
    pub total_index_entries: usize,
    /// Number of entries in the expiry wheel.
    pub expiry_queue_depth: usize,
    /// Number of deferred expirations (reap budget exceeded).
    pub expiry_backlog: usize,
}

impl KvEngine {
    // -----------------------------------------------------------------------
    // Expiry wheel tick — called from the TPC event loop
    // -----------------------------------------------------------------------

    /// Advance the expiry wheel and reap expired keys.
    ///
    /// Call this from the TPC core's event loop at the configured tick interval.
    /// Returns a list of `(tenant_id, collection, key)` for each reaped key,
    /// enabling the caller to produce WAL tombstones and CDC/keyspace events.
    pub fn tick_expiry(&mut self, now_ms: u64) -> Vec<ExpiredKey> {
        let batch = self.expiry.tick(now_ms);
        let mut reaped = Vec::new();

        for (composite_key, expire_at_ms) in &batch.expired {
            if let Some((tid, collection, key)) = parse_expiry_key(composite_key)
                && let Some(table) = self.tables.get_mut(&table_key(tid, &collection))
                && table.reap_expired(&key, *expire_at_ms)
            {
                reaped.push(ExpiredKey {
                    tenant_id: tid,
                    collection,
                    key,
                });
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
    // Truncate
    // -----------------------------------------------------------------------

    /// Truncate: delete all entries in a KV collection. Returns count deleted.
    pub fn truncate(&mut self, tenant_id: u32, collection: &str) -> usize {
        let tkey = table_key(tenant_id, collection);
        let count = self.tables.get(&tkey).map(|t| t.len()).unwrap_or(0);

        // Remove the hash table entirely.
        self.tables.remove(&tkey);
        // Remove all indexes.
        self.indexes.remove(&tkey);
        // Note: expiry wheel entries for this collection will be no-ops
        // when they fire (key won't be found in the hash table).

        count
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

    /// Approximate memory usage for a specific collection. Sums the
    /// hash table's own `mem_usage()` estimate; returns 0 if no table
    /// exists for `(tenant_id, collection)`.
    pub fn collection_mem_usage(&self, tenant_id: u32, collection: &str) -> u64 {
        let tkey = table_key(tenant_id, collection);
        self.tables
            .get(&tkey)
            .map(|t| t.mem_usage() as u64)
            .unwrap_or(0)
    }

    /// Comprehensive observability snapshot for this KV engine.
    pub fn stats(&self) -> KvStats {
        let mut total_entries = 0usize;
        let mut total_mem = 0usize;
        let mut total_index_entries = 0usize;
        let mut is_rehashing = false;
        let mut max_load_factor: f32 = 0.0;

        for table in self.tables.values() {
            total_entries += table.len();
            total_mem += table.mem_usage();
            if table.load_factor() > max_load_factor {
                max_load_factor = table.load_factor();
            }
            if table.is_rehashing() {
                is_rehashing = true;
            }
        }

        for idx_set in self.indexes.values() {
            for field in idx_set.indexed_fields() {
                if let Some(idx) = idx_set.get_index(field) {
                    total_index_entries += idx.entry_count();
                }
            }
        }

        KvStats {
            total_entries,
            total_mem_bytes: total_mem,
            collection_count: self.tables.len(),
            max_load_factor,
            is_rehashing,
            total_index_entries,
            expiry_queue_depth: self.expiry.len(),
            expiry_backlog: self.expiry.backlog(),
        }
    }
}
