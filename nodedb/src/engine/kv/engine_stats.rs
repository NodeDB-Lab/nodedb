// SPDX-License-Identifier: BUSL-1.1

//! KV engine expiry, truncate, and observability stats.
//!
//! Methods on [`super::engine::KvEngine`] for expiry wheel management,
//! collection truncation, and comprehensive stats snapshots.

use super::engine::KvEngine;
use super::engine_helpers::{extract_all_field_values_from_msgpack, parse_expiry_key, table_key};

/// A key a TTL reap ended.
///
/// Returned by [`KvEngine::reap_due`].
#[derive(Debug, Clone)]
pub struct ExpiredKey {
    pub database_id: u64,
    pub tenant_id: u64,
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
    /// Reap the key `composite_key` names when it still holds the expiry
    /// `expire_at_ms`. `None` when it holds another expiry or none, or its
    /// table is gone.
    ///
    /// A reap is a delete, so it maintains the row's secondary, composite, and
    /// sorted index entries exactly as [`KvEngine::delete`] does. Reaping the
    /// hash slot alone would strand them: the sorted index answers `rank` /
    /// `top_k` straight out of its tree with no re-check against the table, so
    /// a stranded entry is a wrong answer (an expired key holding a rank and
    /// displacing every live key below it), and the checkpoint exports index
    /// content verbatim, so it would survive a restart.
    ///
    /// [`KvEngine::delete`]: KvEngine::delete
    pub(in crate::engine::kv) fn reap_composite(
        &mut self,
        composite_key: &[u8],
        expire_at_ms: u64,
    ) -> Option<ExpiredKey> {
        // A reap ends the version at its expiry instant: the last ordinal
        // of the expiry millisecond, raised above the version's own ordinal
        // (`VersionStamp`). The instant is the one the write's record
        // carries, so every replica and every replay ends the version at the
        // same ordinal, whenever its reap runs. The reaped value stays
        // readable at every earlier ordinal.
        let ordinal = reap_ordinal(expire_at_ms);
        let (did, tid, collection, key) = parse_expiry_key(composite_key)?;
        let tkey = table_key(did, tid, &collection);

        // Zero-index fast path: the common index-less TTL collection pays
        // nothing beyond the reap itself.
        let has_indexes = self.indexes.get(&tkey).is_some_and(|s| !s.is_empty());
        let has_sorted = self.sorted_indexes.has_indexes(tkey);

        // Read the indexed field values BEFORE the reap frees the value they
        // live in, and own them so the table borrow ends here — the reap and
        // the index update below both need `&mut self`. Expiry-blind by
        // necessity: the row is expired by definition at this point.
        let old_fields: Option<Vec<(String, Vec<u8>)>> = if has_indexes {
            self.tables
                .get(&tkey)
                .and_then(|t| t.get_ignoring_expiry(&key))
                .map(extract_all_field_values_from_msgpack)
        } else {
            None
        };

        let table = self.table_mut_for_write(tkey)?;
        // A mismatched `expire_at_ms` means the TTL was replaced after this
        // reap was scheduled — the row is still live, so nothing to clean up.
        if !table.reap_expired(&key, expire_at_ms, ordinal) {
            return None;
        }
        self.observe_ordinal(ordinal);
        self.recharge_table(tkey);

        if let Some(fields) = &old_fields
            && let Some(idx_set) = self.indexes.get_mut(&tkey)
        {
            let refs: Vec<(&str, &[u8])> = fields
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_slice()))
                .collect();
            idx_set.on_delete(&key, &refs);
        }

        if has_sorted {
            self.sorted_indexes.on_delete(tkey, &key);
        }

        Some(ExpiredKey {
            database_id: did,
            tenant_id: tid,
            collection,
            key,
        })
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

    /// Truncate: delete all entries in a KV collection at the current write
    /// ordinal. Returns count deleted. Every removed row stays readable at an
    /// earlier ordinal until a purge passes the truncate.
    pub fn truncate(&mut self, database_id: u64, tenant_id: u64, collection: &str) -> usize {
        let tkey = table_key(database_id, tenant_id, collection);
        let ordinal = self.write_ordinal();

        // Bump the write epoch so a cached aggregate result computed before
        // the truncate is a miss.
        self.bump_write_epoch(tkey);
        let count = self
            .tables
            .get_mut(&tkey)
            .map_or(0, |table| table.retire_all(ordinal));
        self.recharge_table(tkey);
        // The rows leave every secondary and sorted index. The registrations
        // stay: TRUNCATE drops no index, so a later write files into them
        // and a rolled-back TRUNCATE refills them as it reinstates each row.
        // A sorted index answers `rank` / `top_k` from its tree alone, so a
        // tree left full would keep ranking removed rows.
        if let Some(idx_set) = self.indexes.get_mut(&tkey) {
            idx_set.clear_entries();
        }
        self.sorted_indexes
            .clear_collection(database_id, tenant_id, collection);
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
    pub fn collection_len(&self, database_id: u64, tenant_id: u64, collection: &str) -> usize {
        let tkey = table_key(database_id, tenant_id, collection);
        self.tables.get(&tkey).map(|t| t.len()).unwrap_or(0)
    }

    /// Approximate memory usage for a specific collection. Sums the
    /// hash table's own `mem_usage()` estimate; returns 0 if no table
    /// exists for `(tenant_id, collection)`.
    pub fn collection_mem_usage(&self, database_id: u64, tenant_id: u64, collection: &str) -> u64 {
        let tkey = table_key(database_id, tenant_id, collection);
        self.tables
            .get(&tkey)
            .map(|t| t.mem_usage() as u64)
            .unwrap_or(0)
    }

    /// Each collection's `(database_id, tenant_id, collection, bytes)`, with
    /// the bytes [`Self::collection_mem_usage`] reports. A table with no
    /// reverse-mapped name or scope is skipped.
    pub fn collection_mem_usages(&self) -> impl Iterator<Item = (u64, u64, &str, u64)> {
        self.tables.iter().filter_map(|(tkey, table)| {
            let database_id = self.hash_to_database.get(tkey)?.as_u64();
            let tenant_id = *self.hash_to_tenant.get(tkey)?;
            let collection = self.hash_to_collection.get(tkey)?;
            Some((
                database_id,
                tenant_id,
                collection.as_str(),
                table.mem_usage() as u64,
            ))
        })
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

/// The ordinal a reap of a version that expires at `expire_at_ms` ends it
/// at: the last ordinal of the expiry millisecond.
pub(super) fn reap_ordinal(expire_at_ms: u64) -> i64 {
    nodedb_types::ms_to_ordinal_upper(i64::try_from(expire_at_ms).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use crate::engine::kv::test_support::row_surrogate;
    use crate::engine::kv::{KvPutParams, RegisterIndexParams};

    use super::*;

    fn now() -> u64 {
        1_000_000
    }

    fn make_engine() -> KvEngine {
        KvEngine::new(now(), 16, 0.75, 4, 64, 1000, 1024)
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
    fn ttl_expiry_via_tick() {
        let mut e = make_engine();
        let n = now();

        // Put with 5-second TTL.
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "sess",
            key: b"s1",
            value: b"data",
            ttl_ms: 5000,
            now_ms: n,
            surrogate: row_surrogate(b"s1"),
        })
        .expect("a bound row writes");
        assert!(e.get(0, 1, "sess", b"s1", n).is_some());

        // Still alive at t+4999.
        assert!(e.get(0, 1, "sess", b"s1", n + 4999).is_some());

        // Expired at t+5000 (lazy fallback).
        assert!(e.get(0, 1, "sess", b"s1", n + 5000).is_none());

        // Tick reaps it.
        let reaped = e.tick_expiry(n + 5000);
        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].collection, "sess");
        assert_eq!(reaped[0].key, b"s1");
        assert_eq!(e.total_entries(), 0);
    }

    #[test]
    fn stats() {
        let mut e = make_engine();
        let n = now();

        assert_eq!(e.total_entries(), 0);

        for i in 0..10u32 {
            e.put(KvPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "c",
                key: &i.to_be_bytes(),
                value: &[0; 32],
                ttl_ms: 0,
                now_ms: n,
                surrogate: row_surrogate(&i.to_be_bytes()),
            })
            .expect("a bound row writes");
        }
        assert_eq!(e.total_entries(), 10);
        assert_eq!(e.collection_len(0, 1, "c"), 10);
        assert!(e.total_mem_usage() > 0);
    }

    // ── TTL × index interaction ──────────────────────────────────────────────
    //
    // The expiry reaper is a delete path, so every index a DELETE maintains it
    // must maintain too. These cases put a TTL and an index on the SAME
    // collection: `ttl_expiry_via_tick` covers TTL on an index-less collection
    // and `index_cleaned_on_delete` covers an index on a TTL-less collection, so
    // neither observes the reaper touching an index.

    /// The reaper must remove a single-field index entry along with the row.
    /// Pre-fix `tick_expiry` reaped the hash slot only, so the index kept
    /// pointing at a key that no longer existed — an unbounded leak that the
    /// checkpoint then persisted verbatim.
    #[test]
    fn index_cleaned_on_ttl_reap() {
        let mut e = make_engine();
        let n = now();

        e.register_index(RegisterIndexParams {
            database_id: 0,
            tenant_id: 1,
            collection: "sess",
            field: "region",
            field_position: 0,
            backfill: false,
            now_ms: n,
        });
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "sess",
            key: b"s1",
            value: &mp_obj(&[("region", "us")]),
            ttl_ms: 5000,
            now_ms: n,
            surrogate: row_surrogate(b"s1"),
        })
        .expect("a bound row writes");
        let us = nodedb_types::Value::String("us".into());
        assert_eq!(e.index_lookup_eq(0, 1, "sess", "region", &us).len(), 1);

        let reaped = e.tick_expiry(n + 5000);
        assert_eq!(reaped.len(), 1);

        assert_eq!(e.total_entries(), 0);
        assert!(
            e.index_lookup_eq(0, 1, "sess", "region", &us).is_empty(),
            "reaping the row must remove its index entry"
        );
        assert_eq!(
            e.stats().total_index_entries,
            0,
            "no index entry may outlive the row it points at"
        );
    }

    /// Composite indexes are cleaned by a separate loop in `KvIndexSet::on_delete`
    /// than single-field ones, so the reaper needs its own case for them.
    ///
    /// Seeded through `KvIndexSet::add_composite_index` directly: that is the only
    /// registration path a composite index has — the engine exposes no
    /// `register_composite_index` counterpart to `register_index`.
    #[test]
    fn composite_index_cleaned_on_ttl_reap() {
        let mut e = make_engine();
        let n = now();
        let tkey = table_key(0, 1, "sess");

        e.indexes.entry(tkey).or_default().add_composite_index(
            vec!["region".into(), "status".into()],
            vec![0, 1],
            crate::engine::kv::index::IndexCoverage::Complete,
        );

        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "sess",
            key: b"s1",
            value: &mp_obj(&[("region", "us"), ("status", "active")]),
            ttl_ms: 5000,
            now_ms: n,
            surrogate: row_surrogate(b"s1"),
        })
        .expect("a bound row writes");

        let ci_fields = vec!["region".to_string(), "status".to_string()];
        let text_key = |s: &str| {
            crate::engine::kv::index::kv_index_key(&nodedb_types::Value::String(s.into()))
        };
        let (us, active) = (text_key("us"), text_key("active"));
        let hits = |e: &KvEngine| -> usize {
            e.indexes
                .get(&tkey)
                .and_then(|s| s.get_composite_index(&ci_fields))
                .map(|ci| ci.lookup_eq(&[&us, &active]).len())
                .unwrap_or(0)
        };
        assert_eq!(hits(&e), 1);

        let reaped = e.tick_expiry(n + 5000);
        assert_eq!(reaped.len(), 1);
        assert_eq!(
            hits(&e),
            0,
            "reaping the row must remove its composite entry"
        );
    }

    /// A rehash moves every existing entry into `rehash_source`; a row reaped
    /// while it sits there is exactly the row whose index cleanup a probe of the
    /// primary slots alone would skip. Guards `get_ignoring_expiry`'s
    /// `rehash_source` fallback.
    #[test]
    fn index_cleaned_on_ttl_reap_during_rehash() {
        let mut e = make_engine();
        let n = now();

        e.register_index(RegisterIndexParams {
            database_id: 0,
            tenant_id: 1,
            collection: "sess",
            field: "region",
            field_position: 0,
            backfill: false,
            now_ms: n,
        });

        // make_engine's table starts at capacity 16 with a 0.75 rehash threshold,
        // so the 13th insert starts a rehash and parks all 13 rows in the source.
        // No PUT follows, so none of them get migrated back out.
        let rows = 13u32;
        for i in 0..rows {
            e.put(KvPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "sess",
                key: &i.to_be_bytes(),
                value: &mp_obj(&[("region", "us")]),
                ttl_ms: 5000,
                now_ms: n,
                surrogate: row_surrogate(&i.to_be_bytes()),
            })
            .expect("a bound row writes");
        }
        assert!(
            e.stats().is_rehashing,
            "test premise: the reaped rows must sit in the rehash source"
        );
        let us = nodedb_types::Value::String("us".into());
        assert_eq!(
            e.index_lookup_eq(0, 1, "sess", "region", &us).len(),
            rows as usize
        );

        let reaped = e.tick_expiry(n + 5000);
        assert_eq!(reaped.len(), rows as usize);
        assert_eq!(e.total_entries(), 0);
        assert!(
            e.index_lookup_eq(0, 1, "sess", "region", &us).is_empty(),
            "rows reaped out of the rehash source must clean their index entries too"
        );
    }
}
