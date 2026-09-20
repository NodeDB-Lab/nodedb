// SPDX-License-Identifier: BUSL-1.1

//! KvEngine write operations: PUT, DELETE, EXPIRE, PERSIST.

use nodedb_types::Surrogate;

use super::engine::KvEngine;
use super::engine_helpers::{expiry_key, extract_all_field_values_from_msgpack, table_key};
use super::entry::NO_EXPIRY;

/// Parameters for [`KvEngine::put`].
#[derive(Debug, Clone, Copy)]
pub struct KvPutParams<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub ttl_ms: u64,
    pub now_ms: u64,
    pub surrogate: Surrogate,
}

impl KvEngine {
    /// PUT: insert or update. Returns old value if overwritten.
    ///
    /// If `ttl_ms > 0`, schedules expiry. If the key already had a TTL,
    /// the old expiry is cancelled and replaced.
    ///
    /// `surrogate` is the row's stable global identity. Pass
    /// `Surrogate::ZERO` from internal RMW callers that do not allocate
    /// one — existing entries preserve their bound surrogate either way.
    pub fn put(&mut self, params: KvPutParams<'_>) -> Option<Vec<u8>> {
        let expire_at = if params.ttl_ms > 0 {
            params.now_ms + params.ttl_ms
        } else {
            NO_EXPIRY
        };
        self.put_resolved(params, expire_at)
    }

    /// PUT installing an already-resolved absolute expiry instant.
    ///
    /// `expire_at_ms` is the absolute wall-clock instant (ms since epoch) the
    /// key expires at, or [`NO_EXPIRY`] (`0`) for no TTL. Unlike [`put`], which
    /// derives expiry as `now_ms + ttl_ms`, this installs the supplied instant
    /// verbatim. WAL redo replay uses it so a TTL'd key recovers with the exact
    /// expiry the original write computed — recomputing `now_ms + ttl_ms` at
    /// recovery time would push expiry forward by the crash-to-restart delay.
    ///
    /// [`put`]: KvEngine::put
    pub fn put_with_absolute_expiry(
        &mut self,
        params: KvPutParams<'_>,
        expire_at_ms: u64,
    ) -> Option<Vec<u8>> {
        self.put_resolved(params, expire_at_ms)
    }

    /// Shared PUT body: insert/update the key with an already-resolved absolute
    /// `expire_at` (or [`NO_EXPIRY`]), maintaining the expiry wheel and both
    /// secondary and sorted indexes. `params.ttl_ms` is intentionally unused
    /// here — expiry is fully determined by `expire_at`.
    fn put_resolved(&mut self, params: KvPutParams<'_>, expire_at: u64) -> Option<Vec<u8>> {
        let KvPutParams {
            database_id,
            tenant_id,
            collection,
            key,
            value,
            ttl_ms: _,
            now_ms,
            surrogate,
        } = params;

        let tkey = table_key(database_id, tenant_id, collection);

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
            let composite = expiry_key(database_id, tenant_id, collection, key);
            self.expiry.cancel(&composite, old_ms);
        }

        // Fetch-or-create the table, bumping its write epoch — the single
        // chokepoint the aggregate result cache reads to detect this write.
        let table = self.table_for_write_or_create(tkey, tenant_id, collection);
        let old = table.put(key, value, expire_at, surrogate);

        // Schedule new expiry.
        if expire_at != NO_EXPIRY {
            let composite = expiry_key(database_id, tenant_id, collection, key);
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
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        keys: &[Vec<u8>],
        now_ms: u64,
    ) -> usize {
        let tkey = table_key(database_id, tenant_id, collection);
        // Bump first (only touches `write_epochs`) so the subsequent `tables`
        // borrow stays disjoint from the `indexes` / `expiry` / `sorted_indexes`
        // accesses interleaved with it below.
        self.bump_write_epoch(tkey);
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
                let composite = expiry_key(database_id, tenant_id, collection, key);
                self.expiry.cancel(&composite, meta.expire_at_ms);
            }

            // Extract field values before deletion (for index cleanup).
            // Expiry-blind: `table.delete` below removes the row regardless of
            // expiry, so for a key whose TTL has elapsed but which the wheel has
            // not reaped yet, a `get(key, now_ms)` would return `None` and leave
            // its index entries stranded behind a successful DELETE.
            let old_fields = if has_indexes {
                table
                    .get_ignoring_expiry(key)
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
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        ttl_ms: u64,
        now_ms: u64,
    ) -> bool {
        self.expire_resolved(database_id, tenant_id, collection, key, now_ms + ttl_ms)
    }

    /// EXPIRE installing an already-resolved absolute expiry instant.
    ///
    /// `expire_at_ms` is the absolute wall-clock instant (ms since epoch) the
    /// key expires at. Unlike [`expire`], which derives it as
    /// `now_ms + ttl_ms`, this installs the supplied instant verbatim. WAL
    /// redo replay uses it so a key's expiry recovers with the exact instant
    /// the original write computed — recomputing `now_ms + ttl_ms` at
    /// recovery time would push expiry forward by the crash-to-restart delay.
    ///
    /// [`expire`]: KvEngine::expire
    pub fn expire_with_absolute_expiry(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        expire_at_ms: u64,
    ) -> bool {
        self.expire_resolved(database_id, tenant_id, collection, key, expire_at_ms)
    }

    /// Shared EXPIRE body: install an already-resolved absolute `expire_at`
    /// on an existing key. Returns true if the key was found and TTL was set.
    fn expire_resolved(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        expire_at: u64,
    ) -> bool {
        let tkey = table_key(database_id, tenant_id, collection);
        self.bump_write_epoch(tkey);
        let table = match self.tables.get_mut(&tkey) {
            Some(t) => t,
            None => return false,
        };

        // Cancel old expiry.
        if let Some(meta) = table.get_entry_meta(key)
            && meta.has_ttl
        {
            let composite = expiry_key(database_id, tenant_id, collection, key);
            self.expiry.cancel(&composite, meta.expire_at_ms);
        }

        if table.set_expire(key, expire_at) {
            let composite = expiry_key(database_id, tenant_id, collection, key);
            self.expiry.insert(composite, expire_at);
            true
        } else {
            false
        }
    }

    /// PERSIST: remove TTL from a key. Returns true if the key was found.
    pub fn persist(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
    ) -> bool {
        let tkey = table_key(database_id, tenant_id, collection);
        self.bump_write_epoch(tkey);
        let table = match self.tables.get_mut(&tkey) {
            Some(t) => t,
            None => return false,
        };

        if let Some(meta) = table.get_entry_meta(key)
            && meta.has_ttl
        {
            let composite = expiry_key(database_id, tenant_id, collection, key);
            self.expiry.cancel(&composite, meta.expire_at_ms);
        }

        table.persist(key)
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::kv::{KvBatchPutParams, RegisterIndexParams};

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
    fn basic_get_put_delete() {
        let mut e = make_engine();
        let n = now();

        assert!(e.get(0, 1, "cache", b"k1", n).is_none());

        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "cache",
            key: b"k1",
            value: b"v1",
            ttl_ms: 0,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });
        assert_eq!(e.get(0, 1, "cache", b"k1", n).unwrap(), b"v1");

        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "cache",
            key: b"k1",
            value: b"v2",
            ttl_ms: 0,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });
        assert_eq!(e.get(0, 1, "cache", b"k1", n).unwrap(), b"v2");

        assert_eq!(e.delete(0, 1, "cache", &[b"k1".to_vec()], n), 1);
        assert!(e.get(0, 1, "cache", b"k1", n).is_none());
    }

    #[test]
    fn persist_removes_ttl() {
        let mut e = make_engine();
        let n = now();

        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "cache",
            key: b"k",
            value: b"v",
            ttl_ms: 3000,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });
        assert!(e.persist(0, 1, "cache", b"k"));

        // Should never expire now.
        assert!(e.get(0, 1, "cache", b"k", n + 100_000).is_some());
    }

    #[test]
    fn expire_sets_ttl() {
        let mut e = make_engine();
        let n = now();

        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "cache",
            key: b"k",
            value: b"v",
            ttl_ms: 0,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });
        assert!(e.get(0, 1, "cache", b"k", n + 100_000).is_some()); // No TTL.

        assert!(e.expire(0, 1, "cache", b"k", 2000, n));
        assert!(e.get(0, 1, "cache", b"k", n + 1999).is_some());
        assert!(e.get(0, 1, "cache", b"k", n + 2000).is_none()); // Expired.
    }

    #[test]
    fn batch_get_and_put() {
        let mut e = make_engine();
        let n = now();

        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..5u8).map(|i| (vec![i], vec![i * 10])).collect();
        let surrogates = vec![Surrogate::ZERO; entries.len()];
        let new_count = e.batch_put(KvBatchPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            entries: &entries,
            ttl_ms: 0,
            now_ms: n,
            surrogates: &surrogates,
        });
        assert_eq!(new_count, 5);

        let keys: Vec<Vec<u8>> = (0..7u8).map(|i| vec![i]).collect();
        let results = e.batch_get(0, 1, "c", &keys, n);
        assert_eq!(results.len(), 7);
        assert_eq!(results[0], Some(vec![0]));
        assert_eq!(results[4], Some(vec![40]));
        assert!(results[5].is_none()); // Key 5 doesn't exist.
        assert!(results[6].is_none());
    }

    /// A native `KvBatchPut` must call `KvEngine::batch_put` with a
    /// per-entry surrogate. Without one, every batch-put row lands with
    /// `Surrogate::ZERO` -- invisible to any surrogate-keyed
    /// cross-engine read/join, unlike a single-key `put` which always
    /// carries a real, CP-assigned surrogate. This asserts `batch_put`
    /// stores the REAL surrogate passed for each entry (observable via
    /// `get_with_surrogate`, the same accessor the clone-delegated read path
    /// uses), exactly mirroring what a loop of single-key `put` calls would
    /// do. Fails pre-fix because pre-fix `batch_put` took no `surrogates`
    /// parameter at all and hardcoded `Surrogate::ZERO` for every entry --
    /// this test would not have compiled against that signature, and the
    /// equivalent assertion against the old code (stubbing `Surrogate::ZERO`
    /// in) observes `get_with_surrogate` returning `Surrogate::ZERO` instead
    /// of the distinct real identity asserted here.
    #[test]
    fn batch_put_stores_real_per_entry_surrogates() {
        let mut e = make_engine();
        let n = now();

        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..3u8).map(|i| (vec![i], vec![i * 10])).collect();
        let surrogates: Vec<Surrogate> = (1..=3u32).map(Surrogate::new).collect();
        let new_count = e.batch_put(KvBatchPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            entries: &entries,
            ttl_ms: 0,
            now_ms: n,
            surrogates: &surrogates,
        });
        assert_eq!(new_count, 3);

        for (i, expected) in surrogates.iter().enumerate() {
            let key = &entries[i].0;
            let (value, stored_surrogate) = e
                .get_with_surrogate(0, 1, "c", key, n)
                .unwrap_or_else(|| panic!("entry {i} must be present"));
            assert_eq!(value, entries[i].1, "entry {i} value must round-trip");
            assert_eq!(
                stored_surrogate, *expected,
                "entry {i} must carry its assigned surrogate, not Surrogate::ZERO"
            );
            assert_ne!(
                stored_surrogate,
                Surrogate::ZERO,
                "entry {i} must not fall back to the unbound sentinel"
            );
        }
    }

    #[test]
    fn tenant_isolation() {
        let mut e = make_engine();
        let n = now();

        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            key: b"k",
            value: b"t1",
            ttl_ms: 0,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 2,
            collection: "c",
            key: b"k",
            value: b"t2",
            ttl_ms: 0,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });

        assert_eq!(e.get(0, 1, "c", b"k", n).unwrap(), b"t1");
        assert_eq!(e.get(0, 2, "c", b"k", n).unwrap(), b"t2");
    }

    /// DELETE of a key whose TTL has elapsed but which the wheel has not reaped
    /// yet. `KvHashTable::delete` succeeds regardless of expiry, so index
    /// cleanup must not read the old field values through the
    /// expiry-checking `get` — that returns `None` and strands the index
    /// entries behind a DELETE that reported success.
    #[test]
    fn index_cleaned_on_delete_of_expired_key() {
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
            surrogate: Surrogate::ZERO,
        });
        assert_eq!(e.index_lookup_eq(0, 1, "sess", "region", b"us").len(), 1);

        // No tick — the row is expired but still present.
        assert_eq!(e.delete(0, 1, "sess", &[b"s1".to_vec()], n + 5000), 1);
        assert!(
            e.index_lookup_eq(0, 1, "sess", "region", b"us").is_empty(),
            "DELETE of an expired-pending-reap key must still clean the index"
        );
    }

    #[test]
    fn raw_put_timing() {
        let mut e = make_engine();
        let n = now();
        let keys: Vec<Vec<u8>> = (0..10_000u32).map(|i| i.to_be_bytes().to_vec()).collect();
        let value = [0u8; 64];

        // Warmup: insert all keys once.
        for key in &keys {
            e.put(KvPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "b",
                key,
                value: &value,
                ttl_ms: 0,
                now_ms: n,
                surrogate: Surrogate::ZERO,
            });
        }

        // Timed: 100K updates (keys already exist).
        let iters = 100_000u64;
        let start = std::time::Instant::now();
        for i in 0..iters {
            let key = &keys[(i as usize) % 10_000];
            e.put(KvPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "b",
                key,
                value: &value,
                ttl_ms: 0,
                now_ms: n,
                surrogate: Surrogate::ZERO,
            });
        }
        let elapsed = start.elapsed();
        let ns_per_op = elapsed.as_nanos() / iters as u128;
        // 691 ns/op measured — well under document's 12μs.
        assert!(ns_per_op < 5_000, "PUT too slow: {ns_per_op} ns/op");
    }
}
