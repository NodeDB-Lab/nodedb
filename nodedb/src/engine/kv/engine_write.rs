// SPDX-License-Identifier: BUSL-1.1

//! KvEngine write operations: PUT, DELETE, EXPIRE, PERSIST.

use nodedb_types::Surrogate;

use super::engine::KvEngine;
use super::engine_helpers::{
    decode_document, expiry_key, extract_all_field_values_from_msgpack, table_key,
};
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
    /// The row's bound identity, never `Surrogate::ZERO`. An existing row
    /// keeps the surrogate it was first bound under.
    pub surrogate: Surrogate,
}

/// Parameters for [`KvEngine::rewrite_with_absolute_expiry`]: a new value
/// for a row the table already holds. It carries no surrogate: the row keeps
/// the identity it was bound under.
#[derive(Debug, Clone, Copy)]
pub struct KvRewriteParams<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub ttl_ms: u64,
    pub now_ms: u64,
}

/// A KV write refused because it binds a row to `Surrogate::ZERO`. Every
/// stored row is bound, so the engine writes nothing for it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "KV write into '{collection}' binds a row to Surrogate::ZERO; every row must be bound \
     before it is stored"
)]
pub struct UnboundKvWrite {
    pub collection: String,
}

impl UnboundKvWrite {
    /// The refusal of a write binding `surrogate` into `collection`, or `Ok`
    /// when it is bound. A multi-row write checks every row with this before
    /// it writes any.
    pub fn check(collection: &str, surrogate: Surrogate) -> Result<(), Self> {
        if surrogate == Surrogate::ZERO {
            return Err(Self {
                collection: collection.to_owned(),
            });
        }
        Ok(())
    }
}

/// See [`UnboundKvWrite::check`].
pub(super) fn require_bound(collection: &str, surrogate: Surrogate) -> Result<(), UnboundKvWrite> {
    UnboundKvWrite::check(collection, surrogate)
}

/// The identity a resolved write leaves on its row.
#[derive(Clone, Copy)]
enum RowIdentity {
    /// Bind the row to this surrogate. An existing row keeps its own.
    Bind(Surrogate),
    /// Keep the existing row's surrogate. An absent row is not written.
    KeepExisting,
}

impl KvEngine {
    /// PUT: insert or update. Returns old value if overwritten.
    ///
    /// If `ttl_ms > 0`, schedules expiry. If the key already had a TTL,
    /// the old expiry is cancelled and replaced.
    ///
    /// `surrogate` is the row's stable global identity. An existing entry
    /// keeps the surrogate it was first bound under. A rewrite of an existing
    /// row that allocates no identity uses
    /// [`KvEngine::rewrite_with_absolute_expiry`]. A `Surrogate::ZERO` write
    /// is refused with [`UnboundKvWrite`] and writes nothing.
    pub fn put(&mut self, params: KvPutParams<'_>) -> Result<Option<Vec<u8>>, UnboundKvWrite> {
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
    ) -> Result<Option<Vec<u8>>, UnboundKvWrite> {
        self.put_resolved(params, expire_at_ms)
    }

    /// Replace the value and expiry of a row the table already holds, keeping
    /// the row's bound surrogate. Returns the old value. An absent row is not
    /// written and returns `None`. `expire_at_ms` is installed verbatim, as
    /// for [`KvEngine::put_with_absolute_expiry`].
    pub fn rewrite_with_absolute_expiry(
        &mut self,
        params: KvRewriteParams<'_>,
        expire_at_ms: u64,
    ) -> Option<Vec<u8>> {
        self.write_resolved(params, RowIdentity::KeepExisting, expire_at_ms)
    }

    /// Shared PUT body: insert/update the key with an already-resolved absolute
    /// `expire_at` (or [`NO_EXPIRY`]), maintaining the expiry wheel and both
    /// secondary and sorted indexes. `params.ttl_ms` is intentionally unused
    /// here — expiry is fully determined by `expire_at`.
    fn put_resolved(
        &mut self,
        params: KvPutParams<'_>,
        expire_at: u64,
    ) -> Result<Option<Vec<u8>>, UnboundKvWrite> {
        let KvPutParams {
            database_id,
            tenant_id,
            collection,
            key,
            value,
            ttl_ms,
            now_ms,
            surrogate,
        } = params;
        require_bound(collection, surrogate)?;
        Ok(self.write_resolved(
            KvRewriteParams {
                database_id,
                tenant_id,
                collection,
                key,
                value,
                ttl_ms,
                now_ms,
            },
            RowIdentity::Bind(surrogate),
            expire_at,
        ))
    }

    /// The write body shared by a bound put and a rewrite.
    fn write_resolved(
        &mut self,
        params: KvRewriteParams<'_>,
        identity: RowIdentity,
        expire_at: u64,
    ) -> Option<Vec<u8>> {
        let KvRewriteParams {
            database_id,
            tenant_id,
            collection,
            key,
            value,
            ttl_ms: _,
            now_ms,
        } = params;

        let tkey = table_key(database_id, tenant_id, collection);
        // One probe serves both the rewrite's presence check and the old expiry.
        let old_meta = self.tables.get(&tkey).and_then(|t| t.get_entry_meta(key));
        if matches!(identity, RowIdentity::KeepExisting) && old_meta.is_none() {
            return None;
        }

        let has_indexes = self.indexes.get(&tkey).is_some_and(|idx| !idx.is_empty());
        let old_expire = old_meta.and_then(|m| m.has_ttl.then_some(m.expire_at_ms));
        let ordinal = self.write_ordinal();

        // Cancel old expiry (before mutating the table).
        if let Some(old_ms) = old_expire {
            let composite = expiry_key(database_id, tenant_id, collection, key);
            self.expiry.cancel(&composite, old_ms);
        }

        // Fetch-or-create the table, bumping its write epoch — the single
        // chokepoint the aggregate result cache reads to detect this write.
        let table = self.table_for_write_or_create(tkey, database_id, tenant_id, collection);
        let old = match identity {
            RowIdentity::Bind(surrogate) => table.put(key, value, expire_at, surrogate, ordinal),
            RowIdentity::KeepExisting => table.rewrite(key, value, expire_at, ordinal),
        };
        self.recharge_table(tkey);

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
            let old_fields = old
                .as_ref()
                .map(|v| extract_all_field_values_from_msgpack(v));

            if has_indexes {
                let new_fields = extract_all_field_values_from_msgpack(&new_value_bytes);
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
                let new_document = decode_document(&new_value_bytes);
                self.sorted_indexes.on_put(tkey, key, new_document.as_ref());
            }
        }

        old
    }

    /// DELETE: remove key(s) at the current write ordinal. Returns count of
    /// keys actually deleted. Each removed version stays readable at an
    /// earlier ordinal. The removal is expiry-blind, so `_now_ms` does not
    /// decide it.
    pub fn delete(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        keys: &[Vec<u8>],
        _now_ms: u64,
    ) -> usize {
        let tkey = table_key(database_id, tenant_id, collection);
        let ordinal = self.write_ordinal();
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

            if table.delete(key, ordinal) {
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
        self.recharge_table(tkey);
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
        let ordinal = self.write_ordinal();
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

        let found = table.set_expire(key, expire_at, ordinal);
        if found {
            let composite = expiry_key(database_id, tenant_id, collection, key);
            self.expiry.insert(composite, expire_at);
        }
        self.recharge_table(tkey);
        found
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
        let ordinal = self.write_ordinal();
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

        let found = table.persist(key, ordinal);
        self.recharge_table(tkey);
        found
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::kv::test_support::row_surrogate;
    use crate::engine::kv::{KvBatchPutParams, RegisterIndexParams};

    use super::*;

    fn now() -> u64 {
        1_000_000
    }

    /// A write binding a row to `Surrogate::ZERO` is refused and stores
    /// nothing, and a bound write of the same row succeeds.
    #[test]
    fn an_unbound_put_is_refused_and_writes_nothing() {
        let mut e = make_engine();
        let n = now();
        let params = KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "cache",
            key: b"k1",
            value: b"v1",
            ttl_ms: 0,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        };
        assert!(e.put(params).is_err());
        assert!(e.get(0, 1, "cache", b"k1", n).is_none());

        let entries = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ];
        let refused = e.batch_put(KvBatchPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "cache",
            entries: &entries,
            ttl_ms: 0,
            now_ms: n,
            surrogates: &[Surrogate::new(1), Surrogate::ZERO],
        });
        assert!(refused.is_err());
        assert!(
            e.get(0, 1, "cache", b"a", n).is_none(),
            "a refused batch writes no entry"
        );

        e.put(KvPutParams {
            surrogate: Surrogate::new(5),
            ..params
        })
        .expect("a bound put writes");
        assert_eq!(e.get(0, 1, "cache", b"k1", n), Some(b"v1".to_vec()));
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
            surrogate: row_surrogate(b"k1"),
        })
        .expect("a bound row writes");
        assert_eq!(e.get(0, 1, "cache", b"k1", n).unwrap(), b"v1");

        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "cache",
            key: b"k1",
            value: b"v2",
            ttl_ms: 0,
            now_ms: n,
            surrogate: row_surrogate(b"k1"),
        })
        .expect("a bound row writes");
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
            surrogate: row_surrogate(b"k"),
        })
        .expect("a bound row writes");
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
            surrogate: row_surrogate(b"k"),
        })
        .expect("a bound row writes");
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
        let surrogates: Vec<Surrogate> =
            entries.iter().map(|(key, _)| row_surrogate(key)).collect();
        let new_count = e
            .batch_put(KvBatchPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "c",
                entries: &entries,
                ttl_ms: 0,
                now_ms: n,
                surrogates: &surrogates,
            })
            .expect("a bound batch writes");
        assert_eq!(new_count, 5);

        let keys: Vec<Vec<u8>> = (0..7u8).map(|i| vec![i]).collect();
        let results = e.batch_get(0, 1, "c", &keys, n);
        assert_eq!(results.len(), 7);
        assert_eq!(results[0], Some(vec![0]));
        assert_eq!(results[4], Some(vec![40]));
        assert!(results[5].is_none()); // Key 5 doesn't exist.
        assert!(results[6].is_none());
    }

    /// `batch_put` stores the surrogate passed for each entry (observable via
    /// `get_with_surrogate`, the same accessor the clone-delegated read path
    /// uses), exactly like a loop of single-key `put` calls.
    #[test]
    fn batch_put_stores_real_per_entry_surrogates() {
        let mut e = make_engine();
        let n = now();

        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..3u8).map(|i| (vec![i], vec![i * 10])).collect();
        let surrogates: Vec<Surrogate> = (1..=3u32).map(Surrogate::new).collect();
        let new_count = e
            .batch_put(KvBatchPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "c",
                entries: &entries,
                ttl_ms: 0,
                now_ms: n,
                surrogates: &surrogates,
            })
            .expect("a bound batch writes");
        assert_eq!(new_count, 3);

        for (i, expected) in surrogates.iter().enumerate() {
            let key = &entries[i].0;
            let (value, stored_surrogate) = e
                .get_with_surrogate(0, 1, "c", key, n)
                .unwrap_or_else(|| panic!("entry {i} must be present"));
            assert_eq!(value, entries[i].1, "entry {i} value must round-trip");
            assert_eq!(
                stored_surrogate, *expected,
                "entry {i} must carry its assigned surrogate"
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
            surrogate: Surrogate::new(1),
        })
        .expect("a bound row writes");
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 2,
            collection: "c",
            key: b"k",
            value: b"t2",
            ttl_ms: 0,
            now_ms: n,
            surrogate: Surrogate::new(2),
        })
        .expect("a bound row writes");

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
            surrogate: row_surrogate(b"s1"),
        })
        .expect("a bound row writes");
        let us = nodedb_types::Value::String("us".into());
        assert_eq!(e.index_lookup_eq(0, 1, "sess", "region", &us).len(), 1);

        // No tick — the row is expired but still present.
        assert_eq!(e.delete(0, 1, "sess", &[b"s1".to_vec()], n + 5000), 1);
        assert!(
            e.index_lookup_eq(0, 1, "sess", "region", &us).is_empty(),
            "DELETE of an expired-pending-reap key must still clean the index"
        );
    }

    /// Repeated PUTs of existing keys update in place: one entry per key, and
    /// a read returns the last value. Throughput lives in the KV benchmark,
    /// never in a unit test, where machine load decides the outcome.
    #[test]
    fn updates_of_existing_keys_keep_one_entry_each() {
        let mut e = make_engine();
        let n = now();
        let keys: Vec<Vec<u8>> = (0..10_000u32).map(|i| i.to_be_bytes().to_vec()).collect();
        let value = [0u8; 64];

        for key in &keys {
            e.put(KvPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "b",
                key,
                value: &value,
                ttl_ms: 0,
                now_ms: n,
                surrogate: row_surrogate(key),
            })
            .expect("a bound row writes");
        }

        // 100K updates: every key is rewritten ten times, the last with its
        // own index as the value.
        let iters = 100_000u32;
        for i in 0..iters {
            let key = &keys[(i as usize) % keys.len()];
            let last = i >= iters - keys.len() as u32;
            let updated = i.to_be_bytes();
            e.put(KvPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "b",
                key,
                value: if last { &updated } else { &value },
                ttl_ms: 0,
                now_ms: n,
                surrogate: row_surrogate(key),
            })
            .expect("a bound row writes");
        }
        assert_eq!(e.collection_len(0, 1, "b"), keys.len());
        for (index, key) in keys.iter().enumerate() {
            let expected = (iters - keys.len() as u32 + index as u32).to_be_bytes();
            assert_eq!(
                e.get(0, 1, "b", key, n),
                Some(expected.to_vec()),
                "key {index}"
            );
        }
    }
}
