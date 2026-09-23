//! Read-path operations for the KV engine: point lookups, TTL queries, and batch reads.

use nodedb_types::Surrogate;

use super::KvEngine;
use crate::engine::kv::KvPutParams;
use crate::engine::kv::engine_helpers::table_key;
use crate::engine::kv::entry::NO_EXPIRY;
use crate::engine::kv::hash_table::{EntryMeta, KvExportEntry};

/// One key of one KV collection.
#[derive(Debug, Clone, Copy)]
pub struct KvKeyRef<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub key: &'a [u8],
}

/// A key's complete stored state: its value, its absolute expiry, and the
/// surrogate bound to it. A rollback reinstalls it exactly, so the key keeps
/// its TTL instant and its identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvEntryImage {
    pub value: Vec<u8>,
    /// Absolute expiry instant in ms since epoch, [`NO_EXPIRY`] for none.
    pub expire_at_ms: u64,
    pub surrogate: Surrogate,
}

impl KvEngine {
    /// Look up the user primary key bytes for a given surrogate within
    /// `(tenant_id, collection)`. Returns `None` when the surrogate is
    /// unbound or the collection is empty.
    pub fn key_for_surrogate(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        surrogate: Surrogate,
    ) -> Option<Vec<u8>> {
        let tkey = table_key(database_id, tenant_id, collection);
        self.tables
            .get(&tkey)?
            .key_for_surrogate(surrogate)
            .map(|k| k.to_vec())
    }

    /// GET: O(1) hash table lookup. Returns None if not found or expired.
    pub fn get(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        now_ms: u64,
    ) -> Option<Vec<u8>> {
        let tkey = table_key(database_id, tenant_id, collection);
        self.tables.get(&tkey)?.get(key, now_ms).map(|v| v.to_vec())
    }

    /// GET with surrogate: returns the value bytes AND the row's stable
    /// surrogate when the binding was made.  Used by the clone-delegated
    /// read path to enforce a per-row surrogate ceiling — bindings the
    /// source allocated AFTER the clone's AS-OF point are filtered out.
    pub fn get_with_surrogate(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        now_ms: u64,
    ) -> Option<(Vec<u8>, nodedb_types::Surrogate)> {
        let tkey = table_key(database_id, tenant_id, collection);
        self.tables
            .get(&tkey)?
            .get_with_surrogate(key, now_ms)
            .map(|(v, s)| (v.to_vec(), s))
    }

    /// GET TTL: Returns the remaining TTL in milliseconds for a key.
    ///
    /// - `None` — key does not exist (or is expired)
    /// - `Some(-1)` — key exists but has no TTL (persistent)
    /// - `Some(remaining_ms)` — key exists and expires in `remaining_ms` milliseconds
    pub fn get_ttl_ms(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        now_ms: u64,
    ) -> Option<i64> {
        let tkey = table_key(database_id, tenant_id, collection);
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

    /// Return the current TTL metadata for a key, or `None` if the key does
    /// not exist in this collection.
    ///
    /// Unlike [`KvEngine::get_ttl_ms`], this does not resolve against
    /// `now_ms` or check expiry -- it returns the raw `(has_ttl,
    /// expire_at_ms)` pair verbatim. Used to capture a key's exact prior TTL
    /// state before `Expire`/`Persist` mutate it, so a transaction rollback
    /// can restore the precise absolute instant rather than an
    /// approximation derived from elapsed wall-clock time.
    pub fn get_ttl_meta(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
    ) -> Option<EntryMeta> {
        let tkey = table_key(database_id, tenant_id, collection);
        self.tables.get(&tkey)?.get_entry_meta(key)
    }

    /// The complete stored state of `key`, or `None` when it is absent or
    /// expired.
    pub fn entry_image(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        now_ms: u64,
    ) -> Option<KvEntryImage> {
        let tkey = table_key(database_id, tenant_id, collection);
        let table = self.tables.get(&tkey)?;
        let (value, surrogate) = table.get_with_surrogate(key, now_ms)?;
        let expire_at_ms = table
            .get_entry_meta(key)
            .map_or(NO_EXPIRY, |meta| meta.expire_at_ms);
        Some(KvEntryImage {
            value: value.to_vec(),
            expire_at_ms,
            surrogate,
        })
    }

    /// Reinstall `image` under `key`: the value, the absolute expiry instant
    /// and the surrogate it held, with every index maintained.
    pub fn restore_entry_image(&mut self, target: KvKeyRef<'_>, image: &KvEntryImage, now_ms: u64) {
        self.put_with_absolute_expiry(
            KvPutParams {
                database_id: target.database_id,
                tenant_id: target.tenant_id,
                collection: target.collection,
                key: target.key,
                value: &image.value,
                ttl_ms: 0,
                now_ms,
                surrogate: image.surrogate,
            },
            image.expire_at_ms,
        );
    }

    /// Put `key` back to `image`, or remove it when `image` is `None`: the
    /// key was absent before.
    pub fn reinstate_entry(
        &mut self,
        target: KvKeyRef<'_>,
        image: Option<&KvEntryImage>,
        now_ms: u64,
    ) {
        match image {
            Some(image) => self.restore_entry_image(target, image, now_ms),
            None => {
                let key = [target.key.to_vec()];
                self.delete(
                    target.database_id,
                    target.tenant_id,
                    target.collection,
                    &key,
                    now_ms,
                );
            }
        }
    }

    /// Every live row of a collection with its complete stored state.
    pub fn export_collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> Vec<KvExportEntry> {
        let tkey = table_key(database_id, tenant_id, collection);
        self.tables
            .get(&tkey)
            .map(|table| table.export_entries_with_surrogates())
            .unwrap_or_default()
    }

    /// BATCH GET: fetch multiple keys. Returns values in order (None for missing).
    pub fn batch_get(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        keys: &[Vec<u8>],
        now_ms: u64,
    ) -> Vec<Option<Vec<u8>>> {
        keys.iter()
            .map(|k| self.get(database_id, tenant_id, collection, k, now_ms))
            .collect()
    }
}
