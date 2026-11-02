// SPDX-License-Identifier: BUSL-1.1

//! Reversing one KV write of a key exactly, as a transaction rollback does.
//!
//! The reversal removes the version the write set and the ended version it
//! appended to history, and reinstalls the prior live version at the
//! ordinal that set it. It draws no ordinal and ends no version, so the key's
//! newest ordinal, its history, and every read at any cut answer as before
//! the write. Expiry and secondary and sorted indexes follow the live entry.

use super::KvEngine;
use super::reads::{KvEntryImage, KvKeyRef};
use crate::engine::kv::UnboundKvWrite;
use crate::engine::kv::engine_helpers::{
    decode_document, expiry_key, extract_all_field_values_from_msgpack, table_key,
};
use crate::engine::kv::entry::NO_EXPIRY;
use crate::engine::kv::hash_table::LiveVersion;

impl KvEngine {
    /// Put `target` back to the state it held before one write. `prior` is
    /// the live version the key stored before the write
    /// ([`KvEngine::stored_image`]), `None` when it stored none. Writes of a
    /// key reverse newest first. A `prior` with no bound surrogate is
    /// refused and nothing changes.
    pub fn restore_key_version(
        &mut self,
        target: KvKeyRef<'_>,
        prior: Option<&KvEntryImage>,
    ) -> Result<(), UnboundKvWrite> {
        let KvKeyRef {
            database_id,
            tenant_id,
            collection,
            key,
        } = target;
        if let Some(prior) = prior {
            UnboundKvWrite::check(collection, prior.surrogate)?;
        }
        let tkey = table_key(database_id, tenant_id, collection);
        if prior.is_none() && !self.tables.contains_key(&tkey) {
            return Ok(());
        }
        let composite = expiry_key(database_id, tenant_id, collection, key);
        let current_meta = self.tables.get(&tkey).and_then(|t| t.get_entry_meta(key));
        if let Some(meta) = current_meta
            && meta.has_ttl
        {
            self.expiry.cancel(&composite, meta.expire_at_ms);
        }

        let removed = self
            .table_for_write_or_create(tkey, database_id, tenant_id, collection)
            .restore_version(
                key,
                prior.map(|p| LiveVersion {
                    value: &p.value,
                    expire_at_ms: p.expire_at_ms,
                    surrogate: p.surrogate,
                    sys_from: p.sys_from,
                }),
            );
        self.recharge_table(tkey);

        if let Some(prior) = prior
            && prior.expire_at_ms != NO_EXPIRY
        {
            // A truncate leaves its rows' wheel entries behind: the stale
            // one leaves first, so the key holds one entry.
            self.expiry.cancel(&composite, prior.expire_at_ms);
            self.expiry.insert(composite, prior.expire_at_ms);
        }

        if let Some(idx_set) = self.indexes.get_mut(&tkey)
            && !idx_set.is_empty()
        {
            if let Some(old) = &removed {
                let fields = extract_all_field_values_from_msgpack(old);
                let refs: Vec<(&str, &[u8])> = fields
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_slice()))
                    .collect();
                idx_set.on_delete(key, &refs);
            }
            if let Some(prior) = prior {
                let fields = extract_all_field_values_from_msgpack(&prior.value);
                let refs: Vec<(&str, &[u8])> = fields
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_slice()))
                    .collect();
                idx_set.on_put(key, &refs, None);
            }
        }
        if self.sorted_indexes.has_indexes(tkey) {
            if removed.is_some() {
                self.sorted_indexes.on_delete(tkey, key);
            }
            if let Some(prior) = prior {
                let document = decode_document(&prior.value);
                self.sorted_indexes.on_put(tkey, key, document.as_ref());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{Surrogate, SystemTimeScope};

    use super::*;
    use crate::engine::kv::KvPutParams;

    const NOW: u64 = 1_000_000;

    fn engine() -> KvEngine {
        KvEngine::new(NOW, 16, 0.75, 4, 64, 1000, 1024)
    }

    fn target(key: &[u8]) -> KvKeyRef<'_> {
        KvKeyRef {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            key,
        }
    }

    fn put_at(e: &mut KvEngine, key: &[u8], value: &[u8], expire_at_ms: u64, ordinal: i64) {
        let scope = e.open_write_scope(Some(ordinal));
        e.put_with_absolute_expiry(
            KvPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "c",
                key,
                value,
                ttl_ms: 0,
                now_ms: NOW,
                surrogate: Surrogate::new(3),
            },
            expire_at_ms,
        )
        .expect("put");
        e.close_write_scope(scope);
    }

    fn at(e: &KvEngine, ordinal: i64) -> Option<Vec<u8>> {
        e.get_at(
            0,
            1,
            "c",
            b"k",
            SystemTimeScope::Pinned {
                ordinal,
                cut_marker: 0,
            },
            NOW,
        )
        .map(|(value, _)| value)
    }

    /// A reversed overwrite leaves the key exactly as before: its newest
    /// ordinal, its history, its expiry, and every read at a cut.
    #[test]
    fn a_reversed_overwrite_restores_the_newest_ordinal_and_history() {
        let mut e = engine();
        put_at(&mut e, b"k", b"v0", NOW + 5_000, 100);
        let prior = e.stored_image(0, 1, "c", b"k");
        let history = e.key_history(0, 1, "c", b"k");
        put_at(&mut e, b"k", b"v1", 0, 200);
        e.restore_key_version(target(b"k"), prior.as_ref())
            .expect("restore");
        assert_eq!(e.key_newest_ordinal(0, 1, "c", b"k"), Some(100));
        assert_eq!(e.key_history(0, 1, "c", b"k"), history);
        assert_eq!(e.stored_image(0, 1, "c", b"k"), prior);
        assert_eq!(at(&e, 250).as_deref(), Some(&b"v0"[..]));
        assert_eq!(at(&e, 99), None);
    }

    /// A reversed delete reinstalls the row at its own ordinal, and a
    /// reversed insert removes the key without a version.
    #[test]
    fn a_reversed_delete_and_insert_leave_no_version_behind() {
        let mut e = engine();
        put_at(&mut e, b"k", b"v0", 0, 100);
        let prior = e.stored_image(0, 1, "c", b"k");
        let scope = e.open_write_scope(Some(300));
        e.delete(0, 1, "c", &[b"k".to_vec()], NOW);
        e.close_write_scope(scope);
        e.restore_key_version(target(b"k"), prior.as_ref())
            .expect("restore");
        assert_eq!(e.key_newest_ordinal(0, 1, "c", b"k"), Some(100));
        assert!(e.key_history(0, 1, "c", b"k").is_empty());

        let before = e.stored_image(0, 1, "c", b"x");
        assert_eq!(before, None);
        put_at(&mut e, b"x", b"x", 0, 400);
        e.restore_key_version(target(b"x"), None).expect("restore");
        assert_eq!(e.key_newest_ordinal(0, 1, "c", b"x"), None);
    }
}
