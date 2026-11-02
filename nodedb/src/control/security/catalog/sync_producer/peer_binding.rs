// SPDX-License-Identifier: BUSL-1.1

//! `_system.sync_peer_bindings` — which producer owns which Loro peer id.
//!
//! A Loro peer id is the identity every operation in a CRDT document is
//! attributed to. Two replicas that claim the same one allocate overlapping
//! `(peer, counter)` ranges for different writes, and the merge resolves that by
//! discarding whichever arrives second — silently, because at the
//! `(peer, counter)` level a colliding write and an idempotent replay are the
//! same thing. Nothing downstream of the merge can tell them apart.
//!
//! Binding the peer id to the producer that first used it moves the decision to
//! where the two *are* distinguishable: the session. One row per
//! `(database, tenant, collection, peer)` — the exact scope of one `LoroDoc`,
//! so a peer id reused across collections, which cannot collide, is not refused.

use crate::control::security::catalog::types::{SystemCatalog, catalog_err};
use redb::{ReadableDatabase, ReadableTable};

/// Durable owner of each Loro peer id.
///
/// Key:   `database_id` ‖ `tenant_id` ‖ `peer_id` ‖ `collection` bytes. The
///         variable-length collection name is last, so no two distinct keys can
///         encode to the same bytes.
/// Value: MessagePack-serialized [`StoredPeerBinding`].
pub const SYNC_PEER_BINDINGS: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("_system.sync_peer_bindings");

/// The `LoroDoc` a peer id is scoped to, plus the peer id itself.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerBindingKey {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: String,
    pub peer_id: u64,
}

impl PeerBindingKey {
    pub fn new(database_id: u64, tenant_id: u64, collection: &str, peer_id: u64) -> Self {
        Self {
            database_id,
            tenant_id,
            collection: collection.to_owned(),
            peer_id,
        }
    }

    /// Deterministic, unambiguous catalog key bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(24 + self.collection.len());
        bytes.extend_from_slice(&self.database_id.to_be_bytes());
        bytes.extend_from_slice(&self.tenant_id.to_be_bytes());
        bytes.extend_from_slice(&self.peer_id.to_be_bytes());
        bytes.extend_from_slice(self.collection.as_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> crate::Result<Self> {
        if bytes.len() < 24 {
            return Err(crate::Error::Storage {
                engine: "catalog".into(),
                detail: "sync peer binding key is truncated".into(),
            });
        }
        let take = |offset: usize| -> u64 {
            let mut word = [0u8; 8];
            word.copy_from_slice(&bytes[offset..offset + 8]);
            u64::from_be_bytes(word)
        };
        let collection = std::str::from_utf8(&bytes[24..]).map_err(|_| crate::Error::Storage {
            engine: "catalog".into(),
            detail: "sync peer binding key holds a non-UTF-8 collection".into(),
        })?;
        Ok(Self {
            database_id: take(0),
            tenant_id: take(8),
            collection: collection.to_owned(),
            peer_id: take(16),
        })
    }
}

/// The producer that owns one Loro peer id.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone, Copy, PartialEq, Eq)]
#[msgpack(map)]
pub struct StoredPeerBinding {
    /// Durable producer id of the Lite client that first used this peer id.
    pub producer_id: u64,
    /// Unix-millisecond timestamp when the binding was created.
    pub bound_ms: i64,
}

impl SystemCatalog {
    /// Create the binding only if the key is free, and report the owner either
    /// way.
    ///
    /// The check and the write share one redb write transaction: two sessions
    /// racing on a fresh peer id must not both observe it as unowned, or the
    /// collision this table exists to prevent happens anyway.
    pub fn bind_peer_if_absent(
        &self,
        key: &[u8],
        binding: &StoredPeerBinding,
    ) -> crate::Result<StoredPeerBinding> {
        let bytes = zerompk::to_msgpack_vec(binding)
            .map_err(|e| catalog_err("serialize sync_peer_binding", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("sync_peer_bindings write txn", e))?;
        let owner = {
            use redb::ReadableTable as _;

            let mut table = txn
                .open_table(SYNC_PEER_BINDINGS)
                .map_err(|e| catalog_err("open sync_peer_bindings", e))?;
            let existing = table
                .get(key)
                .map_err(|e| catalog_err("get sync_peer_bindings", e))?
                .map(|value| {
                    zerompk::from_msgpack::<StoredPeerBinding>(value.value())
                        .map_err(|e| catalog_err("deserialize sync_peer_binding", e))
                })
                .transpose()?;
            match existing {
                Some(existing) => existing,
                None => {
                    table
                        .insert(key, bytes.as_slice())
                        .map_err(|e| catalog_err("insert sync_peer_bindings", e))?;
                    *binding
                }
            }
        };
        txn.commit()
            .map_err(|e| catalog_err("sync_peer_bindings commit", e))?;
        Ok(owner)
    }

    /// Load the binding for one key, or `None` when the peer id is unclaimed.
    pub fn get_peer_binding(&self, key: &[u8]) -> crate::Result<Option<StoredPeerBinding>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("sync_peer_bindings read txn", e))?;
        let table = txn
            .open_table(SYNC_PEER_BINDINGS)
            .map_err(|e| catalog_err("open sync_peer_bindings", e))?;
        match table
            .get(key)
            .map_err(|e| catalog_err("get sync_peer_bindings", e))?
        {
            None => Ok(None),
            Some(value) => {
                Ok(Some(zerompk::from_msgpack(value.value()).map_err(|e| {
                    catalog_err("deserialize sync_peer_binding", e)
                })?))
            }
        }
    }

    /// Load every binding so the registry can answer from memory instead of
    /// opening a read transaction on the delta hot path.
    pub fn list_peer_bindings(&self) -> crate::Result<Vec<(PeerBindingKey, StoredPeerBinding)>> {
        use redb::ReadableTable as _;

        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("sync_peer_bindings read txn", e))?;
        let table = txn
            .open_table(SYNC_PEER_BINDINGS)
            .map_err(|e| catalog_err("open sync_peer_bindings", e))?;
        let mut bindings = Vec::new();
        for row in table
            .iter()
            .map_err(|e| catalog_err("iterate sync_peer_bindings", e))?
        {
            let (key, value) = row.map_err(|e| catalog_err("read sync_peer_bindings", e))?;
            let binding: StoredPeerBinding = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deserialize sync_peer_binding", e))?;
            bindings.push((PeerBindingKey::decode(key.value())?, binding));
        }
        Ok(bindings)
    }

    /// Write a binding verbatim, overwriting any existing row.
    ///
    /// Used only by the replicated apply path, where the entry has already won
    /// the ownership decision on the proposing node and every replica must end
    /// with the identical row.
    pub fn put_peer_binding(&self, key: &[u8], binding: &StoredPeerBinding) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(binding)
            .map_err(|e| catalog_err("serialize sync_peer_binding", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("sync_peer_bindings write txn", e))?;
        {
            let mut table = txn
                .open_table(SYNC_PEER_BINDINGS)
                .map_err(|e| catalog_err("open sync_peer_bindings", e))?;
            table
                .insert(key, bytes.as_slice())
                .map_err(|e| catalog_err("insert sync_peer_bindings", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("sync_peer_bindings commit", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, cat)
    }

    fn binding(producer_id: u64) -> StoredPeerBinding {
        StoredPeerBinding {
            producer_id,
            bound_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn first_writer_owns_the_peer_id() {
        let (_dir, cat) = open();
        let key = PeerBindingKey::new(1, 2, "docs", 7).encode();
        assert_eq!(
            cat.bind_peer_if_absent(&key, &binding(10))
                .unwrap()
                .producer_id,
            10
        );
        // A second producer claiming the same peer id is told who owns it and
        // does not take it over.
        assert_eq!(
            cat.bind_peer_if_absent(&key, &binding(11))
                .unwrap()
                .producer_id,
            10
        );
        assert_eq!(cat.get_peer_binding(&key).unwrap().unwrap().producer_id, 10);
    }

    #[test]
    fn a_peer_id_is_scoped_to_one_collection() {
        // The same peer id in a different collection is a different LoroDoc and
        // cannot collide, so it must not be refused.
        let (_dir, cat) = open();
        let docs = PeerBindingKey::new(1, 2, "docs", 7).encode();
        let notes = PeerBindingKey::new(1, 2, "notes", 7).encode();
        assert_eq!(
            cat.bind_peer_if_absent(&docs, &binding(10))
                .unwrap()
                .producer_id,
            10
        );
        assert_eq!(
            cat.bind_peer_if_absent(&notes, &binding(11))
                .unwrap()
                .producer_id,
            11
        );
    }

    #[test]
    fn keys_with_shifted_boundaries_do_not_collide() {
        // Ambiguous key encoding would silently merge two distinct documents'
        // bindings, so the collection name is pinned to the tail.
        let a = PeerBindingKey::new(1, 2, "ab", 7);
        let b = PeerBindingKey::new(1, 2, "b", 7);
        assert_ne!(a.encode(), b.encode());
    }

    #[test]
    fn every_binding_round_trips_through_the_listing() {
        let (_dir, cat) = open();
        let key = PeerBindingKey::new(3, 4, "orders", 99);
        cat.bind_peer_if_absent(&key.encode(), &binding(12))
            .unwrap();
        let listed = cat.list_peer_bindings().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].0, key);
        assert_eq!(listed[0].1.producer_id, 12);
    }

    #[test]
    fn bindings_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let key = PeerBindingKey::new(1, 1, "docs", 5).encode();
        {
            let cat = SystemCatalog::open(&path).unwrap();
            cat.bind_peer_if_absent(&key, &binding(21)).unwrap();
        }
        let cat = SystemCatalog::open(&path).unwrap();
        assert_eq!(cat.get_peer_binding(&key).unwrap().unwrap().producer_id, 21);
    }
}
