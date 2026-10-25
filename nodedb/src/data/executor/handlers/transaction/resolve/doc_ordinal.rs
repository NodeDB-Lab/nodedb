// SPDX-License-Identifier: BUSL-1.1

//! The system-time ordinal each staged document version of a resolve takes.
//!
//! A transaction's document versions and edge versions take one ordinal, so
//! one `AS OF` cut splits the transaction's documents and edges alike. A
//! Calvin resolve takes the transaction's ordinal, which every participant
//! derives from the epoch instant and position. Any other resolve takes the
//! local clock, frozen in the graph overlay when the transaction stages
//! edges, so a resolve retry and the edge serializer reuse the same value.
//!
//! A version takes that ordinal, unless an earlier write stored a version of
//! the row at or above it (a restored version from a clock that ran ahead, or
//! a write that landed between a frozen first resolve and a retry). The
//! version then takes one above that one, so it sorts after every earlier
//! write, by the rule every bitemporal write stamps by (`KeyStamps`), as an
//! edge version does in `edge_ordinal`. A row lives on one home,
//! and every earlier writer of it flushed before this resolve, so every
//! replica raises alike.

use std::collections::BTreeMap;

use nodedb_types::{RowIdentity, StorageKey, Surrogate};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::bitemporal_time::KeyStamps;
use crate::data::executor::handlers::transaction::overlay::BitemporalStamp;
use crate::engine::sparse::btree::SparseEngine;
use crate::types::{DatabaseId, TenantId, TxnId};

impl CoreLoop {
    /// The system-time ordinal of the transaction `txn_id` resolves: the
    /// Calvin transaction's ordinal, else a fresh one of the local clock.
    /// With a graph overlay the value freezes there on first resolve, so a
    /// retry and the transaction's edge versions take the same ordinal.
    pub(super) fn txn_system_ordinal(&self, txn_id: TxnId) -> i64 {
        let candidate = self.bitemporal_ordinal();
        match self.graph_txn_overlays.get(&txn_id) {
            Some(graph_overlay) => graph_overlay.freeze_system_from(candidate),
            None => candidate,
        }
    }

    /// Pin the resolve-time bitemporal stamp once, in the overlay sidecar,
    /// for every put AND tombstone `txn_id` staged in `collection`, so the
    /// redo carries it and every apply (install, replica, restart) writes
    /// the same version key. A collection that is not bitemporal stamps
    /// nothing. Deterministic doc-id order keeps replicas resolving the same
    /// transaction in sync.
    pub(super) fn pin_doc_stamps(
        &mut self,
        database_id: DatabaseId,
        tid: u64,
        txn_id: TxnId,
        collection: &str,
        txn_ordinal: i64,
    ) -> crate::Result<()> {
        if !self.is_bitemporal(database_id.as_u64(), tid, collection) {
            return Ok(());
        }
        let coll_key = (database_id, TenantId::new(tid), collection.to_string());
        let mut writes: Vec<(&RowIdentity, u32)> = match self.txn_overlays.get(&txn_id) {
            Some(overlay) => overlay
                .iter_doc_entries_for_collection(&coll_key)
                .filter_map(|(doc_id, _staged)| {
                    overlay
                        .surrogate_for_doc_id(&coll_key, doc_id)
                        .map(|surrogate| (doc_id, surrogate))
                })
                .collect(),
            None => Vec::new(),
        };
        writes.sort();
        let mut ordinals = DocOrdinals::new(
            &self.sparse,
            database_id.as_u64(),
            tid,
            collection,
            txn_ordinal,
        );
        let mut stamps: Vec<(u32, BitemporalStamp)> = Vec::with_capacity(writes.len());
        for (_doc_id, surrogate) in writes {
            let key = StorageKey::for_surrogate(Surrogate::new(surrogate));
            stamps.push((
                surrogate,
                BitemporalStamp {
                    sys_from: ordinals.for_row(&key)?,
                    valid_from_ms: i64::MIN,
                    valid_until_ms: i64::MAX,
                },
            ));
        }
        if let Some(overlay) = self.txn_overlays.get_mut(&txn_id) {
            for (surrogate, stamp) in stamps {
                overlay.set_bitemporal(&coll_key, surrogate, stamp);
            }
        }
        Ok(())
    }

    /// The tombstone ordinal of every base row a staged TRUNCATE of
    /// `collection` removes, keyed by surrogate: the transaction's ordinal,
    /// raised above the row's newest stored version. `None` when the
    /// collection is not bitemporal.
    pub(super) fn truncated_row_ordinals(
        &self,
        database_id: DatabaseId,
        tid: u64,
        collection: &str,
        rows: &[(StorageKey, Vec<u8>)],
        txn_ordinal: i64,
    ) -> crate::Result<Option<BTreeMap<u32, i64>>> {
        if !self.is_bitemporal(database_id.as_u64(), tid, collection) {
            return Ok(None);
        }
        let mut ordinals = DocOrdinals::new(
            &self.sparse,
            database_id.as_u64(),
            tid,
            collection,
            txn_ordinal,
        );
        let mut stamps = BTreeMap::new();
        for (key, _body) in rows {
            stamps.insert(key.surrogate().as_u32(), ordinals.for_row(key)?);
        }
        Ok(Some(stamps))
    }
}

/// The stamps of one resolve's document versions in one collection.
pub(super) struct DocOrdinals<'a> {
    sparse: &'a SparseEngine,
    database_id: u64,
    tenant: u64,
    collection: &'a str,
    /// The transaction's ordinal.
    txn: i64,
    stamps: KeyStamps<StorageKey>,
}

impl<'a> DocOrdinals<'a> {
    pub(super) fn new(
        sparse: &'a SparseEngine,
        database_id: u64,
        tenant: u64,
        collection: &'a str,
        txn: i64,
    ) -> Self {
        Self {
            sparse,
            database_id,
            tenant,
            collection,
            txn,
            stamps: KeyStamps::default(),
        }
    }

    /// The system-time ordinal of the version of row `key`: the
    /// transaction's ordinal, raised one above the newest version of the
    /// row ([`KeyStamps`]) when that version sits at or above it.
    ///
    /// `Err` when a stored version does not read.
    pub(super) fn for_row(&mut self, key: &StorageKey) -> crate::Result<i64> {
        let Self {
            sparse,
            database_id,
            tenant,
            collection,
            txn,
            stamps,
        } = self;
        stamps.stamp_row(*txn, vec![*key], |key| {
            sparse.versioned_latest_sys_from(*database_id, *tenant, collection, key)
        })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::data::executor::core_loop::bitemporal_time::raised_above;
    use crate::engine::sparse::btree_versioned::VersionedPut;

    const TXN: i64 = 5_000;

    fn key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate::new(surrogate))
    }

    fn put(sparse: &SparseEngine, surrogate: u32, sys_from: i64) {
        sparse
            .versioned_put(VersionedPut {
                database_id: 0,
                tenant: 1,
                coll: "c",
                doc_id: &key(surrogate),
                sys_from,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                body: b"v",
            })
            .expect("put version");
    }

    #[test]
    fn a_version_takes_the_txn_ordinal_above_older_versions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sparse = SparseEngine::open(&dir.path().join("s.redb")).expect("open");
        put(&sparse, 1, 100);
        put(&sparse, 2, 9_000);
        let mut ordinals = DocOrdinals::new(&sparse, 0, 1, "c", TXN);
        assert_eq!(ordinals.for_row(&key(1)).expect("older"), TXN);
        assert_eq!(
            ordinals.for_row(&key(2)).expect("ahead"),
            9_001,
            "raised to sort after the earlier write"
        );
        assert_eq!(ordinals.for_row(&key(3)).expect("absent"), TXN);
    }

    #[test]
    fn a_stored_version_at_the_txn_ordinal_raises_by_one() {
        assert_eq!(raised_above(TXN, Some(TXN)), TXN + 1);
        assert_eq!(raised_above(TXN, Some(TXN - 1)), TXN);
        assert_eq!(raised_above(TXN, None), TXN);
        assert_eq!(raised_above(TXN, Some(i64::MAX)), i64::MAX);
    }
}
