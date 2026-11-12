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
//!
//! A session resolve's stamp is provisional. No ordinal reservation covers
//! it, so the write funnel raises every stamp of the record below the
//! record's reserved ordinal to that ordinal before it appends the record
//! (`wal::redo::stamp_floor`). A Calvin resolve's stamp is final.

use nodedb_types::{StorageKey, Surrogate};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::bitemporal_time::raised_above;
use crate::engine::sparse::btree::SparseEngine;
use crate::types::TxnId;

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
}

/// The stamps of one resolve's document versions in one collection.
///
/// A resolve stamps each row of a collection once: a staged row once per
/// binding, and a base row a staged TRUNCATE removes once, never a row the
/// overlay stages. So a stamp reads only the row's stored versions, and the
/// resolve keeps no stamp it has given.
pub(super) struct DocOrdinals<'a> {
    sparse: &'a SparseEngine,
    database_id: u64,
    tenant: u64,
    collection: &'a str,
    /// The transaction's ordinal.
    txn: i64,
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
        }
    }

    /// The system-time ordinal of the version of row `key`: the
    /// transaction's ordinal, raised one above the newest version of the
    /// row when that version sits at or above it ([`raised_above`]).
    ///
    /// `Err` when a stored version does not read.
    pub(super) fn for_row(&self, key: &StorageKey) -> crate::Result<i64> {
        let stored = self.sparse.versioned_latest_sys_from(
            self.database_id,
            self.tenant,
            self.collection,
            key,
        )?;
        Ok(raised_above(self.txn, stored))
    }

    /// [`Self::for_row`] of the row stored under `surrogate`.
    pub(super) fn for_staged_row(&self, surrogate: u32) -> crate::Result<i64> {
        self.for_row(&StorageKey::for_surrogate(Surrogate::new(surrogate)))
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
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
        let ordinals = DocOrdinals::new(&sparse, 0, 1, "c", TXN);
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
