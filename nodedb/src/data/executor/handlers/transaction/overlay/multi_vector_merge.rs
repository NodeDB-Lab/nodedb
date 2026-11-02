// SPDX-License-Identifier: BUSL-1.1

//! The multi-vector documents a transaction's staged writes hide from a
//! multi-vector score search inside the block.
//!
//! A committed delete, update or truncate of a row ends the multi-vector
//! document bound to the row's surrogate. Inside the block those writes are
//! staged, so the search hides the documents they will end: a staged
//! delete's, a staged put's over a stored row, and every document after a
//! staged truncate. A staged put of a new row ends nothing.

use std::collections::HashSet;

use nodedb_types::{StorageKey, Surrogate};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::types::{DatabaseId, TenantId, TxnId};

/// The documents a transaction's staged writes hide.
#[derive(Debug, Default, PartialEq, Eq)]
pub(in crate::data::executor) struct MultiVectorHides {
    /// A staged truncate hides every document.
    pub all: bool,
    /// The row surrogates whose documents a staged row write hides.
    pub rows: HashSet<Surrogate>,
}

impl MultiVectorHides {
    /// Whether the document of `surrogate` stays visible.
    pub(in crate::data::executor) fn keeps(&self, surrogate: Surrogate) -> bool {
        !self.all && !self.rows.contains(&surrogate)
    }
}

impl CoreLoop {
    /// The multi-vector documents of `collection` that `txn_id`'s staged
    /// writes hide. A staged put hides its row's document when the row is
    /// stored: the put replaces it at COMMIT.
    pub(in crate::data::executor) fn staged_multi_vector_hides(
        &self,
        txn_id: TxnId,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
    ) -> crate::Result<MultiVectorHides> {
        let coll_key = (database_id, tid, collection.to_string());
        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        let mut hides = MultiVectorHides::default();
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return Ok(hides);
        };
        hides.all = overlay.is_truncated(&coll_key);
        for (surrogate, staged) in overlay.iter_for_collection(&coll_key) {
            let surrogate = Surrogate::new(surrogate);
            let hidden = match staged {
                Staged::Tombstone => true,
                Staged::Put(_) => self
                    .sparse
                    .versioned_get_current(
                        database_id.as_u64(),
                        tid.as_u64(),
                        collection,
                        &StorageKey::for_surrogate(surrogate),
                    )?
                    .is_some(),
            };
            if hidden {
                hides.rows.insert(surrogate);
            }
        }
        Ok(hides)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::RowIdentity;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    const TID: u64 = 1;
    const COLL: &str = "docs";

    fn coll_key() -> (DatabaseId, TenantId, String) {
        (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string())
    }

    /// A staged delete hides its row's document. A staged put hides it only
    /// over a stored row. A staged truncate hides every document.
    #[test]
    fn staged_row_writes_hide_the_documents_they_end() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let stored = Surrogate::new(2);
        core.sparse
            .put(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                COLL,
                &StorageKey::for_surrogate(stored),
                &crate::data::executor::doc_format::encode_to_msgpack(&serde_json::json!({"a": 1})),
                1,
            )
            .expect("store the row");
        let txn = TxnId::new(7);
        let overlay = core.txn_overlays.entry(txn).or_default();
        let deleted = Surrogate::new(1);
        let fresh = Surrogate::new(3);
        overlay.insert_tombstone(
            coll_key(),
            deleted.as_u32(),
            &RowIdentity::for_surrogate(deleted),
        );
        overlay.insert_put(
            coll_key(),
            stored.as_u32(),
            &RowIdentity::for_surrogate(stored),
            Vec::new(),
        );
        overlay.insert_put(
            coll_key(),
            fresh.as_u32(),
            &RowIdentity::for_surrogate(fresh),
            Vec::new(),
        );

        let hides = core
            .staged_multi_vector_hides(txn, DatabaseId::DEFAULT, TenantId::new(TID), COLL)
            .expect("hides");
        assert!(!hides.keeps(deleted), "a staged delete hides");
        assert!(!hides.keeps(stored), "a staged put over a stored row hides");
        assert!(hides.keeps(fresh), "a staged put of a new row ends nothing");

        if let Some(overlay) = core.txn_overlays.get_mut(&txn) {
            overlay.mark_truncated(coll_key());
        }
        let hides = core
            .staged_multi_vector_hides(txn, DatabaseId::DEFAULT, TenantId::new(TID), COLL)
            .expect("hides");
        assert!(
            !hides.keeps(fresh),
            "a staged truncate hides every document"
        );
    }
}
