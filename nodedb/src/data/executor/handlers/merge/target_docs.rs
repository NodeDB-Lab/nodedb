// SPDX-License-Identifier: BUSL-1.1

//! The MERGE target set: every candidate row, from one consistent read view.
//!
//! Kept apart from the source map and from the apply passes because the read
//! view is the whole concern here — base storage alone for autocommit, base
//! folded with the transaction's staging overlay for an in-transaction MERGE.
//! Both the legacy walk and the orchestrated RESOLVE/APPLY passes must see the
//! identical target set or the classification they agree on is meaningless, so
//! there is exactly one place that decides what "the target" is.

use nodedb_types::StorageKey;

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Collect every target row as `(doc_id, stored_bytes)` from a consistent
    /// read snapshot. Shared by the orchestrated resolve/apply classification
    /// so both passes see the same target set.
    ///
    /// `system_as_of` selects the committed versions: `None` reads each row's
    /// current version, `Some(ordinal)` the version visible at a transaction
    /// block's snapshot.
    ///
    /// `txn_id` selects the overlay. `None` (autocommit) reads committed
    /// storage only. `Some(txn)` folds the transaction's staging overlay: a
    /// staged tombstone hides its base row, a staged put replaces the base
    /// body, and a staged put absent from base is appended. The `StorageKey`
    /// this produces matches the overlay's surrogate keying, so staged and
    /// base bodies (Binary Tuple for a strict target, MessagePack for a
    /// schemaless one) merge like-for-like and decode identically downstream
    /// in `decode_target`.
    pub(in crate::data::executor) fn collect_target_docs(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        txn_id: Option<crate::types::TxnId>,
        system_as_of: Option<i64>,
    ) -> crate::Result<Vec<(StorageKey, Vec<u8>)>> {
        // Every target row at the cut, from one read transaction. A read
        // error fails the MERGE: a skipped row would be classified as absent.
        let every_row = |_: &StorageKey, _: &[u8]| true;
        let mut docs =
            self.rows_where_at(database_id, tid, collection, system_as_of, &every_row)?;

        // Read-your-own-writes: fold the transaction's staging overlay over the
        // base set. MERGE classifies the whole target, so every staged row
        // joins. No-op when the transaction has no overlay.
        if let Some(txn_id) = txn_id {
            let coll_key: (crate::types::DatabaseId, crate::types::TenantId, String) = (
                crate::types::DatabaseId::new(database_id),
                crate::types::TenantId::new(tid),
                collection.to_string(),
            );
            self.merge_overlay_into_scan(txn_id, &coll_key, &mut docs, &every_row);
        }
        Ok(docs)
    }
}
