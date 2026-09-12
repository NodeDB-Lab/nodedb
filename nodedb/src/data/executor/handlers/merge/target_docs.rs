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
use redb::{ReadableDatabase, ReadableTable};

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::sparse::btree::tables::{KeyedTable, invalid_storage_key_err};

impl CoreLoop {
    /// Collect every target row as `(doc_id, stored_bytes)` from a consistent
    /// read snapshot. Shared by the legacy walk and the orchestrated
    /// resolve/apply classification so both see the same target set.
    ///
    /// `txn_id` selects the read view. `None` (autocommit) reads committed base
    /// storage only — byte-identical to the pre-staging behavior. `Some(txn)`
    /// folds the transaction's staging overlay: a staged tombstone hides its base
    /// row, a staged put replaces the base body, and a staged put absent from
    /// base is appended — so an in-transaction MERGE resolved at COMMIT sees rows
    /// staged by earlier statements in the same transaction. The `doc_id` this
    /// produces is the storage key's text, matching the overlay's surrogate
    /// keying, so staged and base bodies (same canonical stored form — Binary
    /// Tuple for a strict target, MessagePack for a schemaless one) are merged
    /// like-for-like and decoded identically downstream by `decode_target`.
    pub(in crate::data::executor) fn collect_target_docs(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        txn_id: Option<crate::types::TxnId>,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let prefix = crate::engine::sparse::btree::coll_prefix(database_id, tid, collection);
        let end = format!("{prefix}\u{ffff}");

        let read_txn = self
            .sparse
            .db()
            .begin_read()
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("read txn: {e}"),
            })?;
        let table = read_txn
            .open_table(crate::engine::sparse::btree::DOCUMENTS)
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("open table: {e}"),
            })?;

        let mut docs: Vec<(StorageKey, Vec<u8>)> = Vec::new();
        if let Ok(range) = table.range(prefix.as_str()..end.as_str()) {
            for entry in range.flatten() {
                let key = entry.0.value();
                let bytes = entry.1.value().to_vec();
                if let Some(rest) = key.strip_prefix(&prefix) {
                    let storage_key = StorageKey::parse(rest).ok_or_else(|| {
                        invalid_storage_key_err(KeyedTable::Documents, collection, rest)
                    })?;
                    docs.push((storage_key, bytes));
                }
            }
        }

        // Read-your-own-writes: fold the transaction's staging overlay over the
        // base set. Collect-all predicate — MERGE classifies the whole target,
        // there is no scan filter to re-check. No-op when the transaction has no
        // overlay (or `txn_id` is `None`).
        if let Some(txn_id) = txn_id {
            let coll_key: (crate::types::DatabaseId, crate::types::TenantId, String) = (
                crate::types::DatabaseId::new(database_id),
                crate::types::TenantId::new(tid),
                collection.to_string(),
            );
            self.merge_overlay_into_scan(txn_id, &coll_key, &mut docs, &|_, _| true);
        }
        Ok(docs
            .into_iter()
            .map(|(key, body)| (key.to_string(), body))
            .collect())
    }
}
