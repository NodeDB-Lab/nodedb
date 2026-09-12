// SPDX-License-Identifier: BUSL-1.1

//! Settling the BALANCED entries a write produced against the boundary that
//! owns them.
//!
//! BALANCED is a boundary predicate, not a row predicate: debits and credits
//! arrive on different rows, so no single write can be judged on its own. The
//! boundary is a transaction — and an autocommit statement IS a transaction, so
//! a statement that writes one leg of a journal by itself is unbalanced by the
//! definition and is refused. That is the point of the constraint: a balanced
//! ledger cannot be populated one leg at a time.
//!
//! Which boundary a write belongs to is decided by
//! [`CoreLoop::balanced_txn_entries`]: with a transaction batch open the
//! entries accumulate onto it and the batch checks them once at commit; with
//! none open the statement is its own boundary and checks immediately, before
//! it commits, so a refusal writes nothing.
//!
//! # Why some callers pre-compute their entries here instead of collecting the
//! # funnel's
//!
//! A statement that commits ROW BY ROW (bulk delete, bulk update, truncate,
//! update-from-join, and a MERGE's delete arms) has already made row N durable
//! by the time row N+1 folds, so a check that ran after the last row could only
//! report a violation it can no longer undo. Those paths derive the statement's
//! entries from images they already hold and settle them BEFORE the first row
//! is written — the helpers below exist for exactly that, and they read nothing
//! at all for a collection that declares no BALANCED constraint.

use nodedb_physical::physical_plan::BalancedDef;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::balanced::{self, BalancedEntry};
use crate::data::executor::enforcement::images::RowImages;
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// The BALANCED definition a collection declares, if any.
    pub(in crate::data::executor) fn balanced_def(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Option<BalancedDef> {
        self.doc_configs
            .get(&(
                DatabaseId::new(database_id),
                TenantId::new(tid),
                collection.to_string(),
            ))
            .and_then(|config| config.enforcement.balanced.clone())
    }

    /// Account for the entries one write boundary produced.
    ///
    /// Inside a transaction batch the entries are accumulated onto the batch,
    /// which checks them once at its own commit boundary. Outside one, the
    /// caller IS the boundary and the check runs here — the caller must not
    /// have committed yet, so an `Err` leaves nothing written.
    pub(in crate::data::executor) fn settle_balanced_entries(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        entries: Vec<BalancedEntry>,
    ) -> crate::Result<()> {
        if let Some(pending) = self.balanced_txn_entries.as_mut() {
            pending.extend(
                entries
                    .into_iter()
                    .map(|entry| (collection.to_string(), entry)),
            );
            return Ok(());
        }
        if entries.is_empty() {
            return Ok(());
        }
        let Some(def) = self.balanced_def(database_id, tid, collection) else {
            return Ok(());
        };
        balanced::check_balanced(collection, &def, &entries)
    }

    /// The entries a statement's DELETEs contribute, read from the stored rows
    /// it is about to remove.
    ///
    /// Reads nothing when the collection declares no BALANCED constraint. A row
    /// that is already gone contributes nothing: removing it removes no amount.
    pub(in crate::data::executor) fn balanced_entries_for_stored_deletes(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        document_ids: &[nodedb_types::StorageKey],
    ) -> crate::Result<Vec<BalancedEntry>> {
        let Some(def) = self.balanced_def(database_id, tid, collection) else {
            return Ok(Vec::new());
        };
        let mut entries = Vec::new();
        for key in document_ids {
            let Some(stored) = self.sparse.get(database_id, tid, collection, key)? else {
                continue;
            };
            // A stored row of a collection that declares constraints over its
            // columns must decode: treating an unreadable pre-image as "no
            // contribution" would let an unbalanced removal through, which is
            // the one thing this check exists to refuse.
            let Some(doc) =
                self.decode_stored_for_balance(database_id, tid, collection, &stored)?
            else {
                continue;
            };
            entries.extend(balanced::entries_for(
                &def,
                &RowImages::Delete { old_doc: &doc },
            ));
        }
        Ok(entries)
    }

    /// The entries a statement's DELETEs contribute, from pre-images the caller
    /// already holds as SUBMITTED (MessagePack) bodies.
    ///
    /// A body carrying no readable document holds no column the definition can
    /// read, so it contributes nothing — the same answer the write-path hook
    /// gives a submitted body it cannot decode.
    pub(in crate::data::executor) fn balanced_entries_for_submitted_deletes(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        bodies: &[&[u8]],
    ) -> Vec<BalancedEntry> {
        let Some(def) = self.balanced_def(database_id, tid, collection) else {
            return Vec::new();
        };
        let mut entries = Vec::new();
        for body in bodies {
            let Ok(doc) = doc_format::decode_document(body) else {
                continue;
            };
            entries.extend(balanced::entries_for(
                &def,
                &RowImages::Delete { old_doc: &doc },
            ));
        }
        entries
    }

    /// The entries a statement's UPDATEs contribute, from image pairs the
    /// caller already holds as STORED bytes.
    pub(in crate::data::executor) fn balanced_entries_for_stored_updates(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        images: &[(&[u8], &[u8])],
    ) -> crate::Result<Vec<BalancedEntry>> {
        let Some(def) = self.balanced_def(database_id, tid, collection) else {
            return Ok(Vec::new());
        };
        let mut entries = Vec::new();
        for (old_bytes, new_bytes) in images {
            let (Some(old_doc), Some(new_doc)) = (
                self.decode_stored_for_balance(database_id, tid, collection, old_bytes)?,
                self.decode_stored_for_balance(database_id, tid, collection, new_bytes)?,
            ) else {
                continue;
            };
            entries.extend(balanced::entries_for(
                &def,
                &RowImages::Update {
                    old_doc: &old_doc,
                    new_doc: &new_doc,
                },
            ));
        }
        Ok(entries)
    }

    /// The entries a statement's UPDATEs contribute, from image pairs the
    /// caller already holds as decoded documents.
    pub(in crate::data::executor) fn balanced_entries_for_json_updates(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        images: &[(&serde_json::Value, &serde_json::Value)],
    ) -> Vec<BalancedEntry> {
        let Some(def) = self.balanced_def(database_id, tid, collection) else {
            return Vec::new();
        };
        let mut entries = Vec::new();
        for (old_doc, new_doc) in images {
            entries.extend(balanced::entries_for(
                &def,
                &RowImages::Update { old_doc, new_doc },
            ));
        }
        entries
    }

    /// Decode a stored body in the collection's own storage mode.
    ///
    /// `None` only when the collection is not registered at all, which is the
    /// same state in which it declares no constraint to enforce.
    fn decode_stored_for_balance(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        stored: &[u8],
    ) -> crate::Result<Option<serde_json::Value>> {
        let Some(config) = self.doc_configs.get(&(
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        )) else {
            return Ok(None);
        };
        self.decode_stored_document(config, stored).map(Some)
    }
}
