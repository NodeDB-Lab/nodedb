// SPDX-License-Identifier: BUSL-1.1

//! BALANCED constraint check across all new inserts in a transaction.

use std::collections::HashMap;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::balanced;

use super::UndoEntry;

impl CoreLoop {
    /// Check BALANCED constraints across all new inserts in this transaction.
    ///
    /// For each collection with a BALANCED constraint, collects all new inserts
    /// (undo entries where `old_value == None`), extracts the balanced fields,
    /// and validates that debits == credits per group_key.
    pub(in crate::data::executor::handlers::transaction) fn check_balanced_constraints(
        &self,
        database_id: u64,
        tid: u64,
        undo_log: &[UndoEntry],
    ) -> Result<(), ErrorCode> {
        // Group new inserts by collection: (collection_name → [(doc_id, stored_bytes)]).
        let mut inserts_by_collection: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        for entry in undo_log {
            if let UndoEntry::PutDocument {
                collection,
                document_id,
                old_value: None,
                ..
            } = entry
                && let Ok(Some(stored)) = self.sparse.get(database_id, tid, collection, document_id)
            {
                inserts_by_collection
                    .entry(collection.clone())
                    .or_default()
                    .push(stored);
            }
        }

        for (collection, stored_docs) in &inserts_by_collection {
            let config_key = (
                crate::types::DatabaseId::new(database_id),
                crate::types::TenantId::new(tid),
                collection.to_string(),
            );
            let Some(config) = self.doc_configs.get(&config_key) else {
                continue;
            };
            let Some(ref balanced_def) = config.enforcement.balanced else {
                continue;
            };

            let mut entries = Vec::with_capacity(stored_docs.len());
            for stored_bytes in stored_docs {
                if let Some(json) = doc_format::decode_document(stored_bytes)
                    && let Some(entry) = balanced::extract_entry(balanced_def, &json)
                {
                    entries.push(entry);
                }
            }

            balanced::check_balanced(collection, balanced_def, &entries)?;
        }

        Ok(())
    }
}
