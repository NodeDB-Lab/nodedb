//! Undo log types and rollback logic for transaction batches.

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;

/// Tracks a write operation for rollback purposes.
pub(in crate::data::executor) enum UndoEntry {
    /// Undo a PointPut by deleting the document (or restoring the old value).
    PutDocument {
        collection: String,
        document_id: String,
        /// `None` if the document didn't exist before (inserted); `Some(bytes)`
        /// if it was overwritten (updated).
        old_value: Option<Vec<u8>>,
    },
    /// Undo a PointDelete by re-inserting the document.
    DeleteDocument {
        collection: String,
        document_id: String,
        old_value: Vec<u8>,
    },
    /// Undo a VectorInsert by soft-deleting the inserted vector.
    InsertVector {
        index_key: (crate::types::TenantId, String),
        vector_id: u32,
    },
    /// Undo a VectorDelete by un-deleting (clearing tombstone).
    DeleteVector {
        index_key: (crate::types::TenantId, String),
        vector_id: u32,
    },
    /// Undo an EdgePut by deleting the edge (or restoring old properties).
    PutEdge {
        src_id: String,
        label: String,
        dst_id: String,
        /// `None` if edge didn't exist before (inserted); `Some(bytes)` if overwritten.
        old_properties: Option<Vec<u8>>,
    },
    /// Undo an EdgeDelete by re-inserting the edge with its old properties.
    DeleteEdge {
        src_id: String,
        label: String,
        dst_id: String,
        old_properties: Vec<u8>,
    },
}

impl CoreLoop {
    /// Roll back completed writes in reverse order.
    pub(super) fn rollback_undo_log(&mut self, tid: u32, undo_log: Vec<UndoEntry>) {
        for entry in undo_log.into_iter().rev() {
            match entry {
                UndoEntry::PutDocument {
                    collection,
                    document_id,
                    old_value,
                } => {
                    if let Some(old) = old_value {
                        // Restore previous value.
                        let _ = self.sparse.put(tid, &collection, &document_id, &old);
                    } else {
                        // Document was newly inserted — delete it.
                        let _ = self.sparse.delete(tid, &collection, &document_id);
                    }
                    // Also revert inverted index.
                    let _ = self.inverted.remove_document(
                        crate::types::TenantId::new(tid),
                        &collection,
                        &document_id,
                    );
                }
                UndoEntry::DeleteDocument {
                    collection,
                    document_id,
                    old_value,
                } => {
                    // Re-insert the deleted document.
                    let _ = self.sparse.put(tid, &collection, &document_id, &old_value);
                }
                UndoEntry::InsertVector {
                    index_key,
                    vector_id,
                } => {
                    // Soft-delete the inserted vector.
                    if let Some(index) = self.vector_collections.get_mut(&index_key) {
                        index.delete(vector_id);
                    }
                }
                UndoEntry::DeleteVector {
                    index_key,
                    vector_id,
                } => {
                    // Un-delete: clear tombstone flag.
                    if let Some(index) = self.vector_collections.get_mut(&index_key) {
                        index.undelete(vector_id);
                    }
                }
                UndoEntry::PutEdge {
                    src_id,
                    label,
                    dst_id,
                    old_properties,
                } => {
                    let tenant = nodedb_types::TenantId::new(tid);
                    if let Some(old_props) = old_properties {
                        // Edge was overwritten — restore old properties.
                        let _ = self
                            .edge_store
                            .put_edge(tenant, &src_id, &label, &dst_id, &old_props);
                    } else {
                        // Edge was newly inserted — delete it and remove from CSR.
                        let _ = self
                            .edge_store
                            .delete_edge(tenant, &src_id, &label, &dst_id);
                        self.csr_partition_mut(tid)
                            .remove_edge(&src_id, &label, &dst_id);
                    }
                }
                UndoEntry::DeleteEdge {
                    src_id,
                    label,
                    dst_id,
                    old_properties,
                } => {
                    let tenant = nodedb_types::TenantId::new(tid);
                    // Re-insert the deleted edge with its original properties.
                    let _ =
                        self.edge_store
                            .put_edge(tenant, &src_id, &label, &dst_id, &old_properties);
                    let weight =
                        crate::engine::graph::csr::extract_weight_from_properties(&old_properties);
                    // Rollback path: LabelOverflow here can't be surfaced
                    // (we're already undoing a transaction), but must not
                    // be silently ignored either. Log at warn — the CSR
                    // state is then known to be incomplete, and the outer
                    // transaction error is what the client actually sees.
                    let partition = self.csr_partition_mut(tid);
                    let csr_res = if weight != 1.0 {
                        partition.add_edge_weighted(&src_id, &label, &dst_id, weight)
                    } else {
                        partition.add_edge(&src_id, &label, &dst_id)
                    };
                    if let Err(e) = csr_res {
                        tracing::warn!(
                            core = self.core_id,
                            error = %e,
                            "transaction undo: CSR re-insert failed; CSR may be incomplete"
                        );
                    }
                }
            }
        }
    }

    /// Check BALANCED constraints across all new inserts in this transaction.
    ///
    /// For each collection with a BALANCED constraint, collects all new inserts
    /// (undo entries where `old_value == None`), extracts the balanced fields,
    /// and validates that debits == credits per group_key.
    pub(super) fn check_balanced_constraints(
        &self,
        tid: u32,
        undo_log: &[UndoEntry],
    ) -> Result<(), ErrorCode> {
        use super::super::super::enforcement::balanced;
        use std::collections::HashMap;

        // Group new inserts by collection: (collection_name → [(doc_id, stored_bytes)]).
        let mut inserts_by_collection: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        for entry in undo_log {
            if let UndoEntry::PutDocument {
                collection,
                document_id,
                old_value: None, // Only new inserts, not updates.
            } = entry
            {
                // Read the current stored value to extract balanced fields.
                if let Ok(Some(stored)) = self.sparse.get(tid, collection, document_id) {
                    inserts_by_collection
                        .entry(collection.clone())
                        .or_default()
                        .push(stored);
                }
            }
        }

        // For each collection, check if it has a BALANCED constraint.
        for (collection, stored_docs) in &inserts_by_collection {
            let config_key = (crate::types::TenantId::new(tid), collection.to_string());
            let Some(config) = self.doc_configs.get(&config_key) else {
                continue;
            };
            let Some(ref balanced_def) = config.enforcement.balanced else {
                continue;
            };

            // Extract InsertEntry structs from the stored documents.
            let mut entries = Vec::with_capacity(stored_docs.len());
            for stored_bytes in stored_docs {
                // Decode MessagePack/JSON to serde_json::Value for field extraction.
                if let Some(json) = super::super::super::doc_format::decode_document(stored_bytes)
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
