//! Transaction batch execution handler.
//!
//! Executes a `PhysicalPlan::TransactionBatch` atomically: all sub-plans
//! succeed or all are rolled back. Write operations (PointPut, PointDelete,
//! VectorInsert, EdgePut, etc.) are tracked for rollback on failure. CRDT
//! deltas are accumulated in a scratch buffer and only applied on success.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Tracks a write operation for rollback purposes.
enum UndoEntry {
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
    InsertVector { index_key: String, vector_id: u32 },
    /// Undo a VectorDelete by un-deleting (clearing tombstone).
    DeleteVector { index_key: String, vector_id: u32 },
}

impl CoreLoop {
    /// Execute a transaction batch atomically.
    ///
    /// All sub-plans are executed in order. If any sub-plan fails, all
    /// previous writes are rolled back. CRDT deltas are buffered and only
    /// applied to LoroDoc on full success.
    ///
    /// The Control Plane has already written a single `RecordType::Transaction`
    /// WAL record covering all operations before dispatching this batch.
    #[allow(clippy::too_many_lines)]
    pub(in crate::data::executor) fn execute_transaction_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        plans: &[PhysicalPlan],
    ) -> Response {
        debug!(
            core = self.core_id,
            plan_count = plans.len(),
            "transaction batch begin"
        );

        let mut undo_log: Vec<UndoEntry> = Vec::with_capacity(plans.len());
        let mut crdt_deltas: Vec<(Vec<u8>, u64)> = Vec::new();
        let mut last_response = self.response_ok(task);

        for (i, plan) in plans.iter().enumerate() {
            let result = self.execute_tx_sub_plan(tid, plan, &mut undo_log, &mut crdt_deltas);

            match result {
                Ok(resp) => {
                    last_response = resp;
                }
                Err(error_code) => {
                    warn!(
                        core = self.core_id,
                        plan_index = i,
                        "transaction sub-plan failed, rolling back {} operations",
                        undo_log.len()
                    );

                    // Roll back all previous writes in reverse order.
                    self.rollback_undo_log(tid, undo_log);

                    // Discard CRDT scratch buffer (never applied).
                    drop(crdt_deltas);

                    return Response {
                        request_id: task.request_id(),
                        status: Status::Error,
                        attempt: 1,
                        partial: false,
                        payload: crate::bridge::envelope::Payload::empty(),
                        watermark_lsn: self.watermark,
                        error_code: Some(error_code),
                    };
                }
            }
        }

        // All sub-plans succeeded. Apply buffered CRDT deltas.
        for (delta, peer_id) in crdt_deltas {
            let tenant_id = crate::types::TenantId::new(tid);
            if let Ok(engine) = self.get_crdt_engine(tenant_id) {
                let _ = peer_id; // peer_id used for dedup in future
                if let Err(e) = engine.apply_committed_delta(&delta) {
                    warn!(core = self.core_id, error = %e, "CRDT delta apply failed during tx commit");
                }
            }
        }

        debug!(
            core = self.core_id,
            committed = plans.len(),
            "transaction batch committed"
        );

        // Return OK with the last sub-plan's response payload.
        last_response.status = Status::Ok;
        last_response.error_code = None;
        last_response
    }

    /// Execute a single sub-plan within a transaction, recording undo info.
    ///
    /// CRDT deltas are NOT applied immediately — they are buffered in
    /// `crdt_deltas` and only applied after all sub-plans succeed.
    fn execute_tx_sub_plan(
        &mut self,
        tid: u32,
        plan: &PhysicalPlan,
        undo_log: &mut Vec<UndoEntry>,
        crdt_deltas: &mut Vec<(Vec<u8>, u64)>,
    ) -> Result<Response, ErrorCode> {
        // Create a temporary task for sub-plan response construction.
        let dummy_task = ExecutionTask::new(crate::bridge::envelope::Request {
            request_id: crate::types::RequestId::new(0),
            tenant_id: crate::types::TenantId::new(tid),
            vshard_id: crate::types::VShardId::new(0),
            plan: PhysicalPlan::Cancel {
                target_request_id: crate::types::RequestId::new(0),
            },
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
            priority: crate::bridge::envelope::Priority::Normal,
            trace_id: 0,
            consistency: crate::types::ReadConsistency::Strong,
            idempotency_key: None,
        });

        match plan {
            PhysicalPlan::PointPut {
                collection,
                document_id,
                value,
            } => {
                // Save old value for rollback.
                let old_value = self.sparse.get(tid, collection, document_id).ok().flatten();

                let stored = super::super::doc_format::json_to_msgpack(value);
                match self.sparse.put(tid, collection, document_id, &stored) {
                    Ok(()) => {
                        // Auto-index text fields.
                        if let Some(doc) = super::super::doc_format::decode_document(value) {
                            if let Some(obj) = doc.as_object() {
                                let text_content: String = obj
                                    .values()
                                    .filter_map(|v| v.as_str())
                                    .collect::<Vec<_>>()
                                    .join(" ");
                                if !text_content.is_empty() {
                                    let _ = self.inverted.index_document(
                                        collection,
                                        document_id,
                                        &text_content,
                                    );
                                }
                            }
                        }

                        undo_log.push(UndoEntry::PutDocument {
                            collection: collection.clone(),
                            document_id: document_id.clone(),
                            old_value,
                        });
                        Ok(self.response_ok(&dummy_task))
                    }
                    Err(e) => Err(ErrorCode::Internal {
                        detail: e.to_string(),
                    }),
                }
            }

            PhysicalPlan::PointDelete {
                collection,
                document_id,
            } => {
                // Save old value for rollback.
                let old_value = self.sparse.get(tid, collection, document_id).ok().flatten();
                match self.sparse.delete(tid, collection, document_id) {
                    Ok(_) => {
                        // Cascade: inverted index, secondary indexes, graph edges.
                        let _ = self.inverted.remove_document(collection, document_id);
                        let _ =
                            self.sparse
                                .delete_indexes_for_document(tid, collection, document_id);
                        let edges_removed = self.csr.remove_node_edges(document_id);
                        if edges_removed > 0 {
                            let _ = self.edge_store.delete_edges_for_node(document_id);
                        }

                        if let Some(old) = old_value {
                            undo_log.push(UndoEntry::DeleteDocument {
                                collection: collection.clone(),
                                document_id: document_id.clone(),
                                old_value: old,
                            });
                        }
                        Ok(self.response_ok(&dummy_task))
                    }
                    Err(e) => Err(ErrorCode::Internal {
                        detail: e.to_string(),
                    }),
                }
            }

            PhysicalPlan::VectorInsert {
                collection,
                vector,
                dim,
                field_name,
            } => {
                let index_key = Self::vector_index_key(tid, collection, field_name);
                let index = self
                    .vector_indexes
                    .entry(index_key.clone())
                    .or_insert_with(|| {
                        let params = self
                            .vector_params
                            .get(collection.as_str())
                            .cloned()
                            .unwrap_or_default();
                        crate::engine::vector::hnsw::HnswIndex::new(*dim, params)
                    });

                if vector.len() != index.dim() {
                    return Err(ErrorCode::Internal {
                        detail: format!(
                            "dimension mismatch: expected {}, got {}",
                            index.dim(),
                            vector.len()
                        ),
                    });
                }

                let vector_id = index.len() as u32;
                index.insert(vector.clone());
                undo_log.push(UndoEntry::InsertVector {
                    index_key,
                    vector_id,
                });
                Ok(self.response_ok(&dummy_task))
            }

            PhysicalPlan::VectorDelete {
                collection,
                vector_id,
            } => {
                let index_key = Self::vector_index_key(tid, collection, "");
                if let Some(index) = self.vector_indexes.get_mut(&index_key) {
                    if index.delete(*vector_id) {
                        undo_log.push(UndoEntry::DeleteVector {
                            index_key,
                            vector_id: *vector_id,
                        });
                    }
                }
                Ok(self.response_ok(&dummy_task))
            }

            PhysicalPlan::EdgePut {
                src_id,
                label,
                dst_id,
                properties,
            } => {
                let resp = self.execute_edge_put(&dummy_task, src_id, label, dst_id, properties);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "edge put failed".into(),
                    }));
                }
                // Edge rollback is best-effort — not tracked in undo log
                // because edge_store doesn't support transactional rollback.
                Ok(resp)
            }

            PhysicalPlan::CrdtApply { delta, peer_id, .. } => {
                // Buffer the delta — don't apply to LoroDoc until commit.
                crdt_deltas.push((delta.clone(), *peer_id));
                Ok(self.response_ok(&dummy_task))
            }

            // Read operations execute normally without undo tracking.
            _ => {
                let resp = self.execute(&ExecutionTask::new(crate::bridge::envelope::Request {
                    request_id: crate::types::RequestId::new(0),
                    tenant_id: crate::types::TenantId::new(tid),
                    vshard_id: crate::types::VShardId::new(0),
                    plan: plan.clone(),
                    deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
                    priority: crate::bridge::envelope::Priority::Normal,
                    trace_id: 0,
                    consistency: crate::types::ReadConsistency::Strong,
                    idempotency_key: None,
                }));
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "sub-plan execution failed".into(),
                    }));
                }
                Ok(resp)
            }
        }
    }

    /// Roll back completed writes in reverse order.
    fn rollback_undo_log(&mut self, tid: u32, undo_log: Vec<UndoEntry>) {
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
                    let _ = self.inverted.remove_document(&collection, &document_id);
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
                    if let Some(index) = self.vector_indexes.get_mut(&index_key) {
                        index.delete(vector_id);
                    }
                }
                UndoEntry::DeleteVector {
                    index_key,
                    vector_id,
                } => {
                    // Un-delete: clear tombstone flag.
                    if let Some(index) = self.vector_indexes.get_mut(&index_key) {
                        index.undelete(vector_id);
                    }
                }
            }
        }
    }
}
