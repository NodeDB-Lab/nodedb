//! Per-sub-plan execution within a transaction batch.

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::bridge::physical_plan::{CrdtOp, DocumentOp, GraphOp, MetaOp, VectorOp};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::undo::UndoEntry;

impl CoreLoop {
    /// Execute a single sub-plan within a transaction, recording undo info.
    ///
    /// CRDT deltas are NOT applied immediately — they are buffered in
    /// `crdt_deltas` and only applied after all sub-plans succeed.
    #[allow(clippy::too_many_lines)]
    pub(super) fn execute_tx_sub_plan(
        &mut self,
        tid: u32,
        plan: &PhysicalPlan,
        undo_log: &mut Vec<UndoEntry>,
        crdt_deltas: &mut Vec<(Vec<u8>, u64)>,
        user_roles: &[String],
    ) -> Result<Response, ErrorCode> {
        // Create a temporary task for sub-plan response construction.
        let dummy_task = ExecutionTask::new(crate::bridge::envelope::Request {
            request_id: crate::types::RequestId::new(0),
            tenant_id: crate::types::TenantId::new(tid),
            vshard_id: crate::types::VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Cancel {
                target_request_id: crate::types::RequestId::new(0),
            }),
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
            priority: crate::bridge::envelope::Priority::Normal,
            trace_id: 0,
            consistency: crate::types::ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
        });

        match plan {
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection,
                document_id,
                value,
                ..
            }) => self.tx_point_put(
                &dummy_task,
                tid,
                collection,
                document_id,
                value,
                undo_log,
                user_roles,
            ),

            PhysicalPlan::Document(DocumentOp::PointInsert {
                collection,
                document_id,
                value,
                if_absent,
                surrogate: _,
            }) => {
                // Existence probe against the engine's current state. The
                // transaction buffer is represented by `undo_log`: earlier
                // sub-plans in this same BEGIN block that wrote to
                // `document_id` show up via `sparse.get` because we apply
                // writes eagerly and undo on rollback. Same-tx duplicate
                // keys therefore surface here too.
                let exists = self
                    .sparse
                    .get(tid, collection, document_id)
                    .ok()
                    .flatten()
                    .is_some();
                if exists {
                    if *if_absent {
                        return Ok(self.response_ok(&dummy_task));
                    }
                    return Err(ErrorCode::RejectedConstraint {
                        constraint: "unique".to_string(),
                        detail: format!(
                            "duplicate key value '{document_id}' violates primary-key \
                             uniqueness on '{collection}'"
                        ),
                    });
                }
                self.tx_point_put(
                    &dummy_task,
                    tid,
                    collection,
                    document_id,
                    value,
                    undo_log,
                    user_roles,
                )
            }

            PhysicalPlan::Document(DocumentOp::PointDelete {
                collection,
                document_id,
                ..
            }) => self.tx_point_delete(&dummy_task, tid, collection, document_id, undo_log),

            PhysicalPlan::Vector(VectorOp::Insert {
                collection,
                vector,
                dim,
                field_name,
                surrogate,
            }) => {
                let index_key = Self::vector_index_key(tid, collection, field_name);
                let params = self
                    .vector_params
                    .get(&index_key)
                    .cloned()
                    .unwrap_or_default();
                let index = self
                    .vector_collections
                    .entry(index_key.clone())
                    .or_insert_with(|| {
                        crate::engine::vector::collection::VectorCollection::new(*dim, params)
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
                index.insert_with_surrogate(vector.clone(), *surrogate);
                undo_log.push(UndoEntry::InsertVector {
                    index_key,
                    vector_id,
                });
                Ok(self.response_ok(&dummy_task))
            }

            PhysicalPlan::Vector(VectorOp::Delete {
                collection,
                vector_id,
            }) => {
                let index_key = Self::vector_index_key(tid, collection, "");
                if let Some(index) = self.vector_collections.get_mut(&index_key)
                    && index.delete(*vector_id)
                {
                    undo_log.push(UndoEntry::DeleteVector {
                        index_key,
                        vector_id: *vector_id,
                    });
                }
                Ok(self.response_ok(&dummy_task))
            }

            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection,
                src_id,
                label,
                dst_id,
                properties,
                src_surrogate,
                dst_surrogate,
            }) => {
                // Capture old properties for rollback before writing.
                let old_properties = self
                    .edge_store
                    .get_edge(
                        nodedb_types::TenantId::new(tid),
                        collection,
                        src_id,
                        label,
                        dst_id,
                    )
                    .ok()
                    .flatten();

                let resp = self.execute_edge_put(
                    &dummy_task,
                    tid,
                    collection,
                    src_id,
                    label,
                    dst_id,
                    properties,
                    *src_surrogate,
                    *dst_surrogate,
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "edge put failed".into(),
                    }));
                }

                undo_log.push(UndoEntry::PutEdge {
                    collection: collection.clone(),
                    src_id: src_id.clone(),
                    label: label.clone(),
                    dst_id: dst_id.clone(),
                    old_properties,
                });
                Ok(resp)
            }

            PhysicalPlan::Graph(GraphOp::EdgeDelete {
                collection,
                src_id,
                label,
                dst_id,
            }) => {
                // Capture old properties for rollback before deleting.
                let old_properties = self
                    .edge_store
                    .get_edge(
                        nodedb_types::TenantId::new(tid),
                        collection,
                        src_id,
                        label,
                        dst_id,
                    )
                    .ok()
                    .flatten();

                let resp =
                    self.execute_edge_delete(&dummy_task, tid, collection, src_id, label, dst_id);
                if resp.status == Status::Error {
                    return Err(resp.error_code.unwrap_or(ErrorCode::Internal {
                        detail: "edge delete failed".into(),
                    }));
                }

                // Only track undo if the edge actually existed.
                if let Some(props) = old_properties {
                    undo_log.push(UndoEntry::DeleteEdge {
                        collection: collection.clone(),
                        src_id: src_id.clone(),
                        label: label.clone(),
                        dst_id: dst_id.clone(),
                        old_properties: props,
                    });
                }
                Ok(resp)
            }

            PhysicalPlan::Crdt(CrdtOp::Apply { delta, peer_id, .. }) => {
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
                    event_source: crate::event::EventSource::User,
                    user_roles: Vec::new(),
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

    /// Execute a PointPut within a transaction.
    #[allow(clippy::too_many_arguments)]
    fn tx_point_put(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        value: &[u8],
        undo_log: &mut Vec<UndoEntry>,
        user_roles: &[String],
    ) -> Result<Response, ErrorCode> {
        // Save old value for rollback.
        let old_value = self.sparse.get(tid, collection, document_id).ok().flatten();

        // Enforcement: append-only + period lock + state transitions + transition checks.
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        if let Some(config) = self.doc_configs.get(&config_key) {
            super::super::super::enforcement::append_only::check_point_put(
                collection,
                &config.enforcement,
                &old_value,
            )?;
            // Period lock: check if the period of this document is open.
            if let Some(ref pl) = config.enforcement.period_lock {
                super::super::super::enforcement::period_lock::check_period_lock(
                    &self.sparse,
                    tid,
                    collection,
                    value,
                    pl,
                )?;
            }
            // State transitions + transition checks: only on UPDATE (old exists).
            if old_value.is_some() {
                let old_json = old_value
                    .as_ref()
                    .and_then(|b| super::super::super::doc_format::decode_document(b));
                let new_json = super::super::super::doc_format::decode_document(value);
                if let (Some(old_doc), Some(new_doc)) = (&old_json, &new_json) {
                    if !config.enforcement.state_constraints.is_empty() {
                        super::super::super::enforcement::state_transition::check_state_transitions(
                            collection,
                            &config.enforcement.state_constraints,
                            old_doc,
                            new_doc,
                            user_roles,
                        )?;
                    }
                    if !config.enforcement.transition_checks.is_empty() {
                        super::super::super::enforcement::transition_check::check_transition_predicates(
                            collection,
                            &config.enforcement.transition_checks,
                            old_doc,
                            new_doc,
                        )?;
                    }
                }
            }
        }

        // Encode value for storage — strict collections go through Binary Tuple encoding
        // (with unknown-field rejection + type coercion), schemaless through canonicalize.
        let encode_for_storage = |bytes: &[u8]| -> Result<Vec<u8>, ErrorCode> {
            if let Some(config) = self.doc_configs.get(&config_key)
                && let crate::bridge::physical_plan::StorageMode::Strict { ref schema } =
                    config.storage_mode
            {
                super::super::super::strict_format::bytes_to_binary_tuple(bytes, schema).map_err(
                    |e| ErrorCode::Internal {
                        detail: format!("strict encode: {e}"),
                    },
                )
            } else {
                Ok(super::super::super::doc_format::canonicalize_document_for_storage(bytes))
            }
        };

        // Hash chain: on INSERT, compute chain hash and inject _chain_hash field.
        let stored = if old_value.is_none() {
            let config_key_hc = (crate::types::TenantId::new(tid), collection.to_string());
            let hash_chain_enabled = self
                .doc_configs
                .get(&config_key_hc)
                .is_some_and(|c| c.enforcement.hash_chain);
            match super::super::super::enforcement::hash_chain::apply_chain_on_insert(
                &mut self.chain_hashes,
                tid,
                collection,
                document_id,
                value,
                hash_chain_enabled,
            ) {
                Some(chained) => chained,
                None => encode_for_storage(value)?,
            }
        } else {
            encode_for_storage(value)?
        };
        match self.sparse.put(tid, collection, document_id, &stored) {
            Ok(_prior) => {
                // Auto-index text fields (includes nested block content).
                if let Some(doc) = super::super::super::doc_format::decode_document(value) {
                    let text_content = super::super::document::extract_indexable_text(&doc);
                    if !text_content.is_empty() {
                        let _ = self.inverted.index_document(
                            crate::types::TenantId::new(tid),
                            collection,
                            document_id,
                            &text_content,
                        );
                    }
                }

                undo_log.push(UndoEntry::PutDocument {
                    collection: collection.to_string(),
                    document_id: document_id.to_string(),
                    old_value: old_value.clone(),
                });

                // Materialized sum trigger: on INSERT (not UPDATE).
                if old_value.is_none() {
                    let config_key = (crate::types::TenantId::new(tid), collection.to_string());
                    if let Some(config) = self.doc_configs.get(&config_key)
                        && !config.enforcement.materialized_sum_sources.is_empty()
                        && let Some(src_doc) =
                            super::super::super::doc_format::decode_document(value)
                    {
                        let target_writes =
                            super::super::super::enforcement::materialized_sum::apply_materialized_sums(
                                &self.sparse,
                                tid,
                                &config.enforcement.materialized_sum_sources,
                                &src_doc,
                            )?;
                        for tw in target_writes {
                            undo_log.push(UndoEntry::PutDocument {
                                collection: tw.collection,
                                document_id: tw.document_id,
                                old_value: tw.old_value,
                            });
                        }
                    }
                }

                Ok(self.response_ok(dummy_task))
            }
            Err(e) => Err(ErrorCode::Internal {
                detail: e.to_string(),
            }),
        }
    }

    /// Execute a PointDelete within a transaction.
    fn tx_point_delete(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        // Enforcement: append-only + period lock + retention/hold.
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        let old_value = self.sparse.get(tid, collection, document_id).ok().flatten();
        if let Some(config) = self.doc_configs.get(&config_key) {
            super::super::super::enforcement::append_only::check_point_delete(
                collection,
                &config.enforcement,
            )?;
            // Period lock on the existing document.
            if let Some(ref pl) = config.enforcement.period_lock
                && let Some(ref old_bytes) = old_value
            {
                super::super::super::enforcement::period_lock::check_period_lock(
                    &self.sparse,
                    tid,
                    collection,
                    old_bytes,
                    pl,
                )?;
            }
            // Retention and legal hold.
            let created_at = old_value.as_ref().and_then(|b| {
                super::super::super::enforcement::retention::extract_created_at_secs(b)
            });
            super::super::super::enforcement::retention::check_delete_allowed(
                collection,
                &config.enforcement,
                created_at,
            )?;
        }
        match self.sparse.delete(tid, collection, document_id) {
            Ok(_) => {
                // Cascade: inverted index, secondary indexes, graph edges.
                let _ = self.inverted.remove_document(
                    crate::types::TenantId::new(tid),
                    collection,
                    document_id,
                );
                let _ = self
                    .sparse
                    .delete_indexes_for_document(tid, collection, document_id);
                let edges_removed = self.csr_partition_mut(tid).remove_node_edges(document_id);
                if edges_removed > 0 {
                    let cascade_ord = self.hlc.next_ordinal();
                    let _ = self.edge_store.delete_edges_for_node(
                        nodedb_types::TenantId::new(tid),
                        document_id,
                        cascade_ord,
                    );
                }

                if let Some(old) = old_value {
                    undo_log.push(UndoEntry::DeleteDocument {
                        collection: collection.to_string(),
                        document_id: document_id.to_string(),
                        old_value: old,
                    });
                }
                Ok(self.response_ok(dummy_task))
            }
            Err(e) => Err(ErrorCode::Internal {
                detail: e.to_string(),
            }),
        }
    }
}
