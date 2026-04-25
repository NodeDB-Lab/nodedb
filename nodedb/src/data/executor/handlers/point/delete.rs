//! PointDelete: remove one document plus its cascading side-effects across
//! inverted, secondary, graph, and spatial indexes.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use nodedb_types::Surrogate;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        surrogate: Surrogate,
    ) -> Response {
        let row_key = surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        debug!(core = self.core_id, %collection, %document_id, "point delete");

        // On bitemporal collections: append a doc tombstone + versioned
        // index tombstones for every current field value. `prior` is the
        // pre-delete body so the Event Plane sees `old_value` correctly.
        // Current-state-only indexes (text, graph, spatial, vector) are
        // still cascaded below — they track "what exists now" regardless
        // of bitemporal history.
        let bitemporal = self.is_bitemporal(tid, collection);
        let delete_result: crate::Result<Option<Vec<u8>>> = if bitemporal {
            let prior = self.sparse.versioned_get_current(tid, collection, row_key);
            match prior {
                Ok(Some(body)) => {
                    let sys_from = self.bitemporal_now_ms();
                    let res: crate::Result<()> = (|| {
                        let txn = self.sparse.begin_write()?;
                        self.sparse
                            .versioned_tombstone_in_txn(&txn, tid, collection, row_key, sys_from)?;
                        // Index tombstones: reflect every current value so
                        // `index_lookup_as_of` at or after `sys_from` skips
                        // this doc_id.
                        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
                        if let Some(config) = self.doc_configs.get(&config_key)
                            && let Some(doc) =
                                super::super::super::doc_format::decode_document(&body)
                        {
                            for path in config.index_paths.clone() {
                                for v in crate::engine::document::store::extract_index_values(
                                    &doc,
                                    &path.path,
                                    path.is_array,
                                ) {
                                    let value = if path.case_insensitive {
                                        v.to_lowercase()
                                    } else {
                                        v
                                    };
                                    self.sparse.versioned_index_tombstone_in_txn(
                                        &txn, tid, collection, &path.path, &value, row_key,
                                        sys_from,
                                    )?;
                                }
                            }
                        }
                        txn.commit().map_err(|e| crate::Error::Storage {
                            engine: "sparse".into(),
                            detail: format!("commit: {e}"),
                        })?;
                        Ok(())
                    })();
                    res.map(|()| Some(body))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            }
        } else {
            self.sparse.delete(tid, collection, row_key)
        };

        match delete_result {
            Ok(prior) => {
                // Cascade 1: Remove from full-text inverted index. The
                // inverted index was populated by `apply_point_put` with
                // the substrate row key (hex surrogate), not the
                // user-visible PK — keep the cascade keyed the same way
                // so a delete actually wipes the term postings.
                if let Err(e) = self.inverted.remove_document(
                    crate::types::TenantId::new(tid),
                    collection,
                    row_key,
                ) {
                    warn!(core = self.core_id, %collection, %document_id, error = %e, "inverted index removal failed");
                }

                // Cascade 2: Remove secondary index entries for this document.
                // Secondary indexes use key format "{tenant}:{collection}:{field}:{value}:{doc_id}".
                // We scan and delete all entries ending with this doc_id.
                if let Err(e) = self
                    .sparse
                    .delete_indexes_for_document(tid, collection, row_key)
                {
                    warn!(core = self.core_id, %collection, %document_id, error = %e, "secondary index cascade failed");
                }

                // Cascade 3: Remove graph edges where this document is src or dst.
                let edges_removed = self.csr_partition_mut(tid).remove_node_edges(document_id);
                if edges_removed > 0 {
                    // Also tombstone in persistent edge store.
                    let cascade_ord = self.hlc.next_ordinal();
                    if let Err(e) = self.edge_store.delete_edges_for_node(
                        nodedb_types::TenantId::new(tid),
                        document_id,
                        cascade_ord,
                    ) {
                        warn!(core = self.core_id, %document_id, error = %e, "edge cascade failed");
                    }
                    tracing::trace!(core = self.core_id, %document_id, edges_removed, "EDGE_CASCADE_DELETE");
                }

                // Cascade 4: Remove from spatial R-tree indexes + reverse map.
                // `apply_point_put` hashes the substrate row key as the
                // R-tree entry id, so delete must hash the same key to
                // find the entry. Hashing the user PK would leak ghost
                // bbox entries that survive the row's removal.
                let entry_id = crate::util::fnv1a_hash(row_key.as_bytes());
                let tid_id = crate::types::TenantId::new(tid);
                let spatial_fields: Vec<String> = self
                    .spatial_indexes
                    .keys()
                    .filter(|(t, c, _)| *t == tid_id && c == collection)
                    .map(|(_, _, f)| f.clone())
                    .collect();
                for field in spatial_fields {
                    let skey = (tid_id, collection.to_string(), field.clone());
                    if let Some(rtree) = self.spatial_indexes.get_mut(&skey) {
                        rtree.delete(entry_id);
                    }
                    self.spatial_doc_map
                        .remove(&(tid_id, collection.to_string(), field, entry_id));
                }

                // Record deletion for edge referential integrity.
                self.mark_node_deleted(tid, document_id);

                // Invalidate document cache.
                self.doc_cache.invalidate(tid, collection, row_key);

                self.checkpoint_coordinator.mark_dirty("sparse", 1);

                // Emit delete event to Event Plane if the row actually
                // existed. `sparse.delete` returns the prior bytes — we
                // thread them through so CDC/trigger consumers see the
                // pre-delete state as `old_value`. A delete against a
                // non-existent key is a true no-op and emits nothing.
                if let Some(prior_bytes) = prior.as_deref() {
                    let old_converted = self.resolve_event_payload(tid, collection, prior_bytes);
                    self.emit_write_event(
                        task,
                        collection,
                        crate::event::WriteOp::Delete,
                        document_id,
                        None,
                        Some(old_converted.as_deref().unwrap_or(prior_bytes)),
                    );
                }

                self.response_ok(task)
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
