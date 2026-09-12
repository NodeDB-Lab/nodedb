// SPDX-License-Identifier: BUSL-1.1

//! Document secondary-index maintenance: BackfillIndex, DropIndex.

use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::execute_backfill_index`].
pub(in crate::data::executor) struct BackfillIndexParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub path: &'a str,
    pub is_array: bool,
    pub unique: bool,
    pub case_insensitive: bool,
    pub predicate: Option<&'a str>,
}

impl CoreLoop {
    /// Backfill an index: scan every document in the collection and
    /// populate sparse-index entries for the given field. Atomic — one
    /// write transaction covers the whole backfill and UNIQUE
    /// violations abort it, leaving the index empty (the caller's
    /// Building→Ready flip is skipped, so readers never see a
    /// partial-index view).
    pub(in crate::data::executor) fn execute_backfill_index(
        &mut self,
        task: &ExecutionTask,
        params: BackfillIndexParams<'_>,
    ) -> Response {
        let BackfillIndexParams {
            tid,
            collection,
            path,
            is_array,
            unique,
            case_insensitive,
            predicate,
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            %path,
            unique,
            case_insensitive,
            partial = predicate.is_some(),
            "backfill index"
        );
        if let Some(ref m) = self.metrics {
            m.record_document_index_backfill();
        }

        // Parse the partial-index predicate once, up front. An
        // unparsable predicate is a catalog-level bug — the DDL layer
        // already validates the text at CREATE INDEX time, so a
        // failure here means the stored entry drifted from what the
        // grammar accepts. Refuse the backfill rather than silently
        // over-populating a "partial" index.
        let parsed_predicate = match predicate {
            Some(text) => match crate::engine::document::predicate::IndexPredicate::parse(text) {
                Some(p) => Some(p),
                None => {
                    return self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!(
                                "backfill: partial-index predicate failed to parse: {text}"
                            ),
                        },
                    );
                }
            },
            None => None,
        };

        // Snapshot existing documents outside the write txn. 1,000,000
        // cap matches the Data Plane's other collection-wide scans; rows
        // beyond this are handled by a future chunked backfill (see
        // `scan_documents_chunked`).
        let docs = match self.sparse.scan_documents(
            task.request.database_id.as_u64(),
            tid,
            collection,
            1_000_000,
        ) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: format!("backfill scan: {e}"),
                    },
                );
            }
        };

        // The encoding of these stored bodies is resolved from the collection's
        // registered kind, never sniffed from the bytes: a strict collection
        // stores Binary Tuples and a vector-primary one stores tagged sidecars,
        // and the schemaless MessagePack decoder reads neither. Decoding them
        // all as documents is what made `CREATE INDEX` on a strict collection
        // build an EMPTY index and report success.
        let body_format = self.sparse_body_format(
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection,
        );

        // Deduplicate-unique-as-we-go: track `(normalized_value → doc_id)`
        // so a dup within the existing set is flagged before we ever
        // touch the index table.
        let mut seen: std::collections::HashMap<
            String,
            crate::engine::document::store::StorageKey,
        > = std::collections::HashMap::new();

        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: format!("backfill txn: {e}"),
                    },
                );
            }
        };

        // Index keys are accumulated here and written in one sorted pass after
        // the scan; see the bulk insert below.
        let mut pending_keys: Vec<String> = Vec::with_capacity(docs.len());

        for (doc_id, bytes) in &docs {
            // A row skipped here is a row the finished index permanently omits,
            // and the index is then reported as built — every later lookup on
            // that row's value silently misses it.
            let doc = match decode_scanned_document(bytes, body_format.as_format_ref()) {
                Ok(doc) => doc,
                Err(e) => return self.response_error(task, e),
            };
            // Partial-index predicate: skip rows that don't satisfy
            // the `WHERE` clause. `evaluate` treats NULL / non-bool as
            // false (Postgres partial-index semantics), so only rows
            // for which the predicate is explicitly true are indexed.
            if let Some(ref p) = parsed_predicate
                && !p.evaluate_json(&doc)
            {
                continue;
            }
            let values = crate::engine::document::store::extract_index_values(&doc, path, is_array);
            for raw in values {
                let stored = if case_insensitive {
                    raw.to_lowercase()
                } else {
                    raw
                };
                if unique
                    && let Some(prev) = seen.get(&stored)
                    && prev != doc_id
                {
                    return self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!(
                                "unique index backfill: duplicate value '{stored}' on '{path}' \
                                 (existing '{prev}', new '{doc_id}')"
                            ),
                        },
                    );
                }
                if unique {
                    seen.insert(stored.clone(), *doc_id);
                }
                pending_keys.push(crate::engine::sparse::btree_index::index_key_for(
                    crate::engine::sparse::btree_index::IndexEntryTxn {
                        database_id: task.request.database_id.as_u64(),
                        tenant_id: tid,
                        collection,
                        field: path,
                        value: &stored,
                        document_id: doc_id,
                    },
                ));
            }
        }

        // One sorted pass instead of a descent per value. The scan yields
        // documents in id order while the index is ordered by value, so the
        // per-entry path re-enters the tree at a random position for every key
        // it writes. Buffering the keys and writing them in order turns the
        // whole backfill into a single forward walk.
        if let Err(e) = self.sparse.index_put_sorted_in_txn(&txn, &mut pending_keys) {
            return self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::Internal {
                    detail: format!("backfill index_put: {e}"),
                },
            );
        }

        if let Err(e) = txn.commit() {
            return self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::Internal {
                    detail: format!("backfill commit: {e}"),
                },
            );
        }

        self.response_ok(task)
    }

    /// Drop all secondary index entries for a field across the entire collection.
    ///
    /// Calls `SparseEngine::delete_index_entries_for_field` directly.
    /// Returns `{"removed": N}` as the response payload.
    pub(in crate::data::executor) fn execute_drop_document_index(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field: &str,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %field,
            "drop document index"
        );

        match self.sparse.delete_index_entries_for_field(
            task.request.database_id.as_u64(),
            tid,
            collection,
            field,
        ) {
            Ok(removed) => {
                match super::super::super::response_codec::encode_count("removed", removed) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!("drop index encode: {e}"),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
