// SPDX-License-Identifier: BUSL-1.1

//! Secondary-index lookup / fetch handlers for the document engine.
//!
//! Both `WHERE indexed_field = value` entry points (the native
//! `IndexLookup` op and the pgwire-rewritten `IndexedFetch` op) resolve
//! doc IDs through `DocumentEngine::index_lookup`. Bitemporal collections
//! never populate the plain `INDEXES` table — every secondary-index write
//! lands in the versioned index and every body lives on the versioned
//! document table — so both handlers branch on `self.is_bitemporal(..)`
//! exactly as `execute_document_scan` does: the doc-ID probe goes through
//! the versioned index and the body fetch through `versioned_get_current`.
//! Non-bitemporal collections keep the byte-identical plain
//! `range_scan` + `sparse.get` path.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::filter_match::matches_with_resolved_schema;
use crate::data::executor::task::ExecutionTask;

/// Parameters for `execute_document_indexed_fetch`.
///
/// `filters` is the compound-predicate residual left over after the planner
/// pulls the indexed equality out of the WHERE clause (e.g. the
/// `other_col > 'y'` half of `indexed_col = 'x' AND other_col > 'y'`) — see
/// `nodedb-sql`'s `try_document_index_lookup`. The handler applies it to every
/// fetched body (committed and staged alike), so a row that satisfies the
/// indexed term but fails the residual is excluded exactly as a base scan
/// would exclude it; it is also handed to `merge_overlay_into_index_lookup` so
/// a staged Put failing the residual is never added. `projection` is carried
/// for forward compatibility and not yet applied by the handler.
pub(in crate::data::executor) struct IndexedFetchParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub path: &'a str,
    pub value: &'a str,
    pub filters: &'a [u8],
    pub projection: &'a [String],
    pub limit: usize,
    pub offset: usize,
}

impl CoreLoop {
    /// Execute a secondary index lookup: find all doc IDs where `path = value`.
    ///
    /// Delegates to `DocumentEngine::index_lookup`, which reads the plain
    /// `INDEXES` table for ordinary collections and the versioned index for
    /// bitemporal ones. Returns a JSON array of document IDs as the payload.
    pub(in crate::data::executor) fn execute_document_index_lookup(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        path: &str,
        value: &str,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %path,
            %value,
            "document index lookup"
        );

        let bitemporal = self.is_bitemporal(task.request.database_id.as_u64(), tid, collection);
        let doc_engine = crate::engine::document::store::DocumentEngine::new(
            &self.sparse,
            task.request.database_id.as_u64(),
            tid,
        );
        match doc_engine.index_lookup(collection, path, value, bitemporal) {
            Ok(mut doc_ids) => {
                if let Some(txn_id) = task.request.txn_id {
                    let config_key = (
                        task.request.database_id,
                        crate::types::TenantId::new(tid),
                        collection.to_string(),
                    );
                    let (is_array, case_insensitive) = self.index_path_flags(&config_key, path);
                    let coll_key = (
                        task.request.database_id,
                        crate::types::TenantId::new(tid),
                        collection.to_string(),
                    );
                    self.merge_overlay_into_index_lookup(
                        super::super::transaction::overlay::IndexOverlayMergeParams {
                            txn_id,
                            coll_key: &coll_key,
                            path,
                            value,
                            is_array,
                            case_insensitive,
                            // `DocumentOp::IndexLookup` carries no residual
                            // filters at all (it returns bare doc IDs, used by
                            // bitmap-producer callers) — the empty slice makes
                            // the merge's residual re-check a no-op.
                            residual: &[],
                            strict_schema: None,
                        },
                        &mut doc_ids,
                        &|body| self.decode_indexed_body(&config_key, body),
                    );
                }
                let payload = serde_json::json!(doc_ids);
                match sonic_rs::to_vec(&payload) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("index lookup encode: {e}"),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Execute a SELECT rewritten as a secondary-index fetch.
    ///
    /// Resolves doc IDs through `DocumentEngine::index_lookup`, fetches each
    /// document's raw msgpack bytes, applies `offset`/`limit`, and emits rows
    /// via `encode_raw_document_rows` — the same wire format as a document
    /// scan — so the pgwire decoder doesn't need a special case.
    ///
    /// Bitemporal collections resolve doc IDs through the versioned index and
    /// bodies through `versioned_get_current` (which hides tombstoned /
    /// superseded values); ordinary collections use the plain index and
    /// `sparse.get`.
    ///
    /// The compound-predicate residual (`filters`) IS applied to each fetched
    /// body — committed or staged — so a row matching the indexed term but
    /// failing the leftover conjuncts is excluded like a base scan would.
    /// Projection is not yet applied here; the planner falls back to a full
    /// scan for cases this handler doesn't cover (sort/distinct/window).
    pub(in crate::data::executor) fn execute_document_indexed_fetch(
        &mut self,
        task: &ExecutionTask,
        params: IndexedFetchParams<'_>,
    ) -> Response {
        let IndexedFetchParams {
            tid,
            collection,
            path,
            value,
            filters,
            projection: _projection,
            limit,
            offset,
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            %path,
            %value,
            limit,
            offset,
            "document indexed fetch"
        );

        let database_id = task.request.database_id.as_u64();
        let bitemporal = self.is_bitemporal(database_id, tid, collection);
        let doc_engine =
            crate::engine::document::store::DocumentEngine::new(&self.sparse, database_id, tid);
        let mut doc_ids = match doc_engine.index_lookup(collection, path, value, bitemporal) {
            Ok(ids) => ids,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("indexed fetch: {e}"),
                    },
                );
            }
        };

        // Strict collections store Binary Tuple bytes; the response codec
        // expects msgpack maps. Decode-then-encode here so cross-engine
        // result framing (encode_raw_document_rows) sees valid msgpack.
        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        // Read-your-own-writes for the index-lookup path: the INDEXES table
        // (plain or versioned) is never staged, only body storage is, so a
        // staged insert/update/delete that changes `path == value` is
        // invisible to `doc_engine.index_lookup` above. Decode each
        // candidate's staged body and re-extract `path` the same way the
        // index write path does, dropping doc IDs the staged write moved
        // off the value and adding doc IDs it moved onto it.
        let coll_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        // The residual: the WHERE conjuncts left over after the planner pulled
        // out the indexed equality (e.g. the `other_col > 'y'` half of
        // `indexed_col = 'x' AND other_col > 'y'`) — see `nodedb-sql`'s
        // `try_document_index_lookup`. It is applied to EVERY fetched body
        // below — committed or staged — so a row that satisfies the indexed
        // term but not the residual is excluded exactly like a base scan would
        // exclude it. It is also handed to the overlay merge so a staged Put
        // failing the residual is never even added.
        let residual: Vec<ScanFilter> = if filters.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filters) {
                Ok(f) => f,
                Err(e) => {
                    warn!(core = self.core_id, error = %e, "failed to parse indexed-fetch residual filters");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("malformed indexed-fetch filters: {e}"),
                        },
                    );
                }
            }
        };
        if let Some(txn_id) = task.request.txn_id {
            let (is_array, case_insensitive) = self.index_path_flags(&config_key, path);
            self.merge_overlay_into_index_lookup(
                super::super::transaction::overlay::IndexOverlayMergeParams {
                    txn_id,
                    coll_key: &coll_key,
                    path,
                    value,
                    is_array,
                    case_insensitive,
                    residual: &residual,
                    strict_schema: strict_schema.as_ref(),
                },
                &mut doc_ids,
                &|body| self.decode_indexed_body(&config_key, body),
            );
        }

        let mut rows: Vec<(String, Vec<u8>)> = Vec::new();
        for doc_id in doc_ids.iter().skip(offset).take(limit) {
            // Bitemporal collections keep the current body on the versioned
            // document table; the plain DOCUMENTS table is empty for them.
            // A doc ID the overlay merge added or superseded above has no
            // correct body in base storage — `overlay_or_base_body` prefers
            // the staged `Put` bytes and only falls back to a base fetch
            // when the overlay has nothing staged for this surrogate.
            let fetched = self.overlay_or_base_body(task.request.txn_id, &coll_key, doc_id, || {
                if bitemporal {
                    self.sparse
                        .versioned_get_current(database_id, tid, collection, doc_id)
                } else {
                    self.sparse.get(database_id, tid, collection, doc_id)
                }
            });
            match fetched {
                Ok(Some(bytes)) => {
                    // A row matching the indexed term but failing the residual
                    // (the leftover compound-predicate conjuncts) must be
                    // excluded — checked on the raw stored body, exactly as a
                    // base scan applies its filters, for committed and staged
                    // bodies alike.
                    if !residual.is_empty()
                        && !matches_with_resolved_schema(strict_schema.as_ref(), &residual, &bytes)
                    {
                        continue;
                    }
                    let payload = if let Some(ref schema) = strict_schema {
                        match super::super::super::strict_format::binary_tuple_to_msgpack(
                            &bytes, schema,
                        ) {
                            Some(mp) => mp,
                            None => bytes,
                        }
                    } else {
                        bytes
                    };
                    rows.push((doc_id.clone(), payload));
                }
                Ok(None) => {
                    // Index entry pointed at a deleted doc — skip, don't
                    // fail. A future compaction will purge the orphan.
                }
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("fetch doc {doc_id}: {e}"),
                        },
                    );
                }
            }
        }

        match super::super::super::response_codec::encode_raw_document_rows(&rows) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("indexed fetch encode: {e}"),
                },
            ),
        }
    }
}
