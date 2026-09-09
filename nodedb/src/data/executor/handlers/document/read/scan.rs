// SPDX-License-Identifier: BUSL-1.1

//! Document collection scan handler.

use tracing::{debug, warn};

use super::fetch::{DocFetchParams, DocScanMode};
use super::projection::{apply_projection, apply_projection_msgpack};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::document::sort;
use crate::data::executor::response_codec::DocumentRow;
use crate::data::executor::scan_normalize::sparse_row_to_doc;
use crate::data::executor::sparse_body_format::SparseBodyFormatRef;
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::execute_document_scan`].
pub(in crate::data::executor) struct DocumentScanParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub limit: usize,
    pub offset: usize,
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
    pub filters: &'a [u8],
    pub distinct: bool,
    pub projection: &'a [String],
    pub computed_columns_bytes: &'a [u8],
    pub window_functions_bytes: &'a [u8],
    pub mode: DocScanMode,
    pub prefilter: Option<&'a nodedb_types::SurrogateBitmap>,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_document_scan(
        &mut self,
        task: &ExecutionTask,
        params: DocumentScanParams<'_>,
    ) -> Response {
        let DocumentScanParams {
            tid,
            collection,
            limit,
            offset,
            sort_keys,
            filters,
            distinct,
            projection,
            computed_columns_bytes,
            window_functions_bytes,
            mode,
            prefilter,
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            limit,
            offset,
            sort_fields = sort_keys.len(),
            "document scan"
        );

        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let window_specs: Vec<crate::bridge::window_func::WindowFuncSpec> =
            if window_functions_bytes.is_empty() {
                Vec::new()
            } else {
                zerompk::from_msgpack(window_functions_bytes).unwrap_or_default()
            };

        let computed_cols: Vec<crate::bridge::expr_eval::ComputedColumn> =
            if computed_columns_bytes.is_empty() {
                Vec::new()
            } else {
                zerompk::from_msgpack(computed_columns_bytes).unwrap_or_default()
            };

        let scan_budget_bytes = self.query_tuning.max_scan_result_bytes;

        let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filters) {
                Ok(f) => f,
                Err(e) => {
                    warn!(core = self.core_id, error = %e, "failed to parse scan filters");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("malformed scan filters: {e}"),
                        },
                    );
                }
            }
        };

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

        // Fetch stage: the ONLY part that differs between a current-time read
        // and a bitemporal `AS OF` / all-versions audit read. It returns the
        // raw rows plus the schema the downstream should decode them with
        // (`None` for temporal reads, whose bodies are already normalized to
        // MessagePack with any synthetic `_ts_*` columns injected). Everything
        // below runs identically for every mode, giving `AS OF` reads full
        // ORDER BY / computed-column / window-function / DISTINCT parity.
        let fetched = self.document_scan_fetch(
            task,
            tid,
            DocFetchParams {
                collection,
                mode: &mode,
                limit,
                offset,
                filter_predicates: &filter_predicates,
                strict_schema: strict_schema.as_ref(),
                // A sort reorders the rows and DISTINCT removes some, so the
                // first `limit` rows the store returns are not the first
                // `limit` rows of the answer — the fetch cannot stop there.
                full_fetch: !sort_keys.is_empty() || distinct,
            },
        );

        match fetched {
            Ok(fetched) => {
                // Safe point: the storage scan stopped where its deadline
                // passed, so these rows are a prefix of the answer. Emitting
                // them would hand the client a truncated result set that looks
                // complete.
                if fetched.deadline_expired {
                    return self.response_error(task, ErrorCode::DeadlineExceeded);
                }
                let mut filtered = fetched.rows;
                let effective_schema = fetched.effective_schema;
                // The encoding the rows arrive in from the fetch stage. It is
                // NOT the collection's stored encoding: the fetch stage has
                // already normalized a vector-primary collection's tagged
                // sidecars (and every temporal read's bodies) to standard
                // msgpack, and reports a schema only when the bodies it hands
                // back are still Binary Tuples. Re-resolving the collection's
                // stored kind here would decode already-normalized bodies a
                // second time.
                let body_format = SparseBodyFormatRef::from_schema(effective_schema.as_ref());

                if let Some(ref m) = self.metrics {
                    m.record_document_read();
                }

                // Read-your-own-writes for scans: fold this transaction's
                // staging overlay onto the base result before any budget /
                // sort / projection / limit stage, so staged inserts count
                // against the budget and flow through sort+limit unchanged.
                // Only current-version reads merge staged writes — temporal
                // (`AS OF` / all-versions) reads never see the overlay, whose
                // staged bodies are current-version only.
                if mode.is_current()
                    && let Some(txn_id) = task.request.txn_id
                {
                    let coll_key = (
                        task.request.database_id,
                        crate::types::TenantId::new(tid),
                        collection.to_string(),
                    );
                    // `merge_overlay_into_scan` takes an infallible
                    // `Fn(&str, &[u8]) -> bool` predicate, so a
                    // division/modulo-by-zero is captured via this `Cell`
                    // side-channel and checked once the merge returns.
                    let predicate_err: std::cell::Cell<Option<nodedb_query::EvalError>> =
                        std::cell::Cell::new(None);
                    let matches = |doc_id: &str, value: &[u8]| -> bool {
                        if filter_predicates.is_empty() {
                            return true;
                        }
                        match crate::data::executor::core_loop::filter_match::matches_with_resolved_schema(
                            effective_schema.as_ref(),
                            &filter_predicates,
                            doc_id,
                            value,
                        ) {
                            Ok(b) => b,
                            Err(e) => {
                                predicate_err.set(Some(e));
                                false
                            }
                        }
                    };
                    self.merge_overlay_into_scan(txn_id, &coll_key, &mut filtered, &matches);
                    if predicate_err.take().is_some() {
                        return self.response_error(task, ErrorCode::DivisionByZero);
                    }
                }

                // Bound an unbounded fetch by the memory budget. If the
                // materialized result exceeds `max_scan_result_bytes`, surface
                // a deterministic error instead of silently dropping rows. A
                // scan with a `LIMIT n` the fetch could stop at is already
                // row-bounded; one whose rows are reordered or deduplicated
                // downstream had to gather the whole collection and is bounded
                // here instead.
                if (limit == usize::MAX || !sort_keys.is_empty() || distinct)
                    && crate::data::executor::handlers::scan_budget::scan_bytes_exceeded(
                        &filtered,
                        scan_budget_bytes,
                    )
                {
                    return self.response_error(task, ErrorCode::ResourcesExhausted);
                }

                if let Some(pf) = prefilter {
                    filtered.retain(|(doc_id, _)| {
                        if let Ok(n) = u32::from_str_radix(doc_id, 16) {
                            pf.contains(nodedb_types::Surrogate::new(n))
                        } else {
                            false
                        }
                    });
                }

                // Strict collections may store binary tuples. Sort and projection
                // operate on msgpack, so normalize binary tuples here — through
                // the shared converter, which leaves an already-msgpack body
                // borrowed and so costs nothing on the schemaless path.
                let filtered = if !sort_keys.is_empty() || !projection.is_empty() {
                    filtered
                        .into_iter()
                        .map(|(id, bytes)| sparse_row_to_doc(&id, &bytes, body_format))
                        .collect()
                } else {
                    filtered
                };

                let sorted = if sort_keys.is_empty() {
                    filtered
                } else if filtered.len() <= self.query_tuning.sort_run_size {
                    let mut v = filtered;
                    // Propagate the typed error: a zero divisor in a sort key
                    // is a `22012` statement failure, not an internal fault.
                    if let Err(e) = sort::sort_rows(&mut v, sort_keys) {
                        return self.response_error(task, e);
                    }
                    v
                } else {
                    match self.external_sort(filtered, sort_keys, limit.saturating_add(offset)) {
                        Ok(merged) => merged,
                        Err(e) => {
                            warn!(core = self.core_id, error = %e, "external sort failed");
                            // Same typed propagation as the in-memory path: a
                            // sort-key evaluation failure keeps its SQLSTATE
                            // rather than degrading to a generic internal error.
                            return self.response_error(task, e);
                        }
                    }
                };

                // Safe point: the sort is the one post-fetch stage whose cost
                // grows with the row count, and it has just finished. Nothing
                // has been emitted yet, so stopping here costs the client
                // nothing but the error.
                if crate::data::executor::deadline::DeadlineCheck::for_task(task).expired_now() {
                    return self.response_error(task, ErrorCode::DeadlineExceeded);
                }

                let stream_chunk_size = self.query_tuning.stream_chunk_size;

                if effective_schema.is_some() && window_specs.is_empty() {
                    // SQL DISTINCT semantics require deduplication on the
                    // *projected* row, not the raw document bytes — two rows
                    // with the same `category` but different ids/payload are
                    // distinct as documents but the same under
                    // `SELECT DISTINCT category`. Project first, then dedupe.
                    let projected_rows: Vec<_> = match sorted
                        .into_iter()
                        .map(|(doc_id, val)| {
                            let (doc_id, mp) = sparse_row_to_doc(&doc_id, &val, body_format);
                            let projected =
                                apply_projection_msgpack(&mp, &computed_cols, projection)?;
                            Ok((doc_id, projected))
                        })
                        .collect::<crate::Result<Vec<_>>>()
                    {
                        Ok(rows) => rows,
                        Err(e) => return self.response_error(task, e),
                    };
                    let deduped = if distinct {
                        let mut seen = std::collections::HashSet::new();
                        projected_rows
                            .into_iter()
                            .filter(|(_, value)| seen.insert(value.clone()))
                            .collect::<Vec<_>>()
                    } else {
                        projected_rows
                    };
                    let result: Vec<_> = deduped.into_iter().skip(offset).take(limit).collect();
                    return self.send_document_rows_raw(task, &result, stream_chunk_size);
                }

                if !window_specs.is_empty() {
                    // Route through `sparse_row_to_doc`, like every sibling
                    // branch, so a schemaless row with no `id` field carries
                    // its storage-key identity into the window computation.
                    let mut decoded_rows: Vec<(String, serde_json::Value)> = match sorted
                        .into_iter()
                        .map(|(id, val)| {
                            let (doc_id, mp) = sparse_row_to_doc(&id, &val, body_format);
                            crate::data::executor::doc_format::decode_document(&mp)
                                .map(|doc| (doc_id, doc))
                        })
                        .collect::<crate::Result<Vec<_>>>()
                    {
                        Ok(rows) => rows,
                        Err(e) => return self.response_error(task, e),
                    };
                    if let Err(e) = crate::bridge::window_func::evaluate_window_functions(
                        &mut decoded_rows,
                        &window_specs,
                    ) {
                        return self.response_error(task, crate::Error::from(e));
                    }

                    // Project first, then dedupe on the projected JSON value
                    // so `SELECT DISTINCT col` honours SQL semantics.
                    let projected_rows: Vec<_> = match decoded_rows
                        .into_iter()
                        .map(|(doc_id, data)| {
                            let projected = apply_projection(data, &computed_cols, projection)?;
                            Ok(DocumentRow {
                                id: doc_id,
                                data: projected,
                            })
                        })
                        .collect::<crate::Result<Vec<_>>>()
                    {
                        Ok(rows) => rows,
                        Err(e) => return self.response_error(task, e),
                    };

                    let deduped: Vec<_> = if distinct {
                        let mut seen = std::collections::HashSet::new();
                        projected_rows
                            .into_iter()
                            .filter(|row| seen.insert(row.data.to_string()))
                            .collect()
                    } else {
                        projected_rows
                    };

                    let result: Vec<_> = deduped.into_iter().skip(offset).take(limit).collect();
                    self.send_document_rows_transformed(task, &result, stream_chunk_size)
                } else {
                    let needs_transform = !computed_cols.is_empty() || !projection.is_empty();

                    if needs_transform {
                        // Project first so DISTINCT acts on the projected
                        // row, not the raw document.
                        let projected_rows: Vec<_> = match sorted
                            .into_iter()
                            .map(|(doc_id, value)| {
                                let (doc_id, mp) = sparse_row_to_doc(&doc_id, &value, body_format);
                                let projected =
                                    apply_projection_msgpack(&mp, &computed_cols, projection)?;
                                Ok((doc_id, projected))
                            })
                            .collect::<crate::Result<Vec<_>>>()
                        {
                            Ok(rows) => rows,
                            Err(e) => return self.response_error(task, e),
                        };
                        let deduped = if distinct {
                            let mut seen = std::collections::HashSet::new();
                            projected_rows
                                .into_iter()
                                .filter(|(_, value)| seen.insert(value.clone()))
                                .collect()
                        } else {
                            projected_rows
                        };
                        let result: Vec<_> = deduped.into_iter().skip(offset).take(limit).collect();
                        self.send_document_rows_raw(task, &result, stream_chunk_size)
                    } else {
                        // No projection — `SELECT DISTINCT *` semantics dedupe
                        // on the entire raw value, which is what the
                        // pre-existing path does.
                        let deduped = if distinct {
                            let mut seen = std::collections::HashSet::new();
                            sorted
                                .into_iter()
                                .filter(|(_, value)| seen.insert(value.clone()))
                                .collect()
                        } else {
                            sorted
                        };
                        let rows: Vec<_> = deduped.into_iter().skip(offset).take(limit).collect();
                        self.send_document_rows_raw(task, &rows, stream_chunk_size)
                    }
                }
            }
            Err(e) => self.response_error(task, e),
        }
    }
}
