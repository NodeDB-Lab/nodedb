// SPDX-License-Identifier: BUSL-1.1

//! Row-fetch stage for the document scan pipeline.
//!
//! This is the ONLY stage that differs between a current-time read and a
//! bitemporal `AS OF SYSTEM TIME` / `AS OF VALID TIME` / all-versions audit
//! read. Every post-fetch transform — sort, window functions, computed
//! columns, projection, `DISTINCT` — is shared downstream in
//! [`super::scan`], so temporal reads gain full parity with current-time reads
//! instead of routing through a stunted handler that dropped ordering,
//! computed columns and window functions.
//!
//! A fetch produces the raw rows plus the schema the downstream should decode
//! them with:
//! - **Current**: bodies in their stored encoding (Binary Tuple for strict,
//!   MessagePack/legacy-JSON for schemaless), paired with the collection's real
//!   strict schema; the downstream normalizes as needed.
//! - **AsOf / AllVersions**: bodies already normalized to MessagePack (with the
//!   synthetic `_ts_*` temporal columns injected for the audit case) so
//!   `effective_schema` is `None` and the shared sort/window/computed/projection
//!   pipeline operates on a uniform shape.

use std::cell::Cell;

use tracing::warn;

use nodedb_types::StorageKey;

use super::audit_body::{inject_temporal_columns, strict_audit_body};
use super::fetch_types::Fetched;
pub(in crate::data::executor) use super::fetch_types::{
    DocFetchParams, DocScanMode, FetchedRows, RowOrigin,
};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::filter_match::matches_with_resolved_schema;
use crate::data::executor::scan_normalize::{sparse_body_to_msgpack, sparse_row_to_doc};
use crate::data::executor::sparse_body_format::{SparseBodyFormat, SparseBodyFormatRef};
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Fetch the raw rows for a document scan according to `mode`, feeding the
    /// shared downstream shaping pipeline in [`super::scan`].
    pub(in crate::data::executor) fn document_scan_fetch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: DocFetchParams<'_>,
    ) -> crate::Result<FetchedRows> {
        let collection = params.collection;
        let offset = params.offset;
        let filter_predicates = params.filter_predicates;
        let strict_schema = params.strict_schema;
        let scan_limit = self.effective_fetch_limit(params.limit, offset, params.full_fetch);

        // Safe point: the storage scans below consult this once per scanned
        // row. A statement that goes over while the scan runs stops where it
        // stands, and `deadline_expired` tells the caller the rows are a
        // prefix, not an answer.
        let deadline = crate::data::executor::deadline::DeadlineCheck::for_task(task);
        let stop = || deadline.expired();

        match params.mode {
            DocScanMode::Current => self.fetch_current(task, tid, &params, &deadline),
            DocScanMode::AsOf {
                system_as_of_ms,
                valid_at_ms,
            } => {
                // `versioned_scan_as_of` returns each version's stored body
                // verbatim — strict bodies are Binary Tuples, schemaless bodies
                // may be legacy JSON. Normalize to standard MessagePack so the
                // shared sort/window/computed/projection pipeline (which scans
                // msgpack) operates uniformly, then hand it downstream with no
                // schema (bodies are already normalized).
                // `versioned_scan_as_of` takes an infallible
                // `Fn(&str, &[u8]) -> bool` predicate (a storage-engine
                // primitive out of scope for this fix), so a
                // division/modulo-by-zero is captured via this `Cell`
                // side-channel and checked once the scan returns, rather
                // than silently folded away.
                let predicate_err: Cell<Option<nodedb_query::EvalError>> = Cell::new(None);
                let predicate = |doc_id: &str, body: &[u8]| match matches_with_resolved_schema(
                    strict_schema,
                    filter_predicates,
                    doc_id,
                    body,
                ) {
                    Ok(b) => b,
                    Err(e) => {
                        predicate_err.set(Some(e));
                        false
                    }
                };
                let raw = self.sparse.versioned_scan_as_of(
                    crate::engine::sparse::btree_versioned::VersionedScanParams {
                        database_id: task.request.database_id.as_u64(),
                        tenant: tid,
                        coll: collection,
                        sys_cutoff_ms: *system_as_of_ms,
                        valid_at_ms: *valid_at_ms,
                        limit: scan_limit,
                    },
                    &predicate,
                    &stop,
                )?;
                if let Some(e) = predicate_err.take() {
                    return Err(crate::Error::from(e));
                }
                let rows = raw
                    .into_iter()
                    .map(|(doc_id, body)| {
                        // A temporal read's bodies are strict Binary Tuples or
                        // schemaless (possibly legacy-JSON) document bodies —
                        // never sidecars, which the vector-primary branch
                        // handles on its own — so the schema is the whole
                        // question, and the shared converter answers it.
                        let mp = sparse_body_to_msgpack(
                            &body,
                            SparseBodyFormatRef::from_schema(strict_schema),
                        )
                        .into_owned();
                        (doc_id, mp)
                    })
                    .collect();
                Ok(FetchedRows {
                    rows,
                    origin: RowOrigin::Sparse,
                    effective_schema: None,
                    deadline_expired: deadline.tripped(),
                })
            }
            DocScanMode::AllVersions { valid_at_ms } => {
                // Every system-time version of every document. Each version is
                // normalized to MessagePack and gets the synthetic `_ts_*`
                // temporal columns injected BEFORE the shared downstream runs,
                // so a user can `SELECT` / `ORDER BY` / project on them.
                // See the `AsOf` arm above for the `Cell` side-channel rationale.
                let predicate_err: Cell<Option<nodedb_query::EvalError>> = Cell::new(None);
                let predicate = |doc_id: &str, body: &[u8]| match matches_with_resolved_schema(
                    strict_schema,
                    filter_predicates,
                    doc_id,
                    body,
                ) {
                    Ok(b) => b,
                    Err(e) => {
                        predicate_err.set(Some(e));
                        false
                    }
                };
                let raw = self.sparse.versioned_scan_all(
                    crate::engine::sparse::btree_versioned::VersionedScanParams {
                        database_id: task.request.database_id.as_u64(),
                        tenant: tid,
                        coll: collection,
                        sys_cutoff_ms: None,
                        valid_at_ms: *valid_at_ms,
                        limit: scan_limit,
                    },
                    &predicate,
                    &stop,
                )?;
                if let Some(e) = predicate_err.take() {
                    return Err(crate::Error::from(e));
                }
                let mut rows: Vec<(String, Vec<u8>)> = Vec::with_capacity(raw.len());
                for row in raw {
                    let msgpack_body = match strict_schema {
                        Some(schema) => strict_audit_body(&row.body, schema)?,
                        None => row.body,
                    };
                    let with_ts = inject_temporal_columns(
                        &msgpack_body,
                        row.system_from_ms,
                        row.valid_from_ms,
                        row.valid_until_ms,
                    )?;
                    rows.push((row.doc_id, with_ts));
                }
                Ok(FetchedRows {
                    rows,
                    origin: RowOrigin::Sparse,
                    effective_schema: None,
                    deadline_expired: deadline.tripped(),
                })
            }
        }
    }

    /// The row ceiling the storage scan is allowed to stop at.
    ///
    /// A `full_fetch` scan is treated as unbounded here — its rows are
    /// reordered or deduplicated downstream, so stopping at `limit` would cut
    /// the wrong ones — and is bounded by the memory budget instead.
    fn effective_fetch_limit(&self, limit: usize, offset: usize, full_fetch: bool) -> usize {
        let requested = if full_fetch { usize::MAX } else { limit };
        crate::data::executor::handlers::scan_budget::fetch_limit_for(
            requested,
            offset,
            self.query_tuning.max_scan_result_bytes,
        )
    }

    /// Newest live version per document (current-time read). Bitemporal
    /// collections read current state from the versioned store; plain
    /// collections from the live table with a `scan_collection` fallback.
    fn fetch_current(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: &DocFetchParams<'_>,
        deadline: &crate::data::executor::deadline::DeadlineCheck,
    ) -> crate::Result<FetchedRows> {
        let stop = || deadline.expired();
        let collection = params.collection;
        let filter_predicates = params.filter_predicates;
        let strict_schema = params.strict_schema;

        let fetch_limit =
            self.effective_fetch_limit(params.limit, params.offset, params.full_fetch);
        let database_id = task.request.database_id.as_u64();
        let bitemporal = self.is_bitemporal(database_id, tid, collection);
        // Resolved from the collection's registered kind, never from the bytes:
        // a tagged sidecar and a plain document body are both valid MessagePack
        // maps with the same header, so sniffing necessarily mis-reads one.
        let is_vector_sidecar = matches!(
            self.sparse_body_format(
                crate::types::DatabaseId::new(database_id),
                crate::types::TenantId::new(tid),
                collection,
            ),
            SparseBodyFormat::VectorSidecar
        );

        // `scan_documents_filtered`/`versioned_scan_as_of`/`scan_collection`
        // take an infallible `Fn(&str, &[u8]) -> bool` predicate (a
        // storage-engine primitive out of scope for this fix), so a
        // division/modulo-by-zero is captured via this `Cell` side-channel
        // and checked once every branch below returns, rather than silently
        // folded away.
        let predicate_err: Cell<Option<nodedb_query::EvalError>> = Cell::new(None);
        let matches = |doc_id: &str, value: &[u8]| -> bool {
            if filter_predicates.is_empty() {
                return true;
            }
            // Filters read fields out of a standard msgpack map, so a sidecar
            // must be normalized BEFORE evaluation. Pushing a predicate at the
            // stored tagged bytes matches nothing, which reads as "no rows"
            // rather than as an error.
            let normalized;
            let value = if is_vector_sidecar {
                normalized = sparse_body_to_msgpack(value, SparseBodyFormatRef::VectorSidecar);
                &*normalized
            } else {
                value
            };
            match matches_with_resolved_schema(strict_schema, filter_predicates, doc_id, value) {
                Ok(b) => b,
                Err(e) => {
                    predicate_err.set(Some(e));
                    false
                }
            }
        };
        // `scan_documents_filtered` hands the predicate a typed `StorageKey`;
        // `matches` (and `versioned_scan_as_of`'s predicate, and the
        // `scan_collection` fallback filter) still take the row's storage
        // key as text, so this renders it once per candidate row.
        let matches_by_key = |key: &StorageKey, value: &[u8]| matches(&key.to_string(), value);

        // `versioned_scan_as_of` hands back a rendered storage key, parsed
        // back ONCE here rather than carried as text and re-parsed later. A
        // shape that fails to parse names a fetch-pipeline bug, not a row to
        // skip.
        let parse_row_key = |id: String, body: Vec<u8>| -> crate::Result<(StorageKey, Vec<u8>)> {
            let key = StorageKey::parse(&id).ok_or_else(|| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!(
                    "collection '{collection}' fetched a row whose id is not a valid storage key: '{id}'"
                ),
            })?;
            Ok((key, body))
        };

        let rows: Fetched = if filter_predicates.is_empty() {
            if bitemporal {
                Fetched::Sparse(
                    self.sparse
                        .versioned_scan_as_of(
                            crate::engine::sparse::btree_versioned::VersionedScanParams {
                                database_id,
                                tenant: tid,
                                coll: collection,
                                sys_cutoff_ms: None,
                                valid_at_ms: None,
                                limit: fetch_limit,
                            },
                            &|_, _| true,
                            &stop,
                        )?
                        .into_iter()
                        .map(|(id, body)| parse_row_key(id, body))
                        .collect::<crate::Result<Vec<_>>>()?,
                )
            } else {
                // Routed through the filtered scan with an always-true
                // predicate rather than `scan_documents`: the unfiltered full
                // scan is the longest-running read shape there is, and only
                // this entry point takes the stop signal. Rows, order and the
                // `limit` cutoff are identical.
                let sparse_result = self.sparse.scan_documents_filtered(
                    database_id,
                    tid,
                    collection,
                    fetch_limit,
                    &|_: &StorageKey, _: &[u8]| true,
                    &stop,
                );
                match sparse_result {
                    Ok(docs) if docs.is_empty() => {
                        let fallback =
                            self.scan_collection(database_id, tid, collection, fetch_limit)?;
                        if !fallback.is_empty() {
                            warn!(
                                core = self.core_id,
                                %collection,
                                count = fallback.len(),
                                "document scan fallback to scan_collection"
                            );
                        }
                        Fetched::Foreign(fallback)
                    }
                    other => Fetched::Sparse(other?),
                }
            }
        } else if strict_schema.is_some() {
            if bitemporal {
                Fetched::Sparse(
                    self.sparse
                        .versioned_scan_as_of(
                            crate::engine::sparse::btree_versioned::VersionedScanParams {
                                database_id,
                                tenant: tid,
                                coll: collection,
                                sys_cutoff_ms: None,
                                valid_at_ms: None,
                                limit: fetch_limit,
                            },
                            &matches,
                            &stop,
                        )?
                        .into_iter()
                        .map(|(id, body)| parse_row_key(id, body))
                        .collect::<crate::Result<Vec<_>>>()?,
                )
            } else {
                Fetched::Sparse(self.sparse.scan_documents_filtered(
                    database_id,
                    tid,
                    collection,
                    fetch_limit,
                    &matches_by_key,
                    &stop,
                )?)
            }
        } else if bitemporal {
            Fetched::Sparse(
                self.sparse
                    .versioned_scan_as_of(
                        crate::engine::sparse::btree_versioned::VersionedScanParams {
                            database_id,
                            tenant: tid,
                            coll: collection,
                            sys_cutoff_ms: None,
                            valid_at_ms: None,
                            limit: fetch_limit,
                        },
                        &matches,
                        &stop,
                    )?
                    .into_iter()
                    .map(|(id, body)| parse_row_key(id, body))
                    .collect::<crate::Result<Vec<_>>>()?,
            )
        } else {
            let sparse_result = self.sparse.scan_documents_filtered(
                database_id,
                tid,
                collection,
                fetch_limit,
                &matches_by_key,
                &stop,
            );
            match sparse_result {
                Ok(docs) if docs.is_empty() => Fetched::Foreign(
                    self.scan_collection(database_id, tid, collection, fetch_limit)?
                        .into_iter()
                        .filter(|(id, data)| matches(id, data))
                        .collect(),
                ),
                other => Fetched::Sparse(other?),
            }
        };

        if let Some(e) = predicate_err.take() {
            return Err(crate::Error::from(e));
        }

        // A vector-primary collection's sparse rows are `zerompk` TAGGED
        // metadata sidecars, not document bodies. Normalize them here, at the
        // one point where this handler's raw sparse bytes become "rows", so
        // every downstream transform — sort, window functions, computed
        // columns, projection, DISTINCT — sees the same standard-msgpack shape
        // it sees for every other collection. Without it the tagged values pass
        // through untouched and reach the client as `[4,"alice"]`. The key
        // stays typed from the scan above, so this needs no re-parse.
        let (rows, origin): (Vec<(String, Vec<u8>)>, RowOrigin) = match rows {
            Fetched::Sparse(rows) if is_vector_sidecar => (
                rows.into_iter()
                    .map(|(key, body)| {
                        sparse_row_to_doc(&key, &body, SparseBodyFormatRef::VectorSidecar)
                    })
                    .collect(),
                RowOrigin::Sparse,
            ),
            Fetched::Sparse(rows) => (
                rows.into_iter()
                    .map(|(key, body)| (key.to_string(), body))
                    .collect(),
                RowOrigin::Sparse,
            ),
            // An empty fallback proves nothing about which engine owns the
            // collection, and the rows a transaction overlay merges in later
            // are sparse-shaped, so an empty fetch reports the sparse origin.
            Fetched::Foreign(rows) if rows.is_empty() => (rows, RowOrigin::Sparse),
            Fetched::Foreign(rows) => (rows, RowOrigin::Foreign),
        };

        Ok(FetchedRows {
            rows,
            origin,
            effective_schema: strict_schema.cloned(),
            deadline_expired: deadline.tripped(),
        })
    }
}
