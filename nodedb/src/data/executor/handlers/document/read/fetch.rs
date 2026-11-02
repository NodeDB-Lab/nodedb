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
//!   MessagePack or JSON for schemaless), paired with the collection's real
//!   strict schema; the downstream normalizes as needed.
//! - **AsOf / AllVersions**: bodies already normalized to MessagePack (with the
//!   synthetic `_ts_*` temporal columns injected for the audit case) so
//!   `effective_schema` is `None` and the shared sort/window/computed/projection
//!   pipeline operates on a uniform shape.
//!
//! A vector-primary collection stores `zerompk` TAGGED metadata sidecars. Every
//! mode renders them through [`CoreLoop::render_vector_sidecar_rows`] and
//! evaluates filters on the rendered values, so a cut read returns the same
//! row shape a current read returns.

use nodedb_crs::CrsContext;
use nodedb_query::EvalError;
use nodedb_types::StorageKey;
use nodedb_types::columnar::schema::StrictSchema;

use super::audit_body::{inject_temporal_columns, strict_audit_body};
use super::fetch_types::FetchedRows;
use super::{DocFetchParams, DocScanMode};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::filter_match::{HeldEvalError, matches_with_resolved_schema};
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
        let database_id = task.request.database_id.as_u64();
        let identity_column = self.identity_column(database_id, tid, collection);
        let crs = self.crs_context();
        let stored = self.stored_body(database_id, tid, collection, strict_schema);

        match params.mode {
            DocScanMode::Current => self.fetch_current(task, tid, &params, &deadline),
            DocScanMode::AsOf {
                system_as_of,
                valid_at_ms,
                merge_overlay: _,
            } => {
                // `versioned_scan_as_of` returns each version's stored body
                // verbatim — strict bodies are Binary Tuples, schemaless bodies
                // can be JSON. Normalize to standard MessagePack so the
                // shared sort/window/computed/projection pipeline (which scans
                // msgpack) operates uniformly, then hand it downstream with no
                // schema (bodies are already normalized).
                // `versioned_scan_as_of` takes an infallible predicate, so a
                // division/modulo-by-zero is held and returned once the scan
                // ends, rather than silently folded away.
                let held = HeldEvalError::default();
                let predicate = |doc_id: &StorageKey, body: &[u8]| {
                    held.check(|| {
                        stored.matches(filter_predicates, doc_id, body, &identity_column, &crs)
                    })
                };
                let raw = self.sparse.versioned_scan_as_of(
                    crate::engine::sparse::btree_versioned::VersionedScanParams {
                        database_id,
                        tenant: tid,
                        coll: collection,
                        sys_cutoff: *system_as_of,
                        valid_at_ms: *valid_at_ms,
                        limit: scan_limit,
                    },
                    &predicate,
                    &stop,
                )?;
                let raw = held.finish::<_, crate::Error>(raw)?;
                let rows = match stored {
                    // The row reads with the vector its node carries, as a
                    // point read at the same cut does.
                    StoredBody::VectorSidecar => self.render_vector_sidecar_rows(
                        database_id,
                        tid,
                        collection,
                        &identity_column,
                        raw,
                        *system_as_of,
                    )?,
                    StoredBody::Document(schema) => raw
                        .into_iter()
                        .map(|(doc_id, body)| {
                            let mp = sparse_body_to_msgpack(
                                &body,
                                SparseBodyFormatRef::from_schema(schema),
                            )
                            .into_owned();
                            (doc_id, mp)
                        })
                        .collect(),
                };
                Ok(FetchedRows {
                    rows,
                    effective_schema: None,
                    deadline_expired: deadline.tripped(),
                })
            }
            DocScanMode::AllVersions { valid_at_ms } => {
                // Every system-time version of every document, delete versions
                // included. Each version is normalized to MessagePack and gets
                // the synthetic `_ts_*` temporal columns injected BEFORE the
                // shared downstream runs, so a user can `SELECT` / `ORDER BY` /
                // project on them. A delete version repeats the cells of the
                // version it closes, with its own `_ts_system` and an empty
                // valid interval.
                // See the `AsOf` arm above for the held-error rationale.
                let held = HeldEvalError::default();
                let predicate = |doc_id: &StorageKey, body: &[u8]| {
                    held.check(|| {
                        stored.matches(filter_predicates, doc_id, body, &identity_column, &crs)
                    })
                };
                let raw = self.sparse.versioned_scan_all(
                    crate::engine::sparse::btree_versioned::VersionedScanParams {
                        database_id,
                        tenant: tid,
                        coll: collection,
                        sys_cutoff: None,
                        valid_at_ms: *valid_at_ms,
                        limit: scan_limit,
                    },
                    &predicate,
                    &stop,
                )?;
                let raw = held.finish::<_, crate::Error>(raw)?;
                let mut rows: Vec<(StorageKey, Vec<u8>)> = Vec::with_capacity(raw.len());
                for row in raw {
                    // A sidecar version renders to its values and the vector
                    // its version held, while the index keeps that vector.
                    let msgpack_body = match stored {
                        StoredBody::VectorSidecar => {
                            let values = sparse_body_to_msgpack(
                                &row.body,
                                SparseBodyFormatRef::VectorSidecar,
                            )
                            .into_owned();
                            match self.version_primary_vector(
                                database_id,
                                tid,
                                collection,
                                row.doc_id.surrogate(),
                                row.system_from,
                            ) {
                                Some((field, vector)) => {
                                    crate::data::executor::handlers::vector_row_column::with_vector_column(
                                        &values, &field, &vector,
                                    )?
                                }
                                None => values,
                            }
                        }
                        StoredBody::Document(Some(schema)) => strict_audit_body(&row.body, schema)?,
                        StoredBody::Document(None) => row.body,
                    };
                    let with_ts = inject_temporal_columns(
                        &msgpack_body,
                        row.system_from,
                        row.valid_from_ms,
                        row.valid_until_ms,
                    )?;
                    rows.push((row.doc_id, with_ts));
                }
                Ok(FetchedRows {
                    rows,
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

    /// Newest live version per document (current-time read).
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
        let identity_column = self.identity_column(database_id, tid, collection);
        let stored = self.stored_body(database_id, tid, collection, strict_schema);

        // `scan_documents_filtered` takes an infallible
        // predicate, so a division/modulo-by-zero is held and returned once
        // every branch below returns, rather than silently folded away.
        let held = HeldEvalError::default();
        let crs = self.crs_context();
        let matches = |key: &StorageKey, value: &[u8]| -> bool {
            held.check(|| stored.matches(filter_predicates, key, value, &identity_column, &crs))
        };

        // The filtered scan takes the stop signal: the unfiltered full scan
        // is the longest-running read shape there is.
        let rows: Vec<(StorageKey, Vec<u8>)> = self.sparse.scan_documents_filtered(
            database_id,
            tid,
            collection,
            fetch_limit,
            &matches,
            &stop,
        )?;

        let rows = held.finish::<_, crate::Error>(rows)?;

        // Sidecar rows leave the fetch rendered, so the schema the downstream
        // decodes with is the stored document schema alone.
        let (rows, effective_schema) = match stored {
            StoredBody::VectorSidecar => (
                self.render_vector_sidecar_rows(
                    database_id,
                    tid,
                    collection,
                    &identity_column,
                    rows,
                    None,
                )?,
                None,
            ),
            StoredBody::Document(schema) => (rows, schema.cloned()),
        };

        Ok(FetchedRows {
            rows,
            effective_schema,
            deadline_expired: deadline.tripped(),
        })
    }

    /// The encoding `collection`'s stored bodies use. It is resolved from the
    /// collection's registered kind, never from the bytes: a tagged sidecar and
    /// a plain document body are both valid MessagePack maps with the same
    /// header, so sniffing mis-reads one of them.
    fn stored_body<'s>(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        strict_schema: Option<&'s StrictSchema>,
    ) -> StoredBody<'s> {
        match self.sparse_body_format(
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection,
        ) {
            SparseBodyFormat::VectorSidecar => StoredBody::VectorSidecar,
            SparseBodyFormat::Document | SparseBodyFormat::Strict(_) => {
                StoredBody::Document(strict_schema)
            }
        }
    }

    /// Render stored vector-primary sidecar rows to the row shape every read of
    /// the collection returns.
    ///
    /// A sidecar is `zerompk` TAGGED metadata. Each row decodes to standard
    /// MessagePack with its identity injected, so every downstream transform
    /// (sort, window functions, computed columns, projection, DISTINCT) sees
    /// values, not `[tag, payload]` arrays. Without this the client reads
    /// `[4,"alice"]`. The row's vector joins it under the collection's vector
    /// field, read from the row's HNSW node visible at system-time ordinal
    /// `at` (the live node when `at` is `None`).
    fn render_vector_sidecar_rows(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        identity_column: &str,
        rows: Vec<(StorageKey, Vec<u8>)>,
        at: Option<i64>,
    ) -> crate::Result<Vec<(StorageKey, Vec<u8>)>> {
        let mut rows: Vec<(StorageKey, Vec<u8>)> = rows
            .into_iter()
            .map(|(key, body)| {
                let (_, mp) = sparse_row_to_doc(
                    &key,
                    &body,
                    SparseBodyFormatRef::VectorSidecar,
                    identity_column,
                );
                (key, mp)
            })
            .collect();
        if let Some(field) = self.vector_primary_field(database_id, tid, collection) {
            self.attach_primary_vectors(database_id, tid, collection, &field, &mut rows, at)?;
        }
        Ok(rows)
    }
}

/// How the fetch stage reads one stored body.
#[derive(Clone, Copy)]
enum StoredBody<'s> {
    /// A vector-primary `zerompk` TAGGED metadata sidecar.
    VectorSidecar,
    /// A document body: a Binary Tuple under the strict schema when one is
    /// set, else schemaless MessagePack or JSON.
    Document(Option<&'s StrictSchema>),
}

impl StoredBody<'_> {
    /// Whether the stored `body` under `key` matches `filters`. Empty
    /// `filters` match every row.
    ///
    /// Filters read fields out of a standard MessagePack map. A sidecar is
    /// rendered to one before evaluation: a predicate on the stored tagged
    /// bytes matches nothing, which reads as "no rows" rather than as an
    /// error.
    fn matches(
        self,
        filters: &[ScanFilter],
        key: &StorageKey,
        body: &[u8],
        identity_column: &str,
        crs: &CrsContext<'_>,
    ) -> Result<bool, EvalError> {
        if filters.is_empty() {
            return Ok(true);
        }
        match self {
            StoredBody::VectorSidecar => {
                let rendered = sparse_body_to_msgpack(body, SparseBodyFormatRef::VectorSidecar);
                matches_with_resolved_schema(None, filters, key, &rendered, identity_column, crs)
            }
            StoredBody::Document(schema) => {
                matches_with_resolved_schema(schema, filters, key, body, identity_column, crs)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_physical::physical_plan::{EnforcementOptions, StorageMode};
    use nodedb_types::{Surrogate, Value, VectorPrimaryConfig};

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::bridge::scan_filter::FilterOp;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::handlers::document::write::RegisterDocumentCollectionParams;
    use crate::data::executor::handlers::vector_direct_test_rows::{COLLECTION, FIELD, TID, seed};

    /// Register `COLLECTION` as `primary='vector'` on `FIELD`, then store one
    /// row with `owner = 'a'` and the vector `[1, 0, 0]`.
    fn register_and_seed(core: &mut CoreLoop) {
        let task = make_default_task();
        let config = VectorPrimaryConfig {
            vector_field: FIELD.to_string(),
            dim: 3,
            ..VectorPrimaryConfig::default()
        };
        let resp = core.execute_register_document_collection(
            &task,
            RegisterDocumentCollectionParams {
                tid: TID,
                collection: COLLECTION,
                indexes: &[],
                crdt_enabled: false,
                storage_mode: &StorageMode::Schemaless,
                enforcement: &EnforcementOptions::default(),
                bitemporal: false,
                conflict_policy: None,
                timeseries: None,
                vector_primary: Some(&config),
                vector_fields: &[],
                declared_columns: &[],
                declared_key: None,
                incarnation: nodedb_types::Hlc::ZERO,
            },
        );
        assert_eq!(resp.status, Status::Ok, "register: {:?}", resp.error_code);
        seed(core, Surrogate::new(7));
    }

    fn owner_is(owner: &str) -> Vec<ScanFilter> {
        vec![ScanFilter {
            field: "owner".into(),
            op: FilterOp::Eq,
            value: Value::String(owner.into()),
            clauses: Vec::new(),
            expr: None,
        }]
    }

    fn fetch(core: &mut CoreLoop, mode: &DocScanMode, filters: &[ScanFilter]) -> FetchedRows {
        let task = make_default_task();
        core.document_scan_fetch(
            &task,
            TID,
            DocFetchParams {
                collection: COLLECTION,
                mode,
                limit: usize::MAX,
                offset: 0,
                filter_predicates: filters,
                strict_schema: None,
                full_fetch: false,
            },
        )
        .expect("fetch")
    }

    fn fields(body: &[u8]) -> HashMap<String, Value> {
        match nodedb_types::value_from_msgpack(body).expect("decode row") {
            Value::Object(map) => map,
            other => panic!("a row is a map: {other:?}"),
        }
    }

    /// A read at a cut (`AS OF`, a pinned cut, a transaction snapshot)
    /// renders a sidecar row as the current read does: values,
    /// never `[tag, payload]` arrays, with the row's vector under its field.
    /// A filter on a sidecar column matches at the cut as it does currently.
    #[test]
    fn a_cut_read_renders_sidecar_rows_as_the_current_read_does() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_and_seed(&mut core);

        let current = fetch(&mut core, &DocScanMode::Current, &owner_is("a"));
        assert_eq!(current.rows.len(), 1);
        assert!(current.effective_schema.is_none());
        let row = fields(&current.rows[0].1);
        assert_eq!(row.get("owner"), Some(&Value::String("a".into())));
        assert!(
            matches!(row.get(FIELD), Some(Value::Array(v)) if v.len() == 3),
            "the row reads with its vector: {row:?}"
        );

        for merge_overlay in [false, true] {
            let cut = DocScanMode::AsOf {
                system_as_of: Some(i64::MAX),
                valid_at_ms: None,
                merge_overlay,
            };
            let at_cut = fetch(&mut core, &cut, &owner_is("a"));
            assert!(at_cut.effective_schema.is_none());
            assert_eq!(at_cut.rows.len(), 1);
            assert_eq!(at_cut.rows[0].0, current.rows[0].0);
            assert_eq!(
                fields(&at_cut.rows[0].1),
                row,
                "a cut read returns the current read's row"
            );
            assert!(fetch(&mut core, &cut, &owner_is("b")).rows.is_empty());
        }
    }

    /// An all-versions read renders each sidecar version to its values and
    /// adds the synthetic system-time column.
    #[test]
    fn an_all_versions_read_renders_sidecar_versions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_and_seed(&mut core);

        let audit = fetch(
            &mut core,
            &DocScanMode::AllVersions { valid_at_ms: None },
            &owner_is("a"),
        );
        assert_eq!(audit.rows.len(), 1);
        let row = fields(&audit.rows[0].1);
        assert_eq!(row.get("owner"), Some(&Value::String("a".into())));
        assert!(row.contains_key("_ts_system"), "{row:?}");
    }
}
