// SPDX-License-Identifier: BUSL-1.1

//! CONVERT COLLECTION handler: re-encode documents for a new storage mode.
//!
//! Scans all documents in the collection and re-encodes them in-place.
//! For `TO strict`: injects each row's client-visible `id`, validates the
//! result against the schema, and encodes it as a Binary Tuple via
//! `strict_format::bytes_to_binary_tuple`.
//! For `TO document` or `TO kv`: a Binary Tuple source re-encodes to
//! MessagePack. A schemaless source needs no re-encoding — the sparse
//! engine already stores it as MessagePack.
//! A row that fails to convert fails the whole statement: the handler
//! returns an error response instead of a success payload, so the caller
//! never flips the catalog's collection type over partially-converted data.

use sonic_rs;

use nodedb_physical::physical_plan::StorageMode;
use nodedb_query::msgpack_scan;
use nodedb_types::columnar::{ColumnDef, StrictSchema};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::scan_normalize::sparse_body_to_msgpack;
use crate::data::executor::sparse_body_format::SparseBodyFormat;
use crate::data::executor::task::ExecutionTask;

/// Map the plan's declared source storage mode to the row-decode format.
///
/// The plan carries this instead of the handler reading `doc_configs`: at
/// dispatch time that cache still describes the mode from BEFORE this
/// conversion, since the catalog flip and Data Plane re-register happen
/// only after this op returns successfully.
fn source_format_of(mode: &StorageMode) -> SparseBodyFormat {
    match mode {
        StorageMode::Strict { schema } => SparseBodyFormat::Strict(schema.clone()),
        StorageMode::Schemaless => SparseBodyFormat::Document,
    }
}

impl CoreLoop {
    /// Execute a collection conversion.
    ///
    /// - `TO document` / `TO kv`: re-encodes a Binary Tuple source to
    ///   MessagePack. A schemaless source is left untouched. Catalog update
    ///   happens on the Control Plane, after this returns successfully.
    /// - `TO strict`: re-encode each document as a Binary Tuple using the
    ///   provided schema. A document that fails to encode fails the
    ///   statement.
    pub(in crate::data::executor) fn execute_convert_collection(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        target_type: &str,
        schema_json: &str,
        source_storage_mode: &StorageMode,
    ) -> Response {
        tracing::debug!(
            core = self.core_id,
            %collection,
            target_type,
            "converting collection"
        );

        let source_format = source_format_of(source_storage_mode);

        match target_type {
            "document_strict" => {
                self.convert_to_strict(task, tid, collection, schema_json, source_format)
            }
            "document_schemaless" | "kv" => {
                self.convert_from_strict(task, tid, collection, target_type, source_format)
            }
            other => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("unsupported conversion target: {other}"),
                },
            ),
        }
    }

    /// Convert to strict mode: re-encode each document as a Binary Tuple.
    ///
    /// The sparse engine keys a minted row by its storage key, not its
    /// client-visible `id`. A `SELECT` synthesizes `id` at read time; this
    /// re-encode must do the same before validating and encoding, or a row
    /// with no declared primary key loses its identity and the target
    /// schema's NOT NULL `id` column rejects it.
    fn convert_to_strict(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        schema_json: &str,
        source_format: SparseBodyFormat,
    ) -> Response {
        // Parse the target schema from JSON column definitions.
        let columns: Vec<ColumnDef> = match sonic_rs::from_str(schema_json) {
            Ok(c) => c,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("invalid schema JSON: {e}"),
                    },
                );
            }
        };

        if columns.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "schema must have at least one column".into(),
                },
            );
        }

        let schema = StrictSchema {
            columns,
            version: 1,
            dropped_columns: Vec::new(),
            bitemporal: false,
        };

        // Scan all existing documents.
        let database_id = task.request.database_id.as_u64();
        let docs = match self
            .sparse
            .scan_documents(database_id, tid, collection, usize::MAX)
        {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("scan failed: {e}"),
                    },
                );
            }
        };

        let mut converted = 0u64;

        for (doc_id, doc_bytes) in &docs {
            let normalized = sparse_body_to_msgpack(doc_bytes, source_format.as_format_ref());
            let identity = doc_id.to_identity();
            let with_id = msgpack_scan::inject_str_field(&normalized, "id", identity.as_str());

            let tuple_bytes = match super::super::strict_format::bytes_to_binary_tuple(
                &with_id, &schema, collection,
            ) {
                Ok(bytes) => bytes,
                Err(e) => {
                    return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!(
                                    "collection '{collection}': row '{identity}' failed to convert to document_strict: {e}"
                                ),
                            },
                        );
                }
            };

            if let Err(e) = self
                .sparse
                .put(database_id, tid, collection, doc_id, &tuple_bytes)
            {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!(
                            "collection '{collection}': row '{identity}' failed to write converted document_strict body: {e}"
                        ),
                    },
                );
            }
            // Write-through: a point-get after this statement must see the
            // re-encoded bytes, not a stale cache entry from before the
            // conversion. `sparse.put` alone never touches this cache.
            self.doc_cache
                .put(database_id, tid, collection, doc_id, &tuple_bytes);
            converted += 1;
        }

        tracing::info!(%collection, converted, "collection converted to document_strict");

        let result = serde_json::json!({
            "converted": converted,
            "target_type": "document_strict",
            "collection": collection,
        });
        match response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Convert from strict mode to schemaless document or kv storage.
    ///
    /// A Binary Tuple source re-encodes to MessagePack against its own
    /// strict schema before the catalog flips. A schemaless source needs no
    /// re-encoding: the sparse engine already stores it as MessagePack.
    fn convert_from_strict(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        target_type: &str,
        source_format: SparseBodyFormat,
    ) -> Response {
        let database_id = task.request.database_id.as_u64();

        let docs = match self
            .sparse
            .scan_documents(database_id, tid, collection, usize::MAX)
        {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("scan failed: {e}"),
                    },
                );
            }
        };

        let converted = match source_format {
            SparseBodyFormat::Strict(schema) => {
                let mut converted = 0u64;
                for (doc_id, doc_bytes) in &docs {
                    let identity = doc_id.to_identity();
                    let Some(mp) =
                        super::super::strict_format::binary_tuple_to_msgpack(doc_bytes, &schema)
                    else {
                        let e = super::super::strict_format::undecodable_strict_row(
                            collection,
                            identity.as_str(),
                        );
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!(
                                    "collection '{collection}': row '{identity}' failed to convert to {target_type}: {e}"
                                ),
                            },
                        );
                    };

                    if let Err(e) = self.sparse.put(database_id, tid, collection, doc_id, &mp) {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!(
                                    "collection '{collection}': row '{identity}' failed to write converted {target_type} body: {e}"
                                ),
                            },
                        );
                    }
                    // Write-through: a point-get after this statement must see
                    // the re-encoded bytes, not a stale cache entry from before
                    // the conversion. `sparse.put` alone never touches this cache.
                    self.doc_cache
                        .put(database_id, tid, collection, doc_id, &mp);
                    converted += 1;
                }
                converted
            }
            SparseBodyFormat::Document | SparseBodyFormat::VectorSidecar => docs.len() as u64,
        };

        let result = serde_json::json!({
            "converted": converted,
            "target_type": target_type,
            "collection": collection,
        });
        match response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
