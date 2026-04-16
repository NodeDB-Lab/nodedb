//! CONVERT COLLECTION handler: re-encode documents for a new storage mode.
//!
//! Scans all documents in the collection and re-encodes them in-place.
//! For `TO strict`: validates each doc against the schema and encodes as
//! Binary Tuple via `strict_format::json_to_binary_tuple`.
//! For `TO document` or `TO kv`: no re-encoding needed — sparse engine
//! stores raw bytes regardless of type.

use sonic_rs;

use nodedb_types::columnar::{ColumnDef, StrictSchema};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Execute a collection conversion.
    ///
    /// - `TO document` / `TO kv`: no re-encoding needed. Catalog update on Control Plane.
    /// - `TO strict`: re-encode each document as a Binary Tuple using the provided
    ///   schema. Documents that fail validation are skipped and counted as errors.
    pub(in crate::data::executor) fn execute_convert_collection(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        target_type: &str,
        schema_json: &str,
    ) -> Response {
        tracing::debug!(
            core = self.core_id,
            %collection,
            target_type,
            "converting collection"
        );

        match target_type {
            "strict" => self.convert_to_strict(task, tid, collection, schema_json),
            "document" | "kv" => {
                // No re-encoding needed — sparse engine stores raw MessagePack bytes
                // regardless of collection type. Catalog type update handled by
                // Control Plane after this returns.
                let count = self
                    .sparse
                    .scan_documents(tid, collection, usize::MAX)
                    .map(|docs| docs.len() as u64)
                    .unwrap_or(0);

                let result = serde_json::json!({
                    "converted": count,
                    "target_type": target_type,
                    "collection": collection,
                });
                match response_codec::encode_json(&result) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
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
    fn convert_to_strict(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        schema_json: &str,
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
        };

        // Scan all existing documents.
        let docs = match self.sparse.scan_documents(tid, collection, usize::MAX) {
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

        // Re-encode each document as a Binary Tuple.
        let mut converted = 0u64;
        let mut errors = 0u64;

        for (doc_id, doc_bytes) in &docs {
            match super::super::strict_format::bytes_to_binary_tuple(doc_bytes, &schema) {
                Ok(tuple_bytes) => {
                    if let Err(e) = self.sparse.put(tid, collection, doc_id, &tuple_bytes) {
                        tracing::warn!(doc_id, error = %e, "failed to write converted doc");
                        errors += 1;
                        continue;
                    }
                    converted += 1;
                }
                Err(e) => {
                    tracing::warn!(doc_id, error = e, "strict conversion failed");
                    errors += 1;
                }
            }
        }

        tracing::info!(%collection, converted, errors, "collection converted to strict");

        let result = serde_json::json!({
            "converted": converted,
            "errors": errors,
            "target_type": "strict",
            "collection": collection,
        });
        match response_codec::encode_json(&result) {
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
