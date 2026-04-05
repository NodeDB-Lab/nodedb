//! Upsert handler: insert if absent, merge fields if present.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Upsert: insert if absent, merge fields if present.
    ///
    /// If a document with `document_id` exists, merges `value` fields into the
    /// existing document (preserving fields not in `value`). If it doesn't exist,
    /// inserts as a new document (identical to PointPut).
    pub(in crate::data::executor) fn execute_upsert(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "upsert");

        // Check if document already exists.
        let existing = self.sparse.get(tid, collection, document_id);

        match existing {
            Ok(Some(current_bytes)) => {
                // Merge: read existing doc, overlay new fields.
                let mut doc = match super::super::doc_format::decode_document(&current_bytes) {
                    Some(v) => v,
                    None => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "failed to parse existing document for upsert".into(),
                            },
                        );
                    }
                };

                // Parse incoming value as JSON.
                let new_fields: serde_json::Value = match serde_json::from_slice(value) {
                    Ok(v) => v,
                    Err(_) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "failed to parse upsert value as JSON".into(),
                            },
                        );
                    }
                };

                // Merge new fields into existing document.
                if let (Some(existing_obj), Some(new_obj)) =
                    (doc.as_object_mut(), new_fields.as_object())
                {
                    for (k, v) in new_obj {
                        existing_obj.insert(k.clone(), v.clone());
                    }
                }

                let merged_bytes = super::super::doc_format::encode_to_msgpack(&doc);
                match self.sparse.put(tid, collection, document_id, &merged_bytes) {
                    Ok(()) => {
                        self.doc_cache
                            .put(tid, collection, document_id, &merged_bytes);
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
            Ok(None) => {
                // Insert: document doesn't exist, create new (same as PointPut).
                // Use unified transaction for document + inverted index + stats.
                let txn = match self.sparse.begin_write() {
                    Ok(t) => t,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        );
                    }
                };

                if let Err(e) = self.apply_point_put(&txn, tid, collection, document_id, value) {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }

                if let Err(e) = txn.commit() {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("commit: {e}"),
                        },
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
