//! INSERT ... SELECT handler: copy documents from source to target collection.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// INSERT ... SELECT: scan source collection, insert each document into target.
    ///
    /// Returns `{"inserted": N}` payload.
    pub(in crate::data::executor) fn execute_insert_select(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        target_collection: &str,
        source_collection: &str,
        source_filter_bytes: &[u8],
        source_limit: usize,
    ) -> Response {
        debug!(core = self.core_id, %source_collection, %target_collection, "insert select");

        let filters: Vec<ScanFilter> = if source_filter_bytes.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(source_filter_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("deserialize source filters: {e}"),
                        },
                    );
                }
            }
        };

        // Scan source documents.
        let source_docs = if filters.is_empty() {
            match self
                .sparse
                .scan_documents(tid, source_collection, source_limit)
            {
                Ok(docs) => docs,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("scan source: {e}"),
                        },
                    );
                }
            }
        } else {
            match self.scan_matching_documents(tid, source_collection, &filters) {
                Ok(ids) => {
                    let mut docs = Vec::with_capacity(ids.len().min(source_limit));
                    for doc_id in ids.iter().take(source_limit) {
                        if let Ok(Some(data)) = self.sparse.get(tid, source_collection, doc_id) {
                            docs.push((doc_id.clone(), data));
                        }
                    }
                    docs
                }
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("scan source: {e}"),
                        },
                    );
                }
            }
        };

        // Insert each source document into target collection with auto-generated IDs.
        let mut inserted = 0u64;
        for (_source_id, value) in &source_docs {
            let new_id = format!(
                "{:016x}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
                    .wrapping_add(inserted as u128)
            );
            if self
                .sparse
                .put(tid, target_collection, &new_id, value)
                .is_ok()
            {
                self.doc_cache.put(tid, target_collection, &new_id, value);
                inserted += 1;
            }
        }

        debug!(core = self.core_id, %target_collection, inserted, "insert select complete");
        let result = serde_json::json!({ "inserted": inserted });
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
