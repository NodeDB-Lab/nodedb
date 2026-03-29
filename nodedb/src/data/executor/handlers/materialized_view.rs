//! Materialized view refresh handler.
//!
//! Scans all documents from the source collection and copies them to
//! the target (view) collection. Removes orphaned target docs that
//! no longer exist in the source.

use std::collections::HashSet;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Execute a full materialized view refresh.
    ///
    /// 1. Scan all documents from the source collection
    /// 2. Write each document to the target (view) collection
    /// 3. Delete orphaned docs in target that are not in source
    /// 4. Return the number of rows materialized
    pub(in crate::data::executor) fn execute_refresh_materialized_view(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        view_name: &str,
        source_collection: &str,
    ) -> Response {
        tracing::debug!(
            core = self.core_id,
            view = view_name,
            source = source_collection,
            "refreshing materialized view"
        );

        // 1. Scan all documents from source.
        let source_docs = match self
            .sparse
            .scan_documents(tid, source_collection, usize::MAX)
        {
            Ok(docs) => docs,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!(
                            "failed to scan source collection '{source_collection}': {e}"
                        ),
                    },
                );
            }
        };

        // Collect source IDs for orphan detection.
        let source_ids: HashSet<&str> = source_docs.iter().map(|(id, _)| id.as_str()).collect();

        // 2. Write each source document to the target collection.
        let mut written = 0u64;
        for (doc_id, doc_bytes) in &source_docs {
            if let Err(e) = self.sparse.put(tid, view_name, doc_id, doc_bytes) {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("failed to write to view '{view_name}': {e}"),
                    },
                );
            }
            written += 1;
        }

        // 3. Delete orphaned docs in target that are not in source.
        let existing_target = self
            .sparse
            .scan_documents(tid, view_name, usize::MAX)
            .unwrap_or_default();
        let mut orphans_deleted = 0u64;
        for (target_id, _) in &existing_target {
            if !source_ids.contains(target_id.as_str()) {
                let _ = self.sparse.delete(tid, view_name, target_id);
                orphans_deleted += 1;
            }
        }

        tracing::info!(
            view = view_name,
            source = source_collection,
            rows = written,
            orphans_deleted,
            "materialized view refreshed"
        );

        let result = serde_json::json!({
            "rows_materialized": written,
            "orphans_deleted": orphans_deleted,
            "source": source_collection,
            "view": view_name,
        });
        let payload = serde_json::to_vec(&result).unwrap_or_default();
        self.response_with_payload(task, payload)
    }
}
