//! PointGet: read one document by id, apply RLS filters, return bytes.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_get(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        rls_filters: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "point get");

        // Check if this is a strict collection — affects decode format.
        let config_key = format!("{tid}:{collection}");
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let crate::bridge::physical_plan::StorageMode::Strict { ref schema } = c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        // Fetch data from cache or redb.
        let cached = self
            .doc_cache
            .get(tid, collection, document_id)
            .map(|v| v.to_vec());
        let data = if let Some(data) = cached {
            data
        } else {
            match self.sparse.get(tid, collection, document_id) {
                Ok(Some(data)) => {
                    self.doc_cache.put(tid, collection, document_id, &data);
                    data
                }
                Ok(None) => return self.response_with_payload(task, Vec::new()),
                Err(e) => {
                    tracing::warn!(core = self.core_id, error = %e, "sparse get failed");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            }
        };

        // RLS post-fetch: evaluate filters against msgpack bytes.
        if !rls_filters.is_empty() {
            if let Some(ref schema) = strict_schema {
                // Strict: decode Binary Tuple to msgpack for RLS evaluation.
                if let Some(mp) =
                    super::super::super::strict_format::binary_tuple_to_msgpack(&data, schema)
                    && !super::super::rls_eval::rls_check_msgpack_bytes(rls_filters, &mp)
                {
                    return self.response_with_payload(task, Vec::new());
                }
            } else if !super::super::rls_eval::rls_check_msgpack_bytes(rls_filters, &data) {
                return self.response_with_payload(task, Vec::new());
            }
        }

        // For strict collections, return msgpack (decoded from Binary Tuple).
        if let Some(ref schema) = strict_schema
            && let Some(mp) =
                super::super::super::strict_format::binary_tuple_to_msgpack(&data, schema)
        {
            return self.response_with_payload(task, mp);
        }

        self.response_with_payload(task, data)
    }
}
