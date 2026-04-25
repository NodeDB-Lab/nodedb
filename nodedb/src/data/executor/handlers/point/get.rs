//! PointGet: read one document by id, apply RLS filters, return bytes.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use nodedb_types::Surrogate;

pub(in crate::data::executor) struct PointGetParams<'a> {
    pub tid: u32,
    pub collection: &'a str,
    pub document_id: &'a str,
    /// Catalog-bound identity. Hex-encoded into the substrate row key
    /// at handler entry so storage addressing is independent of the
    /// user-facing PK string.
    pub surrogate: Surrogate,
    pub rls_filters: &'a [u8],
    pub system_as_of_ms: Option<i64>,
    pub valid_at_ms: Option<i64>,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_get(
        &mut self,
        task: &ExecutionTask,
        p: PointGetParams<'_>,
    ) -> Response {
        let PointGetParams {
            tid,
            collection,
            document_id,
            surrogate,
            rls_filters,
            system_as_of_ms,
            valid_at_ms,
        } = p;
        let row_key = surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        debug!(
            core = self.core_id,
            %collection,
            %document_id,
            ?system_as_of_ms,
            ?valid_at_ms,
            "point get"
        );

        // Check if this is a strict collection — affects decode format.
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let crate::bridge::physical_plan::StorageMode::Strict { ref schema } = c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        let bitemporal = self.is_bitemporal(tid, collection);
        let is_temporal_read = system_as_of_ms.is_some() || valid_at_ms.is_some();

        // Fetch data from cache or storage. Temporal reads bypass the
        // doc cache (cache holds current state) and read the versioned
        // table directly via Ceiling at the cutoff.
        let data = if is_temporal_read {
            match self.sparse.versioned_get_as_of(
                tid,
                collection,
                row_key,
                system_as_of_ms,
                valid_at_ms,
            ) {
                Ok(Some(data)) => data,
                Ok(None) => return self.response_with_payload(task, Vec::new()),
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            }
        } else {
            let cached = self
                .doc_cache
                .get(tid, collection, row_key)
                .map(|v| v.to_vec());
            if let Some(data) = cached {
                data
            } else {
                let res = if bitemporal {
                    self.sparse.versioned_get_current(tid, collection, row_key)
                } else {
                    self.sparse.get(tid, collection, row_key)
                };
                match res {
                    Ok(Some(data)) => {
                        self.doc_cache.put(tid, collection, row_key, &data);
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
