// SPDX-License-Identifier: BUSL-1.1

//! PointGet: read one document by id, apply RLS filters, return bytes.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::scan_normalize::{sparse_body_to_msgpack, sparse_row_to_doc};
use crate::data::executor::task::ExecutionTask;
use nodedb_types::{RowIdentity, Surrogate};

pub(in crate::data::executor) struct PointGetParams<'a> {
    pub tid: u64,
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
        let storage_key = nodedb_types::StorageKey::for_surrogate(surrogate);
        debug!(
            core = self.core_id,
            %collection,
            %document_id,
            ?system_as_of_ms,
            ?valid_at_ms,
            "point get"
        );

        let database_id = task.request.database_id.as_u64();
        // How this collection's sparse rows are encoded, resolved from its
        // registered kind. Three encodings share the sparse store — schemaless
        // document bodies, strict Binary Tuples, and vector-primary `zerompk`
        // TAGGED sidecars — and a tagged map and a plain document map are both
        // valid MessagePack maps with the same header, so no inspection of the
        // bytes can separate them.
        let body_format = self.sparse_body_format(
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection,
        );

        let bitemporal = self.is_bitemporal(database_id, tid, collection);
        let is_temporal_read = system_as_of_ms.is_some() || valid_at_ms.is_some();

        // Fetch data from cache or storage. Temporal reads bypass the
        // doc cache (cache holds current state) and read the versioned
        // table directly via Ceiling at the cutoff.
        let data = if is_temporal_read {
            match self.sparse.versioned_get_as_of(
                database_id,
                tid,
                collection,
                &storage_key,
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
        } else if let Some(overlay_data) = self.overlay_point_lookup(
            task,
            tid,
            collection,
            &RowIdentity::from_user_key(document_id),
            surrogate,
        ) {
            match overlay_data {
                Ok(data) => data,
                Err(response) => return response,
            }
        } else {
            let cached = self
                .doc_cache
                .get(database_id, tid, collection, &storage_key)
                .map(|v| v.to_vec());
            if let Some(data) = cached {
                data
            } else {
                let res = if bitemporal {
                    self.sparse
                        .versioned_get_current(database_id, tid, collection, &storage_key)
                } else {
                    self.sparse.get(database_id, tid, collection, &storage_key)
                };
                match res {
                    Ok(Some(data)) => {
                        self.doc_cache
                            .put(database_id, tid, collection, &storage_key, &data);
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

        // Normalize once, then gate on the normalized image and return that
        // same image. Evaluating RLS against the stored bytes drops a strict
        // or vector-primary row on a format mismatch rather than on policy —
        // the predicate finds no field it recognizes in a Binary Tuple or in a
        // tagged sidecar — and returning the stored bytes hands the client
        // `[4,"alice"]` where it asked for `alice`.
        //
        // The normalizer borrows when the stored body needed no transcode, so
        // the common schemaless read costs nothing here; only a body that was
        // actually rewritten yields an owned buffer, and only then is `data`
        // superseded.
        //
        // RLS reads a second image with `id` injected: a schemaless row with
        // no declared `id` field carries its identity only in `row_key`, so a
        // policy naming `id` reads it as absent otherwise. The client still
        // gets the stored body, which never gains a field it did not have.
        let transcoded = {
            let normalized = sparse_body_to_msgpack(&data, body_format.as_format_ref());
            if !rls_filters.is_empty() {
                let (_, gated) =
                    sparse_row_to_doc(&storage_key, &data, body_format.as_format_ref());
                if !super::super::rls_eval::rls_check_msgpack_bytes(rls_filters, &gated) {
                    return self.response_with_payload(task, Vec::new());
                }
            }
            match normalized {
                std::borrow::Cow::Owned(v) => Some(v),
                std::borrow::Cow::Borrowed(_) => None,
            }
        };

        self.response_with_payload(task, transcoded.unwrap_or(data))
    }
}
