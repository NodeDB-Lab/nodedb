// SPDX-License-Identifier: BUSL-1.1

//! `VectorOp::DirectDelete`: remove rows of a vector-primary collection.
//!
//! Every targeted surrogate loses its HNSW node, its payload bitmap
//! entries, and its payload sidecar row. Only surrogates that had a node
//! count; a re-delete reports zero rows.

use nodedb_physical::physical_plan::{ReturningSpec, VectorWriteTargets};
use nodedb_types::{RlsWriteCheck, StorageKey, Surrogate, Value};
use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::rls_write_gate;

/// Parameters for [`CoreLoop::execute_vector_direct_delete`].
pub(in crate::data::executor) struct VectorDirectDeleteParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub targets: &'a VectorWriteTargets,
    /// When `Some`, project each removed row's sidecar instead of a count.
    pub returning: Option<&'a ReturningSpec>,
    /// Read policy bounding which removed rows may be shown back.
    pub rls_filters: &'a [u8],
    /// Write policy decided against each removed row's sidecar image.
    pub rls_write_check: &'a RlsWriteCheck,
}

impl CoreLoop {
    /// Handle `VectorOp::DirectDelete`.
    pub(in crate::data::executor) fn execute_vector_direct_delete(
        &mut self,
        params: VectorDirectDeleteParams<'_>,
    ) -> Response {
        let VectorDirectDeleteParams {
            task,
            tid,
            collection,
            field,
            targets,
            returning,
            rls_filters,
            rls_write_check,
        } = params;
        debug!(core = self.core_id, %collection, %field, "vector direct delete");
        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field);

        // No index means no row was ever written: nothing to remove.
        if !self.vector_collections.contains_key(&index_key) {
            if let Some(spec) = returning {
                return self.vector_stored_returning_response(task, spec, rls_filters, &[]);
            }
            return self.response_affected(task, 0);
        }

        let surrogates: Vec<Surrogate> =
            match self.resolve_vector_direct_targets(database_id, tid, collection, targets) {
                Ok(s) => s
                    .into_iter()
                    .filter(|s| self.vector_direct_node(&index_key, *s).is_some())
                    .collect(),
                Err(e) => return self.response_error(task, e),
            };

        // Gate every row on the write policy BEFORE any removal, so a rejected
        // row cannot leave the rows ahead of it already gone. The sidecar image
        // is the only image a delete has.
        if !matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        ) {
            for surrogate in &surrogates {
                let image = match self.vector_sidecar_row(database_id, tid, collection, *surrogate)
                {
                    Ok(Some(row)) => Value::Object(row.fields),
                    Ok(None) => continue,
                    Err(e) => return self.response_error(task, e),
                };
                if let Err(e) =
                    rls_write_gate::admit_document_value(rls_write_check, &image, tid, collection)
                {
                    return self.response_error(task, e);
                }
            }
        }

        let mut removed: Vec<(StorageKey, Vec<u8>)> = Vec::with_capacity(surrogates.len());
        for surrogate in &surrogates {
            match self.remove_vector_direct_row(&index_key, tid, collection, *surrogate) {
                Ok(Some(row)) => removed.push((StorageKey::for_surrogate(*surrogate), row.bytes)),
                Ok(None) => {}
                Err(e) => return self.response_error(task, e),
            }
        }
        if !removed.is_empty() {
            self.finish_vector_direct_write(task, &index_key, tid, collection, &surrogates);
        }

        if let Some(spec) = returning {
            let rows: Vec<(&StorageKey, &[u8])> =
                removed.iter().map(|(k, b)| (k, b.as_slice())).collect();
            return self.vector_stored_returning_response(task, spec, rls_filters, &rows);
        }
        self.response_affected(task, removed.len() as u64)
    }
}
