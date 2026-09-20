// SPDX-License-Identifier: BUSL-1.1

//! Resolver for a governed vector-primary `DELETE`: every targeted row that
//! exists is read, decided against the write policy, and reported as a
//! `Delete` mutation carrying the sidecar it read.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{VectorResolveOutcome, VectorResolvedMutation};
use nodedb_types::{StorageKey, Surrogate, Value};

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::returning_rows::vector_stored_rows_payload;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::handlers::vector_direct_delete::VectorDirectDeleteParams;
use crate::data::executor::response_codec;

impl CoreLoop {
    /// Resolve `VectorOp::DirectDelete`. Counts and replies exactly as
    /// `execute_vector_direct_delete` does, pre-image `RETURNING` included.
    pub(super) fn resolve_vector_direct_delete(
        &self,
        params: VectorDirectDeleteParams<'_>,
    ) -> Result<VectorResolveOutcome, ErrorCode> {
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
        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field);

        // No index means no row was ever written: nothing to remove.
        let mut rows: Vec<(Surrogate, Vec<u8>)> = Vec::new();
        if self.vector_collections.contains_key(&index_key) {
            let surrogates =
                self.resolve_vector_direct_targets(database_id, tid, collection, targets)?;
            for surrogate in surrogates {
                if self.vector_direct_node(&index_key, surrogate).is_none() {
                    continue;
                }
                // A bound node with no sidecar row has an empty image; a real
                // predicate has nothing to admit and refuses it.
                let (fields, bytes) =
                    match self.vector_sidecar_row(database_id, tid, collection, surrogate)? {
                        Some(row) => (row.fields, row.bytes),
                        None => (HashMap::new(), Vec::new()),
                    };
                rls_write_gate::admit_document_value(
                    rls_write_check,
                    &Value::Object(fields),
                    tid,
                    collection,
                )?;
                rows.push((surrogate, bytes));
            }
        }

        let response_payload = match returning {
            Some(spec) => {
                let keys: Vec<StorageKey> = rows
                    .iter()
                    .map(|(surrogate, _)| StorageKey::for_surrogate(*surrogate))
                    .collect();
                let stored: Vec<(&StorageKey, &[u8])> = keys
                    .iter()
                    .zip(rows.iter())
                    .map(|(key, (_, bytes))| (key, bytes.as_slice()))
                    .collect();
                vector_stored_rows_payload(spec, rls_filters, &stored).map_err(ErrorCode::from)?
            }
            None => response_codec::encode_affected(rows.len() as u64),
        };
        let mutations = rows
            .into_iter()
            .map(|(surrogate, old_payload)| VectorResolvedMutation::Delete {
                surrogate,
                old_payload,
            })
            .collect();
        Ok(VectorResolveOutcome {
            mutations,
            response_payload,
        })
    }
}
