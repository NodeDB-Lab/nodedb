// SPDX-License-Identifier: BUSL-1.1

//! Resolver for a governed vector-primary `UPDATE`: every targeted row that
//! exists is merged through the same planner the live handler uses, decided
//! against the write policy on its post-image, and reported as an `Update`
//! mutation carrying both images.

use nodedb_physical::physical_plan::{VectorResolveOutcome, VectorResolvedMutation};
use nodedb_types::StorageKey;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::returning_rows::{
    VectorRowColumns, vector_stored_rows_payload,
};
use crate::data::executor::handlers::vector_direct_row::VectorDirectIndexSpec;
use crate::data::executor::handlers::vector_direct_update::{
    VectorDirectUpdateParams, VectorPayloadPatch, VectorPlannedRow,
};
use crate::data::executor::response_codec;

impl CoreLoop {
    /// Resolve `VectorOp::DirectUpdate`. Counts and replies exactly as
    /// `execute_vector_direct_update` does, post-image `RETURNING` included.
    pub(super) fn resolve_vector_direct_update(
        &self,
        params: VectorDirectUpdateParams<'_>,
    ) -> Result<VectorResolveOutcome, ErrorCode> {
        let VectorDirectUpdateParams {
            task,
            tid,
            collection,
            field,
            targets,
            new_vector,
            payload_patch,
            quantization,
            storage_dtype,
            payload_indexes,
            returning,
            rls_filters,
            rls_write_check,
        } = params;
        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field);

        // No index means no row was ever written: nothing to rewrite.
        let mut planned: Vec<VectorPlannedRow> = Vec::new();
        if self.vector_collections.contains_key(&index_key) {
            // A re-embed must fit the index the rows live in.
            if let Some(vector) = new_vector {
                self.check_vector_direct_index(
                    database_id,
                    tid,
                    &VectorDirectIndexSpec {
                        collection,
                        field,
                        dim: vector.len(),
                        quantization,
                        storage_dtype,
                        payload_indexes,
                    },
                )?;
            }
            let patch = VectorPayloadPatch {
                database_id,
                tid,
                collection,
                field,
                payload_patch,
                rls_write_check,
            };
            let surrogates =
                self.resolve_vector_direct_targets(database_id, tid, collection, targets)?;
            for surrogate in surrogates {
                if self.vector_direct_node(&index_key, surrogate).is_none() {
                    continue;
                }
                if let Some(row) = self.plan_vector_direct_update_row(&patch, surrogate)? {
                    planned.push(row);
                }
            }
        }

        let response_payload = match returning {
            Some(spec) => {
                let keys: Vec<StorageKey> = planned
                    .iter()
                    .map(|row| StorageKey::for_surrogate(row.surrogate))
                    .collect();
                // The post-image vector: the re-embed, else the node's own.
                let vectors: Vec<Vec<f32>> = planned
                    .iter()
                    .map(|row| match new_vector {
                        Some(vector) => Ok(vector.to_vec()),
                        None => self.committed_primary_vector(
                            database_id,
                            tid,
                            collection,
                            field,
                            row.surrogate,
                        ),
                    })
                    .collect::<crate::Result<_>>()
                    .map_err(ErrorCode::from)?;
                let stored: Vec<(&StorageKey, &[u8], &[f32])> = keys
                    .iter()
                    .zip(planned.iter())
                    .zip(vectors.iter())
                    .map(|((key, row), vector)| (key, row.sidecar.as_slice(), vector.as_slice()))
                    .collect();
                let identity_column = self.identity_column(database_id, tid, collection);
                let columns = VectorRowColumns {
                    identity: &identity_column,
                    vector: field,
                };
                vector_stored_rows_payload(spec, rls_filters, columns, &stored, &self.crs_context())
                    .map_err(ErrorCode::from)?
            }
            None => response_codec::encode_affected(planned.len() as u64),
        };
        let mutations = planned
            .into_iter()
            .map(|row| VectorResolvedMutation::Update {
                surrogate: row.surrogate,
                new_vector: new_vector.map(<[f32]>::to_vec),
                merged_payload: row.sidecar,
                old_payload: row.old_sidecar,
            })
            .collect();
        Ok(VectorResolveOutcome {
            mutations,
            response_payload,
        })
    }
}
