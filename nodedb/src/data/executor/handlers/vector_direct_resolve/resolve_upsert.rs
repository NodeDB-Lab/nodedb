// SPDX-License-Identifier: BUSL-1.1

//! Resolver for a governed vector-primary `UPSERT`: the row's existence is
//! probed, a conflict patch is merged through the same planner the live
//! handler uses and decided against the write policy, and the whole row to
//! store is reported as an `Upsert` mutation carrying the sidecar it read.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{UpdateValue, VectorResolveOutcome, VectorResolvedMutation};
use nodedb_types::{RlsWriteCheck, StorageKey, Surrogate, Value};

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::returning_rows::vector_stored_rows_payload;
use crate::data::executor::handlers::vector_direct_row::{VectorDirectIndexSpec, encode_sidecar};
use crate::data::executor::handlers::vector_upsert::{
    VectorUpsertPatch, decode_payload_lowercased,
};
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::resolve_vector_direct_upsert`]: the fields of
/// `VectorOp::DirectUpsert` the resolve reads. `pk_bytes` travels on the
/// mutation so a follower binds the leader-assigned surrogate to the key.
pub(in crate::data::executor) struct VectorResolveUpsertParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub surrogate: Surrogate,
    pub pk_bytes: &'a [u8],
    pub vector: &'a [f32],
    pub payload: &'a [u8],
    pub quantization: nodedb_types::VectorQuantization,
    pub storage_dtype: nodedb_types::VectorStorageDtype,
    pub payload_indexes: &'a [(String, nodedb_types::PayloadIndexKind)],
    pub on_conflict_updates: &'a [(String, UpdateValue)],
    pub rls_write_check: &'a RlsWriteCheck,
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    pub rls_filters: &'a [u8],
}

impl CoreLoop {
    /// Resolve `VectorOp::DirectUpsert`. Counts and replies exactly as
    /// `execute_vector_direct_upsert` does for the upsert intent, verb and
    /// `RETURNING` included.
    pub(super) fn resolve_vector_direct_upsert(
        &self,
        params: VectorResolveUpsertParams<'_>,
    ) -> Result<VectorResolveOutcome, ErrorCode> {
        let VectorResolveUpsertParams {
            task,
            tid,
            collection,
            field,
            surrogate,
            pk_bytes,
            vector,
            payload,
            quantization,
            storage_dtype,
            payload_indexes,
            on_conflict_updates,
            rls_write_check,
            returning,
            rls_filters,
        } = params;
        let database_id = task.request.database_id.as_u64();

        // The index is created by the apply; here it only has to fit.
        let index_key = self.check_vector_direct_index(
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
        let existing = index_key
            .as_ref()
            .is_some_and(|key| self.vector_direct_node(key, surrogate).is_some());

        let proposed: HashMap<String, Value> = if payload.is_empty() {
            HashMap::new()
        } else {
            decode_payload_lowercased(payload).map_err(|e| ErrorCode::Internal {
                detail: format!("payload decode error: {e}"),
            })?
        };

        // `None` requires the surrogate to stay unbound; a bound node with no
        // sidecar row reads as an empty sidecar, which no encoded one is.
        let (fields, old_payload) = if existing && !on_conflict_updates.is_empty() {
            let patch = VectorUpsertPatch {
                database_id,
                tid,
                collection,
                field,
                on_conflict_updates,
                rls_write_check,
            };
            let planned = self.plan_vector_upsert_patch(&patch, surrogate, proposed)?;
            (
                planned.fields,
                Some(planned.old_sidecar.unwrap_or_default()),
            )
        } else if existing {
            let stored = self
                .vector_sidecar_bytes(database_id, tid, collection, surrogate)?
                .unwrap_or_default();
            (proposed, Some(stored))
        } else {
            (proposed, None)
        };
        let sidecar = encode_sidecar(&fields)?;

        let response_payload = match returning {
            Some(spec) => {
                let key = StorageKey::for_surrogate(surrogate);
                vector_stored_rows_payload(spec, rls_filters, &[(&key, sidecar.as_slice())])
                    .map_err(ErrorCode::from)?
            }
            None if !on_conflict_updates.is_empty() => {
                let op = if existing { "update" } else { "insert" };
                response_codec::encode_affected_with_op(1, op)
            }
            None => response_codec::encode_affected(1),
        };
        Ok(VectorResolveOutcome {
            mutations: vec![VectorResolvedMutation::Upsert {
                surrogate,
                pk_bytes: pk_bytes.to_vec(),
                vector: vector.to_vec(),
                payload: sidecar,
                old_payload,
            }],
            response_payload,
        })
    }
}
