// SPDX-License-Identifier: BUSL-1.1

//! Vector-primary implementation of [`EngineWriteResolver`].
//!
//! Resolves a governed vector-primary write — `DirectDelete`, `DirectUpdate`,
//! or a `DirectUpsert` whose conflict patch is decided on the Data Plane —
//! into `VectorOp::ResolvedDirectWrite`: a row-mutation list plus the
//! response payload, each mutation carrying the sidecar it read so the apply
//! refuses a resolution that drifted.

use async_trait::async_trait;
use nodedb_types::{QualifiedCollection, RlsWriteCheck};

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::dispatch_local;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{VectorOp, VectorResolveOutcome};

use super::resolved_rows::ResolvedRows;
use super::resolver::{EngineWriteResolver, WriteResolveContext};

/// Index settings the resolved write carries so an apply can create the
/// index a first `Upsert` lands in. A `DirectDelete` never creates one.
#[derive(Default)]
struct ResolvedIndexSettings {
    quantization: nodedb_types::VectorQuantization,
    storage_dtype: nodedb_types::VectorStorageDtype,
    payload_indexes: Vec<(String, nodedb_types::PayloadIndexKind)>,
}

/// A governed vector-primary write, extracted at interception.
pub struct VectorWriteResolver {
    /// Routing collection — also the vshard key.
    collection: QualifiedCollection,
    /// Vector column name; keys the HNSW index.
    field: String,
    settings: ResolvedIndexSettings,
    /// The intercepted write verbatim, live write predicate included. The
    /// Data Plane decides the predicate against the images it computes,
    /// where the writing identity is still available.
    op: VectorOp,
}

/// The resolver for `op`, or `None` when it carries no live write predicate.
/// Exhaustive over `VectorOp` — a new op fails to compile here.
pub(super) fn resolver_for_vector_op(op: &VectorOp) -> Option<Box<dyn EngineWriteResolver>> {
    let (collection, field, settings) = match op {
        VectorOp::DirectDelete {
            collection,
            field,
            rls_write_check,
            ..
        } => {
            if !rls_write_check.has_predicate() {
                return None;
            }
            (collection, field, ResolvedIndexSettings::default())
        }
        // Only a conflict patch leaves an upsert's predicate live: a
        // whole-row upsert is decided Control-Plane-side against its payload.
        VectorOp::DirectUpdate {
            collection,
            field,
            quantization,
            storage_dtype,
            payload_indexes,
            rls_write_check,
            ..
        }
        | VectorOp::DirectUpsert {
            collection,
            field,
            quantization,
            storage_dtype,
            payload_indexes,
            rls_write_check,
            ..
        } => {
            if !rls_write_check.has_predicate() {
                return None;
            }
            (
                collection,
                field,
                ResolvedIndexSettings {
                    quantization: *quantization,
                    storage_dtype: *storage_dtype,
                    payload_indexes: payload_indexes.clone(),
                },
            )
        }
        // Decided Control-Plane-side against the payload image.
        VectorOp::DirectInsert { .. }
        | VectorOp::DirectInsertIfAbsent { .. }
        // Already decided, or reads.
        | VectorOp::ResolveDirectWrite(_)
        | VectorOp::ResolvedDirectWrite { .. }
        | VectorOp::Search { .. }
        | VectorOp::Insert { .. }
        | VectorOp::BatchInsert { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::Delete { .. }
        | VectorOp::DeleteBySurrogate { .. }
        | VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::QueryStats { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. }
        | VectorOp::SparseInsert { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::SparseDelete { .. }
        | VectorOp::MultiVectorInsert { .. }
        | VectorOp::MultiVectorDelete { .. }
        | VectorOp::MultiVectorScoreSearch { .. } => return None,
    };
    Some(Box::new(VectorWriteResolver {
        collection: collection.clone(),
        field: field.clone(),
        settings,
        op: op.clone(),
    }))
}

#[async_trait]
impl EngineWriteResolver for VectorWriteResolver {
    fn collection(&self) -> &str {
        self.collection.as_str()
    }

    fn build_resolve_op(&self) -> PhysicalPlan {
        PhysicalPlan::Vector(VectorOp::ResolveDirectWrite(Box::new(self.op.clone())))
    }

    /// A refused row surfaces as `DataPlane(RejectedAuthz)`, same as the op
    /// dispatched directly — the resolve handler runs the same gate.
    async fn resolve(
        &self,
        state: &SharedState,
        ctx: WriteResolveContext,
        op: PhysicalPlan,
    ) -> crate::Result<ResolvedRows> {
        let collection = self.collection.as_str();
        let resp =
            dispatch_local(state, ctx.tenant_id, ctx.database_id, collection, op, None).await?;
        if resp.status != Status::Ok {
            return Err(match resp.error_code {
                Some(code) => crate::Error::DataPlane(*code),
                None => crate::Error::Dispatch {
                    detail: format!(
                        "vector governed write: resolve on '{collection}' returned status {:?} \
                         with no error code",
                        resp.status
                    ),
                },
            });
        }

        let outcome: VectorResolveOutcome =
            zerompk::from_msgpack(&resp.payload).map_err(|e| crate::Error::Codec {
                detail: format!(
                    "vector governed write: could not decode resolved mutations for \
                     '{collection}': {e}"
                ),
            })?;
        Ok(ResolvedRows::Vector {
            mutations: outcome.mutations,
            response_payload: outcome.response_payload,
        })
    }

    fn apply(&self, resolved: ResolvedRows) -> crate::Result<PhysicalPlan> {
        match resolved {
            ResolvedRows::Vector {
                mutations,
                response_payload,
            } => Ok(PhysicalPlan::Vector(VectorOp::ResolvedDirectWrite {
                collection: self.collection.clone(),
                field: self.field.clone(),
                quantization: self.settings.quantization,
                storage_dtype: self.settings.storage_dtype,
                payload_indexes: self.settings.payload_indexes.clone(),
                mutations,
                response_payload,
                rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
            })),
            ResolvedRows::Update(_)
            | ResolvedRows::Delete(_)
            | ResolvedRows::Kv { .. }
            | ResolvedRows::Document { .. }
            | ResolvedRows::Timeseries { .. }
            | ResolvedRows::GraphEdgeDeleteAdmitted => Err(crate::Error::Internal {
                detail: format!(
                    "vector write resolver for '{}' was handed another engine's resolution; \
                     resolver_for_plan dispatched the wrong engine",
                    self.collection.as_str()
                ),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{VectorOp, VectorWriteTargets};
    use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck};

    use super::resolver_for_vector_op;
    use crate::bridge::envelope::PhysicalPlan;

    fn predicate_delete(check: RlsWriteCheck) -> VectorOp {
        VectorOp::DirectDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            field: "emb".into(),
            targets: VectorWriteTargets::Predicate(Vec::new()),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: check,
        }
    }

    /// Only a live predicate needs resolving; a decided or absent policy
    /// replicates the write as it is.
    #[test]
    fn only_a_live_predicate_selects_the_resolver() {
        assert!(
            resolver_for_vector_op(&predicate_delete(RlsWriteCheck::NoPolicyApplies)).is_none()
        );
        assert!(
            resolver_for_vector_op(&predicate_delete(
                RlsWriteCheck::decided_earlier_in_request()
            ))
            .is_none()
        );
        let resolver =
            resolver_for_vector_op(&predicate_delete(RlsWriteCheck::from_injected(vec![
                1, 2, 3,
            ])))
            .expect("a live predicate selects the resolver");
        assert_eq!(resolver.collection(), "vecs");
        assert!(matches!(
            resolver.build_resolve_op(),
            PhysicalPlan::Vector(VectorOp::ResolveDirectWrite(_))
        ));
    }
}
