// SPDX-License-Identifier: BUSL-1.1

//! KV implementation of [`EngineWriteResolver`].
//!
//! Resolves a governed state-dependent KV write (increment, CAS, field
//! merge, TTL, transfer), every predicate UPDATE/DELETE, and every
//! conditional insert (`ON CONFLICT DO NOTHING`, a multi-row
//! `ON CONFLICT DO UPDATE`), into
//! `KvOp::ResolvedWrite`
//! — a mutation list plus response payload, since e.g. a `CAS` miss owes a
//! reply while writing nothing.

use async_trait::async_trait;
use nodedb_types::RlsWriteCheck;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::dispatch_local;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{KvOp, KvResolveOutcome};

use super::resolved_rows::ResolvedRows;
use super::resolver::{EngineWriteResolver, WriteResolveContext};

/// A governed state-dependent KV write, extracted at interception.
pub struct KvWriteResolver {
    /// Routing collection — also the vshard key. `TransferItem` reports its
    /// source collection; both its rows co-locate on one vshard.
    collection: String,
    /// The intercepted write verbatim, live write predicate included.
    op: KvOp,
}

/// The resolver for `op`, or `None` when it carries no live write predicate.
/// Exhaustive over `KvOp` — a new op fails to compile here.
pub(super) fn resolver_for_kv_op(op: &KvOp) -> Option<Box<dyn EngineWriteResolver>> {
    let collection = match op {
        KvOp::InsertOnConflictUpdate {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Delete {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Expire {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Persist {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::FieldSet {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Incr {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::IncrFloat {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Cas {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::GetSet {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Transfer {
            collection,
            rls_write_check,
            ..
        } => {
            if !rls_write_check.has_predicate() {
                return None;
            }
            collection
        }
        // A predicate write always resolves: the row set it scans depends on
        // the clock (TTL expiry), so a log record names the rows instead.
        KvOp::PredicateUpdate { collection, .. } | KvOp::PredicateDelete { collection, .. } => {
            collection
        }
        // A conditional insert always resolves: which rows it writes, and
        // the bodies a merge stores, depend on stored state. A log record of
        // the bare op would make a replica or a WAL replay overwrite a row
        // the leader skipped.
        KvOp::InsertIfAbsent { collection, .. } => collection,
        KvOp::BatchPut {
            collection, intent, ..
        } if intent.depends_on_stored_rows() => collection,
        // Both collections checked: an identity may give a row up but not
        // receive it — `sole_rls_write_check` would wave the write through.
        KvOp::TransferItem {
            source_collection,
            source_rls_write_check,
            dest_rls_write_check,
            ..
        } => {
            if !source_rls_write_check.has_predicate() && !dest_rls_write_check.has_predicate() {
                return None;
            }
            source_collection
        }
        // Already decided, or writes an image the Control Plane already holds.
        KvOp::Get { .. }
        | KvOp::Put { .. }
        | KvOp::Insert { .. }
        | KvOp::Scan { .. }
        | KvOp::GetTtl { .. }
        | KvOp::BatchGet { .. }
        | KvOp::BatchPut { .. }
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::FieldGet { .. }
        | KvOp::Truncate { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        | KvOp::SortedIndexTxnRead { .. }
        | KvOp::MaterializeScan { .. }
        | KvOp::ResolveWrite(_)
        | KvOp::ResolvedWrite { .. } => return None,
    };
    Some(Box::new(KvWriteResolver {
        collection: collection.as_str().to_owned(),
        op: op.clone(),
    }))
}

#[async_trait]
impl EngineWriteResolver for KvWriteResolver {
    fn collection(&self) -> &str {
        &self.collection
    }

    fn build_resolve_op(&self) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::ResolveWrite(Box::new(self.op.clone())))
    }

    /// A refused row surfaces as `DataPlane(RejectedAuthz)`, same as the op
    /// dispatched directly — the resolve handler runs the same gate.
    async fn resolve(
        &self,
        state: &SharedState,
        ctx: WriteResolveContext,
        op: PhysicalPlan,
    ) -> crate::Result<ResolvedRows> {
        let collection = &self.collection;
        // The resolve fits each image under the declared columns resolved
        // here. The resolved rows carry the fitted bytes.
        let mut op = op;
        crate::control::planner::kv_declared_columns::resolve_kv_declared_columns(
            state,
            ctx.tenant_id,
            &mut op,
        )?;
        let resp =
            dispatch_local(state, ctx.tenant_id, ctx.database_id, collection, op, None).await?;
        if resp.status != Status::Ok {
            return Err(match resp.error_code {
                Some(code) => crate::Error::DataPlane(*code),
                None => crate::Error::Dispatch {
                    detail: format!(
                        "kv governed write: resolve on '{collection}' returned status {:?} with \
                         no error code",
                        resp.status
                    ),
                },
            });
        }

        let outcome: KvResolveOutcome =
            zerompk::from_msgpack(&resp.payload).map_err(|e| crate::Error::Codec {
                detail: format!(
                    "kv governed write: could not decode resolved mutations for '{collection}': {e}"
                ),
            })?;
        Ok(ResolvedRows::Kv {
            mutations: outcome.mutations,
            response_payload: outcome.response_payload,
        })
    }

    fn apply(&self, resolved: ResolvedRows) -> crate::Result<PhysicalPlan> {
        match resolved {
            ResolvedRows::Kv {
                mutations,
                response_payload,
            } => Ok(PhysicalPlan::Kv(KvOp::ResolvedWrite {
                mutations,
                response_payload,
                rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
            })),
            ResolvedRows::Update(_)
            | ResolvedRows::Delete(_)
            | ResolvedRows::Document { .. }
            | ResolvedRows::Vector { .. }
            | ResolvedRows::Timeseries { .. }
            | ResolvedRows::GraphEdgeDeleteAdmitted => Err(crate::Error::Internal {
                detail: format!(
                    "kv write resolver for '{}' was handed another engine's resolution; \
                         resolver_for_plan dispatched the wrong engine",
                    self.collection
                ),
            }),
        }
    }
}
