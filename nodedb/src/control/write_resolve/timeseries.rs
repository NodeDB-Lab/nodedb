// SPDX-License-Identifier: BUSL-1.1

//! Timeseries implementation of [`EngineWriteResolver`], and the resolve
//! every timeseries ingest takes before its record is appended.
//!
//! The resolve pass on the collection's core normalizes the ingest, stamps
//! its timestamps, decides its write policy, and resolves it to the exact rows
//! it stores. The ingest is rebuilt as a `ts-resolved` ingest that carries
//! those rows, so its record logs them and its install stores exactly them.

use async_trait::async_trait;
use nodedb_types::RlsWriteCheck;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::dispatch_local;
use crate::control::state::SharedState;
use crate::engine::timeseries::resolved_ingest::{
    RESOLVED_INGEST_FORMAT, ResolveBase, ResolvedTsBatch, TsDriftPolicy,
};
use nodedb_physical::physical_plan::{TimeseriesOp, TimeseriesResolve};

use super::resolved_rows::ResolvedRows;
use super::resolver::{EngineWriteResolver, WriteResolveContext};

/// A governed timeseries ingest, extracted at interception.
pub struct TimeseriesWriteResolver {
    /// Routing collection — also the vshard key.
    collection: String,
    /// The intercepted ingest verbatim, live write predicate included.
    op: TimeseriesOp,
}

/// The resolver for `op`, or `None` when it carries no live write predicate.
/// Exhaustive over `TimeseriesOp` — a new op fails to compile here.
pub(super) fn resolver_for_timeseries_op(
    op: &TimeseriesOp,
) -> Option<Box<dyn EngineWriteResolver>> {
    let collection = match op {
        TimeseriesOp::Ingest {
            collection,
            rls_write_check,
            ..
        } => {
            if !rls_write_check.has_predicate() {
                return None;
            }
            collection
        }
        // Read-only: the scan writes nothing, and `ResolveIngest` is the
        // resolve pass itself.
        TimeseriesOp::Scan { .. } | TimeseriesOp::ResolveIngest(_) => return None,
        // Refused at injection under a write policy; carries no predicate.
        TimeseriesOp::Truncate { .. } => return None,
    };
    Some(Box::new(TimeseriesWriteResolver {
        collection: collection.as_str().to_owned(),
        op: op.clone(),
    }))
}

#[async_trait]
impl EngineWriteResolver for TimeseriesWriteResolver {
    fn collection(&self) -> &str {
        &self.collection
    }

    fn build_resolve_op(&self) -> PhysicalPlan {
        PhysicalPlan::Timeseries(TimeseriesOp::ResolveIngest(Box::new(TimeseriesResolve {
            ingest: self.op.clone(),
            base: None,
        })))
    }

    /// A refused line surfaces as `DataPlane(RejectedAuthz)`, same as a
    /// directly dispatched ingest — the resolve handler runs the same gate.
    async fn resolve(
        &self,
        state: &SharedState,
        ctx: WriteResolveContext,
        op: PhysicalPlan,
    ) -> crate::Result<ResolvedRows> {
        let collection = &self.collection;
        let resp =
            dispatch_local(state, ctx.tenant_id, ctx.database_id, collection, op, None).await?;
        if resp.status != Status::Ok {
            return Err(match resp.error_code {
                Some(code) => crate::Error::DataPlane(*code),
                None => crate::Error::Dispatch {
                    detail: format!(
                        "timeseries governed ingest: resolve on '{collection}' returned status \
                         {:?} with no error code",
                        resp.status
                    ),
                },
            });
        }

        Ok(ResolvedRows::Timeseries {
            batch: resp.payload.to_vec(),
        })
    }

    fn apply(&self, resolved: ResolvedRows) -> crate::Result<PhysicalPlan> {
        let ResolvedRows::Timeseries { batch } = resolved else {
            return Err(crate::Error::Internal {
                detail: format!(
                    "timeseries write resolver for '{}' was handed another engine's resolution; \
                     resolver_for_plan dispatched the wrong engine",
                    self.collection
                ),
            });
        };
        // The resolved ingest is proposed, and a committed entry installs
        // on every replica by column name.
        let batch = ResolvedTsBatch::from_bytes(&batch)?
            .with_drift(TsDriftPolicy::ApplyByName)
            .to_bytes()?;
        resolved_ingest_plan(&self.op, batch)
    }
}

/// `op` rebuilt as the `ts-resolved` ingest of `batch`, an encoded
/// `ResolvedTsBatch`. Every other field carries over verbatim.
pub(crate) fn resolved_ingest_plan(
    op: &TimeseriesOp,
    batch: Vec<u8>,
) -> crate::Result<PhysicalPlan> {
    let TimeseriesOp::Ingest {
        collection,
        payload: _,
        format: _,
        wal_lsn,
        surrogates,
        provenance,
        rls_write_check: _,
        returning,
        rls_filters,
    } = op.clone()
    else {
        return Err(crate::Error::Internal {
            detail: "timeseries resolve: the resolved plan is not an ingest".into(),
        });
    };
    Ok(PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
        collection,
        payload: batch,
        format: RESOLVED_INGEST_FORMAT.to_string(),
        wal_lsn,
        surrogates,
        provenance,
        rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
        returning,
        rls_filters,
    }))
}

/// The plan a record carries in place of `plan`, or `None` when `plan` goes
/// on the record as it is. The record is a Raft entry, or a WAL record whose
/// install is not gate-admitted. An unresolved timeseries ingest resolves to
/// its rows on this node's core for `vshard_id`, before the record exists.
/// Every install then stores exactly those rows, by column name, whatever
/// its node's tag cardinality, memory budget or governor pressure.
pub(crate) async fn resolve_for_log(
    state: &SharedState,
    ctx: WriteResolveContext,
    vshard_id: crate::types::VShardId,
    plan: &PhysicalPlan,
) -> crate::Result<Option<PhysicalPlan>> {
    if !is_unresolved_ingest(plan) {
        return Ok(None);
    }
    resolve_ingest_plan(state, ctx, vshard_id, plan, TsDriftPolicy::ApplyByName)
        .await
        .map(Some)
}

/// `tasks` with every unresolved timeseries ingest resolved on this node's
/// core for its task's vShard, or `None` when no task needs a resolve. Every
/// replica resolves a sequenced transaction on its own, so the submitting
/// node resolves each ingest before the transaction is sequenced.
pub(crate) async fn resolve_tasks_for_log(
    state: &SharedState,
    tasks: &[nodedb_physical::physical_task::PhysicalTask],
) -> crate::Result<Option<Vec<nodedb_physical::physical_task::PhysicalTask>>> {
    if !tasks.iter().any(|task| is_unresolved_ingest(&task.plan)) {
        return Ok(None);
    }
    // Each ingest resolves against the schema the transaction's previous
    // ingest into the same collection resolved to, in task order.
    let mut chain: std::collections::HashMap<(u64, u64, String), ResolveBase> =
        std::collections::HashMap::new();
    let mut resolved = Vec::with_capacity(tasks.len());
    for task in tasks {
        let mut task = task.clone();
        let PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection,
            payload,
            format,
            ..
        }) = &task.plan
        else {
            resolved.push(task);
            continue;
        };
        let key = (
            task.tenant_id.as_u64(),
            task.database_id.as_u64(),
            collection.as_str().to_owned(),
        );
        let resolved_to = if format == RESOLVED_INGEST_FORMAT {
            ResolveBase::of(&ResolvedTsBatch::from_bytes(payload)?)
        } else {
            let base = chain.get(&key).map(ResolveBase::to_bytes).transpose()?;
            let ctx = WriteResolveContext {
                tenant_id: task.tenant_id,
                database_id: task.database_id,
            };
            let (plan, resolved_to) = resolve_ingest_onto(
                state,
                ctx,
                task.vshard_id,
                &task.plan,
                TsDriftPolicy::ApplyByName,
                base,
            )
            .await?;
            task.plan = plan;
            resolved_to
        };
        chain.insert(key, resolved_to);
        resolved.push(task);
    }
    Ok(Some(resolved))
}

/// The number of lines the resolve of `plan` rejected: its batch's count for
/// a resolved timeseries ingest, `0` for every other plan.
pub(crate) fn rejected_lines(plan: &PhysicalPlan) -> crate::Result<u64> {
    match plan {
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            payload, format, ..
        }) if format == RESOLVED_INGEST_FORMAT => {
            ResolvedTsBatch::from_bytes(payload).map(|batch| batch.rejected)
        }
        _ => Ok(0),
    }
}

/// Whether `plan` is a timeseries ingest that has not resolved its rows.
pub(crate) fn is_unresolved_ingest(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { format, .. })
            if format != RESOLVED_INGEST_FORMAT
    )
}

/// Resolve the unresolved timeseries ingest `plan` on this node's core for
/// `vshard_id`, the core its install runs on, and return the `ts-resolved`
/// ingest that stores exactly the rows it resolved to, under `drift`. The
/// resolve decides the write policy, so a refused line surfaces as
/// `DataPlane(RejectedAuthz)`.
pub(crate) async fn resolve_ingest_plan(
    state: &SharedState,
    ctx: WriteResolveContext,
    vshard_id: crate::types::VShardId,
    plan: &PhysicalPlan,
    drift: TsDriftPolicy,
) -> crate::Result<PhysicalPlan> {
    resolve_ingest_onto(state, ctx, vshard_id, plan, drift, None)
        .await
        .map(|(plan, _)| plan)
}

/// [`resolve_ingest_plan`] against `base`, the encoded [`ResolveBase`] an
/// earlier ingest of the same transaction into the same collection resolved
/// to, or against the live schema when `base` is `None`. Also returns the
/// schema this ingest resolved to, the base of the next one.
async fn resolve_ingest_onto(
    state: &SharedState,
    ctx: WriteResolveContext,
    vshard_id: crate::types::VShardId,
    plan: &PhysicalPlan,
    drift: TsDriftPolicy,
    base: Option<Vec<u8>>,
) -> crate::Result<(PhysicalPlan, ResolveBase)> {
    let PhysicalPlan::Timeseries(op @ TimeseriesOp::Ingest { collection, .. }) = plan else {
        return Err(crate::Error::Internal {
            detail: "timeseries resolve: the plan is not a timeseries ingest".into(),
        });
    };
    let resp = crate::control::maintenance::clone_materializer::dispatch_on_this_node(
        state,
        ctx.tenant_id,
        ctx.database_id,
        vshard_id,
        PhysicalPlan::Timeseries(TimeseriesOp::ResolveIngest(Box::new(TimeseriesResolve {
            ingest: op.clone(),
            base,
        }))),
        None,
    )
    .await?;
    if resp.status != Status::Ok {
        return Err(match resp.error_code {
            Some(code) => crate::Error::DataPlane(*code),
            None => crate::Error::Dispatch {
                detail: format!(
                    "timeseries ingest: resolve on '{}' returned status {:?} with no error code",
                    collection.as_str(),
                    resp.status
                ),
            },
        });
    }
    let batch = ResolvedTsBatch::from_bytes(&resp.payload)?.with_drift(drift);
    let resolved_to = ResolveBase::of(&batch);
    Ok((resolved_ingest_plan(op, batch.to_bytes()?)?, resolved_to))
}
