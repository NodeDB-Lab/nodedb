// SPDX-License-Identifier: BUSL-1.1

//! The dispatches a cluster RAG fusion makes: the owner's leg export and the
//! graph owners' bindings.

use std::collections::{HashMap, HashSet};

use nodedb_physical::physical_plan::{GraphOp, RagBindingRow, RagLegs, RagStage};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::gateway::TaskRoute;
use crate::control::gateway::dispatcher::{DispatchRouteParams, dispatch_route};
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::server::exchange::gather::{CoreFan, gather_cores};
use crate::control::server::response_translate::pk_resolve::resolve_surrogate_pks;
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId};
use nodedb_types::Surrogate;

use super::super::cluster_resolve::gateway_shared;
use super::super::hop::local_owner_cores;
use super::super::shard_reads::{ShardReadLog, key_vshards};
use super::super::whole_graph::scatter_to_graph_owners;
use crate::control::gateway::live_leaders::resolve_live_decision;

/// `plan`, a RAG fusion, set to run `stage`.
pub(super) fn with_stage(plan: &PhysicalPlan, stage: RagStage) -> PhysicalPlan {
    let mut plan = plan.clone();
    if let PhysicalPlan::Graph(GraphOp::RagFusion { stage: slot, .. }) = &mut plan {
        *slot = stage;
    }
    plan
}

/// The owner's raw legs and the watermark it served them at.
pub(super) struct ExportedLegs {
    pub legs: RagLegs,
    pub watermark_lsn: Lsn,
}

/// Where and how the export runs.
pub(super) struct ExportScope {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The collection's own vShard, which holds its vector and text indexes.
    pub vshard: u32,
    pub linearizable: bool,
}

/// Run the `ExportLegs` stage of `plan` on the leader of the collection's
/// vShard. `Ok(None)` when no vector index of the collection exists there,
/// the case a single core answers with `NotFound`. The vShard joins `reads`.
pub(super) async fn export_legs(
    state: &SharedState,
    scope: ExportScope,
    plan: &PhysicalPlan,
    reads: &mut ShardReadLog,
) -> crate::Result<Option<ExportedLegs>> {
    let ExportScope {
        tenant_id,
        database_id,
        vshard,
        linearizable,
    } = scope;
    let shared = gateway_shared(state)?;
    let decision = resolve_live_decision(state, vshard);
    let route = TaskRoute {
        plan: with_stage(plan, RagStage::ExportLegs),
        decision,
        vshard_id: vshard,
    };
    let outcome = dispatch_route(DispatchRouteParams {
        route,
        shared: &shared,
        tenant_id,
        database_id,
        trace_id: TraceId::ZERO,
        deadline_ms: StatementDeadline::of_statement(state).hop_budget_ms(),
        version_set: &GatewayVersionSet::from_pairs(Vec::new()),
        txn_id: None,
        linearizable,
        user_roles: &[],
    })
    .await?;
    reads.note([vshard]);
    let watermark_lsn = outcome
        .shard_watermarks
        .iter()
        .map(|(_, lsn)| *lsn)
        .max()
        .unwrap_or(Lsn::ZERO);
    if outcome.not_found {
        return Ok(None);
    }
    // One core holds the index and answers a one-element array. A node that
    // fans the plan over its cores drops the other cores' `NotFound`.
    let mut answers: Vec<RagLegs> = Vec::new();
    for payload in &outcome.payloads {
        if payload.is_empty() {
            continue;
        }
        let mut legs: Vec<RagLegs> =
            zerompk::from_msgpack(payload).map_err(|e| crate::Error::Codec {
                detail: format!("rag fusion legs decode: {e}"),
            })?;
        answers.append(&mut legs);
    }
    let mut answers = answers.into_iter();
    match (answers.next(), answers.next()) {
        (None, _) => Ok(None),
        (Some(legs), None) => Ok(Some(ExportedLegs {
            legs,
            watermark_lsn,
        })),
        (Some(_), Some(_)) => Err(crate::Error::Internal {
            detail: format!(
                "rag fusion: more than one core of vShard {vshard} answered the vector leg"
            ),
        }),
    }
}

/// What the graph owners answered for a `Bindings` stage.
#[derive(Debug, Default)]
pub(super) struct Bindings {
    /// The graph node each requested surrogate names.
    pub by_surrogate: HashMap<u32, String>,
    /// The requested names that carry a surrogate.
    pub bound_names: HashSet<String>,
    /// Some partition holds edges of the collection.
    pub knows_collection: bool,
}

impl Bindings {
    fn absorb(&mut self, rows: Vec<RagBindingRow>) {
        for row in rows {
            match row {
                RagBindingRow::Bound { name, surrogate } => {
                    self.by_surrogate.insert(surrogate, name.clone());
                    self.bound_names.insert(name);
                }
                RagBindingRow::KnowsCollection => self.knows_collection = true,
            }
        }
    }
}

/// The requests of one `Bindings` stage.
pub(super) struct BindingsRequest {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The database-qualified collection, for the transaction read-set.
    pub collection: String,
    pub surrogates: Vec<u32>,
    pub names: Vec<String>,
    pub linearizable: bool,
    /// The caller reads `Bindings::knows_collection`.
    pub learn_collection: bool,
}

/// Run the `Bindings` stage of `plan` and merge the owners' answers.
///
/// A graph node lives on the core that owns its key vShard, and its name is
/// its row's key. The stage therefore names each requested surrogate from the
/// catalog and asks the owner core of each name. It asks every graph owner
/// only when a surrogate has no key here, another node owns a name, or the
/// caller needs `knows_collection` and the owners asked do not hold the
/// collection.
pub(super) async fn bindings(
    state: &SharedState,
    plan: &PhysicalPlan,
    request: BindingsRequest,
) -> crate::Result<Bindings> {
    let Some(mut found) = owner_bindings(state, plan, &request).await? else {
        return scattered_bindings(state, plan, request).await;
    };
    if request.learn_collection && !found.knows_collection {
        let everywhere = BindingsRequest {
            surrogates: Vec::new(),
            names: Vec::new(),
            ..request
        };
        found.knows_collection = scattered_bindings(state, plan, everywhere)
            .await?
            .knows_collection;
    }
    Ok(found)
}

/// The `Bindings` stage on the cores that own the requested nodes. `None`
/// when the owners cannot be named from this node.
async fn owner_bindings(
    state: &SharedState,
    plan: &PhysicalPlan,
    request: &BindingsRequest,
) -> crate::Result<Option<Bindings>> {
    let mut names = request.names.clone();
    if !request.surrogates.is_empty() {
        let keys = resolve_surrogate_pks(
            state,
            request.database_id,
            request.tenant_id,
            &request.collection,
            request.surrogates.iter().map(|&raw| Surrogate::new(raw)),
        )
        .await?;
        for &raw in &request.surrogates {
            match keys.get(Surrogate::new(raw)) {
                Some(name) => names.push(name.to_owned()),
                None => return Ok(None),
            }
        }
    }
    names.sort();
    names.dedup();
    if names.is_empty() {
        return Ok(None);
    }
    let Some(by_core) = local_owner_cores(state, &names)? else {
        return Ok(None);
    };
    let cores: Vec<usize> = by_core.keys().copied().collect();
    let outcome = gather_cores(
        state,
        &CoreFan {
            tenant_id: request.tenant_id,
            database_id: request.database_id,
            trace_id: TraceId::ZERO,
            txn_id: None,
            cores: &cores,
        },
        |core| {
            let names = by_core.get(&core).cloned().unwrap_or_default();
            with_stage(
                plan,
                RagStage::Bindings {
                    surrogates: Vec::new(),
                    names,
                },
            )
        },
    )
    .await?;
    let mut reads = ShardReadLog::new();
    reads.note(key_vshards(&names));
    reads.publish(
        request.tenant_id,
        request.database_id,
        Some(request.collection.clone()),
    );
    let payload = outcome.merged_array()?;
    let rows: Vec<RagBindingRow> =
        zerompk::from_msgpack(&payload).map_err(|e| crate::Error::Codec {
            detail: format!("rag fusion bindings decode: {e}"),
        })?;
    let mut bindings = Bindings::default();
    bindings.absorb(rows);
    Ok(Some(bindings))
}

/// The `Bindings` stage on every graph owner.
async fn scattered_bindings(
    state: &SharedState,
    plan: &PhysicalPlan,
    request: BindingsRequest,
) -> crate::Result<Bindings> {
    let BindingsRequest {
        tenant_id,
        database_id,
        collection,
        surrogates,
        names,
        linearizable,
        learn_collection: _,
    } = request;
    let plan = with_stage(plan, RagStage::Bindings { surrogates, names });
    let payloads = scatter_to_graph_owners(
        state,
        tenant_id,
        database_id,
        plan,
        linearizable,
        Some(collection),
    )
    .await?;
    let mut bindings = Bindings::default();
    for payload in payloads {
        if payload.is_empty() {
            continue;
        }
        let rows: Vec<RagBindingRow> =
            zerompk::from_msgpack(payload.as_ref()).map_err(|e| crate::Error::Codec {
                detail: format!("rag fusion bindings decode: {e}"),
            })?;
        bindings.absorb(rows);
    }
    Ok(bindings)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bindings_merge_every_owner_answer() {
        let mut bindings = Bindings::default();
        bindings.absorb(vec![RagBindingRow::Bound {
            name: "alice".into(),
            surrogate: 7,
        }]);
        bindings.absorb(vec![
            RagBindingRow::KnowsCollection,
            RagBindingRow::Bound {
                name: "bob".into(),
                surrogate: 9,
            },
        ]);
        assert_eq!(
            bindings.by_surrogate.get(&7).map(String::as_str),
            Some("alice")
        );
        assert_eq!(
            bindings.by_surrogate.get(&9).map(String::as_str),
            Some("bob")
        );
        assert!(bindings.bound_names.contains("alice"));
        assert!(bindings.knows_collection);
    }
}
