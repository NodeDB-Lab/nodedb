// SPDX-License-Identifier: BUSL-1.1

//! The dispatches a cluster RAG fusion makes: the owner's leg export and the
//! graph owners' bindings.

use std::collections::{HashMap, HashSet};

use nodedb_physical::physical_plan::{GraphOp, RagBindingRow, RagLegs, RagStage};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::gateway::TaskRoute;
use crate::control::gateway::dispatcher::{
    DispatchRouteParams, dispatch_route, statement_deadline_ms,
};
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId};

use super::super::cluster_resolve::gateway_shared;
use super::super::shard_reads::ShardReadLog;
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
        deadline_ms: statement_deadline_ms(state),
        version_set: &GatewayVersionSet::from_pairs(Vec::new()),
        txn_id: None,
        linearizable,
    })
    .await?;
    reads.note([vshard], &outcome.read_versions);
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
}

/// Run the `Bindings` stage of `plan` on every graph owner and merge their
/// answers.
pub(super) async fn bindings(
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
