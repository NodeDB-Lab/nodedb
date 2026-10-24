// SPDX-License-Identifier: BUSL-1.1

//! A CRDT document delete on the edge recon path.
//!
//! A CRDT document is keyed by its document id, and that id names its graph
//! node. Deleting documents of an edge-bearing CRDT collection runs as one
//! Calvin transaction: a `NodeEdgeGuard` per node whose document is stored,
//! a `NodePresenceGuard` on the collection's vShard, the `DocDelete`s, and
//! an `EdgeDelete` per edge of the collection incident on each stored
//! document's node.
//!
//! A delete of a missing document removes nothing, so its node's edges
//! stay. The planner reads which documents are stored, and the presence
//! guard proves none appeared or vanished since. A guard that finds its
//! state changed aborts the transaction with a drift verdict, and the
//! coordinator reads again and resubmits.

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::{CrdtOp, GraphOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::dependent_recon::DependentReconOutcome;
use super::dependent_recon_finish::finish_committed;
use super::dependent_recon_node_edges::{
    NodeDeletePlan, NodeIncidentEdges, node_delete_tasks, read_node_incident_edges,
};
use super::edge_truncate::collection_is_edge_bearing;
use super::{
    DependentOutcome, DependentRetryArgs, build_single_vshard_tx_class,
    predicate_class_for_filters, run_dependent_with_retry, submit_calvin_routed_assign,
};
use crate::Error;
use crate::control::cluster::calvin::executor::ollp::error::OllpError;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::graph_dispatch::{VShardRead, read_on_vshard};
use crate::control::server::shared::session::StatementDeadline;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::state::SharedState;
use crate::types::VShardId;
use crate::types::{DatabaseId, TenantId};

/// Whether `plan` is a CRDT document delete.
pub fn is_crdt_doc_delete(plan: &PhysicalPlan) -> bool {
    matches!(plan, PhysicalPlan::Crdt(CrdtOp::DocDelete { .. }))
}

/// Dispatch the CRDT document deletes among `tasks` with their nodes' edge
/// tombstones. `None` when `tasks` holds no CRDT document delete.
/// `reads` guard the settled balance deltas that ride in `tasks`.
pub(super) async fn dispatch_crdt_doc_deletes(
    state: &SharedState,
    tasks: &[PhysicalTask],
    identity: Option<&AuthenticatedIdentity>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    reads: &[ReadSetEntry],
) -> crate::Result<Option<DependentReconOutcome>> {
    // The node every deleted document id names, bound or not. An id unbound
    // at planning names no stored document, so the read finds it absent and
    // the presence guard names it: a writer that binds and stores it before
    // the delete's turn fails the guard, and the retry deletes it with its
    // edges. `template` is the first delete: its vShard is the collection's.
    let mut template: Option<(&PhysicalTask, String)> = None;
    let mut nodes = Vec::new();
    for task in tasks {
        if let PhysicalPlan::Crdt(CrdtOp::DocDelete {
            collection: coll,
            document_id,
            ..
        }) = &task.plan
        {
            template.get_or_insert_with(|| (task, coll.as_str().to_string()));
            nodes.push(document_id.clone());
        }
    }
    let Some((template, collection)) = template else {
        return Ok(None);
    };
    if !collection_is_edge_bearing(state, tenant_id, database_id, &collection)? {
        nodes.clear();
    }
    nodes.sort_unstable();
    nodes.dedup();

    let orc = state
        .ollp_orchestrator
        .get()
        .ok_or(Error::SequencerUnavailable)?;
    let registry = state
        .calvin_completion_registry
        .get()
        .ok_or(Error::SequencerUnavailable)?;
    let pred_class = predicate_class_for_filters(&[], &collection);
    let read = || {
        read_crdt_nodes(
            state,
            CrdtNodes {
                tenant_id,
                database_id,
                collection: &collection,
                vshard: template.vshard_id,
            },
            &nodes,
        )
    };
    let initial_predicted = read().await?;

    let submit = |predicted: &CrdtDeleteRead| {
        let predicted = predicted.clone();
        let collection = &collection;
        async move {
            let (guards, deletes) = node_delete_tasks(
                state,
                tenant_id,
                database_id,
                NodeDeletePlan {
                    collection,
                    guarded: &predicted.edges,
                    deleted: &predicted.edges,
                    already_deleted: &Default::default(),
                },
            )
            .await
            .map_err(|e| OllpError::Terminal(Box::new(e)))?;
            let mut submission: Vec<PhysicalTask> = guards;
            submission.extend(presence_guard(template, collection, &predicted));
            submission.extend(tasks.iter().cloned());
            submission.extend(deletes);
            if let Some(identity) = identity {
                let emitter = crate::control::security::audit::ArcAuditEmitter(
                    std::sync::Arc::clone(&state.audit),
                );
                submission = crate::control::server::shared::authorization::authorize_task_set(
                    identity,
                    &submission,
                    &state.permissions,
                    &state.roles,
                    &emitter,
                )
                .map_err(|e| OllpError::Terminal(Box::new(e.into())))?
                .into_tasks()
                .into_iter()
                .map(|task| task.into_physical_task())
                .collect();
            }
            #[cfg(feature = "failpoints")]
            crate::control::fail_gate::before_recon_submit(state.node_id).await;
            orc.submit_with_retry_via(
                pred_class,
                tenant_id,
                || {
                    let tx_class = build_single_vshard_tx_class(&submission, tenant_id, reads)
                        .map_err(|e| OllpError::Terminal(Box::new(e)))?;
                    Ok(Some(tx_class))
                },
                |tx_class| async move {
                    submit_calvin_routed_assign(state, tx_class)
                        .await
                        .map_err(|e| OllpError::Retryable(Box::new(e)))
                },
            )
            .await
        }
    };

    let outcome = run_dependent_with_retry(DependentRetryArgs {
        registry,
        orchestrator: orc,
        predicate_class_hash: pred_class,
        deadline: StatementDeadline::of_statement(state).tokio_instant(),
        ollp_max_retries: orc.ollp_max_retries() as u32,
        initial_predicted,
        submit,
        rescan: read,
    })
    .await?;
    match outcome {
        DependentOutcome::Committed {
            txn_id,
            ack_results,
        } => finish_committed(state, tasks, txn_id, &ack_results)
            .await
            .map(Some),
        DependentOutcome::NoOp => Ok(Some(DependentReconOutcome {
            tasks_dispatched: 0,
            apply_result: None,
        })),
    }
}

/// What the planner read of a CRDT delete's nodes: which documents are
/// stored, and the incident edges of the stored ones.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CrdtDeleteRead {
    present: Vec<String>,
    absent: Vec<String>,
    edges: Vec<NodeIncidentEdges>,
}

/// Where a CRDT delete's nodes are read.
#[derive(Clone, Copy)]
struct CrdtNodes<'a> {
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &'a str,
    /// The collection's vShard, which stores its documents.
    vshard: VShardId,
}

/// Read which of `nodes` have a stored document, on the collection's
/// vShard, and the incident edges of those that do. A delete of a missing
/// document removes nothing, so its node's edges stay.
async fn read_crdt_nodes(
    state: &SharedState,
    at: CrdtNodes<'_>,
    nodes: &[String],
) -> crate::Result<CrdtDeleteRead> {
    if nodes.is_empty() {
        return Ok(CrdtDeleteRead::default());
    }
    let plan = PhysicalPlan::Graph(GraphOp::NodePresenceRead {
        collection: nodedb_types::QualifiedCollection::from_stored(at.collection.to_string()),
        vshard: at.vshard.as_u32(),
        ids: nodes.to_vec(),
    });
    let payload = read_on_vshard(
        state,
        VShardRead {
            tenant_id: at.tenant_id,
            database_id: at.database_id,
            vshard_id: at.vshard.as_u32(),
            txn_id: None,
            linearizable: true,
        },
        plan,
    )
    .await?;
    let stored: BTreeSet<String> = if payload.as_bytes().is_empty() {
        BTreeSet::new()
    } else {
        zerompk::from_msgpack::<Vec<String>>(payload.as_bytes())
            .map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!("crdt delete presence read: {e}"),
            })?
            .into_iter()
            .collect()
    };
    let (present, absent): (Vec<String>, Vec<String>) = nodes
        .iter()
        .cloned()
        .partition(|node| stored.contains(node));
    let edges = read_node_incident_edges(
        state,
        at.tenant_id,
        at.database_id,
        at.collection,
        &present,
        None,
    )
    .await?;
    Ok(CrdtDeleteRead {
        present,
        absent,
        edges,
    })
}

/// The presence guard of `read` on the collection's vShard, beside the
/// delete `template`. `None` when the delete names no bound node.
fn presence_guard(
    template: &PhysicalTask,
    collection: &str,
    read: &CrdtDeleteRead,
) -> Option<PhysicalTask> {
    if read.present.is_empty() && read.absent.is_empty() {
        return None;
    }
    Some(PhysicalTask {
        plan: PhysicalPlan::Graph(GraphOp::NodePresenceGuard {
            collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
            vshard: template.vshard_id.as_u32(),
            present: read.present.clone(),
            absent: read.absent.clone(),
        }),
        ..template.clone()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_crdt_document_delete_takes_this_path() {
        let delete = PhysicalPlan::Crdt(CrdtOp::DocDelete {
            collection: nodedb_types::QualifiedCollection::from_stored("c".to_string()),
            document_id: "a".to_string(),
            surrogate: None,
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(is_crdt_doc_delete(&delete));
        let read = PhysicalPlan::Crdt(CrdtOp::Read {
            collection: nodedb_types::QualifiedCollection::from_stored("c".to_string()),
            document_id: "a".to_string(),
        });
        assert!(!is_crdt_doc_delete(&read));
    }
}
