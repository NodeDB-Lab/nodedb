// SPDX-License-Identifier: BUSL-1.1

//! Node-delete edge tasks for a delete inside a session transaction.
//!
//! A transaction derives the delete's edge tasks at the statement, from the
//! read its prediction takes, and buffers them with it:
//!
//! - One `EdgeDelete` per edge an edge document's `_from`/`_to` mirrors.
//! - One `EdgeDelete` per edge of the collection incident on each deleted
//!   row's node, including edges the transaction staged itself.
//! - One `NodeEdgeGuard` per node over the node's edges in the store. COMMIT
//!   checks it, and a node whose edges changed since the statement fails the
//!   transaction with a serialization error the client retries.

use nodedb_physical::physical_plan::{CrdtOp, DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::dependent_recon_node_edges::{
    NodeDeletePlan, NodeIncidentEdges, node_delete_tasks, planned_edge_deletes,
    read_node_incident_edges,
};
use super::preexec::{PreexecRequest, PreexecScan, RowIdentityRead, run_preexec_scan};
use crate::control::planner::implicit_edges::append_implicit_edge_delete_tasks;
use crate::control::state::SharedState;
use crate::types::{TraceId, TxnId};

/// The edge tasks a delete `task` inside transaction `txn_id` carries: a
/// `BulkDelete`, a `PointDelete` of a bound surrogate, or a CRDT
/// `DocDelete` of a bound document. Empty for any
/// other task, and for a delete on a collection no edge was ever written
/// into. A TRUNCATE of an edge-bearing collection is refused.
///
/// `prediction` is the statement's own read of a predicate delete's rows.
/// The edge tasks derive from it, so they cover exactly the rows it stages.
///
/// Guards come first in the returned list.
pub async fn txn_node_delete_tasks(
    state: &SharedState,
    task: &PhysicalTask,
    txn_id: Option<TxnId>,
    prediction: Option<&PreexecScan>,
) -> crate::Result<Vec<PhysicalTask>> {
    let (tenant_id, database_id) = (task.tenant_id, task.database_id);
    // The rows the statement deletes: a predicate, one row by surrogate, or
    // one CRDT document by its id.
    let (collection, target) = match &task.plan {
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection,
            filters,
            system_time,
            ..
        }) => (
            collection,
            DeleteTarget::Rows {
                filters: filters.clone(),
                prefilter: None,
                system_time: *system_time,
            },
        ),
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection,
            surrogate: Some(surrogate),
            ..
        }) => (
            collection,
            DeleteTarget::Rows {
                filters: Vec::new(),
                prefilter: Some([*surrogate].into_iter().collect()),
                // A point delete names its row: the row's current version
                // with the staged writes on top.
                system_time: nodedb_types::SystemTimeScope::Current,
            },
        ),
        PhysicalPlan::Crdt(CrdtOp::DocDelete {
            collection,
            document_id,
            surrogate: Some(_),
            ..
        }) => (collection, DeleteTarget::Document(document_id.clone())),
        // A TRUNCATE's edge shares stage nothing: each tombstones its
        // vShard's edges at the transaction's turn. A read later in the same
        // transaction block still sees the edges, so the block refuses it.
        PhysicalPlan::Document(DocumentOp::Truncate { collection, .. }) => {
            if super::edge_truncate::collection_is_edge_bearing(
                state,
                tenant_id,
                database_id,
                collection.as_str(),
            )? {
                return Err(crate::Error::BadRequest {
                    detail: format!(
                        "TRUNCATE of '{collection}' cannot run inside a transaction block: \
                         graph edges were written into it, and the block's later reads would \
                         still see them. Run the TRUNCATE outside the transaction."
                    ),
                });
            }
            return Ok(Vec::new());
        }
        _ => return Ok(Vec::new()),
    };
    let bare = nodedb_types::CollectionKey::from_qualified(database_id, collection)?.name();
    let Some(coll) =
        state
            .credentials
            .catalog()
            .get_collection(database_id, tenant_id.as_u64(), bare)?
    else {
        return Ok(Vec::new());
    };
    if !coll.has_implicit_edges {
        return Ok(Vec::new());
    }
    let declared_primary_key = coll.declared_primary_key;

    // The rows as this transaction sees them, read before the delete stages.
    let (identities, scanned_edges) = match (target, prediction) {
        // The statement's prediction read these rows already.
        (DeleteTarget::Rows { .. }, Some(scan)) => (scan.identities.clone(), scan.edges.clone()),
        (
            DeleteTarget::Rows {
                filters,
                prefilter,
                system_time,
            },
            None,
        ) => {
            let scan = run_preexec_scan(
                state,
                tenant_id,
                database_id,
                PreexecRequest {
                    collection: collection.as_str(),
                    filters,
                    prefilter,
                    identity: RowIdentityRead::Read {
                        declared_primary_key: declared_primary_key.as_deref(),
                    },
                    txn_id,
                    system_time,
                },
            )
            .await?;
            (scan.identities, scan.edges)
        }
        (DeleteTarget::Document(document_id), _) => (vec![document_id], Vec::new()),
    };
    let mut edge_tasks = Vec::new();
    append_implicit_edge_delete_tasks(
        state,
        &mut edge_tasks,
        tenant_id,
        database_id,
        TraceId::ZERO,
        collection.as_str(),
        &scanned_edges,
    )
    .await?;

    // The guards check the store. The deletes also cover the edges this
    // transaction staged.
    let stored = read_node_incident_edges(
        state,
        tenant_id,
        database_id,
        collection.as_str(),
        &identities,
        None,
    )
    .await?;
    let seen = match txn_id {
        Some(_) => {
            read_node_incident_edges(
                state,
                tenant_id,
                database_id,
                collection.as_str(),
                &identities,
                txn_id,
            )
            .await?
        }
        None => Vec::new(),
    };
    let deleted = union_by_node(&stored, &seen);
    let (mut guards, deletes) = node_delete_tasks(
        state,
        tenant_id,
        database_id,
        NodeDeletePlan {
            collection: collection.as_str(),
            guarded: &stored,
            deleted: &deleted,
            already_deleted: &planned_edge_deletes(&edge_tasks),
        },
    )
    .await?;
    guards.extend(edge_tasks);
    guards.extend(deletes);
    Ok(guards)
}

/// What a transaction delete removes.
enum DeleteTarget {
    /// The rows a scan with these filters matches.
    Rows {
        filters: Vec<u8>,
        prefilter: Option<nodedb_types::SurrogateBitmap>,
        /// The scope the rows are matched at, the delete's own.
        system_time: nodedb_types::SystemTimeScope,
    },
    /// One CRDT document, whose id names its node.
    Document(String),
}

/// Each node of `a` with its edges in `a` or `b`. `a` and `b` read the same
/// nodes in the same order.
fn union_by_node(a: &[NodeIncidentEdges], b: &[NodeIncidentEdges]) -> Vec<NodeIncidentEdges> {
    a.iter()
        .map(|node| {
            let mut edges = node.edges.clone();
            if let Some(other) = b.iter().find(|other| other.node == node.node) {
                edges.extend(other.edges.iter().cloned());
                edges.sort_unstable();
                edges.dedup();
            }
            NodeIncidentEdges {
                node: node.node.clone(),
                edges,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edge(src: &str, dst: &str) -> (String, String, String) {
        (src.to_string(), "L".to_string(), dst.to_string())
    }

    #[test]
    fn staged_edges_join_the_stored_edges_of_the_same_node() {
        let stored = vec![NodeIncidentEdges {
            node: "a".to_string(),
            edges: vec![edge("a", "b")],
        }];
        let seen = vec![NodeIncidentEdges {
            node: "a".to_string(),
            edges: vec![edge("a", "b"), edge("c", "a")],
        }];
        assert_eq!(
            union_by_node(&stored, &seen),
            vec![NodeIncidentEdges {
                node: "a".to_string(),
                edges: vec![edge("a", "b"), edge("c", "a")],
            }]
        );
        assert_eq!(union_by_node(&stored, &[]), stored);
    }
}
