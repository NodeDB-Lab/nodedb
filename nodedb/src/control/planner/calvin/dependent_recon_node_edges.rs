// SPDX-License-Identifier: BUSL-1.1

//! Incident-edge reconnaissance and task generation for a node delete.
//!
//! A document row of an edge-bearing collection is also a graph node, keyed
//! by the row's identity. Deleting the row tombstones every live edge of the
//! same collection incident on that node, in the delete's own Calvin
//! transaction:
//!
//! - The recon reads the node's incident edges on the node's key vShard,
//!   which holds every edge incident on the node.
//! - The transaction carries one `EdgeDelete` per edge. Each one stamps the
//!   ordinal the transaction decides, identically on both homes of the edge.
//! - The transaction carries one `NodeEdgeGuard` per node, on the node's key
//!   vShard. It refuses the transaction with `OllpRetryRequired` when the
//!   node's live edges differ from the ones the recon read. The coordinator
//!   then reads again and resubmits.
//!
//! Edges of other collections that name the node are not touched.

use std::collections::{BTreeSet, HashMap};

use nodedb_physical::physical_plan::{BatchEdge, GraphOp, PhysicalPlan};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_types::graph::Direction;
use nodedb_types::{QualifiedCollection, Surrogate, SystemTimeScope, Value};

use super::dependent_recon_predicate::EdgeLifecycle;
use super::preexec::{PreexecRequest, PreexecScan, RowIdentityRead, run_preexec_scan};
use crate::control::server::graph_dispatch::read_on_key_owner;
use crate::control::server::surrogate_exchange::assign_surrogates_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, RecordHomes, TenantId, TraceId, TxnId, VShardId};

/// One graph edge as `(src, label, dst)`.
pub type EdgeTriple = (String, String, String);

/// The live edges of one collection incident on one node, as the recon read
/// them. `edges` is sorted and holds no duplicates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeIncidentEdges {
    pub node: String,
    pub edges: Vec<EdgeTriple>,
}

/// Run the dependent write's reconnaissance: the predicate scan, and for a
/// delete, the incident edges of every matched row's node. The initial
/// prediction and every rescan after drift both come from here.
pub(super) async fn reconnoitre(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    filters: Vec<u8>,
    lifecycle: &EdgeLifecycle,
) -> crate::Result<PreexecScan> {
    let identity = match lifecycle {
        EdgeLifecycle::Delete {
            declared_primary_key,
        } => RowIdentityRead::Read {
            declared_primary_key: declared_primary_key.as_deref(),
        },
        EdgeLifecycle::Update(_) => RowIdentityRead::Skip,
    };
    let mut scan = run_preexec_scan(
        state,
        tenant_id,
        database_id,
        PreexecRequest {
            collection,
            filters,
            prefilter: None,
            identity,
            txn_id: None,
        },
    )
    .await?;
    if let EdgeLifecycle::Delete { .. } = lifecycle {
        scan.node_edges = read_node_incident_edges(
            state,
            tenant_id,
            database_id,
            collection,
            &scan.identities,
            None,
        )
        .await?;
    }
    Ok(scan)
}

/// Read the live edges of `collection` incident on each node in `nodes`.
///
/// `collection` is the database-qualified name. Each node's edges are read
/// on the leader of its key vShard, out-edges and in-edges separately. A
/// self-loop appears in both reads and is kept once. With `txn_id`, the read
/// also folds in the edge writes that session transaction staged.
pub(super) async fn read_node_incident_edges(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    nodes: &[String],
    txn_id: Option<TxnId>,
) -> crate::Result<Vec<NodeIncidentEdges>> {
    let mut out = Vec::with_capacity(nodes.len());
    for node in nodes {
        let mut edges = BTreeSet::new();
        for direction in [Direction::Out, Direction::In] {
            // The current-state edge-store read, the source the node's guard
            // checks. The CSR can lag it, and a read that disagrees with the
            // guard refuses every retry.
            let plan = PhysicalPlan::Graph(GraphOp::TemporalNeighbors {
                collection: QualifiedCollection::from_stored(collection.to_string()),
                node_id: node.clone(),
                edge_label: None,
                direction,
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
                rls_filters: Vec::new(),
            });
            let payload =
                read_on_key_owner(state, tenant_id, database_id, node, plan, txn_id, true).await?;
            for (label, other) in decode_neighbors(payload.as_bytes())? {
                edges.insert(match direction {
                    Direction::In => (other, label, node.clone()),
                    Direction::Out | Direction::Both => (node.clone(), label, other),
                });
            }
        }
        out.push(NodeIncidentEdges {
            node: node.clone(),
            edges: edges.into_iter().collect(),
        });
    }
    Ok(out)
}

/// Decode a neighbors payload, a msgpack array of `{label, node}` maps.
fn decode_neighbors(payload: &[u8]) -> crate::Result<Vec<(String, String)>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    let value =
        nodedb_types::value_from_msgpack(payload).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("node delete incident-edge read: {e}"),
        })?;
    let Value::Array(entries) = value else {
        return Err(crate::Error::Serialization {
            format: "msgpack".into(),
            detail: "node delete incident-edge read: payload is not an array".into(),
        });
    };
    let mut out = Vec::with_capacity(entries.len());
    for entry in &entries {
        let Value::Object(fields) = entry else {
            continue;
        };
        if let (Some(label), Some(node)) = (
            fields.get("label").and_then(Value::as_str),
            fields.get("node").and_then(Value::as_str),
        ) {
            out.push((label.to_string(), node.to_string()));
        }
    }
    Ok(out)
}

/// The `(src, label, dst)` of every `EdgeDelete` among `tasks`.
pub(super) fn planned_edge_deletes(tasks: &[PhysicalTask]) -> BTreeSet<EdgeTriple> {
    tasks
        .iter()
        .filter_map(|task| match &task.plan {
            PhysicalPlan::Graph(GraphOp::EdgeDelete {
                src_id,
                label,
                dst_id,
                ..
            }) => Some((src_id.clone(), label.clone(), dst_id.clone())),
            _ => None,
        })
        .collect()
}

/// What a node delete removes and what its guards check.
pub(super) struct NodeDeletePlan<'a> {
    /// The database-qualified collection the rows and edges belong to.
    pub collection: &'a str,
    /// Each node's live edges in the store. One `NodeEdgeGuard` per node
    /// checks the store still holds exactly these.
    pub guarded: &'a [NodeIncidentEdges],
    /// Each node's edges the transaction tombstones: the guarded edges, plus
    /// the edges a session transaction staged itself.
    pub deleted: &'a [NodeIncidentEdges],
    /// Edges another task of the transaction already deletes.
    pub already_deleted: &'a BTreeSet<EdgeTriple>,
}

/// Build the node-delete tasks of `plan`: one `NodeEdgeGuard` per guarded
/// node, and one `EdgeDelete` per deleted edge that `already_deleted` does
/// not hold. Returns `(guards, deletes)`.
///
/// Guards come first in a transaction's task list. A guard compares the
/// store as the transaction found it, before any of the transaction's own
/// deletes.
pub(super) async fn node_delete_tasks(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: NodeDeletePlan<'_>,
) -> crate::Result<(Vec<PhysicalTask>, Vec<PhysicalTask>)> {
    // Every endpoint's surrogate, resolved in one batch. The edge's insert
    // bound each endpoint in the collection, so none is minted here.
    let endpoints: BTreeSet<&str> = plan
        .guarded
        .iter()
        .chain(plan.deleted)
        .flat_map(|node| node.edges.iter())
        .flat_map(|(src, _, dst)| [src.as_str(), dst.as_str()])
        .collect();
    let endpoints: Vec<&str> = endpoints.into_iter().collect();
    let key = nodedb_types::CollectionKey::from_qualified_str(database_id, plan.collection)?;
    let keys: Vec<&[u8]> = endpoints.iter().map(|node| node.as_bytes()).collect();
    let surrogates = assign_surrogates_routed(state, key, tenant_id, &keys, TraceId::ZERO).await?;
    let bound: HashMap<&str, Surrogate> = endpoints.into_iter().zip(surrogates).collect();
    build_node_delete_tasks(tenant_id, database_id, plan, &bound)
}

/// [`node_delete_tasks`] with every endpoint's surrogate in `bound`.
fn build_node_delete_tasks(
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: NodeDeletePlan<'_>,
    bound: &HashMap<&str, Surrogate>,
) -> crate::Result<(Vec<PhysicalTask>, Vec<PhysicalTask>)> {
    let NodeDeletePlan {
        collection,
        guarded,
        deleted: to_delete,
        already_deleted,
    } = plan;
    let surrogate_of = |node: &str| {
        bound
            .get(node)
            .copied()
            .ok_or_else(|| crate::Error::Internal {
                detail: format!("node delete: no surrogate resolved for endpoint '{node}'"),
            })
    };

    let qualified = QualifiedCollection::from_stored(collection.to_string());
    let mut deletes = Vec::new();
    let mut deleted: BTreeSet<&EdgeTriple> = BTreeSet::new();
    for node in to_delete {
        for edge in &node.edges {
            let (src, label, dst) = edge;
            // An edge between two deleted nodes, or one an edge document's
            // own delete already covers, is deleted once.
            if already_deleted.contains(edge) || !deleted.insert(edge) {
                continue;
            }
            let (src_surrogate, dst_surrogate) = (surrogate_of(src)?, surrogate_of(dst)?);
            deletes.push(PhysicalTask {
                tenant_id,
                vshard_id: RecordHomes::edge(src, dst).owner(),
                database_id,
                plan: PhysicalPlan::Graph(GraphOp::EdgeDelete {
                    collection: qualified.clone(),
                    src_id: src.clone(),
                    label: label.clone(),
                    dst_id: dst.clone(),
                    src_surrogate,
                    dst_surrogate,
                    // The node's own delete is what removes the edge, and the
                    // policy on this collection decided that delete before
                    // this task was derived.
                    rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
                }),
                post_set_op: PostSetOp::None,
                txn_id: None,
            });
        }
    }
    let mut guards = Vec::with_capacity(guarded.len());
    for node in guarded {
        let mut expected = Vec::with_capacity(node.edges.len());
        for (src, label, dst) in &node.edges {
            expected.push(BatchEdge {
                collection: qualified.clone(),
                src_id: src.clone(),
                label: label.clone(),
                dst_id: dst.clone(),
                src_surrogate: surrogate_of(src)?,
                dst_surrogate: surrogate_of(dst)?,
                write_gate: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            });
        }
        guards.push(PhysicalTask {
            tenant_id,
            vshard_id: VShardId::from_key(node.node.as_bytes()),
            database_id,
            plan: PhysicalPlan::Graph(GraphOp::NodeEdgeGuard {
                collection: qualified.clone(),
                node_id: node.node.clone(),
                expected,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        });
    }
    Ok((guards, deletes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn triple(src: &str, dst: &str) -> EdgeTriple {
        (src.to_string(), "KNOWS".to_string(), dst.to_string())
    }

    /// A node on another key vShard than `node`.
    fn node_homed_apart_from(node: &str) -> String {
        let home = VShardId::from_key(node.as_bytes());
        (0..)
            .map(|i| format!("peer{i}"))
            .find(|peer| VShardId::from_key(peer.as_bytes()) != home)
            .unwrap_or_default()
    }

    /// A node delete whose incident edge's other endpoint homes on another
    /// vShard guards the node on its key home and deletes the edge from the
    /// owner, and the transaction takes both homes of the edge as
    /// participants. Both homes then apply one `EdgeDelete` under the one
    /// ordinal the transaction decides.
    #[test]
    fn a_cross_shard_incident_edge_is_deleted_on_both_homes() {
        let peer = node_homed_apart_from("alice");
        let nodes = vec![NodeIncidentEdges {
            node: "alice".to_string(),
            edges: vec![triple("alice", &peer)],
        }];
        let bound: HashMap<&str, Surrogate> = HashMap::from([
            ("alice", Surrogate::new(1)),
            (peer.as_str(), Surrogate::new(2)),
        ]);
        let (guards, deletes) = build_node_delete_tasks(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            NodeDeletePlan {
                collection: "users",
                guarded: &nodes,
                deleted: &nodes,
                already_deleted: &BTreeSet::new(),
            },
            &bound,
        )
        .expect("tasks");

        assert_eq!(guards.len(), 1);
        assert_eq!(guards[0].vshard_id, VShardId::from_key(b"alice"));
        let PhysicalPlan::Graph(GraphOp::NodeEdgeGuard { expected, .. }) = &guards[0].plan else {
            panic!("expected a node guard, got {:?}", guards[0].plan);
        };
        assert_eq!(expected.len(), 1);
        assert_eq!(
            (expected[0].src_surrogate, expected[0].dst_surrogate),
            (Surrogate::new(1), Surrogate::new(2))
        );

        assert_eq!(deletes.len(), 1);
        let homes = RecordHomes::edge("alice", &peer);
        assert_eq!(deletes[0].vshard_id, homes.owner());

        let mut tasks = guards;
        tasks.extend(deletes);
        let tx = super::super::build_single_vshard_tx_class(&tasks, TenantId::new(1), &[])
            .expect("tx class");
        let participants: Vec<u32> = tx
            .participating_vshards()
            .iter()
            .map(|v| v.as_u32())
            .collect();
        for home in [homes.owner(), homes.second()] {
            assert!(
                participants.contains(&home.as_u32()),
                "home {home:?} participates: {participants:?}"
            );
        }
    }

    /// An edge between two deleted nodes, or one another task already
    /// deletes, is deleted once. Each node still guards all its edges.
    #[test]
    fn a_shared_or_covered_edge_is_deleted_once() {
        let nodes = vec![
            NodeIncidentEdges {
                node: "a".to_string(),
                edges: vec![triple("a", "b"), triple("a", "c")],
            },
            NodeIncidentEdges {
                node: "b".to_string(),
                edges: vec![triple("a", "b")],
            },
        ];
        let bound: HashMap<&str, Surrogate> = HashMap::from([
            ("a", Surrogate::new(1)),
            ("b", Surrogate::new(2)),
            ("c", Surrogate::new(3)),
        ]);
        let covered = BTreeSet::from([triple("a", "c")]);
        let (guards, deletes) = build_node_delete_tasks(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            NodeDeletePlan {
                collection: "users",
                guarded: &nodes,
                deleted: &nodes,
                already_deleted: &covered,
            },
            &bound,
        )
        .expect("tasks");
        assert_eq!(guards.len(), 2);
        assert_eq!(
            planned_edge_deletes(&deletes),
            BTreeSet::from([triple("a", "b")])
        );
        assert_eq!(deletes.len(), 1);
    }

    /// An endpoint the resolver did not bind is an error, never a zero
    /// surrogate.
    #[test]
    fn an_unbound_endpoint_is_an_error() {
        let nodes = vec![NodeIncidentEdges {
            node: "a".to_string(),
            edges: vec![triple("a", "b")],
        }];
        let bound: HashMap<&str, Surrogate> = HashMap::from([("a", Surrogate::new(1))]);
        assert!(
            build_node_delete_tasks(
                TenantId::new(1),
                DatabaseId::DEFAULT,
                NodeDeletePlan {
                    collection: "users",
                    guarded: &nodes,
                    deleted: &nodes,
                    already_deleted: &BTreeSet::new(),
                },
                &bound,
            )
            .is_err()
        );
    }

    fn entry(label: &str, node: &str) -> Value {
        Value::Object(HashMap::from([
            ("label".to_string(), Value::String(label.to_string())),
            ("node".to_string(), Value::String(node.to_string())),
        ]))
    }

    #[test]
    fn a_neighbors_payload_decodes_to_label_node_pairs() {
        let payload = nodedb_types::value_to_msgpack(&Value::Array(vec![
            entry("KNOWS", "bob"),
            entry("LIKES", "carol"),
        ]))
        .expect("encode");
        assert_eq!(
            decode_neighbors(&payload).expect("decode"),
            vec![
                ("KNOWS".to_string(), "bob".to_string()),
                ("LIKES".to_string(), "carol".to_string()),
            ]
        );
        assert!(decode_neighbors(&[]).expect("empty").is_empty());
    }

    #[test]
    fn a_neighbors_payload_that_is_not_an_array_is_an_error() {
        let payload = nodedb_types::value_to_msgpack(&entry("KNOWS", "bob")).expect("encode");
        assert!(decode_neighbors(&payload).is_err());
    }

    #[test]
    fn planned_edge_deletes_names_only_edge_deletes() {
        let task = |plan| PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        };
        let tasks = vec![
            task(PhysicalPlan::Graph(GraphOp::EdgeDelete {
                collection: QualifiedCollection::from_stored("c".to_string()),
                src_id: "a".to_string(),
                label: "L".to_string(),
                dst_id: "b".to_string(),
                src_surrogate: nodedb_types::Surrogate::new(1),
                dst_surrogate: nodedb_types::Surrogate::new(2),
                rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
            })),
            task(PhysicalPlan::Graph(GraphOp::NodeEdgeGuard {
                collection: QualifiedCollection::from_stored("c".to_string()),
                node_id: "a".to_string(),
                expected: Vec::new(),
            })),
        ];
        assert_eq!(
            planned_edge_deletes(&tasks),
            BTreeSet::from([("a".to_string(), "L".to_string(), "b".to_string())])
        );
    }
}
