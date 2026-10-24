// SPDX-License-Identifier: BUSL-1.1

//! Implicit-edge INSERT lifecycle: scan document-write tasks for `_from`/`_to`
//! documents and append the mirrored `GraphOp::EdgePut` tasks.

use nodedb_physical::physical_plan::{DocumentOp, GraphOp, PhysicalPlan};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use std::collections::{HashMap, HashSet};

use nodedb_types::Surrogate;

use super::catalog::mark_collection_edge_bearing;
use super::extract::{extract_edge, weight_properties};
use crate::control::server::surrogate_exchange::assign_surrogates_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, RecordHomes, TenantId, TraceId};

/// Scan the current document-write tasks for `_from` / `_to` documents and
/// append a `GraphOp::EdgePut` task per implicit edge.
///
/// Each appended task is built exactly like an explicit `GRAPH INSERT EDGE`:
/// the edge is homed on `from_key(_from)` with both endpoints' canonical
/// surrogates resolved via the routed surrogate exchange, so the downstream
/// classify/Calvin/single-shard logic dual-homes cross-shard edges and
/// single-homes same-shard edges identically to explicit edges.
///
/// A mirrored edge carries NO row-level-security write gate, and must not: it
/// is derived from a `DocumentOp` write on the same collection, that write was
/// already decided against the collection's write policy before this runs, and
/// a denial there fails the statement before any mirror is derived. Deciding
/// the mirror as well will refuse every governed document insert on the
/// strength of its own edge, whose property object holds a weight and none of
/// the columns a policy names.
pub async fn append_implicit_edge_tasks(
    state: &SharedState,
    tasks: &mut Vec<PhysicalTask>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<()> {
    // Collect a SNAPSHOT of candidate edges first so the immutable scan of
    // `tasks` does not borrow-conflict with the `&mut Vec` we push into below.
    let mut edges = Vec::new();
    for task in tasks.iter() {
        match &task.plan {
            PhysicalPlan::Document(DocumentOp::PointInsert {
                collection, value, ..
            })
            | PhysicalPlan::Document(DocumentOp::Upsert {
                collection, value, ..
            }) => {
                if let Some(edge) = extract_edge(collection.as_str(), value) {
                    edges.push(edge);
                }
            }
            PhysicalPlan::Document(
                DocumentOp::BatchInsert {
                    collection,
                    documents,
                    ..
                }
                | DocumentOp::BatchUpsert {
                    collection,
                    documents,
                    ..
                },
            ) => {
                for (_doc_id, value) in documents {
                    if let Some(edge) = extract_edge(collection.as_str(), value) {
                        edges.push(edge);
                    }
                }
            }
            // Every other plan (other DocumentOp variants, and non-Document
            // plans) carries no implicit edge — intentionally skipped.
            _ => {}
        }
    }

    // Flag each DISTINCT edge-bearing collection exactly once. Only runs when at
    // least one implicit edge was found, so non-edge inserts do zero catalog
    // work. The mark is idempotent and skips the Raft write when already set.
    let mut marked: Vec<&str> = Vec::new();
    for edge in &edges {
        if !marked.contains(&edge.collection.as_str()) {
            marked.push(edge.collection.as_str());
            mark_collection_edge_bearing(state, database_id, tenant_id, &edge.collection).await?;
        }
    }

    // Both endpoints' surrogates come from the collection home, where every
    // key of the collection is minted. Every endpoint of one collection
    // resolves in one routed batch.
    let mut resolved: HashMap<String, HashMap<String, Surrogate>> = HashMap::new();
    for &collection in &marked {
        // `edge.collection` is the plan's database-qualified name.
        let key = nodedb_types::CollectionKey::from_qualified_str(database_id, collection)?;
        let mut endpoints: Vec<&str> = Vec::new();
        let mut seen: HashSet<&str> = HashSet::new();
        for edge in edges.iter().filter(|edge| edge.collection == collection) {
            for id in [edge.src.as_str(), edge.dst.as_str()] {
                if seen.insert(id) {
                    endpoints.push(id);
                }
            }
        }
        let pks: Vec<&[u8]> = endpoints.iter().map(|id| id.as_bytes()).collect();
        let bound = assign_surrogates_routed(state, key, tenant_id, &pks, trace_id).await?;
        if bound.len() != endpoints.len() {
            return Err(crate::Error::Internal {
                detail: format!(
                    "implicit edges of '{collection}': {} of {} endpoints got a surrogate",
                    bound.len(),
                    endpoints.len()
                ),
            });
        }
        resolved.insert(
            collection.to_string(),
            endpoints
                .into_iter()
                .map(str::to_string)
                .zip(bound)
                .collect(),
        );
    }
    drop(marked);

    for edge in edges {
        // The write routes to the source endpoint's home.
        let vsrc = RecordHomes::edge(&edge.src, &edge.dst).owner();
        let endpoint = |id: &str| {
            resolved
                .get(&edge.collection)
                .and_then(|by_id| by_id.get(id))
                .copied()
                .ok_or_else(|| crate::Error::Internal {
                    detail: format!(
                        "implicit edge endpoint '{id}' of '{}' got no surrogate",
                        edge.collection
                    ),
                })
        };
        let src_surrogate = endpoint(&edge.src)?;
        let dst_surrogate = endpoint(&edge.dst)?;

        let properties = match edge.weight {
            Some(w) => weight_properties(w),
            None => Vec::new(),
        };

        tasks.push(PhysicalTask {
            tenant_id,
            vshard_id: vsrc,
            database_id,
            plan: PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: nodedb_types::QualifiedCollection::from_stored(edge.collection),
                src_id: edge.src,
                label: edge.label,
                dst_id: edge.dst,
                properties,
                src_surrogate,
                dst_surrogate,
                // An implicit edge takes its write's own valid time.
                valid_from_ms: None,
                valid_until_ms: None,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        });
    }

    Ok(())
}
