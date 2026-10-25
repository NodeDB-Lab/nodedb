// SPDX-License-Identifier: BUSL-1.1

//! The edges a graph algorithm runs over, and the CSR built from them.
//!
//! The local stage reads this core's edge store. The export stage answers
//! those edges so the coordinator can gather them from every core. The
//! gathered stage builds its CSR from the union. All three go through
//! [`csr_from_edges`], so a CSR built from the same edges in the same order
//! is the same CSR on every path.

use nodedb_graph::CsrIndex;
use nodedb_graph::csr::weights::extract_weight_from_properties;
use nodedb_physical::physical_plan::{AlgoEdge, GraphReadScope};
use nodedb_types::{DatabaseId, QualifiedCollection, TenantId};

use crate::engine::graph::edge_store::EdgeStore;

/// The edge-store name of an algorithm's collection in `database`.
/// `AlgoParams::collection` names it bare; an already qualified name stays
/// as it is.
pub(in crate::data::executor) fn algo_collection(database: DatabaseId, collection: &str) -> String {
    let bare = QualifiedCollection::from_stored(collection.to_string())
        .collection_name(database)
        .to_string();
    QualifiedCollection::new(database, &bare)
        .as_str()
        .to_string()
}

/// The system-time ordinal an algorithm reads at `scope`. `AllVersions` has
/// no single graph to run over.
pub(in crate::data::executor) fn algo_cutoff(scope: &GraphReadScope) -> crate::Result<i64> {
    scope
        .cutoff_ordinal()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: nodedb_physical::physical_plan::ALL_VERSIONS_REFUSED.into(),
        })
}

/// The edges of `(database, tid, collection)` in this core's edge store at
/// `scope`, optionally one label only, in edge-store order.
pub(super) fn collection_edges(
    edge_store: &EdgeStore,
    database_id: u64,
    tid: u64,
    collection: &str,
    edge_label: Option<&str>,
    scope: &GraphReadScope,
) -> crate::Result<Vec<AlgoEdge>> {
    let collection = algo_collection(DatabaseId::new(database_id), collection);
    let records = edge_store.scan_collection_as_of(
        database_id,
        TenantId::new(tid),
        &collection,
        edge_label,
        algo_cutoff(scope)?,
        scope.valid_at_ms,
    )?;
    Ok(records
        .into_iter()
        .map(|edge| AlgoEdge {
            weight: extract_weight_from_properties(&edge.properties),
            src: edge.src,
            label: edge.label,
            dst: edge.dst,
        })
        .collect())
}

/// Build a CSR from `edges`, in their order.
///
/// Two passes: first intern every endpoint so isolated nodes get stable ids,
/// then insert the edges. This matches `CsrSnapshot::from_edge_store_as_of`.
pub(super) fn csr_from_edges(
    edges: &[AlgoEdge],
    memory: nodedb_mem::ScopedMemory,
) -> crate::Result<CsrIndex> {
    let mut csr = CsrIndex::new(memory);
    for edge in edges {
        csr.add_node(&edge.src)?;
        csr.add_node(&edge.dst)?;
    }
    for edge in edges {
        if edge.weight != 1.0 {
            csr.add_edge_weighted(&edge.src, &edge.label, &edge.dst, edge.weight)?;
        } else {
            csr.add_edge(&edge.src, &edge.label, &edge.dst)?;
        }
    }
    csr.compact()?;
    Ok(csr)
}
