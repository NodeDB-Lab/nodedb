//! Origin-specific CSR rebuild from EdgeStore.

#[cfg(test)]
use nodedb_graph::CsrIndex;
use nodedb_graph::ShardedCsrIndex;
use nodedb_graph::csr::weights::extract_weight_from_properties;
#[cfg(test)]
use nodedb_types::TenantId;

use crate::engine::graph::edge_store::EdgeStore;

/// Rebuild the sharded CSR index from an EdgeStore.
pub fn rebuild_sharded_from_store(store: &EdgeStore) -> crate::Result<ShardedCsrIndex> {
    let mut sharded = ShardedCsrIndex::new();
    let all_edges = store.scan_all_edges_decoded()?;

    // First pass: materialize every (tenant, node) so isolated endpoints
    // get stable node ids before edge insertion.
    for (tid, src, _label, dst, _props) in &all_edges {
        let partition = sharded.get_or_create(*tid);
        partition.add_node(src);
        partition.add_node(dst);
    }

    // Second pass: insert edges into their tenant's partition.
    for (tid, src, label, dst, props) in &all_edges {
        let partition = sharded.get_or_create(*tid);
        let weight = extract_weight_from_properties(props);
        let res = if weight != 1.0 {
            partition.add_edge_weighted(src, label, dst, weight)
        } else {
            partition.add_edge(src, label, dst)
        };
        res.map_err(|e| crate::Error::Internal {
            detail: format!("CSR rebuild: {e}"),
        })?;
    }

    sharded.compact_all();
    Ok(sharded)
}

/// Test shim: collapse the sharded rebuild into a single `CsrIndex`.
/// Used by test harnesses that insert under one tenant at a time.
#[cfg(test)]
pub fn rebuild_from_store(store: &EdgeStore) -> crate::Result<CsrIndex> {
    use std::collections::hash_map::Entry;

    let mut sharded = rebuild_sharded_from_store(store)?;
    let tid = sharded
        .iter()
        .map(|(tid, _)| *tid)
        .next()
        .unwrap_or_else(|| TenantId::new(0));
    match sharded.entry(tid) {
        Entry::Occupied(entry) => Ok(entry.remove()),
        Entry::Vacant(_) => Ok(CsrIndex::new()),
    }
}
