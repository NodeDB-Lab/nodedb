// SPDX-License-Identifier: BUSL-1.1

//! Write keys of a RESTORE batch.
//!
//! A restored row locks what every point writer of the row locks: its
//! surrogate and its row id. A restored KV key locks its key, as every KV
//! point writer locks it. A restored edge version locks what every edge
//! write locks: its surrogate pair and both endpoints' node lock pairs, on
//! both endpoint homes. Restored node labels lock what every label write of
//! the node locks. So a restored write orders against every other writer of
//! the same row, edge or node labels, and against a node delete's guard.
//! Restored array cells lock the array on the batch's vShard, as every array
//! cell write locks it. A restored columnar key locks its row surrogate,
//! which every columnar writer's whole-collection lock covers. A restored
//! vector row locks its surrogate, as every vector point writer locks it.
//! Restored timeseries rows lock their collection whole, as every
//! timeseries writer locks it: a timeseries row has no key.

use nodedb_physical::physical_plan::RestoredRedo;

use super::graph::NODE_LABEL_LOCKS;
use super::plan::unsequenced;
use super::set::WriteKeys;

/// Add the write keys of the RESTORE batch `batch` to `keys`.
pub(super) fn add_keys(keys: &mut WriteKeys, batch: &RestoredRedo) -> crate::Result<()> {
    if batch.rows.is_empty()
        && batch.kv_keys.is_empty()
        && batch.edges.is_empty()
        && batch.node_labels.is_empty()
        && batch.arrays.is_empty()
        && batch.columnar_keys.is_empty()
        && batch.vector_rows.is_empty()
        && batch.timeseries.is_empty()
    {
        return Err(unsequenced(
            "a RESTORE batch carries no row, no KV key, no edge, no node label, no array cell, \
             no columnar key, no vector row and no timeseries row",
        ));
    }
    for row in &batch.rows {
        keys.rows(&row.collection, [row.surrogate]);
        keys.row_id(&row.collection, &row.document_id);
    }
    for kv in &batch.kv_keys {
        keys.kv_keys(&kv.collection, [kv.key.clone()]);
    }
    for key in &batch.columnar_keys {
        keys.rows(&key.collection, [key.surrogate]);
    }
    for row in &batch.vector_rows {
        keys.vector_rows(&row.collection, [row.surrogate]);
    }
    for collection in &batch.timeseries {
        keys.whole_collection(collection);
    }
    for edge in &batch.edges {
        keys.edge(
            &edge.collection,
            &edge.src_id,
            &edge.dst_id,
            (edge.src_surrogate, edge.dst_surrogate),
        );
    }
    for node in &batch.node_labels {
        keys.node(NODE_LABEL_LOCKS, &node.node_id);
    }
    for array in &batch.arrays {
        let collection = nodedb_types::QualifiedCollection::new(
            array.array_id.database_id,
            &array.array_id.name,
        );
        keys.array_cells(collection.as_str(), batch.vshard);
    }
    Ok(())
}
