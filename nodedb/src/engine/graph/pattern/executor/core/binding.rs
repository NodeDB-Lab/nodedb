// SPDX-License-Identifier: BUSL-1.1

//! Node-binding resolution and compatibility checks shared by triple
//! evaluation and continuation resumption.

use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::pattern::ast::NodeBinding;
use crate::engine::graph::pattern::executor::row::{Bound, bind_text, lookup};
use crate::engine::graph::pattern::executor::types::BindingRow;

/// Resolve a node binding to the set of candidate node ids.
///
/// If `binding.name` is already bound in `row`, returns the single resolved
/// id (or empty if the label constraint fails). Otherwise enumerates all
/// nodes, filtered by label and the optional frontier bitmap.
pub(super) fn resolve_binding(
    binding: &NodeBinding,
    csr: &CsrIndex,
    row: &BindingRow,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
) -> Vec<u32> {
    if let Some(ref name) = binding.name {
        let value = match lookup(row, name) {
            Bound::Unbound => None,
            // A variable bound to NULL matched nothing: no node continues it.
            Bound::Null => return Vec::new(),
            Bound::Text(text) => Some(text),
        };
        if let Some(value) = value {
            return resolve_bound(binding, csr, value);
        }
    }
    // No binding yet — enumerate all nodes, filtering by label and bitmap.
    let all = 0..csr.node_count() as u32;
    all.filter(|&id| {
        let label_ok = binding
            .label
            .as_ref()
            .is_none_or(|l| csr.node_has_label(id, l));
        let bitmap_ok = frontier_bitmap
            .is_none_or(|bm| bm.contains(nodedb_types::Surrogate::new(csr.node_surrogate_raw(id))));
        label_ok && bitmap_ok
    })
    .collect()
}

/// The single node `value` names, when it passes the label constraint.
fn resolve_bound(binding: &NodeBinding, csr: &CsrIndex, value: &str) -> Vec<u32> {
    match csr.node_id_raw(value) {
        Some(id)
            if binding
                .label
                .as_ref()
                .is_none_or(|label| csr.node_has_label(id, label)) =>
        {
            vec![id]
        }
        _ => Vec::new(),
    }
}

pub(in crate::engine::graph::pattern::executor) fn binding_compatible(
    binding: &NodeBinding,
    csr: &CsrIndex,
    row: &BindingRow,
    node_id: u32,
) -> bool {
    // Check label constraint.
    if let Some(ref label) = binding.label
        && !csr.node_has_label(node_id, label)
    {
        return false;
    }
    if let Some(ref name) = binding.name {
        return match lookup(row, name) {
            Bound::Unbound => true,
            Bound::Null => false,
            Bound::Text(existing) => existing == csr.node_name_raw(node_id),
        };
    }
    true
}

pub(in crate::engine::graph::pattern::executor) fn bind_node(
    row: &mut BindingRow,
    binding: &NodeBinding,
    csr: &CsrIndex,
    node_id: u32,
) {
    if let Some(ref name) = binding.name {
        bind_text(row, name, csr.node_name_raw(node_id));
    }
}
