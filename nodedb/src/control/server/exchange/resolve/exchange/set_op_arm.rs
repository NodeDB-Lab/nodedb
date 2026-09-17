// SPDX-License-Identifier: BUSL-1.1

//! `SetOp` exchange resolution: materialize every branch on the coordinator,
//! merge the rows with the set operation, and lower to a `ProviderScan`.

use nodedb_physical::physical_plan::{PhysicalPlan, SetOpKind};

use crate::control::server::exchange::resolve::capture::DistributedReadCapture;
use crate::control::server::payload_merge::merge_msgpack_arrays;
use crate::control::server::set_op_merge::{
    SetMergeMode, dedup_union_payloads, merge_set_op_payloads,
};
use crate::control::state::SharedState;

use super::dispatch::ResolveCtx;
use super::entry::Resolved;
use super::post_process_arm::{ChildRows, materialize_child_rows, provider_scan_of_rows};

/// Resolve a `QueryOp::SetOp` node.
///
/// Every branch is an independent body, so all of them materialize
/// concurrently, each into its own read-capture list. The captures are then
/// appended to `captures` in branch order, so the in-transaction read-set
/// sees every branch's base collection exactly once. A branch that resolves
/// to a root `Gathered` / `Stream` result is returned as-is, the way the
/// post-processor returns it.
///
/// The merged rows are embedded as a `ProviderScan{provider: None}` with an
/// empty relational tail; the enclosing post-processor or input-sourced
/// aggregate supplies its own tail over these rows.
pub(super) async fn resolve_set_op(
    state: &SharedState,
    ctx: ResolveCtx,
    captures: &mut Vec<DistributedReadCapture>,
    inputs: Vec<PhysicalPlan>,
    op: SetOpKind,
) -> crate::Result<Resolved> {
    let branches = inputs.into_iter().map(|input| async move {
        let mut branch_captures = Vec::new();
        let rows = materialize_child_rows(state, ctx, &mut branch_captures, input).await?;
        Ok::<_, crate::Error>((rows, branch_captures))
    });
    let materialized = futures::future::try_join_all(branches).await?;

    let mut payloads = Vec::with_capacity(materialized.len());
    for (rows, branch_captures) in materialized {
        captures.extend(branch_captures);
        match rows {
            ChildRows::Rows(rows) => payloads.push(rows),
            ChildRows::Passthrough(resolved) => return Ok(resolved),
        }
    }

    let merged = merge_set_op_rows(&payloads, op);
    Ok(Resolved::Plan(Box::new(provider_scan_of_rows(merged))))
}

/// Merge materialized branch payloads with `op`. Each payload is one msgpack
/// array of flat row maps. The mapping from kind to merge mirrors the pgwire
/// per-task set-op path: `INTERSECT [ALL]` and `EXCEPT [ALL]` share one
/// value-keyed merge each, `UNION` dedups on raw bytes, and `UNION ALL`
/// concatenates.
fn merge_set_op_rows(payloads: &[Vec<u8>], op: SetOpKind) -> Vec<u8> {
    match op {
        SetOpKind::UnionAll => merge_msgpack_arrays(payloads),
        SetOpKind::UnionDistinct => dedup_union_payloads(payloads),
        SetOpKind::Intersect | SetOpKind::IntersectAll => {
            merge_set_op_payloads(payloads, SetMergeMode::Intersect)
        }
        SetOpKind::Except | SetOpKind::ExceptAll => {
            merge_set_op_payloads(payloads, SetMergeMode::Except)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::merge_set_op_rows;
    use nodedb_physical::physical_plan::SetOpKind;

    fn encode_array(rows: &[serde_json::Value]) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::Value::Array(rows.to_vec())).unwrap()
    }

    fn decode(payload: &[u8]) -> String {
        crate::data::executor::response_codec::decode_payload_to_json(payload)
    }

    #[test]
    fn union_all_keeps_every_row_in_branch_order() {
        let left = encode_array(&[serde_json::json!({"x": 1}), serde_json::json!({"x": 2})]);
        let right = encode_array(&[serde_json::json!({"x": 2})]);
        let merged = merge_set_op_rows(&[left, right], SetOpKind::UnionAll);
        assert_eq!(decode(&merged), r#"[{"x":1},{"x":2},{"x":2}]"#);
    }

    #[test]
    fn union_distinct_drops_duplicates() {
        let left = encode_array(&[serde_json::json!({"x": 1}), serde_json::json!({"x": 2})]);
        let right = encode_array(&[serde_json::json!({"x": 2})]);
        let merged = merge_set_op_rows(&[left, right], SetOpKind::UnionDistinct);
        assert_eq!(decode(&merged), r#"[{"x":1},{"x":2}]"#);
    }

    #[test]
    fn intersect_keeps_rows_present_in_every_branch() {
        let left = encode_array(&[serde_json::json!({"x": 1}), serde_json::json!({"x": 2})]);
        let right = encode_array(&[serde_json::json!({"x": 2}), serde_json::json!({"x": 3})]);
        let merged = merge_set_op_rows(&[left, right], SetOpKind::Intersect);
        assert_eq!(decode(&merged), r#"[{"x":2}]"#);
    }

    #[test]
    fn except_drops_rows_present_in_later_branches() {
        let left = encode_array(&[serde_json::json!({"x": 1}), serde_json::json!({"x": 2})]);
        let right = encode_array(&[serde_json::json!({"x": 2})]);
        let merged = merge_set_op_rows(&[left, right], SetOpKind::Except);
        assert_eq!(decode(&merged), r#"[{"x":1}]"#);
    }
}
