// SPDX-License-Identifier: BUSL-1.1

//! Graph walk response shaping: turn the node names a walk reached into
//! rows before the protocol layer shapes them.
//!
//! A `Hop` answers a msgpack array of every reached node name. A `Path`
//! answers a msgpack array `[src, …, dst]`. The Data Plane and the
//! cross-shard walk coordinators (`graph_dispatch::walk_reads`) both answer
//! this shape. A bare name is not a row, and the generic row flattener
//! (`push_flat_rows`) drops every scalar. So each name becomes one
//! `{node}` row here, in the order the walk gave it.

use std::collections::HashMap;

use crate::bridge::envelope::PhysicalPlan;
use crate::data::executor::response_codec::decode_payload_value;
use nodedb_physical::physical_plan::GraphOp;
use nodedb_types::Value;

/// The column a walk row names its node under.
pub const WALK_NODE_COLUMN: &str = "node";

/// When `plan` is a `Hop` or a `Path`, return its node names as `{node}`
/// rows, msgpack-encoded. Every other plan's payload is returned unchanged.
///
/// A walk payload that is not an array of names is an error: a walk row
/// is never dropped.
pub fn apply_walk_wrap(plan: &PhysicalPlan, payload: &[u8]) -> crate::Result<Vec<u8>> {
    let is_walk = matches!(
        plan,
        PhysicalPlan::Graph(GraphOp::Hop { .. } | GraphOp::Path { .. })
    );
    if !is_walk || payload.is_empty() {
        return Ok(payload.to_vec());
    }
    let Value::Array(names) = decode_payload_value(payload)? else {
        return Err(crate::Error::Codec {
            detail: "graph walk response is not an array of node names".into(),
        });
    };
    let rows = names
        .into_iter()
        .map(|name| match name {
            Value::String(node) => {
                let mut row = HashMap::with_capacity(1);
                row.insert(WALK_NODE_COLUMN.to_string(), Value::String(node));
                Ok(Value::Object(row))
            }
            other => Err(crate::Error::Codec {
                detail: format!("graph walk response holds a non-name entry: {other:?}"),
            }),
        })
        .collect::<crate::Result<Vec<Value>>>()?;
    nodedb_types::value_to_msgpack(&Value::Array(rows)).map_err(|e| crate::Error::Codec {
        detail: format!("graph walk rows encode: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::response_shape::compose::{ShapeOutcome, shape_payload_no_plan};
    use crate::control::server::response_shape::types::PlanKind;
    use nodedb_graph::{Direction, GraphTraversalOptions};

    fn hop() -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::Hop {
            collection: None,
            start_nodes: vec!["a".into()],
            depth: 2,
            edge_labels: Vec::new(),
            direction: Direction::Out,
            options: GraphTraversalOptions::default(),
            rls_filters: Vec::new(),
            frontier_bitmap: None,
        })
    }

    /// Every reached name becomes one row, in walk order, and the shaped
    /// rows keep all of them.
    #[test]
    fn every_reached_name_is_a_row() {
        let names = vec!["a".to_string(), "c".to_string(), "b".to_string()];
        let payload = zerompk::to_msgpack_vec(&names).expect("names encode");
        let wrapped = apply_walk_wrap(&hop(), &payload).expect("walk wraps");
        let Ok(ShapeOutcome::Rows(shaped)) =
            shape_payload_no_plan(&wrapped, PlanKind::MultiRow, None, None, None)
        else {
            panic!("a walk shapes into rows");
        };
        let got: Vec<Value> = shaped
            .rows
            .iter()
            .filter_map(|row| row.get(WALK_NODE_COLUMN).cloned())
            .collect();
        assert_eq!(
            got,
            names.into_iter().map(Value::String).collect::<Vec<_>>()
        );
    }

    /// A walk payload that is not a name array is refused, never dropped.
    #[test]
    fn a_non_name_entry_is_refused() {
        let payload = zerompk::to_msgpack_vec(&vec![1u32]).expect("encode");
        assert!(apply_walk_wrap(&hop(), &payload).is_err());
    }
}
