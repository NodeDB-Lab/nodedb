// SPDX-License-Identifier: BUSL-1.1

//! Composed, protocol-neutral response shaping: the materialized entry
//! points, the pure decoded-value kernel, and the array-slice envelope.

pub mod array_slice;
pub mod graph_rag;
pub mod kernel;
pub mod materialized;

pub use graph_rag::shape_graph_rag;
pub use kernel::shape_decoded_rows;
pub use materialized::{
    ShapeOutcome, shape_payload_no_plan, shape_response_materialized, shape_returning_materialized,
};
