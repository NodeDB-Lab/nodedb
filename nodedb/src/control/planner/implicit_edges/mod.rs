// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane implicit graph-edge extraction and lifecycle.
//!
//! A schemaless document carrying the reserved `_from` / `_to` (and optional
//! `_type` / `weight`) fields is mirrored as a graph edge so `MATCH ...` and
//! `GRAPH ALGO ...` see edges inserted via plain document `INSERT`. The
//! mirrored edge is maintained as a transactionally-consistent secondary index
//! in the SAME distributed (Calvin) transaction as the document write.
//!
//! This extraction runs on the Control Plane — BEFORE dispatch classification —
//! so an implicit edge routes through the SAME path as an explicit
//! `GRAPH INSERT EDGE` (`GraphOp::EdgePut`): single-home Raft dispatch when src
//! and dst share a home vShard, Calvin dual-home when they straddle a shard
//! boundary. Resolving each endpoint's home vShard and canonical surrogate here
//! makes implicit edges identical to explicit ones.
//!
//! # Plane discipline
//!
//! Runs on the coordinator's Control Plane (Tokio). `assign_surrogate_routed`
//! performs Control-Plane RPC I/O only — no storage I/O, no io_uring, no
//! Data-Plane access from this module.

pub mod catalog;
pub mod delete;
pub mod extract;
pub mod insert;
#[cfg(test)]
mod insert_tests;
mod routed;
pub mod update;

pub use catalog::mark_collection_edge_bearing;
pub use delete::append_implicit_edge_delete_tasks;
pub use insert::{append_implicit_edge_tasks, append_implicit_edge_tasks_traced};
pub use update::{
    EdgeFieldOverrides, EdgeUpdateCtx, FieldUpdate, WeightUpdate,
    append_implicit_edge_update_tasks, parse_edge_field_overrides,
};
