// SPDX-License-Identifier: BUSL-1.1

//! Convert committed ReplicatedWrite entries back to PhysicalPlan for Data Plane execution.
//!
//! Split by the `PhysicalPlan` family each decode helper produces. The
//! `entry_*` modules hold the per-engine grouped match arm (variant →
//! per-op helper); the sibling modules hold the per-op decoders they call
//! into:
//! - [`entry`]: thin top-level dispatcher (`from_replicated_entry`, `decode_replicated_entry`).
//! - [`ctx`]: shared `DecodeCtx` (tenancy scope).
//! - [`entry_document`] / [`document`] / [`document_join`]: `PhysicalPlan::Document`.
//! - [`entry_array`]: Raft-native array cell writes → `PhysicalPlan::Array`.
//! - [`entry_kv`] / [`kv`] / [`kv_resolved`]: `PhysicalPlan::Kv`.
//! - [`entry_graph`] / [`graph`]: `PhysicalPlan::Graph`.
//! - [`entry_crdt`] / [`crdt`]: `PhysicalPlan::Crdt`.
//! - [`entry_columnar_family`] / [`columnar`]: `PhysicalPlan::Columnar` /
//!   `Timeseries` / `Text` / `Spatial`.
//! - [`vector`]: `PhysicalPlan::Vector` (grouped `decode_arm`).
//! - [`vector_direct`]: the vector-primary `DELETE` / `UPDATE` decoders.
//! - [`transaction_redo`]: a committed transaction's redo entry.

mod columnar;
mod crdt;
mod ctx;
mod document;
mod document_join;
mod entry;
mod entry_array;
mod entry_columnar_family;
mod entry_crdt;
mod entry_document;
mod entry_graph;
mod entry_kv;
mod graph;
mod kv;
mod kv_resolved;
mod transaction_redo;
mod vector;
mod vector_direct;

pub use entry::{decode_parsed_entry, decode_replicated_entry, from_replicated_entry};
pub use entry_graph::edge_write_plan;
pub use transaction_redo::{ChunkedRedo, DecodedTransactionRedo, decode_transaction_redo};
