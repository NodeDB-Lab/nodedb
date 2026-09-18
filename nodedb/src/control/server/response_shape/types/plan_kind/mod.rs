// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral plan classification.
//!
//! One file per engine so every `*Op` enum is matched exhaustively in one
//! place and a new variant fails to compile until it is classified.

mod array;
mod columnar_family;
mod crdt;
mod describe;
mod document;
mod graph;
mod kind;
mod kv;
mod query;
mod search;

pub use describe::describe_plan;
pub use kind::PlanKind;
