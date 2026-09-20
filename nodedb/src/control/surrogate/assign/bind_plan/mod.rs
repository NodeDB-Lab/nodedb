// SPDX-License-Identifier: BUSL-1.1

//! Install every `(collection, pk) → surrogate` identity a `PhysicalPlan`
//! carries into this node's catalog, first-wins, and rewrite the plan with the
//! authoritative surrogate.
//!
//! One walk over the plan serves both apply seams — the replicated-write
//! decoder and the Calvin scheduler dispatch — so neither can bind a subset the
//! other misses.

mod array;
mod binder;
mod crdt;
mod document;
mod graph;
mod kv;
mod vector;

pub use binder::{IdentityBinder, bind_plan_identities};
