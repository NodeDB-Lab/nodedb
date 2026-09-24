// SPDX-License-Identifier: Apache-2.0

//! KV engine operations dispatched to the Data Plane.

pub mod collection;
pub mod counter_shape;
pub mod op;
pub mod resolved_mutation;
pub mod sorted_read;

pub use counter_shape::KvCounterShape;
pub use op::KvOp;
pub use resolved_mutation::{KvResolveOutcome, KvResolvedMutation};
pub use sorted_read::{SortedIndexRead, SortedIndexSpec};
