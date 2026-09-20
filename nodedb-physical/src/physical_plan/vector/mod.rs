// SPDX-License-Identifier: Apache-2.0

//! Vector engine operations dispatched to the Data Plane.

pub mod collection;
pub mod op;
pub mod resolved_mutation;
pub mod write;

pub use op::VectorOp;
pub use resolved_mutation::{VectorResolveOutcome, VectorResolvedMutation};
pub use write::{VectorDirectWriteIntent, VectorWriteTargets};
