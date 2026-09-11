// SPDX-License-Identifier: Apache-2.0

//! Shared physical-plan layer for NodeDB.
//!
//! Owns the `PhysicalTask` IR and the `SqlPlan → PhysicalPlan` converter.
//! Origin (server) and Lite (embedded) both consume the same `PhysicalTask`;
//! per-deployment executors implement `PhysicalTaskVisitor` against their own
//! storage backends. Origin-specific concerns (vShard routing, MessagePack
//! pre-serialisation, cross-plane envelope fields) live in an Origin-side
//! wrapper that contains a `PhysicalTask`, not in this crate.

pub mod convert_context;
pub mod error;
pub mod physical_plan;
pub mod physical_task;
pub mod surrogate;
pub mod visitor;

pub use convert_context::SharedConvertContext;
pub use error::ConvertError;
pub use surrogate::{SurrogateAssignError, SurrogateAssigner};
pub use visitor::{PhysicalTaskVisitor, dispatch};
