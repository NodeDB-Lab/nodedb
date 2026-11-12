// SPDX-License-Identifier: Apache-2.0

// Library code propagates errors; tests may unwrap.
#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! Shared physical-plan layer for NodeDB.
//!
//! Owns the `PhysicalTask` IR and the `SqlPlan → PhysicalPlan` converter.
//! Origin (server) and Lite (embedded) both consume the same `PhysicalTask`;
//! per-deployment executors implement `PhysicalTaskVisitor` against their own
//! storage backends. Origin-specific concerns (vShard routing, MessagePack
//! pre-serialisation, cross-plane envelope fields) live in an Origin-side
//! wrapper that contains a `PhysicalTask`, not in this crate.

pub mod error;
pub mod kv_atomic;
pub mod physical_plan;
pub mod physical_task;
pub mod visitor;

pub use error::ConvertError;
pub use visitor::{PhysicalTaskVisitor, dispatch};
