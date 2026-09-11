// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane resolution of materialized-sum target rows.
//!
//! A materialized-sum binding joins a source row's `join_column` VALUE to the
//! TARGET collection's primary key. Turning that value into the target row's
//! storage key needs the PK→surrogate map, which lives in the catalog redb —
//! Control-Plane state the Data Plane never opens. So the resolution happens
//! here, at plan time, and travels on the plan in each write op's
//! `resolved_sum_targets` slot.
//!
//! # Plane discipline
//!
//! Runs on the coordinator's Control Plane (Tokio). The routed lookup performs
//! Control-Plane RPC I/O only — no storage I/O, no io_uring, no Data-Plane
//! access from this module.

pub mod cross_shard;
pub mod extract;
pub mod index;
pub mod predicate;
pub mod recon;
pub mod resolve;
pub mod resolve_target;
pub mod settle;
pub mod stored;

pub use cross_shard::append_cross_shard_balance_tasks;
pub use extract::join_value_from_body;
pub use index::MaterializedSumIndex;
pub use resolve::{
    resolve_materialized_sum_targets, resolve_sum_targets_for_bodies, source_drives_bindings,
};
pub use resolve_target::ResolvedTargets;
