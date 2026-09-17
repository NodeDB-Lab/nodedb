// SPDX-License-Identifier: BUSL-1.1

//! Pass 2 of plan resolution: Exchange node resolution.
//!
//! - `Gather{as_aggregate}` at the plan root → fan child to all vShards,
//!   merge, and return `Resolved::Gathered`.
//! - `Broadcast` inside a `HashJoin.left_input` / `right_input` →
//!   gather child to coordinator, encode as a merged msgpack array, and
//!   embed as `ProviderScan{provider: None, rows}`.  The modified join is
//!   self-contained and returned as `Resolved::Plan`.
//! - Root `Shuffle{keys, num_parts}` wrapping a `HashJoin` → orchestrate a
//!   cross-node grace hash join (`super::shuffle`) and return the merged rows
//!   as `Resolved::Gathered`. `Shuffle` as a join INPUT is a typed error (it
//!   only ever wraps a complete join).
//! - `Aggregate{input: Some}` whose child is not yet materialized rows →
//!   materialize the child on the coordinator and embed it as
//!   `ProviderScan{provider: None, rows}`; return `Resolved::Plan`.
//! - No Exchange / no empty ProviderScan → `Resolved::Plan` unchanged.

mod aggregate_input_arm;
mod dispatch;
mod entry;
mod gather_arm;
mod hash_join_arm;
mod post_process_arm;
mod set_op_arm;
mod shuffle_arm;

pub use entry::{Resolved, resolve_and_materialize, resolve_exchange_in_plan};
pub(crate) use post_process_arm::provider_scan_of_rows;
