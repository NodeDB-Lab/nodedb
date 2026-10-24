// SPDX-License-Identifier: Apache-2.0

//! Reductions over a sparse tile.
//!
//! Each reducer folds one attribute column into an [`AggregateResult`]
//! partial. Partials merge exactly across tiles, cores and shards, so a
//! merged result equals a single pass over every cell.
//!
//! Group-by ([`group_by_dim`]) buckets cells by one dim's values and
//! returns the groups in first-seen order.

pub mod fold;
pub mod reducer;
pub mod result;

pub use fold::{AggRead, GroupAggregate, aggregate_attr, group_by_dim};
pub use reducer::Reducer;
pub use result::{AggOutput, AggregateResult};
