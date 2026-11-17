// SPDX-License-Identifier: BUSL-1.1

//! Resolve and apply for a governed point/bulk document write.

pub(in crate::data::executor) mod apply;
mod apply_row;
mod apply_state;
mod bulk;
mod context;
pub(in crate::data::executor) mod dispatch;
mod point;
mod txn_rows;
mod upsert;
mod upsert_batch;

#[cfg(test)]
mod tests;
