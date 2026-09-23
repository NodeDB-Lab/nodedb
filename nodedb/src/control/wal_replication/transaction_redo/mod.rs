// SPDX-License-Identifier: BUSL-1.1

//! Committed-transaction redo replication: one apply log per vShard.
//!
//! - [`payload`]: the redo a commit hands to the data-group log.
//! - [`apply`]: the one path that appends and applies it on a node.
//! - [`collections`] / [`sum_targets`]: what the payload derives from the
//!   commit's buffered plans.

pub mod apply;
pub mod collections;
pub mod payload;
pub mod sum_targets;

pub use apply::RedoTarget;
pub(crate) use apply::apply_transaction_redo;
pub use payload::TransactionRedoPayload;
