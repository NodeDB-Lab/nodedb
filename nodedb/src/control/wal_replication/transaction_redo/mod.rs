// SPDX-License-Identifier: BUSL-1.1

//! Committed-transaction redo replication: one apply log per vShard.
//!
//! - [`payload`]: the redo a commit hands to the data-group log.
//! - [`apply`]: the one path that appends and applies it on a node.
//! - [`chunks`]: the streams a redo past the entry limit travels in.
//! - [`propose`]: the proposal of a committed redo, inline or chunked.
//! - [`abandon`]: the retry of a failed stream abandon.
//! - [`collections`] / [`sum_targets`]: what the payload derives from the
//!   commit's buffered plans.

pub mod abandon;
pub mod apply;
pub mod chunks;
pub mod collections;
pub mod payload;
pub mod propose;
pub mod sum_targets;

pub(crate) use abandon::spawn_redo_abandoner;
pub use apply::RedoTarget;
pub(crate) use apply::{enqueue_transaction_redo, record_cross_shard_key};
pub use chunks::{RedoChunkError, RedoChunkStore};
pub use payload::TransactionRedoPayload;
pub(crate) use propose::propose_transaction_redo;
