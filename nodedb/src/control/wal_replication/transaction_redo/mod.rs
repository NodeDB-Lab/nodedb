// SPDX-License-Identifier: BUSL-1.1

//! Committed-transaction redo replication: one apply log per vShard.
//!
//! - [`payload`]: the redo a commit hands to the data-group log.
//! - [`apply`]: the one path that appends and applies it on a node.
//! - [`chunks`]: the streams a redo past the entry limit travels in.
//! - [`propose`]: the proposal of a committed redo, inline or chunked.
//! - [`propose_stream`]: the proposal of a redo whose content arrives in
//!   parts.
//! - [`abandon`]: the retry of a failed stream abandon.
//! - [`sum_targets`]: what the payload derives from the commit's buffered
//!   plans.
//! - [`pins`]: the config content the proposer pins for the validate.
//! - [`stream_header`]: the Control Plane's reads of a chunked redo's
//!   content, one page at a time.

pub mod abandon;
pub mod apply;
pub mod chunks;
pub mod payload;
pub mod pins;
pub mod propose;
pub mod propose_stream;
pub mod stream_header;
pub mod sum_targets;

pub(crate) use abandon::spawn_redo_abandoner;
pub use apply::RedoTarget;
pub(crate) use apply::{enqueue_transaction_redo, record_cross_shard_key};
pub use chunks::{RedoChunkError, RedoChunkStore};
pub use payload::{CalvinSlice, TransactionRedoPayload};
pub(crate) use propose::{propose_inline_redo, propose_transaction_redo};
pub(crate) use propose_stream::RedoStreamProposal;
pub use stream_header::StreamedFinal;
pub(crate) use stream_header::{
    ChangesAt, StreamedChanges, stage_streamed_changes, streamed_payload,
};
