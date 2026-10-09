// SPDX-License-Identifier: BUSL-1.1

//! Chunked transaction redo: the streams a redo past the entry limit travels
//! in, held on every replica until the stream's final entry.
//!
//! - [`store`]: the node's open streams, leader admission, and stream close.
//! - [`stream`]: one open stream and its assembly.
//! - [`rebuild`]: boot rebuild from the WAL.
//! - [`carry`]: the open streams a group snapshot carries.
//! - [`leave`]: the drop of a left group's streams.
//! - [`owed`]: the groups whose replica here owes a snapshot install.
//! - [`abandon`]: the session streams whose abandon proposal failed.
//! - [`error`]: why a stream refused an entry.

pub mod abandon;
pub mod carry;
pub mod error;
pub mod leave;
pub mod owed;
pub mod rebuild;
pub mod store;
pub mod stream;

pub use abandon::DueAbandon;
pub use error::RedoChunkError;
pub use rebuild::RebuildStats;
pub use store::{ChunkApply, RedoChunkLimits, RedoChunkStore};
pub use stream::OpenStream;
