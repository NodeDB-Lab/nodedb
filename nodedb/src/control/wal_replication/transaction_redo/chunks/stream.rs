// SPDX-License-Identifier: BUSL-1.1

//! One open chunked redo stream on this node.
//!
//! The stream holds no chunk bytes. Each chunk's records are durable in the
//! WAL before the chunk counts, so the stream holds where each chunk sits:
//! its records' LSNs, its length and its checksum. The final entry hands
//! those references on, and every reader reads the bytes back from the WAL.

use crate::wal::manager::WalFloorHold;
use crate::wal::{RedoChunkRef, RedoStreamId, RedoStreamRef};

use super::error::RedoChunkError;

/// The chunk references one stream holds, and the WAL floor that keeps
/// their records.
#[derive(Debug)]
pub struct OpenStream {
    pub(super) stream: RedoStreamId,
    /// The data group whose log carries the stream.
    pub(super) group_id: u64,
    /// The Raft term of the stream's first chunk entry.
    pub(super) term: u64,
    /// The byte length every chunk declares for the whole stream.
    pub(super) len: u64,
    /// Where each chunk sits in the WAL, in index order.
    pub(super) chunks: Vec<RedoChunkRef>,
    /// The bytes the chunks hold.
    pub(super) held: u64,
    /// Keeps the stream's `RedoChunk` records until the stream closes.
    pub(super) hold: WalFloorHold,
}

impl OpenStream {
    pub fn stream(&self) -> RedoStreamId {
        self.stream
    }

    /// The reference of the stream's content, when it holds exactly
    /// `count` chunks of `len` bytes in all, as its final entry names.
    pub fn stream_ref(&self, count: u32, len: u64) -> Result<RedoStreamRef, RedoChunkError> {
        if self.chunks.len() != count as usize || self.held != len || self.len != len {
            return Err(RedoChunkError::Mismatch {
                stream: self.stream,
                count,
                len,
                held_count: self.chunks.len(),
                held: self.held,
            });
        }
        Ok(self.reference())
    }

    /// The reference of the chunks the stream holds now.
    pub(super) fn reference(&self) -> RedoStreamRef {
        RedoStreamRef {
            stream: self.stream,
            len: self.len,
            chunks: self.chunks.clone(),
            publish_position: None,
            entry: None,
            stamp_floor: None,
        }
    }

    /// The floor guard, once no reader needs the records.
    pub(super) fn into_hold(self) -> WalFloorHold {
        self.hold
    }
}
