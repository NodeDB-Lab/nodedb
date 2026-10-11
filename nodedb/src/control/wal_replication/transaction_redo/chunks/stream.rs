// SPDX-License-Identifier: BUSL-1.1

//! One open chunked redo stream on this node.

use crate::wal::RedoStreamId;
use crate::wal::manager::WalFloorHold;

use super::error::RedoChunkError;

/// The chunks one stream holds, and the WAL floor that keeps their records.
#[derive(Debug)]
pub struct OpenStream {
    pub(super) stream: RedoStreamId,
    /// The data group whose log carries the stream.
    pub(super) group_id: u64,
    /// The Raft term of the stream's first chunk entry.
    pub(super) term: u64,
    /// The byte length every chunk declares for the whole stream.
    pub(super) len: u64,
    /// The chunks, in index order.
    pub(super) chunks: Vec<Vec<u8>>,
    /// The bytes `chunks` hold.
    pub(super) held: u64,
    /// Keeps the stream's `RedoChunk` records until the stream closes.
    pub(super) hold: WalFloorHold,
}

impl OpenStream {
    pub fn stream(&self) -> RedoStreamId {
        self.stream
    }

    /// The stream's bytes, when it holds exactly `count` chunks of `len`
    /// bytes in all, as its final entry names.
    pub fn assemble(&self, count: u32, len: u64) -> Result<Vec<u8>, RedoChunkError> {
        if self.chunks.len() != count as usize || self.held != len || self.len != len {
            return Err(RedoChunkError::Mismatch {
                stream: self.stream,
                count,
                len,
                held_count: self.chunks.len(),
                held: self.held,
            });
        }
        let mut bytes = Vec::with_capacity(self.held as usize);
        for chunk in &self.chunks {
            bytes.extend_from_slice(chunk);
        }
        Ok(bytes)
    }

    /// The floor guard, once the bytes are no longer needed.
    pub(super) fn into_hold(self) -> WalFloorHold {
        self.hold
    }
}
