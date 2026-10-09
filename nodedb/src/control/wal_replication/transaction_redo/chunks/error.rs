// SPDX-License-Identifier: BUSL-1.1

//! Why a chunked redo stream refused an entry.

use crate::bridge::envelope::ErrorCode;
use crate::wal::RedoStreamId;

/// A chunked redo stream refused an entry, or its storage failed.
///
/// Every variant but [`Self::Storage`] is a refusal: the log alone decides
/// it, so every replica refuses the same entry, and the entry applied
/// nothing. A refusal reaches the proposer as a retryable refusal.
#[derive(Debug, thiserror::Error)]
pub enum RedoChunkError {
    #[error(
        "redo stream {stream:?}: this node holds {open} bytes of open redo streams, and \
         {len} more pass the cap of {cap} (tuning.calvin.max_open_redo_bytes); retry once \
         open streams finish"
    )]
    StoreFull {
        stream: RedoStreamId,
        open: u64,
        len: u64,
        cap: u64,
    },
    #[error("redo stream {stream:?} is not open on this node")]
    NotOpen { stream: RedoStreamId },
    #[error("redo stream {stream:?}: chunk {index} arrived, chunk {expected} was next")]
    OutOfOrder {
        stream: RedoStreamId,
        index: u32,
        expected: u32,
    },
    #[error("redo stream {stream:?}: a chunk of term {term} reached a stream of term {open_term}")]
    TermChanged {
        stream: RedoStreamId,
        term: u64,
        open_term: u64,
    },
    #[error("redo stream {stream:?}: a chunk declares {declared} bytes, the stream {len}")]
    LengthChanged {
        stream: RedoStreamId,
        declared: u64,
        len: u64,
    },
    #[error("redo stream {stream:?}: chunks hold {held} bytes, past the declared {len}")]
    Overflow {
        stream: RedoStreamId,
        held: u64,
        len: u64,
    },
    #[error(
        "redo stream {stream:?}: the final entry names {count} chunks of {len} bytes, the \
         stream holds {held_count} chunks of {held} bytes"
    )]
    Mismatch {
        stream: RedoStreamId,
        count: u32,
        len: u64,
        held_count: usize,
        held: u64,
    },
    #[error("redo stream {stream:?}: {source}")]
    Storage {
        stream: RedoStreamId,
        source: Box<crate::Error>,
    },
}

impl RedoChunkError {
    /// Whether the log decides this outcome alike on every replica.
    pub fn is_refusal(&self) -> bool {
        !matches!(self, Self::Storage { .. })
    }

    pub(super) fn storage(stream: RedoStreamId, source: crate::Error) -> Self {
        Self::Storage {
            stream,
            source: Box::new(source),
        }
    }
}

impl From<RedoChunkError> for crate::Error {
    fn from(error: RedoChunkError) -> Self {
        match error {
            RedoChunkError::Storage { source, .. } => *source,
            refusal => crate::Error::DataPlane(ErrorCode::RetryableRefusal {
                reason: refusal.to_string(),
            }),
        }
    }
}
