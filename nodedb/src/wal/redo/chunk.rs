// SPDX-License-Identifier: BUSL-1.1

//! Chunked transaction redo: the stream a redo's chunks share, and the WAL
//! record of one chunk.
//!
//! A committed redo past the redo entry limit travels through its data-group
//! log as chunk entries followed by a final entry. Every replica appends each
//! chunk it applies as `RedoChunk` records and holds the bytes until the final
//! entry assembles them. Boot rebuilds the open streams from these records.

use serde::{Deserialize, Serialize};

/// The stream one chunked redo's chunks and final entry share.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RedoStreamId {
    /// A committed Calvin slice. Each proposal attempt of the position
    /// opens its own stream.
    Calvin {
        vshard: u32,
        epoch: u64,
        position: u32,
        attempt: u32,
    },
    /// A session COMMIT. The final entry's idempotency key names the stream.
    Session { vshard: u32, idempotency_key: u64 },
}

impl RedoStreamId {
    /// The vShard whose data-group log carries the stream.
    pub fn vshard(&self) -> u32 {
        match self {
            Self::Calvin { vshard, .. } | Self::Session { vshard, .. } => *vshard,
        }
    }
}

/// Where one chunk sits in its stream.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RedoChunkHeader {
    pub stream: RedoStreamId,
    /// The data group whose log carried the chunk.
    pub group_id: u64,
    /// The Raft term of the stream's first chunk entry.
    pub term: u64,
    /// The byte length of the whole stream, as every chunk declares it.
    pub len: u64,
    /// The chunk's place in the stream, from `0`.
    pub index: u32,
}

/// One WAL piece of one chunk.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RedoChunkPiece {
    pub header: RedoChunkHeader,
    /// The piece's place in the chunk, from `0`.
    pub piece: u32,
    /// How many pieces the chunk spans.
    pub pieces: u32,
    pub bytes: Vec<u8>,
}

/// The payload of a `RedoChunk` WAL record.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RedoChunkRecord {
    /// One piece of one chunk.
    Piece(RedoChunkPiece),
    /// The stream closed without its final entry. Boot opens it no more.
    Closed { stream: RedoStreamId },
}

impl RedoChunkRecord {
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("redo chunk record encode: {e}"),
        })
    }

    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("redo chunk record decode: {e}"),
        })
    }
}

/// One open stream a data-group snapshot carries to the follower that
/// installs it.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CarriedRedoStream {
    pub stream: RedoStreamId,
    pub term: u64,
    pub len: u64,
    /// The chunks the stream holds, in index order.
    pub chunks: Vec<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_chunk_record_round_trips() {
        let record = RedoChunkRecord::Piece(RedoChunkPiece {
            header: RedoChunkHeader {
                stream: RedoStreamId::Session {
                    vshard: 3,
                    idempotency_key: 0xAB,
                },
                group_id: 2,
                term: 5,
                len: 10,
                index: 1,
            },
            piece: 0,
            pieces: 2,
            bytes: vec![1, 2, 3],
        });
        let bytes = record.to_bytes().expect("encode");
        assert_eq!(RedoChunkRecord::from_bytes(&bytes).expect("decode"), record);
        let closed = RedoChunkRecord::Closed {
            stream: RedoStreamId::Calvin {
                vshard: 1,
                epoch: 9,
                position: 4,
                attempt: 2,
            },
        };
        let bytes = closed.to_bytes().expect("encode");
        assert_eq!(RedoChunkRecord::from_bytes(&bytes).expect("decode"), closed);
    }
}
