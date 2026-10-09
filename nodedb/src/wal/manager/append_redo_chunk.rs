// SPDX-License-Identifier: BUSL-1.1

//! WAL appends for chunked transaction redo streams.
//!
//! A chunk past the WAL record limit spans several `RedoChunk` pieces. Only
//! the last piece carries the chunk entry's apply key. The WAL is written in
//! order, so a durable last piece proves every earlier piece durable, and the
//! proposal ledger never counts a chunk applied whose bytes are partial.

use nodedb_wal::record::RecordType;

use super::appender::NO_APPLY_KEY;
use super::core::WalManager;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::redo::{RedoChunkHeader, RedoChunkPiece, RedoChunkRecord, RedoStreamId};

/// Room one piece record keeps for its header beside the chunk bytes.
const PIECE_HEADER_BYTES: usize = 256;

/// The header fields of every record a stream appends.
#[derive(Debug, Clone, Copy)]
pub struct RedoChunkPlacement {
    pub tenant_id: TenantId,
    pub vshard_id: VShardId,
    pub database_id: DatabaseId,
}

/// The LSNs of the first and the last record one chunk appended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkLsns {
    pub first: Lsn,
    pub last: Lsn,
}

impl WalManager {
    /// Append chunk `header.index` of its stream, holding `bytes`. The last
    /// piece carries `apply_key`, every other piece [`NO_APPLY_KEY`].
    pub fn append_redo_chunk(
        &self,
        apply_key: u64,
        placement: RedoChunkPlacement,
        header: RedoChunkHeader,
        bytes: &[u8],
    ) -> crate::Result<ChunkLsns> {
        let room = self
            .max_payload()
            .checked_sub(PIECE_HEADER_BYTES)
            .filter(|room| *room > 0)
            .ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "WAL record limit {} leaves no room for a redo chunk piece",
                    self.max_payload()
                ),
            })?;
        let parts: Vec<&[u8]> = if bytes.is_empty() {
            vec![bytes]
        } else {
            bytes.chunks(room).collect()
        };
        let pieces = u32::try_from(parts.len()).map_err(|_| crate::Error::Internal {
            detail: format!("a redo chunk spans {} WAL pieces", parts.len()),
        })?;
        let mut first = None;
        let mut last = Lsn::new(0);
        for (piece, part) in (0u32..).zip(parts) {
            let key = if piece + 1 == pieces {
                apply_key
            } else {
                NO_APPLY_KEY
            };
            let record = RedoChunkRecord::Piece(RedoChunkPiece {
                header,
                piece,
                pieces,
                bytes: part.to_vec(),
            });
            last = self.appender(key).append_record(
                RecordType::RedoChunk,
                placement.tenant_id,
                placement.vshard_id,
                placement.database_id,
                &record.to_bytes()?,
            )?;
            first.get_or_insert(last);
        }
        Ok(ChunkLsns {
            first: first.unwrap_or(last),
            last,
        })
    }

    /// Append the close of `stream`: boot opens it no more.
    pub fn append_redo_stream_closed(
        &self,
        apply_key: u64,
        placement: RedoChunkPlacement,
        stream: RedoStreamId,
    ) -> crate::Result<Lsn> {
        self.appender(apply_key).append_record(
            RecordType::RedoChunk,
            placement.tenant_id,
            placement.vshard_id,
            placement.database_id,
            &RedoChunkRecord::Closed { stream }.to_bytes()?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placement() -> RedoChunkPlacement {
        RedoChunkPlacement {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(4),
            database_id: DatabaseId::DEFAULT,
        }
    }

    fn header() -> RedoChunkHeader {
        RedoChunkHeader {
            stream: RedoStreamId::Session {
                vshard: 4,
                idempotency_key: 77,
            },
            group_id: 1,
            term: 2,
            len: 0,
            index: 0,
        }
    }

    #[test]
    fn a_chunk_past_the_record_limit_keys_only_its_last_piece() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let bytes: Vec<u8> = (0..wal.max_payload() * 2).map(|i| i as u8).collect();
        let mut head = header();
        head.len = bytes.len() as u64;
        let lsns = wal
            .append_redo_chunk(0xC0FFEE, placement(), head, &bytes)
            .expect("append");
        wal.sync().expect("sync");
        assert!(lsns.last > lsns.first);

        let records = wal.replay().expect("replay");
        let mut joined = Vec::new();
        let mut keys = Vec::new();
        for record in &records {
            let RedoChunkRecord::Piece(piece) =
                RedoChunkRecord::from_bytes(&record.payload).expect("decode")
            else {
                panic!("expected a piece");
            };
            assert_eq!(piece.pieces as usize, records.len());
            joined.extend_from_slice(&piece.bytes);
            keys.push(record.apply_key());
        }
        assert_eq!(joined, bytes);
        let (last, rest) = keys.split_last().expect("pieces");
        assert_eq!(*last, 0xC0FFEE);
        assert!(rest.iter().all(|key| *key == NO_APPLY_KEY));
    }
}
