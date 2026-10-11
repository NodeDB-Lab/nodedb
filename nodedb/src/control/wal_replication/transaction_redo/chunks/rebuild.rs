// SPDX-License-Identifier: BUSL-1.1

//! Rebuild the open streams from the WAL at boot.
//!
//! A chunk entry's records are durable before its entry concludes, and its
//! key rides its last piece, so a re-delivered chunk entry is skipped as a
//! repeat. The store must hold that chunk again from its records. Boot reads
//! every `RedoChunk` record in WAL order and keeps a stream open unless:
//!
//! - a `Closed` record names it (an abandon);
//! - a later snapshot install of its group replaced the group's streams;
//! - a session stream's final entry key is on a record (the final applied);
//! - a stamped transaction redo names a Calvin stream's position.
//!
//! A stream keeps its chunks from index `0` up to the first chunk with a
//! missing piece. That chunk's entry never concluded, so it applies again.

use std::collections::{BTreeMap, HashSet};

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use crate::types::Lsn;
use crate::wal::{RedoChunkHeader, RedoChunkRecord, RedoRecord, RedoStreamId};

use super::store::RedoChunkStore;
use super::stream::OpenStream;

/// What a boot rebuild found.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RebuildStats {
    /// Streams the store holds open again.
    pub open: usize,
    /// Streams the WAL shows closed.
    pub closed: usize,
}

/// The pieces of one chunk read so far.
struct ChunkPieces {
    pieces: u32,
    got: BTreeMap<u32, Vec<u8>>,
    first: u64,
}

/// One stream read so far.
struct Building {
    group_id: u64,
    term: u64,
    len: u64,
    chunks: BTreeMap<u32, ChunkPieces>,
}

impl Building {
    fn new(header: &RedoChunkHeader) -> Self {
        Self {
            group_id: header.group_id,
            term: header.term,
            len: header.len,
            chunks: BTreeMap::new(),
        }
    }

    /// The whole chunks from index `0`, the first record's LSN, and the
    /// bytes they hold. `None` when chunk `0` is not whole.
    fn whole_prefix(self) -> Option<(Vec<Vec<u8>>, u64, u64)> {
        let mut chunks = Vec::new();
        let mut first = u64::MAX;
        let mut held = 0u64;
        for (index, chunk) in self.chunks {
            let whole = index as usize == chunks.len()
                && chunk.got.len() == chunk.pieces as usize
                && chunk.got.keys().copied().eq(0..chunk.pieces);
            if !whole {
                break;
            }
            first = first.min(chunk.first);
            let bytes: Vec<u8> = chunk.got.into_values().flatten().collect();
            held += bytes.len() as u64;
            chunks.push(bytes);
        }
        (!chunks.is_empty()).then_some((chunks, first, held))
    }
}

impl RedoChunkStore {
    /// Rebuild the open streams from `records`, the whole WAL in LSN order.
    /// Runs at boot, before the data-group apply loop starts.
    pub fn rebuild(&self, records: &[WalRecord]) -> crate::Result<RebuildStats> {
        let mut building: BTreeMap<RedoStreamId, Building> = BTreeMap::new();
        let mut keys: HashSet<u64> = HashSet::new();
        let mut redo_records: Vec<&WalRecord> = Vec::new();
        let mut closed = 0usize;
        for record in records {
            let key = record.apply_key();
            if key != 0 {
                keys.insert(key);
            }
            match RecordType::from_raw(record.logical_record_type()) {
                Some(RecordType::RedoChunk) => {
                    match RedoChunkRecord::from_bytes(&record.payload)? {
                        RedoChunkRecord::Piece(piece) => {
                            let stream = building
                                .entry(piece.header.stream)
                                .or_insert_with(|| Building::new(&piece.header));
                            let chunk =
                                stream
                                    .chunks
                                    .entry(piece.header.index)
                                    .or_insert(ChunkPieces {
                                        pieces: piece.pieces,
                                        got: BTreeMap::new(),
                                        first: record.header.lsn,
                                    });
                            chunk.first = chunk.first.min(record.header.lsn);
                            chunk.got.insert(piece.piece, piece.bytes);
                        }
                        RedoChunkRecord::Closed { stream } => {
                            closed += usize::from(building.remove(&stream).is_some());
                        }
                    }
                }
                Some(RecordType::SnapshotInstalled) => {
                    if let Ok(raw) = <[u8; 8]>::try_from(record.payload.as_slice()) {
                        let group_id = u64::from_le_bytes(raw);
                        let before = building.len();
                        building.retain(|_, stream| stream.group_id != group_id);
                        closed += before - building.len();
                    }
                }
                Some(RecordType::TransactionRedo) => redo_records.push(record),
                _ => {}
            }
        }
        let applied_positions = if building
            .keys()
            .any(|stream| matches!(stream, RedoStreamId::Calvin { .. }))
        {
            stamped_positions(&redo_records)?
        } else {
            HashSet::new()
        };
        let mut state = self.lock();
        let mut open = 0usize;
        for (stream, built) in building {
            let finished = match stream {
                RedoStreamId::Session {
                    idempotency_key, ..
                } => keys.contains(&idempotency_key),
                RedoStreamId::Calvin {
                    vshard,
                    epoch,
                    position,
                    ..
                } => applied_positions.contains(&(vshard, epoch, position)),
            };
            if finished {
                closed += 1;
                continue;
            }
            let (group_id, term, len) = (built.group_id, built.term, built.len);
            let Some((chunks, first, held)) = built.whole_prefix() else {
                continue;
            };
            let hold = self.wal.floor_holds().hold(Lsn::new(first));
            state.streams.insert(
                stream,
                OpenStream {
                    stream,
                    group_id,
                    term,
                    len,
                    chunks,
                    held,
                    hold,
                },
            );
            open += 1;
        }
        self.note_open(&state);
        Ok(RebuildStats { open, closed })
    }
}

/// The `(vshard, epoch, position)` every stamped transaction redo names.
fn stamped_positions(records: &[&WalRecord]) -> crate::Result<HashSet<(u32, u64, u32)>> {
    let mut positions = HashSet::new();
    for record in records {
        if let Some(stamp) = RedoRecord::from_bytes(&record.payload)?.calvin_stamp {
            positions.insert((stamp.vshard_id, stamp.epoch, stamp.position));
        }
    }
    Ok(positions)
}

#[cfg(test)]
mod tests {
    use super::super::store::tests::{chunk, session, wal};
    use super::super::store::{RedoChunkLimits, RedoChunkStore};
    use super::*;

    fn limits() -> RedoChunkLimits {
        RedoChunkLimits {
            max_entry_bytes: 64 * 1024,
            max_open_bytes: 1 << 20,
        }
    }

    #[tokio::test]
    async fn boot_rebuilds_an_open_stream() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = wal(&dir);
        let live = RedoChunkStore::new(std::sync::Arc::clone(&wal), limits());
        let open = session(41);
        let finished = session(42);
        let abandoned = session(43);
        for (index, part) in [&b"abc"[..], b"de"].into_iter().enumerate() {
            live.apply_chunk(chunk(open, index as u32, 5, part))
                .await
                .expect("chunk");
        }
        live.apply_chunk(chunk(finished, 0, 2, b"xy"))
            .await
            .expect("chunk");
        live.apply_chunk(chunk(abandoned, 0, 2, b"zz"))
            .await
            .expect("chunk");
        // The finished stream's final entry appended a record under its key.
        wal.appender(42)
            .append_proposal_applied(
                crate::types::TenantId::new(1),
                crate::types::VShardId::new(2),
                crate::types::DatabaseId::DEFAULT,
            )
            .expect("final record");
        live.abandon(abandoned, 77, chunk(abandoned, 0, 0, b"").placement)
            .await
            .expect("abandon");
        wal.sync().expect("sync");

        let rebuilt = RedoChunkStore::new(std::sync::Arc::clone(&wal), limits());
        let stats = rebuilt
            .rebuild(&wal.replay().expect("replay"))
            .expect("rebuild");
        assert_eq!(stats, RebuildStats { open: 1, closed: 2 });
        let stream = rebuilt.take_for_final(&open).expect("open stream rebuilt");
        assert_eq!(stream.assemble(2, 5).expect("assembles"), b"abcde".to_vec());
        assert!(rebuilt.take_for_final(&finished).is_none());
        assert!(rebuilt.take_for_final(&abandoned).is_none());
    }

    #[tokio::test]
    async fn a_snapshot_install_drops_the_groups_earlier_streams() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = wal(&dir);
        let live = RedoChunkStore::new(std::sync::Arc::clone(&wal), limits());
        live.apply_chunk(chunk(session(5), 0, 2, b"ab"))
            .await
            .expect("chunk");
        wal.appender(0)
            .append_snapshot_installed(1)
            .expect("install marker");
        live.apply_chunk(chunk(session(6), 0, 2, b"cd"))
            .await
            .expect("chunk");
        wal.sync().expect("sync");

        let rebuilt = RedoChunkStore::new(std::sync::Arc::clone(&wal), limits());
        rebuilt
            .rebuild(&wal.replay().expect("replay"))
            .expect("rebuild");
        assert!(rebuilt.take_for_final(&session(5)).is_none());
        assert!(rebuilt.take_for_final(&session(6)).is_some());
    }
}
