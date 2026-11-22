// SPDX-License-Identifier: BUSL-1.1

//! Rebuild the record a stream marker installs, for a reader of the WAL.
//!
//! A chunked redo's `TransactionRedo` record is a marker: its only
//! sub-record is a [`RedoStreamRef`], and the `RedoChunk` records it names
//! hold the content. A WAL read hands the marker out as it is stored, so
//! every record's header and checksum describe that record. A reader that
//! applies or replays the redo's rows (restart replay on a core, the Event
//! Plane catch-up) asks [`StreamContents`] for the record the marker
//! installs. Readers of the header alone (the Calvin recovery scan, the
//! cross-shard dedup, the write-group settle) read the marker itself: it
//! carries the Calvin stamp and the cross-shard key.
//!
//! A marker whose chunks lie below the oldest retained segment installs
//! nothing a reader replays. Truncation removes a chunk only once a
//! checkpoint passed its marker (see `WalFloorHolds::hold_until_checkpoint`),
//! so every engine already holds what the marker installed.

use std::collections::BTreeMap;

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use super::chunk::RedoChunkRecord;
use super::frame::FramedRedo;
use super::record::RedoRecord;
use super::stream_ref::RedoStreamRef;

/// Where a reader finds a marker's chunks that its read did not return: the
/// WAL on disk. `Ok(None)` when they lie below the oldest retained segment.
pub type ChunkFetch<'a> = dyn FnMut(&RedoStreamRef) -> crate::Result<Option<Vec<u8>>> + 'a;

fn is_chunk(record: &WalRecord) -> bool {
    RecordType::from_raw(record.logical_record_type()) == Some(RecordType::RedoChunk)
}

/// The chunk records of one WAL read, and where to find the rest.
pub struct StreamContents<'r, 'f, 'c> {
    /// Chunk record LSN to the record.
    chunks: BTreeMap<u64, &'r WalRecord>,
    fetch: &'f mut ChunkFetch<'c>,
}

impl<'r, 'f, 'c> StreamContents<'r, 'f, 'c> {
    /// The contents of the markers in `records`. `fetch` reads a marker's
    /// chunks that `records` does not hold.
    pub fn new(records: &'r [WalRecord], fetch: &'f mut ChunkFetch<'c>) -> Self {
        let chunks = records
            .iter()
            .filter(|record| is_chunk(record))
            .map(|record| (record.header.lsn, record))
            .collect();
        Self { chunks, fetch }
    }

    /// The record the stream marker `marker`, which holds `reference`,
    /// installs: its content's sub-records and lists, with the marker's
    /// stamps. `Ok(None)` when its chunks lie below the oldest retained
    /// segment.
    pub fn installed(
        &mut self,
        marker: &RedoRecord,
        reference: &RedoStreamRef,
    ) -> crate::Result<Option<RedoRecord>> {
        let content = match joined_in(&self.chunks, reference)? {
            Some(content) => content,
            None => match (self.fetch)(reference)? {
                Some(content) => content,
                None => return Ok(None),
            },
        };
        FramedRedo::decode(&content)?
            .into_installed(marker, reference)
            .map(Some)
    }
}

/// Every stream marker `records` holds: its WAL LSN and its reference.
pub fn markers_in(records: &[WalRecord]) -> crate::Result<Vec<(u64, RedoStreamRef)>> {
    let mut markers = Vec::new();
    for record in records {
        if RecordType::from_raw(record.logical_record_type()) != Some(RecordType::TransactionRedo) {
            continue;
        }
        if let Some(reference) =
            RedoStreamRef::of_marker(&RedoRecord::from_bytes(&record.payload)?)?
        {
            markers.push((record.header.lsn, reference));
        }
    }
    Ok(markers)
}

/// The content of `reference` joined from the chunk records `chunks` holds,
/// `None` when a chunk has no piece among them.
fn joined_in(
    chunks: &BTreeMap<u64, &WalRecord>,
    reference: &RedoStreamRef,
) -> crate::Result<Option<Vec<u8>>> {
    let mut content = Vec::with_capacity(usize::try_from(reference.len).unwrap_or(0));
    for chunk in &reference.chunks {
        let start = content.len();
        let mut next_piece = 0u32;
        let mut pieces = None;
        for record in chunks
            .range(chunk.first_lsn..=chunk.last_lsn)
            .map(|(_, record)| record)
        {
            let RedoChunkRecord::Piece(piece) = RedoChunkRecord::from_bytes(&record.payload)?
            else {
                continue;
            };
            if piece.header.stream != reference.stream || piece.header.index != chunk.index {
                continue;
            }
            if piece.piece != next_piece {
                return Err(crate::Error::SegmentCorrupted {
                    detail: format!(
                        "chunk {} of redo stream {:?}: piece {} sits where piece {next_piece} \
                         belongs",
                        chunk.index, reference.stream, piece.piece
                    ),
                });
            }
            pieces = Some(piece.pieces);
            next_piece += 1;
            content.extend_from_slice(&piece.bytes);
        }
        if pieces.is_none() {
            return Ok(None);
        }
        if pieces != Some(next_piece) {
            return Err(crate::Error::SegmentCorrupted {
                detail: format!(
                    "chunk {} of redo stream {:?}: {next_piece} of {pieces:?} pieces in the WAL",
                    chunk.index, reference.stream
                ),
            });
        }
        chunk.check(reference.stream, content.get(start..).unwrap_or_default())?;
    }
    Ok(Some(content))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventSource;
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
    use crate::wal::manager::{RedoChunkPlacement, WalManager};
    use crate::wal::redo::chunk::{RedoChunkHeader, RedoStreamId};
    use crate::wal::redo::frame::{RedoContentHeader, RedoFrameTag, push_frame};
    use crate::wal::redo::record::RedoSubRecord;
    use crate::wal::redo::stream_ref::{RedoChunkRef, chunk_crc};

    fn stream() -> RedoStreamId {
        RedoStreamId::Session {
            vshard: 2,
            idempotency_key: 55,
        }
    }

    fn ops() -> Vec<RedoSubRecord> {
        (0..6u8)
            .map(|byte| RedoSubRecord {
                record_type: 7,
                payload: vec![byte; 40],
            })
            .collect()
    }

    fn content() -> Vec<u8> {
        let header = RedoContentHeader {
            version: 1,
            calvin_stamp: None,
            cross_shard_applied: None,
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
            pins: Default::default(),
        };
        let mut out = Vec::new();
        push_frame(&mut out, RedoFrameTag::Header, &header).expect("header");
        for op in ops() {
            push_frame(&mut out, RedoFrameTag::Op, &op).expect("op");
        }
        out
    }

    /// Append the content as chunks of 100 bytes and return its reference.
    fn append_chunks(wal: &WalManager, content: &[u8]) -> RedoStreamRef {
        let placement = RedoChunkPlacement {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(2),
            database_id: DatabaseId::DEFAULT,
        };
        let mut chunks = Vec::new();
        for (index, bytes) in (0u32..).zip(content.chunks(100)) {
            let header = RedoChunkHeader {
                stream: stream(),
                group_id: 1,
                term: 1,
                len: content.len() as u64,
                index,
            };
            let lsns = wal
                .append_redo_chunk(10 + u64::from(index), placement, header, bytes)
                .expect("chunk");
            chunks.push(RedoChunkRef {
                index,
                first_lsn: lsns.first.as_u64(),
                last_lsn: lsns.last.as_u64(),
                len: bytes.len() as u64,
                crc: chunk_crc(bytes),
            });
        }
        RedoStreamRef {
            stream: stream(),
            len: content.len() as u64,
            chunks,
            publish_position: None,
            entry: None,
            stamp_floor: None,
            keys: Default::default(),
        }
    }

    fn append_marker(wal: &WalManager, reference: &RedoStreamRef) -> Lsn {
        let marker = reference.marker(None, None, Vec::new()).expect("marker");
        wal.appender(99)
            .with_event_source(EventSource::User)
            .append_transaction_redo(
                TenantId::new(1),
                VShardId::new(2),
                DatabaseId::DEFAULT,
                &marker,
            )
            .expect("marker record")
    }

    /// The record at `lsn` of `records`, and what it installs.
    fn installed_at(
        wal: &WalManager,
        records: &[WalRecord],
        lsn: Lsn,
    ) -> (WalRecord, Option<RedoRecord>) {
        let record = records
            .iter()
            .find(|record| record.header.lsn == lsn.as_u64())
            .expect("marker record read back")
            .clone();
        let marker = RedoRecord::from_bytes(&record.payload).expect("decode");
        let reference = RedoStreamRef::of_marker(&marker)
            .expect("ref")
            .expect("a marker");
        let mut fetch = |reference: &RedoStreamRef| wal.fetch_stream_content(reference);
        let installed = StreamContents::new(records, &mut fetch)
            .installed(&marker, &reference)
            .expect("installed");
        (record, installed)
    }

    /// A read hands the marker out as stored, so its checksum holds, and the
    /// contents rebuild what it installs from the chunk records.
    #[test]
    fn a_marker_reads_back_as_stored_and_installs_its_content() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
        let reference = append_chunks(&wal, &content());
        assert!(reference.chunks.len() > 2);
        let marker = append_marker(&wal, &reference);
        wal.sync().expect("sync");

        let records = wal.replay().expect("replay");
        let (record, installed) = installed_at(&wal, &records, marker);
        record.verify_checksum().expect("the stored marker checks");
        assert_eq!(
            markers_in(&records).expect("markers"),
            vec![(marker.as_u64(), reference)]
        );
        assert_eq!(installed.expect("content").ops, ops());
    }

    /// A read that starts past the chunks reads them from disk.
    #[test]
    fn a_read_from_the_marker_fetches_the_chunks_from_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
        let reference = append_chunks(&wal, &content());
        let marker = append_marker(&wal, &reference);
        wal.sync().expect("sync");

        let records = wal.replay_from(marker).expect("replay");
        let (_, installed) = installed_at(&wal, &records, marker);
        assert_eq!(installed.expect("content").ops, ops());
    }

    /// Truncation past a checkpoint that passed the marker removes the
    /// chunks. The marker then installs nothing a reader replays: every
    /// engine holds what it installed.
    #[test]
    fn a_marker_whose_chunks_lie_below_the_retained_floor_installs_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
        let reference = append_chunks(&wal, &content());
        wal.sync().expect("sync");
        wal.seal_active_segment().expect("seal the chunk segment");
        let marker = append_marker(&wal, &reference);
        wal.sync().expect("sync");
        let deleted = wal.truncate_before(marker).expect("truncate");
        assert!(deleted.segments_deleted > 0);

        let records = wal.replay().expect("replay");
        let (_, installed) = installed_at(&wal, &records, marker);
        assert!(installed.is_none());
    }

    /// A checkpoint hold keeps the chunks through a truncation whose
    /// checkpoint does not pass the marker.
    #[test]
    fn the_chunks_stay_until_a_checkpoint_passes_the_marker() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
        let reference = append_chunks(&wal, &content());
        wal.sync().expect("sync");
        wal.seal_active_segment().expect("seal the chunk segment");
        let marker = append_marker(&wal, &reference);
        wal.sync().expect("sync");
        let first = Lsn::new(reference.first_lsn().expect("chunks"));
        wal.floor_holds().hold_until_checkpoint(first, marker);

        let kept = wal.truncate_before(marker).expect("truncate");
        assert_eq!(kept.segments_deleted, 0, "the marker is not checkpointed");
        let records = wal.replay().expect("replay");
        let (_, installed) = installed_at(&wal, &records, marker);
        assert_eq!(installed.expect("content").ops, ops());

        let past = Lsn::new(marker.as_u64() + 1);
        let removed = wal.truncate_before(past).expect("truncate");
        assert!(removed.segments_deleted > 0);
        assert_eq!(wal.floor_holds().lowest(), None);
    }
}
