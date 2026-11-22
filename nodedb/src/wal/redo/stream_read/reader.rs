// SPDX-License-Identifier: BUSL-1.1

//! The chunks of one stream, read back from the WAL by its reference.
//!
//! The reader walks the WAL forward once. The reference lists every chunk
//! in WAL order, so a chunk's records always sit past the previous chunk's.
//! A chunk's pieces sit between its first and last LSN, and other records
//! can sit between them: the reader steps over every record that is not one
//! of the chunk's pieces without reading its payload. It holds one chunk at
//! a time and checks each against the length and checksum the reference
//! recorded.

use std::path::PathBuf;

use nodedb_wal::RecordHeader;
use nodedb_wal::record::RecordType;

use super::cursor::WalRecordCursor;
use super::source::WalReadSource;
use crate::wal::redo::chunk::RedoChunkRecord;
use crate::wal::redo::frame::ChunkSource;
use crate::wal::redo::stream_ref::{RedoChunkRef, RedoStreamRef};

/// One segment the reader walks.
struct OpenSegment<C> {
    /// The segment's place in `segments`.
    index: usize,
    cursor: C,
    /// A header read whose payload the cursor has not passed yet.
    pending: Option<RecordHeader>,
}

/// Reads the chunks of one stream, in index order.
pub struct RedoStreamReader<C: WalRecordCursor> {
    ring: Option<nodedb_wal::crypto::KeyRing>,
    reference: RedoStreamRef,
    /// The next chunk to read.
    next: usize,
    /// Bytes the next chunk drops from its front: a seek landed inside it.
    skip_leading: usize,
    /// Every segment's path and first LSN, in LSN order.
    segments: Vec<(PathBuf, u64)>,
    open: Option<OpenSegment<C>>,
}

fn missing(reference: &RedoStreamRef, chunk: &RedoChunkRef, detail: &str) -> crate::Error {
    crate::Error::SegmentCorrupted {
        detail: format!(
            "chunk {} of redo stream {:?} at WAL LSNs {}..={}: {detail}",
            chunk.index, reference.stream, chunk.first_lsn, chunk.last_lsn
        ),
    }
}

impl<C: WalRecordCursor> RedoStreamReader<C> {
    /// A reader of `reference`'s chunks in the WAL `source` names.
    pub fn open(source: &WalReadSource, reference: RedoStreamRef) -> crate::Result<Self> {
        reference.check_shape()?;
        let mut segments: Vec<(PathBuf, u64)> = nodedb_wal::segment::discover_segments(&source.dir)
            .map_err(crate::Error::Wal)?
            .into_iter()
            .map(|segment| (segment.path, segment.first_lsn))
            .collect();
        segments.sort_by_key(|(_, first)| *first);
        Ok(Self {
            ring: source.ring.clone(),
            reference,
            next: 0,
            skip_leading: 0,
            segments,
            open: None,
        })
    }

    /// Start the next read at content byte `offset`: the chunk that holds it
    /// is the next one, with the bytes before `offset` dropped. An offset at
    /// the content's end leaves no chunk to read. An offset past it is
    /// refused.
    pub fn seek_content(&mut self, offset: u64) -> crate::Result<()> {
        let mut start = 0u64;
        for (index, chunk) in self.reference.chunks.iter().enumerate() {
            let end = start.saturating_add(chunk.len);
            if offset < end {
                self.next = index;
                // `offset - start` is below one chunk's length, a `usize`.
                self.skip_leading = usize::try_from(offset - start).unwrap_or(usize::MAX);
                return Ok(());
            }
            start = end;
        }
        if offset == start {
            self.next = self.reference.chunks.len();
            self.skip_leading = 0;
            return Ok(());
        }
        Err(crate::Error::Internal {
            detail: format!(
                "a read of redo stream {:?} seeks to byte {offset}, past its {start} bytes",
                self.reference.stream
            ),
        })
    }

    /// Whether the stream's first chunk lies below the oldest segment the
    /// WAL retains: truncation removed it once a checkpoint passed the
    /// stream's marker.
    pub fn below_retained_floor(&self) -> bool {
        match (self.reference.first_lsn(), self.segments.first()) {
            (Some(first), Some((_, floor))) => first < *floor,
            (Some(_), None) => true,
            (None, _) => false,
        }
    }

    /// Open the segment that holds `lsn`, unless the open one comes at or
    /// after it.
    fn position(&mut self, lsn: u64) -> crate::Result<bool> {
        let Some(target) = self.segments.iter().rposition(|(_, first)| *first <= lsn) else {
            return Ok(false);
        };
        if self.open.as_ref().is_some_and(|open| open.index >= target) {
            return Ok(true);
        }
        self.open_segment(target)?;
        Ok(true)
    }

    fn open_segment(&mut self, index: usize) -> crate::Result<()> {
        let Some((path, _)) = self.segments.get(index) else {
            self.open = None;
            return Ok(());
        };
        let cursor = C::open(path, self.ring.as_ref())?;
        self.open = Some(OpenSegment {
            index,
            cursor,
            pending: None,
        });
        Ok(())
    }

    /// The next record's header, from the next segment once one ends.
    fn next_header(&mut self) -> crate::Result<Option<RecordHeader>> {
        loop {
            let Some(open) = self.open.as_mut() else {
                return Ok(None);
            };
            if let Some(header) = open.pending.take() {
                return Ok(Some(header));
            }
            if let Some(header) = open.cursor.next_header()? {
                return Ok(Some(header));
            }
            let next = open.index + 1;
            if next >= self.segments.len() {
                self.open = None;
                return Ok(None);
            }
            self.open_segment(next)?;
        }
    }

    fn cursor(&mut self) -> crate::Result<&mut C> {
        self.open
            .as_mut()
            .map(|open| &mut open.cursor)
            .ok_or_else(|| crate::Error::Internal {
                detail: "redo stream reader lost its open segment after a header".into(),
            })
    }

    /// Read the chunk `chunk` from the records between its LSNs.
    fn read_chunk(&mut self, chunk: RedoChunkRef) -> crate::Result<Vec<u8>> {
        if !self.position(chunk.first_lsn)? {
            return Err(missing(
                &self.reference,
                &chunk,
                "no WAL segment holds its first record",
            ));
        }
        let stream = self.reference.stream;
        let mut bytes = Vec::with_capacity(usize::try_from(chunk.len).unwrap_or(0));
        let mut next_piece = 0u32;
        let mut pieces: Option<u32> = None;
        loop {
            let Some(header) = self.next_header()? else {
                return Err(missing(
                    &self.reference,
                    &chunk,
                    "the WAL ends before its last record",
                ));
            };
            if header.lsn > chunk.last_lsn {
                if let Some(open) = self.open.as_mut() {
                    open.pending = Some(header);
                }
                break;
            }
            let wanted = header.lsn >= chunk.first_lsn
                && RecordType::from_raw(header.logical_record_type())
                    == Some(RecordType::RedoChunk);
            if !wanted {
                self.cursor()?.skip_payload(&header)?;
                continue;
            }
            let payload = self.cursor()?.read_payload(&header)?;
            if let RedoChunkRecord::Piece(piece) = RedoChunkRecord::from_bytes(&payload)?
                && piece.header.stream == stream
                && piece.header.index == chunk.index
            {
                if piece.piece != next_piece {
                    return Err(missing(
                        &self.reference,
                        &chunk,
                        &format!(
                            "piece {} sits where piece {next_piece} belongs",
                            piece.piece
                        ),
                    ));
                }
                pieces = Some(piece.pieces);
                next_piece += 1;
                bytes.extend_from_slice(&piece.bytes);
            }
            if header.lsn == chunk.last_lsn {
                break;
            }
        }
        if pieces != Some(next_piece) {
            return Err(missing(
                &self.reference,
                &chunk,
                &format!("{next_piece} of {pieces:?} pieces read back"),
            ));
        }
        chunk.check(stream, &bytes)?;
        Ok(bytes)
    }
}

/// The whole content of `reference` in the WAL `source` names, read through
/// cursor `C` and checked chunk by chunk. `Ok(None)` when its chunks lie
/// below the oldest retained segment.
pub fn read_stream_content<C: WalRecordCursor>(
    source: &WalReadSource,
    reference: &RedoStreamRef,
) -> crate::Result<Option<Vec<u8>>> {
    let mut reader = RedoStreamReader::<C>::open(source, reference.clone())?;
    if reader.below_retained_floor() {
        return Ok(None);
    }
    let mut content = Vec::with_capacity(usize::try_from(reference.len).unwrap_or(0));
    while let Some(chunk) = reader.next_chunk()? {
        content.extend_from_slice(&chunk);
    }
    Ok(Some(content))
}

impl<C: WalRecordCursor> ChunkSource for RedoStreamReader<C> {
    fn next_chunk(&mut self) -> crate::Result<Option<Vec<u8>>> {
        let Some(chunk) = self.reference.chunks.get(self.next).copied() else {
            return Ok(None);
        };
        let mut bytes = self.read_chunk(chunk)?;
        self.next += 1;
        if self.skip_leading > 0 {
            bytes.drain(..self.skip_leading.min(bytes.len()));
            self.skip_leading = 0;
        }
        Ok(Some(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::RedoStreamId;
    use crate::wal::manager::{RedoChunkPlacement, WalManager};
    use crate::wal::redo::chunk::RedoChunkHeader;
    use crate::wal::redo::frame::FrameBatcher;
    use crate::wal::redo::stream_ref::chunk_crc;

    fn placement() -> RedoChunkPlacement {
        RedoChunkPlacement {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(2),
            database_id: DatabaseId::DEFAULT,
        }
    }

    fn stream() -> RedoStreamId {
        RedoStreamId::Session {
            vshard: 2,
            idempotency_key: 31,
        }
    }

    /// Append `chunks` of `stream()`, with an unrelated record between each,
    /// and return their reference.
    fn append(wal: &WalManager, chunks: &[Vec<u8>]) -> RedoStreamRef {
        let len = chunks.iter().map(|chunk| chunk.len() as u64).sum();
        let mut refs = Vec::new();
        for (index, bytes) in (0u32..).zip(chunks) {
            let header = RedoChunkHeader {
                stream: stream(),
                group_id: 1,
                term: 1,
                len,
                index,
            };
            let lsns = wal
                .append_redo_chunk(100 + u64::from(index), placement(), header, bytes)
                .expect("append chunk");
            refs.push(RedoChunkRef {
                index,
                first_lsn: lsns.first.as_u64(),
                last_lsn: lsns.last.as_u64(),
                len: bytes.len() as u64,
                crc: chunk_crc(bytes),
            });
            wal.appender(0)
                .append_snapshot_installed(9)
                .expect("unrelated record");
        }
        wal.sync().expect("sync");
        RedoStreamRef {
            stream: stream(),
            len,
            chunks: refs,
            publish_position: None,
            entry: None,
            stamp_floor: None,
            keys: Default::default(),
        }
    }

    fn read_all(wal: &WalManager, reference: RedoStreamRef) -> crate::Result<Vec<Vec<u8>>> {
        let mut reader =
            RedoStreamReader::<nodedb_wal::LazyWalReader>::open(&wal.read_source(), reference)?;
        let mut out = Vec::new();
        while let Some(chunk) = reader.next_chunk()? {
            out.push(chunk);
        }
        Ok(out)
    }

    #[test]
    fn every_chunk_reads_back_between_unrelated_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
        // The middle chunk spans several WAL pieces.
        let big: Vec<u8> = (0..wal.max_payload() * 2).map(|i| i as u8).collect();
        let chunks = vec![b"first".to_vec(), big, b"last".to_vec()];
        let reference = append(&wal, &chunks);
        assert_eq!(read_all(&wal, reference).expect("read"), chunks);
    }

    #[test]
    fn a_chunk_whose_checksum_or_length_differs_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
        let reference = append(&wal, &[b"abcd".to_vec(), b"ef".to_vec()]);
        let mut bad_crc = reference.clone();
        bad_crc.chunks[1].crc ^= 1;
        assert!(read_all(&wal, bad_crc).is_err());
        let mut bad_len = reference;
        bad_len.chunks[1].len = 3;
        bad_len.len = 7;
        assert!(read_all(&wal, bad_len).is_err());
    }

    /// The batcher over the WAL reader cuts the content into whole records,
    /// though the chunks split records at arbitrary bytes.
    #[test]
    fn batches_over_the_wal_reader_hold_whole_records() {
        use crate::wal::RedoSubRecord;
        use crate::wal::redo::frame::{RedoFrameTag, RedoFrames, push_frame};
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
        let mut content = Vec::new();
        for byte in 0..30u8 {
            let op = RedoSubRecord {
                record_type: 7,
                payload: vec![byte; 70],
            };
            push_frame(&mut content, RedoFrameTag::Op, &op).expect("frame");
        }
        let chunks: Vec<Vec<u8>> = content.chunks(101).map(<[u8]>::to_vec).collect();
        let reference = append(&wal, &chunks);
        let reader =
            RedoStreamReader::<nodedb_wal::LazyWalReader>::open(&wal.read_source(), reference)
                .expect("open");
        let mut batcher = FrameBatcher::new(reader, 300);
        let mut joined = Vec::new();
        while let Some(batch) = batcher.next_batch().expect("batch") {
            assert!(RedoFrames::new(&batch).all(|frame| frame.is_ok()));
            joined.extend(batch);
        }
        assert_eq!(joined, content);
    }
}
