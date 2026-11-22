// SPDX-License-Identifier: BUSL-1.1

//! io_uring record cursor over one WAL segment, for a Data Plane core.
//!
//! A core reads a chunked redo's stream back from the WAL through this
//! cursor (see `crate::wal::redo::RedoStreamReader`). It walks the segment
//! with [`UringSeqReader`]: one `IORING_OP_READ` per refill, a bounded
//! buffer, and no blocking `std::fs` read. A payload the caller skips costs
//! no read.
//!
//! The WAL writes its segments with `O_DIRECT`. The cursor opens them
//! buffered, so its offsets and buffers carry no alignment constraint, and
//! the kernel drops cached pages a direct write replaces. The cursor only
//! reads records the WAL made durable before it opened.

use std::path::Path;

use nodedb_wal::RecordHeader;
use nodedb_wal::crypto::KeyRing;
use nodedb_wal::preamble::{PREAMBLE_SIZE, WAL_PREAMBLE_MAGIC, parse_leading_preamble};
use nodedb_wal::record::{HEADER_SIZE, RecordType};
use nodedb_wal::segment::SegmentDecryptor;

use super::uring_seq_reader::UringSeqReader;
use crate::wal::redo::WalRecordCursor;

/// Refill size of the cursor's read buffer.
const REFILL_BYTES: usize = 256 * 1024;

/// A forward record cursor over one segment.
pub struct UringWalCursor {
    reader: UringSeqReader,
    /// The file offset of the next unread byte.
    offset: u64,
    decryptor: SegmentDecryptor,
}

fn unavailable(path: &Path) -> crate::Error {
    crate::Error::Storage {
        engine: "wal_reader".into(),
        detail: format!(
            "io_uring could not open WAL segment '{}' for a redo stream read",
            path.display()
        ),
    }
}

impl WalRecordCursor for UringWalCursor {
    fn open(path: &Path, ring: Option<&KeyRing>) -> crate::Result<Self> {
        let mut reader =
            UringSeqReader::open(path, REFILL_BYTES).ok_or_else(|| unavailable(path))?;
        let mut head = [0u8; PREAMBLE_SIZE];
        let preamble = if reader.read_exact(&mut head)? {
            parse_leading_preamble(&head, &WAL_PREAMBLE_MAGIC).map_err(crate::Error::Wal)?
        } else {
            None
        };
        let offset = if preamble.is_some() {
            PREAMBLE_SIZE as u64
        } else {
            // A segment written without encryption starts with its first
            // record: read it from offset 0.
            reader.seek(0);
            0
        };
        Ok(Self {
            reader,
            offset,
            decryptor: SegmentDecryptor::new(preamble.as_ref(), ring),
        })
    }

    fn next_header(&mut self) -> crate::Result<Option<RecordHeader>> {
        loop {
            let mut buf = [0u8; HEADER_SIZE];
            if !self.reader.read_exact(&mut buf)? {
                return Ok(None);
            }
            let header = RecordHeader::from_bytes(&buf);
            // A header that does not validate is where the segment's
            // records end: the unwritten tail, or a torn last record.
            if header.validate(self.offset).is_err() {
                return Ok(None);
            }
            self.offset += HEADER_SIZE as u64;
            let logical = header.logical_record_type();
            match RecordType::from_raw(logical) {
                Some(RecordType::Noop) => {
                    self.skip_payload(&header)?;
                    continue;
                }
                Some(_) => {}
                None if RecordType::is_required(logical) => {
                    return Err(crate::Error::Wal(
                        nodedb_wal::WalError::UnknownRequiredRecordType {
                            record_type: header.record_type,
                            lsn: header.lsn,
                        },
                    ));
                }
                None => {}
            }
            return Ok(Some(header));
        }
    }

    fn read_payload(&mut self, header: &RecordHeader) -> crate::Result<Vec<u8>> {
        let mut payload = vec![0u8; header.payload_len as usize];
        if !self.reader.read_exact(&mut payload)? {
            return Err(crate::Error::SegmentCorrupted {
                detail: format!(
                    "the WAL record at LSN {} ends before its {}-byte payload",
                    header.lsn, header.payload_len
                ),
            });
        }
        self.offset += u64::from(header.payload_len);
        if header.crc32c != header.compute_checksum(&payload) {
            return Err(crate::Error::SegmentCorrupted {
                detail: format!(
                    "the WAL record at LSN {} fails its checksum on a redo stream read",
                    header.lsn
                ),
            });
        }
        self.decryptor
            .decrypt_payload(header, payload)
            .map_err(crate::Error::Wal)
    }

    fn skip_payload(&mut self, header: &RecordHeader) -> crate::Result<()> {
        self.reader.skip(u64::from(header.payload_len))?;
        self.offset += u64::from(header.payload_len);
        Ok(())
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::{RedoChunkPlacement, WalManager};
    use crate::wal::redo::frame::ChunkSource;
    use crate::wal::redo::{RedoChunkRef, RedoStreamReader, RedoStreamRef, chunk_crc};
    use crate::wal::{RedoChunkHeader, RedoStreamId};

    /// The io_uring cursor reads the same chunks the blocking reader reads.
    #[test]
    fn the_uring_cursor_reads_a_stream_back() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
        let stream = RedoStreamId::Session {
            vshard: 1,
            idempotency_key: 4,
        };
        let big: Vec<u8> = (0..wal.max_payload() + 17).map(|i| i as u8).collect();
        let chunks = vec![b"head".to_vec(), big];
        let len = chunks.iter().map(|chunk| chunk.len() as u64).sum();
        let mut refs = Vec::new();
        for (index, bytes) in (0u32..).zip(&chunks) {
            let lsns = wal
                .append_redo_chunk(
                    1 + u64::from(index),
                    RedoChunkPlacement {
                        tenant_id: TenantId::new(1),
                        vshard_id: VShardId::new(1),
                        database_id: DatabaseId::DEFAULT,
                    },
                    RedoChunkHeader {
                        stream,
                        group_id: 1,
                        term: 1,
                        len,
                        index,
                    },
                    bytes,
                )
                .expect("chunk");
            refs.push(RedoChunkRef {
                index,
                first_lsn: lsns.first.as_u64(),
                last_lsn: lsns.last.as_u64(),
                len: bytes.len() as u64,
                crc: chunk_crc(bytes),
            });
        }
        wal.sync().expect("sync");
        let reference = RedoStreamRef {
            stream,
            len,
            chunks: refs,
            publish_position: None,
            entry: None,
            stamp_floor: None,
            keys: Default::default(),
        };
        let mut reader =
            RedoStreamReader::<UringWalCursor>::open(&wal.read_source(), reference).expect("open");
        let mut read = Vec::new();
        while let Some(chunk) = reader.next_chunk().expect("chunk") {
            read.push(chunk);
        }
        assert_eq!(read, chunks);
    }
}
