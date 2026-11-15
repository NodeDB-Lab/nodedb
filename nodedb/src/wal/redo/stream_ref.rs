// SPDX-License-Identifier: BUSL-1.1

//! The reference a chunked transaction redo's `TransactionRedo` record holds
//! in place of its content.
//!
//! A redo past the entry limit reaches every replica as chunk entries. Each
//! replica appends each chunk as `RedoChunk` records before the final entry
//! applies, so the WAL holds the content once already. The final entry's
//! `TransactionRedo` record holds a [`RedoStreamRef`] as its first
//! sub-record instead of the content: the stream, and the position, length
//! and checksum of each chunk.
//!
//! - The live install reads the chunks back from the WAL.
//! - Every reader of the whole WAL reads the chunk records the marker names
//!   and decodes the content from them (see [`super::stream_expand`]).
//!
//! A marker holds the reference as its only sub-record. No engine arm
//! claims the reference.

use nodedb_wal::record::RecordType;

use super::chunk::RedoStreamId;
use super::record::{PublishPosition, RedoRecord, RedoSubRecord};

/// Where one chunk of a stream sits in the WAL.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RedoChunkRef {
    /// The chunk's place in its stream, from `0`.
    pub index: u32,
    /// The LSN of the chunk's first `RedoChunk` piece record.
    pub first_lsn: u64,
    /// The LSN of the chunk's last piece record.
    pub last_lsn: u64,
    /// The chunk's byte length.
    pub len: u64,
    /// The CRC32C of the chunk's bytes.
    pub crc: u32,
}

/// The data-group entry whose apply installed a marker.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RedoEntryAt {
    pub group_id: u64,
    pub log_index: u64,
}

/// The chunk stream that holds a redo's framed content.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RedoStreamRef {
    pub stream: RedoStreamId,
    /// The byte length of the whole content.
    pub len: u64,
    /// Every chunk, in index order.
    pub chunks: Vec<RedoChunkRef>,
    /// The replicated position every `PUBLISH TO` message of the content
    /// takes. The apply that installs the record sets it. `None` on a node
    /// without Raft, where the record's LSN names each message.
    pub publish_position: Option<PublishPosition>,
    /// The entry that installs the record. The apply sets it beside
    /// `publish_position`. A restart reads it to stage the entry's change
    /// events again when the entry is delivered again.
    pub entry: Option<RedoEntryAt>,
    /// The ordinal every version of the content stamps at least. The funnel
    /// sets it for a session commit before the record is appended.
    pub stamp_floor: Option<i64>,
}

/// The CRC32C of one chunk's bytes.
pub fn chunk_crc(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

fn ref_error(detail: String) -> crate::Error {
    crate::Error::Serialization {
        format: "redo stream reference".into(),
        detail,
    }
}

impl RedoChunkRef {
    /// Check `bytes`, read back for this chunk of `stream`, against the
    /// length and checksum the chunk's apply recorded.
    pub fn check(&self, stream: RedoStreamId, bytes: &[u8]) -> crate::Result<()> {
        if bytes.len() as u64 != self.len {
            return Err(crate::Error::SegmentCorrupted {
                detail: format!(
                    "chunk {} of redo stream {stream:?} read back {} bytes from WAL LSNs \
                     {}..={}, and its apply recorded {}",
                    self.index,
                    bytes.len(),
                    self.first_lsn,
                    self.last_lsn,
                    self.len
                ),
            });
        }
        let crc = chunk_crc(bytes);
        if crc != self.crc {
            return Err(crate::Error::SegmentCorrupted {
                detail: format!(
                    "chunk {} of redo stream {stream:?} read back from WAL LSNs {}..={} has \
                     checksum {crc:#010x}, and its apply recorded {:#010x}",
                    self.index, self.first_lsn, self.last_lsn, self.crc
                ),
            });
        }
        Ok(())
    }
}

impl RedoStreamRef {
    /// The sub-record that carries this reference.
    pub fn to_sub_record(&self) -> crate::Result<RedoSubRecord> {
        let payload = zerompk::to_msgpack_vec(self)
            .map_err(|e| ref_error(format!("encode the reference of {:?}: {e}", self.stream)))?;
        Ok(RedoSubRecord {
            record_type: RecordType::RedoStreamRef as u32,
            payload,
        })
    }

    /// Whether `sub` carries a stream reference.
    pub fn is_ref(sub: &RedoSubRecord) -> bool {
        RecordType::from_raw(sub.record_type) == Some(RecordType::RedoStreamRef)
    }

    /// The reference `sub` carries.
    pub fn from_sub_record(sub: &RedoSubRecord) -> crate::Result<Self> {
        if !Self::is_ref(sub) {
            return Err(ref_error(format!(
                "sub-record type {} carries no stream reference",
                sub.record_type
            )));
        }
        zerompk::from_msgpack(&sub.payload)
            .map_err(|e| ref_error(format!("decode a stream reference: {e}")))
    }

    /// The reference `record` holds first, `None` for a record that holds
    /// its content inline.
    pub fn of(record: &RedoRecord) -> crate::Result<Option<Self>> {
        match record.ops.first() {
            Some(sub) if Self::is_ref(sub) => Self::from_sub_record(sub).map(Some),
            _ => Ok(None),
        }
    }

    /// The reference `record` holds when it holds no other sub-record: a
    /// marker whose content the WAL keeps in the chunk records.
    pub fn of_marker(record: &RedoRecord) -> crate::Result<Option<Self>> {
        if record.ops.len() != 1 {
            return Ok(None);
        }
        Self::of(record)
    }

    /// The marker record of this reference. `calvin_stamp` and
    /// `cross_shard_applied` are the ones the apply installs under, and
    /// `row_changes` the rows the Control-Plane change stream publishes.
    pub fn marker(
        &self,
        calvin_stamp: Option<super::record::CalvinStamp>,
        cross_shard_applied: Option<super::record::CrossShardAppliedKey>,
        row_changes: Vec<super::row_changes::RedoRowChange>,
    ) -> crate::Result<RedoRecord> {
        Ok(RedoRecord {
            version: 1,
            ops: vec![self.to_sub_record()?],
            calvin_stamp,
            cross_shard_applied,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes,
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
        })
    }

    /// Replace the reference `record` holds with this one.
    pub fn store_in(&self, record: &mut RedoRecord) -> crate::Result<()> {
        let sub = self.to_sub_record()?;
        match record.ops.first_mut() {
            Some(first) if Self::is_ref(first) => {
                *first = sub;
                Ok(())
            }
            _ => Err(ref_error(format!(
                "the record of {:?} holds no stream reference to replace",
                self.stream
            ))),
        }
    }

    /// The LSN of the stream's first chunk record.
    pub fn first_lsn(&self) -> Option<u64> {
        self.chunks.iter().map(|chunk| chunk.first_lsn).min()
    }

    /// Check that the chunks are whole: indexes run from `0` with no gap, the
    /// piece LSNs ascend, and the lengths add up to `len`.
    pub fn check_shape(&self) -> crate::Result<()> {
        let mut total = 0u64;
        let mut previous_last = None;
        for (expected, chunk) in (0u32..).zip(&self.chunks) {
            if chunk.index != expected {
                return Err(ref_error(format!(
                    "stream {:?} names chunk {} where chunk {expected} belongs",
                    self.stream, chunk.index
                )));
            }
            if chunk.last_lsn < chunk.first_lsn
                || previous_last.is_some_and(|last| chunk.first_lsn <= last)
            {
                return Err(ref_error(format!(
                    "stream {:?} names chunk {} at LSNs {}..={}, out of WAL order",
                    self.stream, chunk.index, chunk.first_lsn, chunk.last_lsn
                )));
            }
            previous_last = Some(chunk.last_lsn);
            total = total.saturating_add(chunk.len);
        }
        if total != self.len {
            return Err(ref_error(format!(
                "stream {:?} declares {} bytes and its chunks hold {total}",
                self.stream, self.len
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream_ref() -> RedoStreamRef {
        RedoStreamRef {
            stream: RedoStreamId::Session {
                vshard: 3,
                idempotency_key: 9,
            },
            len: 7,
            chunks: vec![
                RedoChunkRef {
                    index: 0,
                    first_lsn: 10,
                    last_lsn: 11,
                    len: 4,
                    crc: chunk_crc(b"abcd"),
                },
                RedoChunkRef {
                    index: 1,
                    first_lsn: 14,
                    last_lsn: 14,
                    len: 3,
                    crc: chunk_crc(b"efg"),
                },
            ],
            publish_position: None,
            entry: None,
            stamp_floor: Some(5),
        }
    }

    #[test]
    fn a_marker_round_trips_its_reference() {
        let reference = stream_ref();
        let record = reference.marker(None, None, Vec::new()).expect("marker");
        let bytes = record.to_bytes().expect("encode record");
        let decoded = RedoRecord::from_bytes(&bytes).expect("decode record");
        assert_eq!(
            RedoStreamRef::of(&decoded).expect("ref"),
            Some(reference.clone())
        );
        assert_eq!(
            RedoStreamRef::of_marker(&decoded).expect("marker"),
            Some(reference)
        );
    }

    #[test]
    fn a_record_with_content_is_no_marker() {
        let reference = stream_ref();
        let mut record = reference.marker(None, None, Vec::new()).expect("marker");
        record.ops.push(RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: vec![1],
        });
        assert!(RedoStreamRef::of_marker(&record).expect("decode").is_none());
        assert!(RedoStreamRef::of(&record).expect("decode").is_some());
        record.ops.remove(0);
        assert!(RedoStreamRef::of(&record).expect("decode").is_none());
    }

    #[test]
    fn the_shape_check_refuses_a_gap_a_wrong_length_and_a_wrong_order() {
        let reference = stream_ref();
        reference.check_shape().expect("whole");
        let mut gap = reference.clone();
        gap.chunks[1].index = 2;
        assert!(gap.check_shape().is_err());
        let mut short = reference.clone();
        short.len = 9;
        assert!(short.check_shape().is_err());
        let mut backwards = reference;
        backwards.chunks[1].first_lsn = 11;
        assert!(backwards.check_shape().is_err());
    }

    #[test]
    fn a_chunk_read_back_must_match_its_length_and_checksum() {
        let reference = stream_ref();
        let chunk = reference.chunks[0];
        chunk.check(reference.stream, b"abcd").expect("matches");
        assert!(chunk.check(reference.stream, b"abc").is_err());
        assert!(chunk.check(reference.stream, b"abce").is_err());
    }

    #[test]
    fn store_in_replaces_the_reference() {
        let reference = stream_ref();
        let mut record = reference.marker(None, None, Vec::new()).expect("marker");
        let mut moved = reference;
        moved.publish_position = Some(PublishPosition {
            partition: 3,
            epoch: 2,
            index: 40,
        });
        moved.store_in(&mut record).expect("store");
        assert_eq!(RedoStreamRef::of(&record).expect("ref"), Some(moved));
    }
}
