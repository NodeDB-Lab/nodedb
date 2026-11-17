// SPDX-License-Identifier: BUSL-1.1

//! One record of a sorted spill run, and the decoding of a run block.
//!
//! A record is one frame of a [`SpillPartitionWriter`] file (`[u32 LE
//! len][body]`). Its body is a MessagePack array `[bin key, u64 seq, bin
//! value]`. `key` and `value` are the MessagePack encodings of the run's key
//! and value types. A run orders records by key bytes ascending, then by
//! `seq` descending, so the first record of a key is its newest version.
//!
//! [`SpillPartitionWriter`]: crate::data::executor::handlers::join::SpillPartitionWriter

use std::borrow::Cow;
use std::path::Path;

use zerompk::{Read, Write};

use crate::data::executor::handlers::spill::error::SpillError;

/// One spilled version of a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::data::executor) struct RunRecord {
    /// MessagePack encoding of the key.
    pub key: Vec<u8>,
    /// Journal position of the write that produced this version.
    pub seq: u64,
    /// MessagePack encoding of the value.
    pub value: Vec<u8>,
}

/// Borrowed form of a record, for encoding without a copy.
pub(in crate::data::executor) struct RunRecordRef<'a> {
    pub key: &'a [u8],
    pub seq: u64,
    pub value: &'a [u8],
}

/// A record decoded in place: key and value borrow the frame body. A point
/// lookup compares keys on this form and copies only the record it returns.
pub(in crate::data::executor) struct RunRecordView<'a> {
    pub key: Cow<'a, [u8]>,
    pub seq: u64,
    pub value: Cow<'a, [u8]>,
}

impl zerompk::ToMessagePack for RunRecordRef<'_> {
    fn write<W: Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(3)?;
        writer.write_binary(self.key)?;
        writer.write_u64(self.seq)?;
        writer.write_binary(self.value)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for RunRecord {
    fn read<R: Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        reader.check_array_len(3)?;
        let key = reader.read_binary()?.into_owned();
        let seq = reader.read_u64()?;
        let value = reader.read_binary()?.into_owned();
        Ok(Self { key, seq, value })
    }
}

impl<'a> zerompk::FromMessagePack<'a> for RunRecordView<'a> {
    fn read<R: Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        reader.check_array_len(3)?;
        let key = reader.read_binary()?;
        let seq = reader.read_u64()?;
        let value = reader.read_binary()?;
        Ok(Self { key, seq, value })
    }
}

impl<'a> RunRecordView<'a> {
    /// Decode one frame body without copying its key or value.
    pub(in crate::data::executor) fn decode(
        path: &Path,
        body: &'a [u8],
    ) -> Result<Self, SpillError> {
        zerompk::from_msgpack(body).map_err(|e| SpillError::Codec {
            path: path.to_path_buf(),
            detail: e.to_string(),
        })
    }
}

impl RunRecordRef<'_> {
    /// The record's frame body.
    pub(in crate::data::executor) fn encode(&self, path: &Path) -> Result<Vec<u8>, SpillError> {
        encode_value(path, self)
    }
}

impl RunRecord {
    /// Decode one frame body.
    pub(in crate::data::executor) fn decode(path: &Path, body: &[u8]) -> Result<Self, SpillError> {
        zerompk::from_msgpack(body).map_err(|e| SpillError::Codec {
            path: path.to_path_buf(),
            detail: e.to_string(),
        })
    }

    /// The borrowed form of this record.
    pub(in crate::data::executor) fn as_ref(&self) -> RunRecordRef<'_> {
        RunRecordRef {
            key: &self.key,
            seq: self.seq,
            value: &self.value,
        }
    }
}

/// The frame bodies of a block of whole frames, in order. A frame cut
/// short yields one error and ends the iteration.
pub(in crate::data::executor) struct Frames<'p, 'a> {
    path: &'p Path,
    block: &'a [u8],
    at: usize,
}

impl<'p, 'a> Frames<'p, 'a> {
    pub(in crate::data::executor) fn new(path: &'p Path, block: &'a [u8]) -> Self {
        Self { path, block, at: 0 }
    }

    fn read_frame(&mut self) -> Result<&'a [u8], SpillError> {
        let at = self.at;
        let header = self
            .block
            .get(at..at + 4)
            .ok_or_else(|| SpillError::Corrupt {
                path: self.path.to_path_buf(),
                detail: format!("frame header cut short at block byte {at}"),
            })?;
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(header);
        let len = u32::from_le_bytes(len_bytes) as usize;
        let body = self
            .block
            .get(at + 4..at + 4 + len)
            .ok_or_else(|| SpillError::Corrupt {
                path: self.path.to_path_buf(),
                detail: format!("frame body of {len} bytes cut short at block byte {at}"),
            })?;
        self.at = at + 4 + len;
        Ok(body)
    }
}

impl<'a> Iterator for Frames<'_, 'a> {
    type Item = Result<&'a [u8], SpillError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.at >= self.block.len() {
            return None;
        }
        let frame = self.read_frame();
        if frame.is_err() {
            self.at = self.block.len();
        }
        Some(frame)
    }
}

/// Encode `value` as MessagePack for a run record or a journal entry.
pub(in crate::data::executor) fn encode_value<T: zerompk::ToMessagePack>(
    path: &Path,
    value: &T,
) -> Result<Vec<u8>, SpillError> {
    zerompk::to_msgpack_vec(value).map_err(|e| SpillError::Codec {
        path: path.to_path_buf(),
        detail: e.to_string(),
    })
}

/// Decode a MessagePack value of a run record or a journal entry.
pub(in crate::data::executor) fn decode_value<T>(path: &Path, bytes: &[u8]) -> Result<T, SpillError>
where
    T: for<'de> zerompk::FromMessagePack<'de>,
{
    zerompk::from_msgpack(bytes).map_err(|e| SpillError::Codec {
        path: path.to_path_buf(),
        detail: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Split a block of whole frames into frame bodies.
    fn frame_bodies<'a>(path: &Path, block: &'a [u8]) -> Result<Vec<&'a [u8]>, SpillError> {
        Frames::new(path, block).collect()
    }

    #[test]
    fn record_round_trips_and_frames_split() {
        let path = Path::new("r");
        let a = RunRecordRef {
            key: &[1, 2],
            seq: 7,
            value: &[0xff; 300],
        }
        .encode(path)
        .unwrap();
        let b = RunRecordRef {
            key: &[3],
            seq: 0,
            value: &[],
        }
        .encode(path)
        .unwrap();
        let mut block = Vec::new();
        for body in [&a, &b] {
            block.extend_from_slice(&(body.len() as u32).to_le_bytes());
            block.extend_from_slice(body);
        }
        let bodies = frame_bodies(path, &block).unwrap();
        assert_eq!(bodies.len(), 2);
        let first = RunRecord::decode(path, bodies[0]).unwrap();
        assert_eq!(first.key, vec![1, 2]);
        assert_eq!(first.seq, 7);
        assert_eq!(first.value, vec![0xff; 300]);
        assert!(frame_bodies(path, &block[..block.len() - 1]).is_err());
    }
}
