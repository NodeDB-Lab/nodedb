// SPDX-License-Identifier: BUSL-1.1

//! Pack framed redo records into parts of a bounded size.

use std::collections::VecDeque;

use super::codec::{RedoFrameTag, RedoFrames, decode_frame, push_frame};
use crate::wal::{RedoRowChange, RedoRowSource, RedoSubRecord};

/// Where a [`RedoPartWriter`] puts each part it closes. The writer reuses
/// its part buffer, so an output copies the bytes it keeps.
pub trait PartOut {
    fn put(&mut self, part: &[u8]) -> crate::Result<()>;
}

/// The parts of one framed redo, in order, held in memory.
#[derive(Debug, Default)]
pub struct RedoParts {
    pub parts: VecDeque<Vec<u8>>,
    /// The byte length of every part.
    pub len: u64,
}

impl PartOut for RedoParts {
    fn put(&mut self, part: &[u8]) -> crate::Result<()> {
        self.len += part.len() as u64;
        self.parts.push_back(part.to_vec());
        Ok(())
    }
}

/// The part buffer a writer starts with. It grows to the largest part and
/// is reused for every later one.
const INITIAL_PART_CAPACITY: usize = 64 * 1024;

/// Packs whole records into parts and hands each closed part to its
/// output. A part holds at most `max_part_bytes`, or one record past that
/// bound alone. The writer holds one part at a time, in one buffer.
pub struct RedoPartWriter<O: PartOut> {
    max_part_bytes: usize,
    current: Vec<u8>,
    out: O,
    len: u64,
    parts: u64,
}

impl<O: PartOut> RedoPartWriter<O> {
    pub fn new(max_part_bytes: usize, out: O) -> Self {
        Self {
            max_part_bytes,
            current: Vec::with_capacity(max_part_bytes.min(INITIAL_PART_CAPACITY)),
            out,
            len: 0,
            parts: 0,
        }
    }

    /// Append the record of `item` under `tag`.
    pub fn push<T: zerompk::ToMessagePack>(
        &mut self,
        tag: RedoFrameTag,
        item: &T,
    ) -> crate::Result<()> {
        let start = self.current.len();
        push_frame(&mut self.current, tag, item)?;
        if self.current.len() > self.max_part_bytes && start > 0 {
            // The new record passes the bound: the records before it close
            // a part, and the record starts the next one.
            self.close(start)?;
            self.current.drain(..start);
        }
        if self.current.len() >= self.max_part_bytes {
            self.close(self.current.len())?;
            self.current.clear();
        }
        Ok(())
    }

    /// Hand the first `end` buffered bytes to the output as one part.
    fn close(&mut self, end: usize) -> crate::Result<()> {
        self.len += end as u64;
        self.parts += 1;
        self.out.put(&self.current[..end])
    }

    /// Close the last part. Returns the output, the byte length of every
    /// part, and the part count.
    pub fn finish(mut self) -> crate::Result<(O, u64, u64)> {
        if !self.current.is_empty() {
            self.close(self.current.len())?;
        }
        Ok((self.out, self.len, self.parts))
    }
}

/// The row-sized lists a resolve produces, framed into parts of at most
/// `max_part_bytes`: row sources, then sub-records, then row changes. Each
/// list is consumed as it frames.
pub fn frame_resolved_lists(
    row_sources: Vec<RedoRowSource>,
    ops: Vec<RedoSubRecord>,
    row_changes: Vec<RedoRowChange>,
    max_part_bytes: usize,
) -> crate::Result<RedoParts> {
    let mut writer = RedoPartWriter::new(max_part_bytes, RedoParts::default());
    for source in row_sources {
        writer.push(RedoFrameTag::RowSource, &source)?;
    }
    for op in ops {
        writer.push(RedoFrameTag::Op, &op)?;
    }
    for change in row_changes {
        writer.push(RedoFrameTag::RowChange, &change)?;
    }
    Ok(writer.finish()?.0)
}

/// The record a run of resolved-list frames holds: its row sources,
/// sub-records and row changes, with every header field empty. A frame of
/// another kind is refused.
pub fn lists_record(bytes: &[u8]) -> crate::Result<crate::wal::RedoRecord> {
    let mut record = crate::wal::RedoRecord {
        version: 1,
        ops: Vec::new(),
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: Vec::new(),
        sum_targets: Vec::new(),
        pins: Default::default(),
    };
    for frame in RedoFrames::new(bytes) {
        let (tag, body) = frame?;
        match tag {
            RedoFrameTag::RowSource => record.row_sources.push(decode_frame(tag, body)?),
            RedoFrameTag::Op => record.ops.push(decode_frame(tag, body)?),
            RedoFrameTag::RowChange => record.row_changes.push(decode_frame(tag, body)?),
            RedoFrameTag::Header
            | RedoFrameTag::Publish
            | RedoFrameTag::Identity
            | RedoFrameTag::Read => {
                return Err(crate::Error::Serialization {
                    format: "redo frame".into(),
                    detail: format!("a resolved-list run holds a {tag:?} record"),
                });
            }
        }
    }
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(byte: u8, len: usize) -> RedoSubRecord {
        RedoSubRecord {
            record_type: 7,
            payload: vec![byte; len],
        }
    }

    fn decode_ops(bytes: &[u8]) -> Vec<RedoSubRecord> {
        RedoFrames::new(bytes)
            .map(|frame| {
                let (tag, body) = frame.expect("frame");
                decode_frame(tag, body).expect("decode")
            })
            .collect()
    }

    #[test]
    fn parts_stay_within_the_bound_and_join_to_every_record() {
        let ops: Vec<_> = (0..200u8).map(|b| op(b, 100)).collect();
        let parts = frame_resolved_lists(Vec::new(), ops.clone(), Vec::new(), 1024).expect("frame");
        assert!(parts.parts.len() > 10);
        let mut joined = Vec::new();
        for part in &parts.parts {
            assert!(part.len() <= 1024, "a part of {} bytes", part.len());
            assert!(!part.is_empty());
            // Every part decodes on its own.
            assert!(!decode_ops(part).is_empty());
            joined.extend_from_slice(part);
        }
        assert_eq!(joined.len() as u64, parts.len);
        assert_eq!(decode_ops(&joined), ops);
    }

    #[test]
    fn a_record_past_the_bound_travels_alone() {
        let ops = vec![op(1, 10), op(2, 5000), op(3, 10)];
        let parts = frame_resolved_lists(Vec::new(), ops.clone(), Vec::new(), 1024).expect("frame");
        let sizes: Vec<usize> = parts.parts.iter().map(Vec::len).collect();
        assert_eq!(parts.parts.len(), 3, "sizes {sizes:?}");
        assert_eq!(decode_ops(&parts.parts[1]), vec![op(2, 5000)]);
    }

    #[test]
    fn no_record_frames_no_part() {
        let parts = frame_resolved_lists(Vec::new(), Vec::new(), Vec::new(), 1024).expect("frame");
        assert!(parts.parts.is_empty());
        assert_eq!(parts.len, 0);
    }
}
