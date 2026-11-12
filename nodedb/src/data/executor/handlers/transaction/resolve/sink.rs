// SPDX-License-Identifier: BUSL-1.1

//! Where a resolve puts what it produces, as it produces it.
//!
//! Every serializer pushes each sub-record, row change and row source into a
//! sink and keeps none. A commit's resolve, a session's or a Calvin slice's,
//! frames them into parts and appends each part to the transaction's resolve
//! spill file, so the core holds one part at a time. A resolve test collects
//! them in memory.
//!
//! A sink push does not fail. The first error a sink meets stops every
//! later write, and [`RedoSink::finish`] returns it, so a resolve whose sink
//! failed answers that error and proposes nothing.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::data::executor::handlers::join::SpillPartitionWriter;
use crate::engine::timeseries::resolved_rejections::resolved_rejection;
use crate::wal::redo::frame::{PartOut, RedoFrameTag, RedoPartWriter};
use crate::wal::{RedoRowChange, RedoRowSource, RedoSubRecord};

#[cfg(test)]
use super::entry::ResolvedLists;

/// Takes the sub-records a serializer produces, in order.
pub(in crate::data::executor) trait OpSink {
    fn push(&mut self, op: RedoSubRecord);
}

/// Takes the row changes a resolve produces, in order.
pub(in crate::data::executor) trait ChangeSink {
    fn push(&mut self, change: RedoRowChange);
}

impl OpSink for Vec<RedoSubRecord> {
    fn push(&mut self, op: RedoSubRecord) {
        Vec::push(self, op);
    }
}

impl ChangeSink for Vec<RedoRowChange> {
    fn push(&mut self, change: RedoRowChange) {
        Vec::push(self, change);
    }
}

/// The resolve spill file a part writer appends each part to.
struct PartFile {
    writer: SpillPartitionWriter,
}

impl PartOut for PartFile {
    fn put(&mut self, part: &[u8]) -> crate::Result<()> {
        self.writer.append_row(part)
    }
}

enum Target {
    #[cfg(test)]
    Memory(ResolvedLists),
    Spill(RedoPartWriter<PartFile>),
}

/// The sink of one resolve. See the module doc.
pub(super) struct RedoSink {
    target: Target,
    op_count: u64,
    rejected: BTreeMap<String, u64>,
    error: Option<crate::Error>,
}

/// What a finished sink holds.
pub(super) enum SinkOutput {
    /// Every list, in memory.
    #[cfg(test)]
    Memory(ResolvedLists),
    /// The resolve spill file: one row per part.
    Spill {
        path: PathBuf,
        /// The byte length of every part.
        len: u64,
        /// How many parts the file holds.
        parts: u64,
    },
}

/// A finished sink.
pub(super) struct FinishedSink {
    pub output: SinkOutput,
    /// How many sub-records the sink took.
    pub op_count: u64,
    /// The timeseries lines the resolve rejected, by collection.
    pub rejected: BTreeMap<String, u64>,
}

impl RedoSink {
    /// A sink that collects every list in memory.
    #[cfg(test)]
    pub(super) fn memory() -> Self {
        Self::with_target(Target::Memory(ResolvedLists {
            ops: Vec::new(),
            row_changes: Vec::new(),
            row_sources: Vec::new(),
        }))
    }

    /// A sink that frames into parts of at most `max_part_bytes` and appends
    /// each part to a new spill file at `path`.
    pub(super) fn spill(path: &std::path::Path, max_part_bytes: usize) -> crate::Result<Self> {
        let writer = SpillPartitionWriter::create(path)?;
        Ok(Self::with_target(Target::Spill(RedoPartWriter::new(
            max_part_bytes,
            PartFile { writer },
        ))))
    }

    fn with_target(target: Target) -> Self {
        Self {
            target,
            op_count: 0,
            rejected: BTreeMap::new(),
            error: None,
        }
    }

    fn record(&mut self, write: impl FnOnce(&mut Target) -> crate::Result<()>) {
        if self.error.is_some() {
            return;
        }
        if let Err(e) = write(&mut self.target) {
            self.error = Some(e);
        }
    }

    /// Take one row source.
    pub(super) fn push_source(&mut self, source: RedoRowSource) {
        self.record(|target| match target {
            #[cfg(test)]
            Target::Memory(lists) => {
                lists.row_sources.push(source);
                Ok(())
            }
            Target::Spill(writer) => writer.push(RedoFrameTag::RowSource, &source),
        });
    }

    /// Finish the sink. Returns the first error any push met.
    pub(super) fn finish(self) -> crate::Result<FinishedSink> {
        if let Some(e) = self.error {
            return Err(e);
        }
        let output = match self.target {
            #[cfg(test)]
            Target::Memory(lists) => SinkOutput::Memory(lists),
            Target::Spill(writer) => {
                let (file, len, parts) = writer.finish()?;
                SinkOutput::Spill {
                    path: file.writer.finish()?,
                    len,
                    parts,
                }
            }
        };
        Ok(FinishedSink {
            output,
            op_count: self.op_count,
            rejected: self.rejected,
        })
    }
}

impl OpSink for RedoSink {
    fn push(&mut self, op: RedoSubRecord) {
        if self.error.is_some() {
            return;
        }
        self.op_count += 1;
        if let Some((collection, lines)) = resolved_rejection(&op) {
            *self.rejected.entry(collection).or_default() += lines;
        }
        self.record(|target| match target {
            #[cfg(test)]
            Target::Memory(lists) => {
                lists.ops.push(op);
                Ok(())
            }
            Target::Spill(writer) => writer.push(RedoFrameTag::Op, &op),
        });
    }
}

impl ChangeSink for RedoSink {
    fn push(&mut self, change: RedoRowChange) {
        self.record(|target| match target {
            #[cfg(test)]
            Target::Memory(lists) => {
                lists.row_changes.push(change);
                Ok(())
            }
            Target::Spill(writer) => writer.push(RedoFrameTag::RowChange, &change),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::handlers::join::FrameStreamReader;
    use crate::wal::redo::frame::{RedoFrames, decode_frame};

    fn op(byte: u8) -> RedoSubRecord {
        RedoSubRecord {
            record_type: 1,
            payload: vec![byte; 100],
        }
    }

    /// A spill sink writes one file row per part, each within the bound,
    /// and the rows join to every record in push order.
    #[test]
    fn a_spill_sink_writes_bounded_parts_that_join_to_every_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("resolve");
        let mut sink = RedoSink::spill(&path, 512).expect("sink");
        for byte in 0..50 {
            OpSink::push(&mut sink, op(byte));
        }
        let finished = sink.finish().expect("finish");
        assert_eq!(finished.op_count, 50);
        let SinkOutput::Spill { path, len, parts } = finished.output else {
            panic!("a spill sink finishes into its file");
        };
        let mut reader = FrameStreamReader::open(&path).expect("open");
        let mut joined = Vec::new();
        let mut rows = 0u64;
        while let Some(part) = reader.next_row().expect("row") {
            assert!(part.len() <= 512);
            joined.extend_from_slice(&part);
            rows += 1;
        }
        assert_eq!(rows, parts);
        assert_eq!(joined.len() as u64, len);
        let decoded: Vec<RedoSubRecord> = RedoFrames::new(&joined)
            .map(|frame| {
                let (tag, body) = frame.expect("frame");
                decode_frame(tag, body).expect("op")
            })
            .collect();
        assert_eq!(decoded, (0..50).map(op).collect::<Vec<_>>());
    }
}
