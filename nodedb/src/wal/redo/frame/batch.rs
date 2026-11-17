// SPDX-License-Identifier: BUSL-1.1

//! Cut a chunk stream's framed content into batches of whole records.
//!
//! A stream's chunks split its framed content at fixed byte counts, so a
//! record can start in one chunk and end in a later one. The batcher holds
//! the bytes of at most one batch, one chunk and one record. Each batch it
//! hands out is a run of whole records, so each decodes on its own.

use super::codec::FRAME_HEAD_BYTES;

/// Where the chunks of one stream come from, in index order.
pub trait ChunkSource {
    /// The next chunk's bytes, `None` after the last one.
    fn next_chunk(&mut self) -> crate::Result<Option<Vec<u8>>>;
}

/// An in-memory chunk list, in index order.
impl ChunkSource for std::vec::IntoIter<Vec<u8>> {
    fn next_chunk(&mut self) -> crate::Result<Option<Vec<u8>>> {
        Ok(self.next())
    }
}

/// Where the next batch ends in the held bytes.
enum Cut {
    /// The first `n` held bytes are whole records and close a batch.
    At(usize),
    /// The held bytes end before the batch closes.
    NeedMore,
}

/// Hands out a stream's content as batches of whole records.
pub struct FrameBatcher<S: ChunkSource> {
    source: S,
    /// A batch holds at most this many bytes, or one record past the bound
    /// alone.
    max_batch_bytes: usize,
    /// Bytes read from the source and not handed out yet.
    held: Vec<u8>,
    /// Whether the source handed out its last chunk.
    ended: bool,
    /// Content bytes handed out in earlier batches.
    handed_out: u64,
}

impl<S: ChunkSource> FrameBatcher<S> {
    pub fn new(source: S, max_batch_bytes: usize) -> Self {
        Self {
            source,
            max_batch_bytes: max_batch_bytes.max(FRAME_HEAD_BYTES),
            held: Vec::new(),
            ended: false,
            handed_out: 0,
        }
    }

    /// The next batch, `None` once the content ended on a record boundary.
    /// Content that ends inside a record is refused.
    pub fn next_batch(&mut self) -> crate::Result<Option<Vec<u8>>> {
        loop {
            match self.cut() {
                Cut::At(end) => {
                    let rest = self.held.split_off(end);
                    self.handed_out += end as u64;
                    return Ok(Some(std::mem::replace(&mut self.held, rest)));
                }
                Cut::NeedMore if self.ended => {
                    if self.held.is_empty() {
                        return Ok(None);
                    }
                    // Both numbers count from the content's start, so every
                    // node names the same record whatever its batch bound.
                    let start = self.last_record_start();
                    return Err(crate::Error::SegmentCorrupted {
                        detail: format!(
                            "a redo stream's content ends {} bytes into the framed record \
                             at content offset {}",
                            self.held.len() - start,
                            self.handed_out + start as u64
                        ),
                    });
                }
                Cut::NeedMore => match self.source.next_chunk()? {
                    Some(chunk) if self.held.is_empty() => self.held = chunk,
                    Some(chunk) => self.held.extend_from_slice(&chunk),
                    None => self.ended = true,
                },
            }
        }
    }

    /// Where the record the held bytes end inside starts, in the held bytes.
    fn last_record_start(&self) -> usize {
        let mut start = 0usize;
        while let Some(head) = self.held.get(start..start + FRAME_HEAD_BYTES) {
            let body = u32::from_le_bytes([head[1], head[2], head[3], head[4]]) as usize;
            let next = start.saturating_add(FRAME_HEAD_BYTES).saturating_add(body);
            if next > self.held.len() {
                break;
            }
            start = next;
        }
        start
    }

    /// Where the held bytes close a batch.
    fn cut(&self) -> Cut {
        let mut end = 0usize;
        while let Some(head) = self.held.get(end..end + FRAME_HEAD_BYTES) {
            let body = u32::from_le_bytes([head[1], head[2], head[3], head[4]]) as usize;
            let next = end.saturating_add(FRAME_HEAD_BYTES).saturating_add(body);
            // A record that passes the bound starts the next batch, unless it
            // is the batch's first record.
            if end > 0 && next > self.max_batch_bytes {
                return Cut::At(end);
            }
            if next > self.held.len() {
                break;
            }
            end = next;
            if end >= self.max_batch_bytes {
                return Cut::At(end);
            }
        }
        if self.ended && end > 0 && end == self.held.len() {
            return Cut::At(end);
        }
        Cut::NeedMore
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::RedoSubRecord;
    use crate::wal::redo::frame::{RedoFrameTag, RedoFrames, decode_frame, push_frame};

    fn framed(count: u8, len: usize) -> (Vec<u8>, Vec<RedoSubRecord>) {
        let mut out = Vec::new();
        let mut ops = Vec::new();
        for byte in 0..count {
            let op = RedoSubRecord {
                record_type: 7,
                payload: vec![byte; len],
            };
            push_frame(&mut out, RedoFrameTag::Op, &op).expect("frame");
            ops.push(op);
        }
        (out, ops)
    }

    fn decode(bytes: &[u8]) -> Vec<RedoSubRecord> {
        RedoFrames::new(bytes)
            .map(|frame| {
                let (tag, body) = frame.expect("whole record");
                decode_frame(tag, body).expect("decode")
            })
            .collect()
    }

    fn chunks(bytes: &[u8], size: usize) -> std::vec::IntoIter<Vec<u8>> {
        bytes
            .chunks(size)
            .map(<[u8]>::to_vec)
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Chunks that cut records in two still batch into whole records, and
    /// the batches join to every record in order.
    #[test]
    fn a_record_split_across_chunks_lands_whole_in_one_batch() {
        let (content, ops) = framed(40, 90);
        let mut batcher = FrameBatcher::new(chunks(&content, 37), 400);
        let mut decoded = Vec::new();
        let mut batches = 0;
        while let Some(batch) = batcher.next_batch().expect("batch") {
            assert!(batch.len() <= 400, "a batch of {} bytes", batch.len());
            decoded.extend(decode(&batch));
            batches += 1;
        }
        assert!(batches > 5);
        assert_eq!(decoded, ops);
    }

    /// A record past the batch bound travels alone.
    #[test]
    fn a_record_past_the_bound_is_a_batch_of_its_own() {
        let mut content = Vec::new();
        let small = RedoSubRecord {
            record_type: 7,
            payload: vec![1; 10],
        };
        let large = RedoSubRecord {
            record_type: 7,
            payload: vec![2; 1000],
        };
        for op in [&small, &large, &small] {
            push_frame(&mut content, RedoFrameTag::Op, op).expect("frame");
        }
        let mut batcher = FrameBatcher::new(chunks(&content, 64), 100);
        let mut sizes = Vec::new();
        while let Some(batch) = batcher.next_batch().expect("batch") {
            sizes.push(decode(&batch).len());
        }
        assert_eq!(sizes, vec![1, 1, 1]);
    }

    #[test]
    fn content_that_ends_inside_a_record_is_refused() {
        let (mut content, _) = framed(3, 50);
        content.truncate(content.len() - 7);
        let mut batcher = FrameBatcher::new(chunks(&content, 30), 1 << 20);
        assert!(batcher.next_batch().is_err());
    }

    /// The refusal names the cut record by its place in the content, so
    /// every batch bound gives the same detail.
    #[test]
    fn the_cut_record_is_named_alike_at_every_batch_bound() {
        let (mut content, _) = framed(6, 50);
        content.truncate(content.len() - 7);
        let detail = |bound: usize| {
            let mut batcher = FrameBatcher::new(chunks(&content, 30), bound);
            loop {
                match batcher.next_batch() {
                    Ok(Some(_)) => continue,
                    Ok(None) => panic!("the cut content ended whole"),
                    Err(error) => return error.to_string(),
                }
            }
        };
        let first = detail(1 << 20);
        for bound in [1, 64, 120, 200] {
            assert_eq!(detail(bound), first, "batch bound {bound}");
        }
    }

    #[test]
    fn empty_content_ends_at_once() {
        let mut batcher = FrameBatcher::new(Vec::<Vec<u8>>::new().into_iter(), 64);
        assert!(batcher.next_batch().expect("end").is_none());
    }
}
