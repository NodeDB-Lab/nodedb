// SPDX-License-Identifier: BUSL-1.1

//! Framed redo: one redo as a sequence of self-delimiting records.
//!
//! Each record is `tag: u8 | len: u32 LE | item`. `item` is one MessagePack
//! value of the type `tag` names. A record encodes and decodes on its own,
//! so any run of whole records is a part of the redo, and the concatenation
//! of its parts is the redo.

/// Bytes of a record's tag and length.
pub const FRAME_HEAD_BYTES: usize = 5;

/// What one framed record holds.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RedoFrameTag {
    /// The fields of the redo that are not row-sized lists.
    Header = 1,
    /// One engine-native sub-record.
    Op = 2,
    /// One row change.
    RowChange = 3,
    /// One row-source group.
    RowSource = 4,
    /// One `PUBLISH TO` message.
    Publish = 5,
    /// One identity every replica binds before the apply.
    Identity = 6,
    /// One read the apply validates.
    Read = 7,
}

impl RedoFrameTag {
    fn from_byte(byte: u8) -> Option<Self> {
        Some(match byte {
            1 => Self::Header,
            2 => Self::Op,
            3 => Self::RowChange,
            4 => Self::RowSource,
            5 => Self::Publish,
            6 => Self::Identity,
            7 => Self::Read,
            _ => return None,
        })
    }
}

fn frame_error(detail: String) -> crate::Error {
    crate::Error::Serialization {
        format: "redo frame".into(),
        detail,
    }
}

/// Append the record of `item` under `tag` to `out`. On error `out` keeps
/// the bytes it held.
pub fn push_frame<T: zerompk::ToMessagePack>(
    out: &mut Vec<u8>,
    tag: RedoFrameTag,
    item: &T,
) -> crate::Result<()> {
    let start = out.len();
    out.push(tag as u8);
    out.extend_from_slice(&[0; 4]);
    if let Err(e) = zerompk::write_msgpack(out, item) {
        out.truncate(start);
        return Err(frame_error(format!("encode a {tag:?} record: {e}")));
    }
    let body = out.len() - start - FRAME_HEAD_BYTES;
    let Ok(len) = u32::try_from(body) else {
        out.truncate(start);
        return Err(frame_error(format!(
            "a {tag:?} record of {body} bytes passes the {} byte record bound",
            u32::MAX
        )));
    };
    out[start + 1..start + FRAME_HEAD_BYTES].copy_from_slice(&len.to_le_bytes());
    Ok(())
}

/// The records of a framed redo, in order. A malformed record ends the
/// walk with an error.
pub struct RedoFrames<'a> {
    rest: &'a [u8],
}

impl<'a> RedoFrames<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { rest: bytes }
    }
}

impl<'a> Iterator for RedoFrames<'a> {
    type Item = crate::Result<(RedoFrameTag, &'a [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rest.is_empty() {
            return None;
        }
        let rest = std::mem::take(&mut self.rest);
        let Some((head, after)) = rest.split_at_checked(FRAME_HEAD_BYTES) else {
            return Some(Err(frame_error(format!(
                "a record head needs {FRAME_HEAD_BYTES} bytes, {} remain",
                rest.len()
            ))));
        };
        let Some(tag) = RedoFrameTag::from_byte(head[0]) else {
            return Some(Err(frame_error(format!("unknown record tag {}", head[0]))));
        };
        let len = u32::from_le_bytes([head[1], head[2], head[3], head[4]]) as usize;
        let Some((body, next)) = after.split_at_checked(len) else {
            return Some(Err(frame_error(format!(
                "a {tag:?} record declares {len} bytes, {} remain",
                after.len()
            ))));
        };
        self.rest = next;
        Some(Ok((tag, body)))
    }
}

/// Decode the item of one record.
pub fn decode_frame<T: zerompk::FromMessagePackOwned>(
    tag: RedoFrameTag,
    body: &[u8],
) -> crate::Result<T> {
    zerompk::from_msgpack(body).map_err(|e| frame_error(format!("decode a {tag:?} record: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::RedoSubRecord;

    fn op(byte: u8) -> RedoSubRecord {
        RedoSubRecord {
            record_type: 7,
            payload: vec![byte; 32],
        }
    }

    #[test]
    fn records_decode_in_order() {
        let mut out = Vec::new();
        for byte in 0..4 {
            push_frame(&mut out, RedoFrameTag::Op, &op(byte)).expect("encode");
        }
        let decoded: Vec<RedoSubRecord> = RedoFrames::new(&out)
            .map(|frame| {
                let (tag, body) = frame.expect("frame");
                assert_eq!(tag, RedoFrameTag::Op);
                decode_frame(tag, body).expect("decode")
            })
            .collect();
        assert_eq!(decoded, (0..4).map(op).collect::<Vec<_>>());
    }

    #[test]
    fn a_truncated_record_is_refused() {
        let mut out = Vec::new();
        push_frame(&mut out, RedoFrameTag::Op, &op(1)).expect("encode");
        out.pop();
        let frames: Vec<_> = RedoFrames::new(&out).collect();
        assert_eq!(frames.len(), 1);
        assert!(frames[0].is_err());
    }

    #[test]
    fn an_unknown_tag_is_refused() {
        let mut out = Vec::new();
        push_frame(&mut out, RedoFrameTag::Op, &op(1)).expect("encode");
        out[0] = 0xff;
        assert!(RedoFrames::new(&out).next().expect("one record").is_err());
    }
}
