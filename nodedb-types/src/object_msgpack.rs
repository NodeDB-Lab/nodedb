// SPDX-License-Identifier: Apache-2.0

//! Object-context decoding for plain maps and stored tagged `Value::Object` bodies.

use std::borrow::Cow;

use zerompk::{FromMessagePack, Read, SliceReader};

use crate::msgpack_span::skip_value;
use crate::{MsgpackError, MsgpackResult, Value};

/// Encoding of the fields inside an object body.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectFormat {
    Plain,
    Native,
}

/// A borrowed object body with an explicit field encoding.
#[derive(Clone, Copy, Debug)]
pub struct ObjectView<'a> {
    map: &'a [u8],
    format: ObjectFormat,
    count: usize,
    header_len: usize,
}

impl<'a> ObjectView<'a> {
    /// Read exactly one object. Arrays only denote objects in this object context.
    pub fn new(bytes: &'a [u8]) -> MsgpackResult<Self> {
        let mut reader = SliceReader::new(bytes);
        let (format, start) = match reader.peek_marker()? {
            0x80..=0x8f | 0xde | 0xdf => (ObjectFormat::Plain, 0),
            marker => {
                reader.check_array_len(2)?;
                let tag = reader.read_u8()?;
                if tag != 7 {
                    return Err(zerompk::Error::InvalidMarker(tag).into());
                }
                let tag_start = header_len(marker).ok_or_else(truncated)?;
                (
                    ObjectFormat::Native,
                    skip_value(bytes, tag_start).ok_or_else(truncated)?,
                )
            }
        };
        let map = bytes.get(start..).ok_or_else(truncated)?;
        let mut reader = SliceReader::new(map);
        let count = reader.read_map_len()?;
        let header_len = map
            .first()
            .copied()
            .and_then(header_len)
            .ok_or_else(truncated)?;
        let mut pos = header_len;
        for _ in 0..count {
            let (_, value, end) = next_entry(map, pos)?;
            if format == ObjectFormat::Native {
                check_native(&mut SliceReader::new(value))?;
            }
            pos = end;
        }
        finish(start.saturating_add(pos), bytes.len())?;
        Ok(Self {
            map,
            format,
            count,
            header_len,
        })
    }

    pub fn format(self) -> ObjectFormat {
        self.format
    }

    /// Borrow the inner map. Field bytes retain their declared encoding.
    pub fn map_bytes(self) -> &'a [u8] {
        self.map
    }

    /// Iterate string keys and complete encoded field values.
    pub fn fields(self) -> ObjectFields<'a> {
        ObjectFields {
            map: self.map,
            pos: self.header_len,
            remaining: self.count,
        }
    }

    /// The value of `field`. A repeated key resolves to its last entry, as
    /// [`Self::decode`] resolves it.
    pub fn get(self, field: &str) -> MsgpackResult<Option<Value>> {
        let mut last = None;
        for entry in self.fields() {
            let (key, bytes) = entry?;
            if key == field {
                last = Some(bytes);
            }
        }
        last.map(|bytes| self.decode_value(bytes)).transpose()
    }

    /// Decode a field using this object's encoding, without guessing from its bytes.
    pub fn decode_value(self, bytes: &[u8]) -> MsgpackResult<Value> {
        match self.format {
            ObjectFormat::Plain => crate::value_from_msgpack(bytes),
            ObjectFormat::Native => {
                check_native(&mut SliceReader::new(bytes))?;
                let end = skip_value(bytes, 0).ok_or_else(truncated)?;
                finish(end, bytes.len())?;
                Ok(Value::read(&mut SliceReader::new(bytes))?)
            }
        }
    }

    pub fn decode(self) -> MsgpackResult<Value> {
        let mut object = std::collections::HashMap::new();
        for entry in self.fields() {
            let (key, bytes) = entry?;
            object.insert(key.to_owned(), self.decode_value(bytes)?);
        }
        Ok(Value::Object(object))
    }
}

/// Decode an expected object, retaining native value types.
pub fn decode_object(bytes: &[u8]) -> MsgpackResult<Value> {
    ObjectView::new(bytes)?.decode()
}

pub struct ObjectFields<'a> {
    map: &'a [u8],
    pos: usize,
    remaining: usize,
}

impl<'a> Iterator for ObjectFields<'a> {
    type Item = MsgpackResult<(&'a str, &'a [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        match next_entry(self.map, self.pos) {
            Ok((key, value, end)) => {
                self.pos = end;
                Some(Ok((key, value)))
            }
            Err(err) => {
                self.remaining = 0;
                Some(Err(err))
            }
        }
    }
}

/// Byte length of the array or map header that starts with `marker`.
fn header_len(marker: u8) -> Option<usize> {
    match marker {
        0x80..=0x9f => Some(1),
        0xdc | 0xde => Some(3),
        0xdd | 0xdf => Some(5),
        _ => None,
    }
}

fn truncated() -> MsgpackError {
    zerompk::Error::BufferTooSmall.into()
}

/// Read the string key at `pos` and the value after it. Returns the key, the
/// value bytes, and the offset of the next entry.
fn next_entry(map: &[u8], pos: usize) -> MsgpackResult<(&str, &[u8], usize)> {
    let value_start = skip_value(map, pos).ok_or_else(truncated)?;
    let end = skip_value(map, value_start).ok_or_else(truncated)?;
    let key_bytes = map.get(pos..value_start).ok_or_else(truncated)?;
    let value = map.get(value_start..end).ok_or_else(truncated)?;
    let key = match SliceReader::new(key_bytes).read_string()? {
        Cow::Borrowed(key) => key,
        Cow::Owned(_) => return Err(truncated()),
    };
    Ok((key, value, end))
}

fn finish(consumed: usize, total: usize) -> MsgpackResult<()> {
    if consumed == total {
        Ok(())
    } else {
        Err(MsgpackError::TrailingBytes { consumed, total })
    }
}

fn check_native<'a, R: Read<'a>>(reader: &mut R) -> zerompk::Result<()> {
    reader.increment_depth()?;
    let result = check_native_inner(reader);
    reader.decrement_depth();
    result
}

fn check_native_inner<'a, R: Read<'a>>(reader: &mut R) -> zerompk::Result<()> {
    let actual = reader.read_array_len()?;
    let tag = reader.read_u8()?;
    let expected = match tag {
        0 => 1,
        1..=12 | 14 | 15 | 18..=20 => 2,
        13 => 5,
        16 => 4,
        17 => 3,
        _ => return Err(zerompk::Error::InvalidMarker(tag)),
    };
    if actual != expected {
        return Err(zerompk::Error::ArrayLengthMismatch { expected, actual });
    }
    match tag {
        0 => {}
        1 => {
            reader.read_boolean()?;
        }
        2 => {
            reader.read_i64()?;
        }
        3 => {
            reader.read_f64()?;
        }
        4 | 8 | 9 | 15 => {
            reader.read_string()?;
        }
        5 | 12 | 20 => {
            let bytes = reader.read_binary()?;
            if (tag == 12 && bytes.len() != 16) || (tag == 20 && bytes.len() % 4 != 0) {
                return Err(zerompk::Error::BufferTooSmall);
            }
        }
        6 | 14 => {
            let count = reader.read_array_len()?;
            for _ in 0..count {
                check_native(reader)?;
            }
        }
        7 => {
            let count = reader.read_map_len()?;
            for _ in 0..count {
                reader.read_string()?;
                check_native(reader)?;
            }
        }
        16 => {
            for _ in 0..2 {
                if reader.peek_marker()? == 0xc0 {
                    reader.read_nil()?;
                } else {
                    check_native(reader)?;
                }
            }
            reader.read_boolean()?;
        }
        17 => {
            reader.read_string()?;
            reader.read_string()?;
        }
        10 | 19 => {
            crate::NdbDateTime::read(reader)?;
        }
        11 => {
            crate::NdbDuration::read(reader)?;
        }
        13 => {
            crate::geometry::tagged::msgpack::read_fields(reader)?;
        }
        18 => {
            crate::ArrayCell::read(reader)?;
        }
        _ => return Err(zerompk::Error::InvalidMarker(tag)),
    }
    Ok(())
}

#[cfg(test)]
#[path = "object_msgpack_tests.rs"]
mod tests;
