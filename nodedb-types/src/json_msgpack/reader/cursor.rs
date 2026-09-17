// SPDX-License-Identifier: Apache-2.0

//! Byte cursor shared by the msgpack readers and the JSON transcoder.

use super::super::error::{MsgpackError, MsgpackResult};

pub(crate) struct Cursor<'a> {
    pub(crate) data: &'a [u8],
    pub(crate) pos: usize,
    pub(crate) depth: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            depth: 0,
        }
    }

    #[inline]
    pub(crate) fn peek(&self) -> zerompk::Result<u8> {
        self.data
            .get(self.pos)
            .copied()
            .ok_or(zerompk::Error::BufferTooSmall)
    }

    #[inline]
    pub(crate) fn take(&mut self) -> zerompk::Result<u8> {
        let b = self.peek()?;
        self.pos += 1;
        Ok(b)
    }

    #[inline]
    pub(crate) fn take_n(&mut self, n: usize) -> zerompk::Result<&'a [u8]> {
        if self.pos + n > self.data.len() {
            return Err(zerompk::Error::BufferTooSmall);
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    pub(crate) fn read_u16_be(&mut self) -> zerompk::Result<u16> {
        let b = self.take_n(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    pub(crate) fn read_u32_be(&mut self) -> zerompk::Result<u32> {
        let b = self.take_n(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Read the ext type byte and eight-byte payload of a `fixext8` whose
    /// marker is already consumed.
    pub(crate) fn take_fixext8(&mut self) -> zerompk::Result<(i8, &'a [u8])> {
        let ext_type = self.take()? as i8;
        let payload = self.take_n(8)?;
        Ok((ext_type, payload))
    }

    /// Assert the whole input belonged to the value just read.
    ///
    /// A body carries exactly one top-level value, so anything left over means
    /// the bytes are not the value they claim to be — a stray suffix, a
    /// truncated body with another appended, or two values in one slot.
    /// Returning the leading value and dropping the rest would report success
    /// on corrupt bytes, so the remainder is an error.
    pub(crate) fn finish(&self) -> MsgpackResult<()> {
        if self.pos == self.data.len() {
            Ok(())
        } else {
            Err(MsgpackError::TrailingBytes {
                consumed: self.pos,
                total: self.data.len(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::json_msgpack::error::MsgpackError;
    use crate::json_msgpack::reader::{json_from_msgpack, value_from_msgpack};
    use crate::json_msgpack::transcoder::msgpack_to_json_string;
    use crate::json_msgpack::writer::json_to_msgpack;
    use serde_json::json;

    /// The readers decode exactly one top-level value. Bytes left over mean the
    /// input is not the value it claims to be, so returning the leading value would
    /// report success on corrupt input.
    #[test]
    fn trailing_byte_is_rejected() {
        let val = json!({"a": 1, "b": "two"});
        let mut bytes = json_to_msgpack(&val).unwrap();
        assert_eq!(json_from_msgpack(&bytes).unwrap(), val);

        bytes.push(0xC0);
        match json_from_msgpack(&bytes) {
            Err(MsgpackError::TrailingBytes { consumed, total }) => {
                assert_eq!(total, consumed + 1);
            }
            other => panic!("expected TrailingBytes, got {other:?}"),
        }
        assert!(value_from_msgpack(&bytes).is_err());
        assert!(msgpack_to_json_string(&bytes).is_err());
    }

    #[test]
    fn two_concatenated_values_are_rejected() {
        let first = json_to_msgpack(&json!({"a": 1})).unwrap();
        let second = json_to_msgpack(&json!({"b": 2})).unwrap();
        let mut joined = first.clone();
        joined.extend_from_slice(&second);

        assert!(json_from_msgpack(&first).is_ok());
        assert!(json_from_msgpack(&joined).is_err());
        assert!(value_from_msgpack(&joined).is_err());
        assert!(msgpack_to_json_string(&joined).is_err());
    }

    #[test]
    fn empty_input_is_unchanged() {
        // Readers fail on empty input; the transcoder yields "".
        assert!(json_from_msgpack(&[]).is_err());
        assert!(value_from_msgpack(&[]).is_err());
        assert_eq!(msgpack_to_json_string(&[]).unwrap(), "");
    }
}
