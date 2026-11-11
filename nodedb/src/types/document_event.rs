// SPDX-License-Identifier: BUSL-1.1

//! Public document event images prepared before source commit.
//!
//! Both images encode as MessagePack `bin`, the old image as `nil` when the
//! row had none, so each costs its length plus a 5-byte header at most in a
//! write-set part.

use zerompk::{Read, Write};

/// The MessagePack `nil` marker.
const NIL: u8 = 0xc0;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DocumentPutEvent {
    pub new_value: Vec<u8>,
    pub old_value: Option<Vec<u8>>,
}

impl zerompk::ToMessagePack for DocumentPutEvent {
    fn write<W: Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(2)?;
        writer.write_binary(&self.new_value)?;
        match &self.old_value {
            Some(old) => writer.write_binary(old),
            None => writer.write_nil(),
        }
    }
}

impl<'a> zerompk::FromMessagePack<'a> for DocumentPutEvent {
    fn read<R: Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        reader.check_array_len(2)?;
        let new_value = reader.read_binary()?.into_owned();
        let old_value = if reader.peek_marker()? == NIL {
            reader.read_nil()?;
            None
        } else {
            Some(reader.read_binary()?.into_owned())
        };
        Ok(Self {
            new_value,
            old_value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_images_round_trip_as_bin() {
        for old_value in [None, Some(vec![0xff; 300])] {
            let event = DocumentPutEvent {
                new_value: vec![0x80; 300],
                old_value,
            };
            let bytes = zerompk::to_msgpack_vec(&event).expect("encode");
            // An array header, two `bin 16` headers or one and a `nil`, and
            // one byte per image byte.
            let images = 300 + event.old_value.as_ref().map_or(0, Vec::len);
            assert!(bytes.len() <= 1 + 2 * 3 + images, "{} bytes", bytes.len());
            let decoded: DocumentPutEvent = zerompk::from_msgpack(&bytes).expect("decode");
            assert_eq!(decoded, event);
        }
    }
}
