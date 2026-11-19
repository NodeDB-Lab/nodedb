// SPDX-License-Identifier: Apache-2.0

//! A byte string that encodes as one msgpack `bin` value.
//!
//! A plain `Vec<u8>` field under the zerompk derive encodes as an array of
//! integers, so each byte of 0x80 or above takes two bytes. `WireBytes`
//! writes the bytes as `bin`, one byte each plus a header.

use std::fmt;
use std::ops::Deref;

use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Owned bytes whose msgpack form is `bin`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WireBytes(Vec<u8>);

impl WireBytes {
    /// The bytes as a slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// The owned bytes.
    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

impl Deref for WireBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for WireBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl zerompk::ToMessagePack for WireBytes {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_binary(&self.0)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for WireBytes {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        Ok(Self(reader.read_binary()?.into_owned()))
    }
}

impl Serialize for WireBytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

struct WireBytesVisitor;

impl<'de> Visitor<'de> for WireBytesVisitor {
    type Value = WireBytes;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a byte string")
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<WireBytes, E> {
        Ok(WireBytes(v.to_vec()))
    }

    fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<WireBytes, E> {
        Ok(WireBytes(v))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<WireBytes, A::Error> {
        let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(4096));
        while let Some(byte) = seq.next_element::<u8>()? {
            bytes.push(byte);
        }
        Ok(WireBytes(bytes))
    }
}

impl<'de> Deserialize<'de> for WireBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_byte_buf(WireBytesVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_byte_values() -> WireBytes {
        WireBytes::from((0..=255u8).collect::<Vec<u8>>())
    }

    #[test]
    fn round_trips_through_msgpack() {
        for bytes in [
            WireBytes::default(),
            WireBytes::from(vec![0x00, 0x7f, 0x80, 0xff]),
            all_byte_values(),
        ] {
            let encoded = zerompk::to_msgpack_vec(&bytes).expect("encode");
            let decoded: WireBytes = zerompk::from_msgpack(&encoded).expect("decode");
            assert_eq!(decoded, bytes);
        }
    }

    #[test]
    fn every_byte_value_encodes_as_bin16_plus_the_bytes() {
        let bytes = all_byte_values();
        let encoded = zerompk::to_msgpack_vec(&bytes).expect("encode");
        assert_eq!(encoded.len(), 3 + 256);
        assert_eq!(encoded[..3], [0xc5, 0x01, 0x00]);
        assert_eq!(&encoded[3..], bytes.as_slice());
    }

    #[test]
    fn accessors_expose_the_bytes() {
        let bytes = WireBytes::from(vec![1, 2, 3]);
        assert_eq!(&*bytes, &[1, 2, 3][..]);
        assert_eq!(bytes.as_slice(), &[1, 2, 3]);
        assert_eq!(bytes.into_vec(), vec![1, 2, 3]);
    }
}
