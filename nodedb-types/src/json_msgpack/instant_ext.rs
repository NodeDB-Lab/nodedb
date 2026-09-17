// SPDX-License-Identifier: Apache-2.0

//! Msgpack ext encoding for typed instants.
//!
//! A typed instant is written to plain msgpack as `fixext8`: marker `0xD7`,
//! one ext type byte, then epoch microseconds as big-endian `i64`. Ten bytes
//! in total. Ext type `1` is a UTC instant (`Value::DateTime`), ext type `2`
//! is a naive instant (`Value::NaiveDateTime`).
//!
//! The payload is byte-comparable within one kind for non-negative micros
//! only. Comparators decode and compare `(kind, i64)` signed.

use serde::{Deserialize, Serialize};

use crate::datetime::{NdbDateTime, NdbDateTimeError};
use crate::value::Value;

/// Ext type byte for a UTC instant.
pub const EXT_INSTANT_UTC: i8 = 1;
/// Ext type byte for a naive (timezone-less) instant.
pub const EXT_INSTANT_NAIVE: i8 = 2;
/// Encoded length of an instant: marker + ext type + `i64` payload.
pub const INSTANT_EXT_LEN: usize = 10;

/// Msgpack `fixext8` marker.
const FIXEXT8: u8 = 0xD7;

/// Which instant variant an ext payload carries.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum InstantKind {
    /// Timezone-aware instant, `Value::DateTime`.
    Utc,
    /// Timezone-less instant, `Value::NaiveDateTime`.
    Naive,
}

impl InstantKind {
    /// Ext type byte for this kind.
    pub fn ext_type(self) -> i8 {
        match self {
            Self::Utc => EXT_INSTANT_UTC,
            Self::Naive => EXT_INSTANT_NAIVE,
        }
    }

    /// Kind for an ext type byte. `None` when the byte is not an instant type.
    pub fn from_ext_type(t: i8) -> Option<Self> {
        match t {
            EXT_INSTANT_UTC => Some(Self::Utc),
            EXT_INSTANT_NAIVE => Some(Self::Naive),
            _ => None,
        }
    }

    /// Wrap a timestamp in the `Value` variant for this kind.
    pub fn value(self, dt: NdbDateTime) -> Value {
        match self {
            Self::Utc => Value::DateTime(dt),
            Self::Naive => Value::NaiveDateTime(dt),
        }
    }

    /// Build the `Value` for this kind from epoch milliseconds.
    pub fn from_millis(self, ms: i64) -> Result<Value, NdbDateTimeError> {
        Ok(self.value(NdbDateTime::from_millis(ms)?))
    }

    /// Build the `Value` for this kind from epoch microseconds.
    pub fn from_micros(self, micros: i64) -> Value {
        self.value(NdbDateTime::from_micros(micros))
    }
}

/// Append the ten-byte `fixext8` encoding of an instant to `buf`.
pub fn write_instant(buf: &mut Vec<u8>, kind: InstantKind, micros: i64) {
    buf.push(FIXEXT8);
    buf.push(kind.ext_type() as u8);
    buf.extend_from_slice(&micros.to_be_bytes());
}

/// Decode an instant from its ext type byte and eight-byte payload.
///
/// `None` when the type byte is not an instant type or the payload is not
/// exactly eight bytes.
pub fn instant_from_ext(ext_type: i8, payload: &[u8]) -> Option<(InstantKind, i64)> {
    let kind = InstantKind::from_ext_type(ext_type)?;
    let bytes: [u8; 8] = payload.try_into().ok()?;
    Some((kind, i64::from_be_bytes(bytes)))
}

/// Decode the instant at `offset` in `bytes`.
///
/// `None` when the bytes at `offset` are not a `fixext8` of an instant type
/// or the buffer is truncated.
pub fn read_instant(bytes: &[u8], offset: usize) -> Option<(InstantKind, i64)> {
    let ext = bytes.get(offset..offset + INSTANT_EXT_LEN)?;
    if ext[0] != FIXEXT8 {
        return None;
    }
    instant_from_ext(ext[1] as i8, &ext[2..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_both_kinds() {
        for kind in [InstantKind::Utc, InstantKind::Naive] {
            let mut buf = Vec::new();
            write_instant(&mut buf, kind, 1_700_000_000_123_456);
            assert_eq!(buf.len(), INSTANT_EXT_LEN);
            assert_eq!(buf[0], 0xD7);
            assert_eq!(buf[1] as i8, kind.ext_type());
            assert_eq!(read_instant(&buf, 0), Some((kind, 1_700_000_000_123_456)));
        }
    }

    #[test]
    fn negative_micros_round_trip() {
        let mut buf = vec![0xC0];
        write_instant(&mut buf, InstantKind::Naive, -86_400_000_000);
        assert_eq!(
            read_instant(&buf, 1),
            Some((InstantKind::Naive, -86_400_000_000))
        );
    }

    #[test]
    fn non_ext_byte_is_none() {
        let buf = [0x2A, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(read_instant(&buf, 0), None);
    }

    #[test]
    fn unknown_ext_type_is_none() {
        let buf = [0xD7, 0x07, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(read_instant(&buf, 0), None);
    }

    #[test]
    fn truncated_is_none() {
        let mut buf = Vec::new();
        write_instant(&mut buf, InstantKind::Utc, 42);
        buf.truncate(9);
        assert_eq!(read_instant(&buf, 0), None);
        assert_eq!(instant_from_ext(EXT_INSTANT_UTC, &buf[2..]), None);
    }

    #[test]
    fn kind_value_mapping() {
        let dt = NdbDateTime::from_micros(5);
        assert_eq!(InstantKind::Utc.value(dt), Value::DateTime(dt));
        assert_eq!(InstantKind::Naive.value(dt), Value::NaiveDateTime(dt));
        assert_eq!(
            Value::DateTime(dt).as_instant(),
            Some((InstantKind::Utc, dt))
        );
        assert_eq!(
            Value::NaiveDateTime(dt).as_instant(),
            Some((InstantKind::Naive, dt))
        );
        assert_eq!(Value::Integer(5).as_instant(), None);
        assert_eq!(
            InstantKind::Utc.from_millis(3).unwrap(),
            Value::DateTime(NdbDateTime::from_micros(3_000))
        );
        assert!(InstantKind::Utc.from_millis(i64::MAX).is_err());
    }
}
