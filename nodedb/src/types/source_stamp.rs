// SPDX-License-Identifier: BUSL-1.1

//! The stamps of a source row version a clone copies.
//!
//! A clone copy of a source row is the same logical version as the source
//! row. It installs at the source version's system-time ordinal, never at a
//! fresh one. A materialize scan hands each row's stamps to the Control
//! Plane in the MessagePack shapes this module writes and reads.

use nodedb_query::msgpack_scan;

/// The stamps of one document version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DocVersionStamp {
    /// The version's system-time ordinal.
    pub sys_from: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
}

impl DocVersionStamp {
    /// Append `stamp` as `[sys_from, valid_from_ms, valid_until_ms]`, or nil
    /// for `None`.
    pub fn write(stamp: Option<&Self>, buf: &mut Vec<u8>) {
        let Some(stamp) = stamp else {
            msgpack_scan::write_null(buf);
            return;
        };
        msgpack_scan::write_array_header(buf, 3);
        msgpack_scan::write_i64(buf, stamp.sys_from);
        msgpack_scan::write_i64(buf, stamp.valid_from_ms);
        msgpack_scan::write_i64(buf, stamp.valid_until_ms);
    }

    /// Read the value [`Self::write`] wrote at `*off` and move past it.
    /// `Some(None)` for nil. `None` when the bytes hold neither shape.
    pub fn read(buf: &[u8], off: &mut usize) -> Option<Option<Self>> {
        if msgpack_scan::read_null(buf, *off) {
            *off = msgpack_scan::skip_value(buf, *off)?;
            return Some(None);
        }
        let (len, mut at) = msgpack_scan::array_header(buf, *off)?;
        if len != 3 {
            return None;
        }
        let mut next = || {
            let value = msgpack_scan::read_i64(buf, at)?;
            at = msgpack_scan::skip_value(buf, at)?;
            Some(value)
        };
        let stamp = Self {
            sys_from: next()?,
            valid_from_ms: next()?,
            valid_until_ms: next()?,
        };
        *off = at;
        Some(Some(stamp))
    }
}

/// The stamps of one KV version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KvVersionStamp {
    /// The ordinal of the write that set the value.
    pub sys_from: i64,
    /// Absolute expiry instant in ms, `0` for none.
    pub expire_at_ms: u64,
}

impl KvVersionStamp {
    /// Append the stamp as two integers: `sys_from`, then `expire_at_ms`.
    pub fn write(&self, buf: &mut Vec<u8>) {
        msgpack_scan::write_i64(buf, self.sys_from);
        msgpack_scan::write_i64(buf, i64::try_from(self.expire_at_ms).unwrap_or(i64::MAX));
    }

    /// Read the two integers [`Self::write`] wrote at `*off` and move past
    /// them. `None` when the bytes hold no such pair.
    pub fn read(buf: &[u8], off: &mut usize) -> Option<Self> {
        let sys_from = msgpack_scan::read_i64(buf, *off)?;
        let at = msgpack_scan::skip_value(buf, *off)?;
        let expire_at_ms = u64::try_from(msgpack_scan::read_i64(buf, at)?).ok()?;
        *off = msgpack_scan::skip_value(buf, at)?;
        Some(Self {
            sys_from,
            expire_at_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_document_stamp_and_nil_round_trip() {
        let stamp = DocVersionStamp {
            sys_from: 1_700_000_000_000_000_001,
            valid_from_ms: i64::MIN,
            valid_until_ms: i64::MAX,
        };
        let mut buf = Vec::new();
        DocVersionStamp::write(Some(&stamp), &mut buf);
        DocVersionStamp::write(None, &mut buf);
        let mut off = 0;
        assert_eq!(DocVersionStamp::read(&buf, &mut off), Some(Some(stamp)));
        assert_eq!(DocVersionStamp::read(&buf, &mut off), Some(None));
        assert_eq!(off, buf.len());
    }

    #[test]
    fn a_kv_stamp_round_trips() {
        let stamp = KvVersionStamp {
            sys_from: 42,
            expire_at_ms: 9_000,
        };
        let mut buf = Vec::new();
        stamp.write(&mut buf);
        let mut off = 0;
        assert_eq!(KvVersionStamp::read(&buf, &mut off), Some(stamp));
        assert_eq!(off, buf.len());
    }
}
