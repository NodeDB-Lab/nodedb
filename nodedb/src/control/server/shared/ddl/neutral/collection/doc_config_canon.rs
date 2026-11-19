// SPDX-License-Identifier: BUSL-1.1

//! Canonical MessagePack: the same value always yields the same bytes.
//!
//! A map's entries are written in byte order of their encoded key, so a
//! `HashMap` encoded in any iteration order reduces to one byte string.
//! Every other element is copied unchanged.

use std::ops::Range;

/// The MessagePack bytes did not parse as one complete value.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CanonError {
    #[error("msgpack value is truncated at byte {offset}")]
    Truncated { offset: usize },
    #[error("msgpack value has the reserved marker 0xc1 at byte {offset}")]
    ReservedMarker { offset: usize },
    #[error("msgpack value has {extra} trailing bytes")]
    TrailingBytes { extra: usize },
}

/// Re-encode `input`, which holds exactly one value, with every map sorted.
pub fn canonicalize(input: &[u8]) -> Result<Vec<u8>, CanonError> {
    let mut out = Vec::with_capacity(input.len());
    let end = element(input, 0, &mut out)?;
    if end != input.len() {
        return Err(CanonError::TrailingBytes {
            extra: input.len() - end,
        });
    }
    Ok(out)
}

fn take(buf: &[u8], offset: usize, len: usize) -> Result<&[u8], CanonError> {
    offset
        .checked_add(len)
        .and_then(|end| buf.get(offset..end))
        .ok_or(CanonError::Truncated { offset })
}

fn be_len(buf: &[u8], offset: usize, width: usize) -> Result<usize, CanonError> {
    let mut value: usize = 0;
    for byte in take(buf, offset, width)? {
        value = (value << 8) | usize::from(*byte);
    }
    Ok(value)
}

/// Copy the element at `offset` into `out`. Return the offset after it.
fn element(buf: &[u8], offset: usize, out: &mut Vec<u8>) -> Result<usize, CanonError> {
    let marker = *buf.get(offset).ok_or(CanonError::Truncated { offset })?;
    let body = offset + 1;
    // Bytes after the marker that belong to the element and need no rewrite.
    let (header, payload) = match marker {
        0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => (0, 0),
        0x80..=0x8f => return map(buf, offset, usize::from(marker & 0x0f), 0, out),
        0x90..=0x9f => return array(buf, offset, usize::from(marker & 0x0f), 0, out),
        0xa0..=0xbf => (0, usize::from(marker & 0x1f)),
        0xc1 => return Err(CanonError::ReservedMarker { offset }),
        0xc4 | 0xd9 => (1, be_len(buf, body, 1)?),
        0xc5 | 0xda => (2, be_len(buf, body, 2)?),
        0xc6 | 0xdb => (4, be_len(buf, body, 4)?),
        0xc7 => (1, be_len(buf, body, 1)? + 1),
        0xc8 => (2, be_len(buf, body, 2)? + 1),
        0xc9 => (4, be_len(buf, body, 4)? + 1),
        0xca | 0xce | 0xd2 => (0, 4),
        0xcb | 0xcf | 0xd3 => (0, 8),
        0xcc | 0xd0 => (0, 1),
        0xcd | 0xd1 => (0, 2),
        0xd4 => (0, 2),
        0xd5 => (0, 3),
        0xd6 => (0, 5),
        0xd7 => (0, 9),
        0xd8 => (0, 17),
        0xdc => return array(buf, offset, be_len(buf, body, 2)?, 2, out),
        0xdd => return array(buf, offset, be_len(buf, body, 4)?, 4, out),
        0xde => return map(buf, offset, be_len(buf, body, 2)?, 2, out),
        0xdf => return map(buf, offset, be_len(buf, body, 4)?, 4, out),
    };
    let end = body + header + payload;
    out.extend_from_slice(take(buf, offset, end - offset)?);
    Ok(end)
}

fn array(
    buf: &[u8],
    offset: usize,
    count: usize,
    width: usize,
    out: &mut Vec<u8>,
) -> Result<usize, CanonError> {
    let mut cursor = offset + 1 + width;
    out.extend_from_slice(take(buf, offset, 1 + width)?);
    for _ in 0..count {
        cursor = element(buf, cursor, out)?;
    }
    Ok(cursor)
}

fn map(
    buf: &[u8],
    offset: usize,
    count: usize,
    width: usize,
    out: &mut Vec<u8>,
) -> Result<usize, CanonError> {
    let mut cursor = offset + 1 + width;
    out.extend_from_slice(take(buf, offset, 1 + width)?);
    // One scratch buffer holds every entry; spans index its key and value.
    let mut scratch = Vec::new();
    let mut spans: Vec<(Range<usize>, Range<usize>)> = Vec::with_capacity(count.min(buf.len()));
    for _ in 0..count {
        let key_start = scratch.len();
        cursor = element(buf, cursor, &mut scratch)?;
        let value_start = scratch.len();
        cursor = element(buf, cursor, &mut scratch)?;
        spans.push((key_start..value_start, value_start..scratch.len()));
    }
    spans.sort_unstable_by(|(ka, va), (kb, vb)| {
        (&scratch[ka.clone()], &scratch[va.clone()])
            .cmp(&(&scratch[kb.clone()], &scratch[vb.clone()]))
    });
    for (key, value) in spans {
        out.extend_from_slice(&scratch[key]);
        out.extend_from_slice(&scratch[value]);
    }
    Ok(cursor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_entries_sort_by_encoded_key() {
        // {"b":1,"a":2} and {"a":2,"b":1}
        let first = [0x82, 0xa1, b'b', 0x01, 0xa1, b'a', 0x02];
        let second = [0x82, 0xa1, b'a', 0x02, 0xa1, b'b', 0x01];
        assert_eq!(canonicalize(&first), canonicalize(&second));
        assert_eq!(canonicalize(&first).unwrap(), second.to_vec());
    }

    #[test]
    fn nested_maps_sort_inside_arrays() {
        let first = [0x91, 0x82, 0x01, 0x0a, 0x00, 0x0b];
        let second = [0x91, 0x82, 0x00, 0x0b, 0x01, 0x0a];
        assert_eq!(canonicalize(&first), canonicalize(&second));
    }

    #[test]
    fn truncated_input_is_an_error() {
        assert!(matches!(
            canonicalize(&[0x82, 0xa1, b'a']),
            Err(CanonError::Truncated { .. })
        ));
    }

    #[test]
    fn trailing_bytes_are_an_error() {
        assert_eq!(
            canonicalize(&[0x01, 0x02]),
            Err(CanonError::TrailingBytes { extra: 1 })
        );
    }
}
