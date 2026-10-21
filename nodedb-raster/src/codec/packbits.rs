// SPDX-License-Identifier: Apache-2.0

//! PackBits (code 32773): byte runs and literals.
//!
//! A header byte `n` in `0..=127` copies the next `n + 1` bytes. `n` in
//! `-127..=-1` repeats the next byte `1 - n` times. `-128` is a no-op.

use crate::codec::error::CodecError;

/// Unpack `data` into exactly `expected` bytes. A run past `expected` is
/// cut at `expected`.
pub fn unpack(data: &[u8], expected: usize) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::with_capacity(expected);
    let mut at = 0usize;
    while out.len() < expected {
        let Some(&header) = data.get(at) else {
            break;
        };
        at += 1;
        let room = expected - out.len();
        let n = i8::from_ne_bytes([header]);
        if n >= 0 {
            let count = usize::from(n.unsigned_abs()) + 1;
            let literal = data
                .get(at..at + count)
                .ok_or(CodecError::PackBitsOverrun { offset: at - 1 })?;
            out.extend_from_slice(literal.get(..count.min(room)).unwrap_or_default());
            at += count;
        } else if n != i8::MIN {
            let count = usize::from(n.unsigned_abs()) + 1;
            let &byte = data
                .get(at)
                .ok_or(CodecError::PackBitsOverrun { offset: at - 1 })?;
            at += 1;
            out.resize(out.len() + count.min(room), byte);
        }
    }
    if out.len() < expected {
        return Err(CodecError::Short {
            expected,
            found: out.len(),
        });
    }
    Ok(out)
}

/// Unpack `data`, of unknown decoded size, into at most `max` bytes.
pub fn unpack_bounded(data: &[u8], max: usize) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::new();
    let mut at = 0usize;
    while let Some(&header) = data.get(at) {
        at += 1;
        let n = i8::from_ne_bytes([header]);
        let count = usize::from(n.unsigned_abs()) + 1;
        if n >= 0 {
            let literal = data
                .get(at..at + count)
                .ok_or(CodecError::PackBitsOverrun { offset: at - 1 })?;
            if out.len() + count > max {
                return Err(CodecError::Long { expected: max });
            }
            out.extend_from_slice(literal);
            at += count;
        } else if n != i8::MIN {
            let &byte = data
                .get(at)
                .ok_or(CodecError::PackBitsOverrun { offset: at - 1 })?;
            at += 1;
            if out.len() + count > max {
                return Err(CodecError::Long { expected: max });
            }
            out.resize(out.len() + count, byte);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The worked example of the TIFF 6.0 specification, section 9.
    #[test]
    fn the_specification_example_unpacks() {
        let packed = [
            0xFE, 0xAA, 0x02, 0x80, 0x00, 0x2A, 0xFD, 0xAA, 0x03, 0x80, 0x00, 0x2A, 0x22, 0xF7,
            0xAA,
        ];
        let want = [
            0xAA, 0xAA, 0xAA, 0x80, 0x00, 0x2A, 0xAA, 0xAA, 0xAA, 0xAA, 0x80, 0x00, 0x2A, 0x22,
            0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
        ];
        assert_eq!(unpack(&packed, want.len()).ok(), Some(want.to_vec()));
    }

    #[test]
    fn bounded_unpack_refuses_more_than_the_cap() {
        assert_eq!(
            unpack_bounded(&[0xFD, 7, 0x00, 1], 8).ok(),
            Some(vec![7, 7, 7, 7, 1])
        );
        assert!(matches!(
            unpack_bounded(&[0x81, 7], 8),
            Err(CodecError::Long { expected: 8 })
        ));
    }

    #[test]
    fn noops_are_skipped_and_runs_are_cut_at_the_chunk() {
        assert_eq!(unpack(&[0x80, 0xFD, 7], 2).ok(), Some(vec![7, 7]));
    }

    #[test]
    fn overruns_and_short_streams_are_typed_errors() {
        assert!(matches!(
            unpack(&[0x05, 1, 2], 6),
            Err(CodecError::PackBitsOverrun { offset: 0 })
        ));
        assert!(matches!(
            unpack(&[0xFF], 2),
            Err(CodecError::PackBitsOverrun { offset: 0 })
        ));
        assert!(matches!(
            unpack(&[0x00, 9], 3),
            Err(CodecError::Short {
                expected: 3,
                found: 1
            })
        ));
    }
}
