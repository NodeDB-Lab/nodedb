// SPDX-License-Identifier: Apache-2.0

// Checked readers over a tile payload.
//
// A decoded length is untrusted and reaches `usize::MAX` on a 32-bit target,
// where `pos + len` wraps. These readers use checked arithmetic and bounds-
// checked slices, so a hostile length is `SegmentCorruption` on every target.

use crate::error::{ArrayError, ArrayResult};

/// Take `len` bytes at `*pos` and advance past them. `what` names the field in
/// the error. On an error `*pos` stays where it was.
pub fn take_bytes<'a>(
    data: &'a [u8],
    pos: &mut usize,
    len: usize,
    what: &str,
) -> ArrayResult<&'a [u8]> {
    let bytes = pos
        .checked_add(len)
        .and_then(|end| data.get(*pos..end))
        .ok_or_else(|| ArrayError::SegmentCorruption {
            detail: format!("{what} (need {len} bytes at {}, have {})", *pos, data.len()),
        })?;
    *pos += len;
    Ok(bytes)
}

/// Take a little-endian `u32` at `*pos` and advance past it.
pub fn take_u32_le(data: &[u8], pos: &mut usize, what: &str) -> ArrayResult<u32> {
    let word = data
        .get(*pos..)
        .and_then(|rest| rest.first_chunk::<4>())
        .ok_or_else(|| ArrayError::SegmentCorruption {
            detail: what.to_string(),
        })?;
    *pos += 4;
    Ok(u32::from_le_bytes(*word))
}

/// Take a little-endian `u32` length at `*pos` as a `usize`.
pub fn take_len(data: &[u8], pos: &mut usize, what: &str) -> ArrayResult<usize> {
    Ok(take_u32_le(data, pos, what)? as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_length_that_wraps_the_position_is_corruption_not_a_panic() {
        let data = [0u8; 8];
        let mut pos = usize::MAX - 1;
        let error = take_bytes(&data, &mut pos, 4, "test field").unwrap_err();
        assert!(matches!(error, ArrayError::SegmentCorruption { .. }));
        assert_eq!(pos, usize::MAX - 1);
    }

    #[test]
    fn a_maximal_length_at_the_start_is_corruption() {
        let data = [0u8; 8];
        let mut pos = 0;
        assert!(take_bytes(&data, &mut pos, usize::MAX, "test field").is_err());
        assert_eq!(pos, 0);
    }

    #[test]
    fn a_position_past_the_end_is_corruption() {
        let data = [0u8; 8];
        let mut pos = 100;
        assert!(take_u32_le(&data, &mut pos, "test field").is_err());
        assert_eq!(pos, 100);
    }

    #[test]
    fn a_short_word_is_corruption() {
        let mut pos = 0;
        assert!(take_u32_le(&[1, 2, 3], &mut pos, "test field").is_err());
        assert_eq!(pos, 0);
    }

    #[test]
    fn the_readers_advance_past_what_they_take() {
        let data = [1, 0, 0, 0, 9, 9];
        let mut pos = 0;
        assert_eq!(take_len(&data, &mut pos, "test field").unwrap(), 1);
        assert_eq!(pos, 4);
        assert_eq!(
            take_bytes(&data, &mut pos, 2, "test field").unwrap(),
            [9, 9]
        );
        assert_eq!(pos, 6);
    }
}
