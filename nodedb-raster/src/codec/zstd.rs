// SPDX-License-Identifier: Apache-2.0

//! ZSTD (code 50000), through the pure-Rust `ruzstd` decoder.

use ruzstd::decoding::errors::FrameDecoderError;
use ruzstd::decoding::{DEFAULT_MAX_WINDOW_SIZE, FrameDecoder};

use crate::codec::error::CodecError;
use crate::limits::checked_raster_capacity;

/// Smallest window the decoder accepts, whatever the chunk size.
const MIN_WINDOW: u64 = 1 << 20;

/// A frame decoder whose window is bounded by `content` decoded bytes. A
/// frame needs no window larger than its content. Twice the content,
/// rounded up, bounds the decode buffer for any sane encoder.
fn decoder_for(content: usize) -> FrameDecoder {
    let mut decoder = FrameDecoder::new();
    let window = (content as u64)
        .max(1)
        .checked_next_power_of_two()
        .map_or(DEFAULT_MAX_WINDOW_SIZE, |w| w.saturating_mul(2))
        .clamp(MIN_WINDOW, DEFAULT_MAX_WINDOW_SIZE);
    decoder.set_max_window_size(window);
    decoder
}

/// Decode the ZSTD frames `data`, of unknown decoded size, into at most
/// `max` bytes.
pub fn decode_bounded(data: &[u8], max: usize) -> Result<Vec<u8>, CodecError> {
    let max = checked_raster_capacity(max)?;
    let mut decoder = decoder_for(max);
    let mut out = Vec::with_capacity(max);
    decoder
        .decode_all_to_vec(data, &mut out)
        .map_err(|error| match error {
            FrameDecoderError::TargetTooSmall => CodecError::Long { expected: max },
            other => CodecError::Zstd(other),
        })?;
    Ok(out)
}

/// Decode the ZSTD frames `data` into exactly `expected` bytes.
pub fn decode(data: &[u8], expected: usize) -> Result<Vec<u8>, CodecError> {
    let expected = checked_raster_capacity(expected)?;
    let mut decoder = decoder_for(expected);
    let mut out = vec![0u8; expected];
    let written = decoder
        .decode_all(data, &mut out)
        .map_err(|error| match error {
            FrameDecoderError::TargetTooSmall => CodecError::Long { expected },
            other => CodecError::Zstd(other),
        })?;
    if written < expected {
        return Err(CodecError::Short {
            expected,
            found: written,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One raw ZSTD block frame of `raw`: magic, a single-segment frame
    /// header with a one-byte content size, and a last raw block.
    fn raw_frame(raw: &[u8]) -> Vec<u8> {
        let len = u8::try_from(raw.len()).expect("short");
        let mut frame = vec![0x28, 0xB5, 0x2F, 0xFD, 0x20, len];
        let block_header = (u32::from(len) << 3) | 1;
        frame.extend_from_slice(&block_header.to_le_bytes()[..3]);
        frame.extend_from_slice(raw);
        frame
    }

    #[test]
    fn a_frame_decodes_to_the_chunk() {
        let raw = [5u8, 6, 7, 8];
        assert_eq!(decode(&raw_frame(&raw), 4).ok(), Some(raw.to_vec()));
    }

    #[test]
    fn short_long_and_corrupt_frames_are_typed_errors() {
        let frame = raw_frame(&[1, 2, 3, 4]);
        assert!(matches!(
            decode(&frame, 8),
            Err(CodecError::Short {
                expected: 8,
                found: 4
            })
        ));
        assert!(matches!(
            decode(&frame, 2),
            Err(CodecError::Long { expected: 2 })
        ));
        assert!(matches!(decode(b"garbage!", 4), Err(CodecError::Zstd(_))));
    }

    #[test]
    fn bounded_decode_returns_the_content() {
        let frame = raw_frame(&[1, 2, 3, 4]);
        assert_eq!(decode_bounded(&frame, 16).ok(), Some(vec![1, 2, 3, 4]));
        assert!(matches!(
            decode_bounded(&frame, 2),
            Err(CodecError::Long { expected: 2 })
        ));
    }
}
