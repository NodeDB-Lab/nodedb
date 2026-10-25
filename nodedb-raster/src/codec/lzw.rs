// SPDX-License-Identifier: Apache-2.0

//! LZW (code 5): MSB-first codes with the TIFF early size switch, or the
//! LSB-first codes of pre-5.0 libtiff.

use weezl::decode::Decoder;
use weezl::{BitOrder, LzwStatus};

use crate::codec::error::CodecError;
use crate::limits::checked_raster_capacity;

/// Decode the LZW stream `data`, of unknown decoded size, into at most
/// `max` bytes.
pub fn decode_bounded(data: &[u8], max: usize) -> Result<Vec<u8>, CodecError> {
    let max = checked_raster_capacity(max)?;
    let mut decoder = decoder_for(data);
    let mut out = vec![0u8; max];
    let mut filled = 0usize;
    let mut input = data;
    loop {
        let output = out.get_mut(filled..).unwrap_or_default();
        let result = decoder.decode_bytes(input, output);
        input = input.get(result.consumed_in..).unwrap_or_default();
        filled += result.consumed_out;
        match result.status {
            Ok(LzwStatus::Done) => break,
            Ok(LzwStatus::NoProgress) if filled >= max => {
                return Err(CodecError::Long { expected: max });
            }
            Ok(LzwStatus::NoProgress) => break,
            Ok(LzwStatus::Ok) if result.consumed_in == 0 && result.consumed_out == 0 => {
                if filled >= max {
                    return Err(CodecError::Long { expected: max });
                }
                break;
            }
            Ok(LzwStatus::Ok) => {}
            Err(error) => return Err(CodecError::Lzw(error)),
        }
    }
    out.truncate(filled);
    Ok(out)
}

/// The decoder for `data`: libtiff reads a stream that opens with 0x00
/// and an odd second byte as the old LSB-first code order.
fn decoder_for(data: &[u8]) -> Decoder {
    if matches!(data, [0, second, ..] if second & 1 == 1) {
        Decoder::new(BitOrder::Lsb, 8)
    } else {
        Decoder::with_tiff_size_switch(BitOrder::Msb, 8)
    }
}

/// Decode the LZW stream `data` into exactly `expected` bytes.
pub fn decode(data: &[u8], expected: usize) -> Result<Vec<u8>, CodecError> {
    let expected = checked_raster_capacity(expected)?;
    let mut decoder = decoder_for(data);
    let mut out = vec![0u8; expected];
    let mut filled = 0usize;
    let mut input = data;
    while filled < expected {
        let output = out.get_mut(filled..).unwrap_or_default();
        let result = decoder.decode_bytes(input, output);
        input = input.get(result.consumed_in..).unwrap_or_default();
        filled += result.consumed_out;
        match result.status {
            Ok(LzwStatus::Done | LzwStatus::NoProgress) => break,
            Ok(LzwStatus::Ok) if result.consumed_in == 0 && result.consumed_out == 0 => break,
            Ok(LzwStatus::Ok) => {}
            // Trailing garbage after a whole chunk is harmless.
            Err(_) if filled >= expected => break,
            Err(error) => return Err(CodecError::Lzw(error)),
        }
    }
    if filled < expected {
        return Err(CodecError::Short {
            expected,
            found: filled,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use weezl::encode::Encoder;

    use super::*;

    #[test]
    fn a_tiff_lzw_stream_decodes() {
        let raw: Vec<u8> = (0..4000u32).map(|i| (i * 7 % 256) as u8).collect();
        let encoded = Encoder::with_tiff_size_switch(BitOrder::Msb, 8)
            .encode(&raw)
            .expect("encodes");
        assert_eq!(decode(&encoded, raw.len()).ok(), Some(raw));
    }

    #[test]
    fn short_and_corrupt_streams_are_typed_errors() {
        let raw = vec![3u8; 64];
        let encoded = Encoder::with_tiff_size_switch(BitOrder::Msb, 8)
            .encode(&raw)
            .expect("encodes");
        assert!(matches!(
            decode(&encoded, 128),
            Err(CodecError::Short { expected: 128, .. })
        ));
        assert!(decode(&[0xFF; 16], 64).is_err());
        assert!(decode(&[], 4).is_err());
    }
}
