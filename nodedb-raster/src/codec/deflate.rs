// SPDX-License-Identifier: Apache-2.0

//! Deflate (codes 8 and 32946): a zlib stream.

use flate2::{Decompress, FlushDecompress, Status};

use crate::codec::error::CodecError;

/// Inflate the zlib stream `data` into exactly `expected` bytes.
pub fn inflate(data: &[u8], expected: usize) -> Result<Vec<u8>, CodecError> {
    let mut out = vec![0u8; expected];
    let mut stream = Decompress::new(true);
    let mut filled = 0usize;
    loop {
        let consumed = usize::try_from(stream.total_in()).unwrap_or(data.len());
        let input = data.get(consumed..).unwrap_or_default();
        let output = out.get_mut(filled..).unwrap_or_default();
        let (in_before, out_before) = (stream.total_in(), stream.total_out());
        let status = stream
            .decompress(input, output, FlushDecompress::Finish)
            .map_err(CodecError::Deflate)?;
        filled = usize::try_from(stream.total_out()).unwrap_or(expected);
        let stalled = stream.total_in() == in_before && stream.total_out() == out_before;
        match status {
            Status::StreamEnd => break,
            Status::Ok | Status::BufError if filled >= expected || stalled => break,
            Status::Ok | Status::BufError => {}
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

/// Bytes the bounded inflate grows its output by per step.
const GROW: usize = 64 * 1024;

/// Inflate the zlib stream `data`, of unknown decoded size, into at most
/// `max` bytes.
pub fn inflate_bounded(data: &[u8], max: usize) -> Result<Vec<u8>, CodecError> {
    let mut out: Vec<u8> = Vec::new();
    let mut stream = Decompress::new(true);
    loop {
        // One byte past `max` stays reservable, so a stream that ends at
        // exactly `max` can still report its end.
        if out.len() == out.capacity() {
            if out.len() > max {
                return Err(CodecError::Long { expected: max });
            }
            out.reserve_exact(GROW.min(max + 1 - out.len()));
        }
        let consumed = usize::try_from(stream.total_in()).unwrap_or(data.len());
        let input = data.get(consumed..).unwrap_or_default();
        let (in_before, out_before) = (stream.total_in(), stream.total_out());
        let status = stream
            .decompress_vec(input, &mut out, FlushDecompress::None)
            .map_err(CodecError::Deflate)?;
        if status == Status::StreamEnd {
            if out.len() > max {
                return Err(CodecError::Long { expected: max });
            }
            return Ok(out);
        }
        let stalled = stream.total_in() == in_before && stream.total_out() == out_before;
        if stalled && out.len() < out.capacity() {
            return Err(CodecError::Truncated { found: out.len() });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::write::ZlibEncoder;

    use super::*;

    fn zlib(raw: &[u8]) -> Vec<u8> {
        let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(raw).expect("writes");
        encoder.finish().expect("finishes")
    }

    #[test]
    fn a_zlib_stream_inflates_to_the_chunk() {
        let raw: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        assert_eq!(inflate(&zlib(&raw), raw.len()).ok(), Some(raw.clone()));
        assert_eq!(inflate(&zlib(&raw), 10).ok(), Some(raw[..10].to_vec()));
    }

    #[test]
    fn bounded_inflate_stops_at_the_cap() {
        let raw: Vec<u8> = (0..200_000u32).map(|i| (i % 13) as u8).collect();
        let stream = zlib(&raw);
        assert_eq!(inflate_bounded(&stream, raw.len()).ok(), Some(raw.clone()));
        assert!(matches!(
            inflate_bounded(&stream, 1000),
            Err(CodecError::Long { expected: 1000 })
        ));
        assert!(matches!(
            inflate_bounded(&stream[..stream.len() / 2], raw.len()),
            Err(CodecError::Truncated { .. } | CodecError::Deflate(_))
        ));
    }

    #[test]
    fn short_and_corrupt_streams_are_typed_errors() {
        let raw = vec![9u8; 100];
        assert!(matches!(
            inflate(&zlib(&raw), 200),
            Err(CodecError::Short {
                expected: 200,
                found: 100
            })
        ));
        let stream = zlib(&raw);
        assert!(inflate(&stream[..stream.len() / 2], 100).is_err());
        assert!(matches!(
            inflate(b"not zlib at all", 10),
            Err(CodecError::Deflate(_))
        ));
    }
}
