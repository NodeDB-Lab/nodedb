// SPDX-License-Identifier: Apache-2.0

//! A bounded pre-walk of the Lerc2 blobs in a chunk, before the LERC
//! decoder sees them.
//!
//! Each blob must open with the `Lerc2 ` magic, declare a size that covers
//! its header and fits the chunk, and (version 3 on) match its Fletcher-32
//! checksum. The blobs must fill the chunk exactly. The decoder walks the
//! same blob chain without these checks.

use crate::codec::error::CodecError;

const MAGIC: &[u8; 6] = b"Lerc2 ";
/// Lerc2 versions the decoder reads.
const VERSIONS: std::ops::RangeInclusive<i32> = 2..=6;
/// Bytes before the checksum field, which the checksum excludes.
const CHECKSUM_START: usize = 14;
/// Most blobs one chunk may hold: one per band.
const MAX_BLOBS: usize = u16::MAX as usize;

/// The number of blobs in `chunk`, after checking every one.
pub fn check_blobs(chunk: &[u8]) -> Result<usize, CodecError> {
    let mut offset = 0usize;
    let mut blobs = 0usize;
    while offset < chunk.len() {
        if blobs == MAX_BLOBS {
            return Err(blob_error(offset, "more blobs than bands"));
        }
        let blob = chunk.get(offset..).unwrap_or_default();
        let size = check_blob(blob).map_err(|reason| blob_error(offset, reason))?;
        offset += size;
        blobs += 1;
    }
    if blobs == 0 {
        return Err(blob_error(0, "the chunk is empty"));
    }
    Ok(blobs)
}

/// The size of the blob at the start of `blob`.
fn check_blob(blob: &[u8]) -> Result<usize, &'static str> {
    if blob.get(..MAGIC.len()) != Some(MAGIC.as_slice()) {
        return Err("no Lerc2 magic");
    }
    let version = read_i32(blob, 6).ok_or("truncated header")?;
    if !VERSIONS.contains(&version) {
        return Err("unsupported Lerc2 version");
    }
    // magic, version, [checksum], rows, cols, [depth], valid pixels,
    // micro block size, then the blob size.
    let checksum_len = if version >= 3 { 4 } else { 0 };
    let depth_len = if version >= 4 { 4 } else { 0 };
    let size_at = 10 + checksum_len + 4 + 4 + depth_len + 4 + 4;
    let size = read_i32(blob, size_at).ok_or("truncated header")?;
    let size = usize::try_from(size).map_err(|_| "negative blob size")?;
    if size < size_at + 4 {
        return Err("blob size smaller than its header");
    }
    let body = blob.get(..size).ok_or("blob size past the chunk")?;
    if version >= 3 {
        let stored = read_i32(blob, 10).ok_or("truncated header")? as u32;
        let computed = lerc::checksum::fletcher32(body.get(CHECKSUM_START..).unwrap_or_default());
        if stored != computed {
            return Err("checksum mismatch");
        }
    }
    Ok(size)
}

fn read_i32(bytes: &[u8], at: usize) -> Option<i32> {
    let raw: [u8; 4] = bytes.get(at..at + 4)?.try_into().ok()?;
    Some(i32::from_le_bytes(raw))
}

fn blob_error(offset: usize, reason: &'static str) -> CodecError {
    CodecError::LercBlob { offset, reason }
}

#[cfg(test)]
mod tests {
    use lerc::bitmask::BitMask;
    use lerc::{DataType, Image, Precision, SampleData};

    use super::*;

    fn blob() -> Vec<u8> {
        let image = Image {
            width: 3,
            height: 2,
            depth: 1,
            bands: 1,
            data_type: DataType::Byte,
            valid_masks: vec![BitMask::all_valid(6)],
            data: SampleData::U8(vec![1, 2, 3, 4, 5, 250]),
            no_data_value: None,
        };
        lerc::encode(&image, Precision::Lossless).expect("encodes")
    }

    #[test]
    fn whole_blobs_pass_and_count() {
        let one = blob();
        assert_eq!(check_blobs(&one).ok(), Some(1));
        let mut two = one.clone();
        two.extend_from_slice(&one);
        assert_eq!(check_blobs(&two).ok(), Some(2));
    }

    #[test]
    fn corrupt_sizes_and_bodies_are_refused() {
        let good = blob();
        let mut zero_size = good.clone();
        zero_size[34..38].copy_from_slice(&0i32.to_le_bytes());
        assert!(matches!(
            check_blobs(&zero_size),
            Err(CodecError::LercBlob { .. })
        ));
        let mut flipped = good.clone();
        let last = flipped.len() - 1;
        flipped[last] ^= 0x10;
        assert!(matches!(
            check_blobs(&flipped),
            Err(CodecError::LercBlob {
                reason: "checksum mismatch",
                ..
            })
        ));
        assert!(check_blobs(&good[..good.len() - 1]).is_err());
        let mut trailing = good.clone();
        trailing.push(0);
        assert!(check_blobs(&trailing).is_err());
        assert!(check_blobs(b"").is_err());
        assert!(check_blobs(b"CntZImage ").is_err());
    }
}
