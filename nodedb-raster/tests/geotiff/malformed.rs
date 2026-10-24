// SPDX-License-Identifier: Apache-2.0

//! Hostile files over the generated fixtures: truncation, corruption,
//! absurd counts and offsets, IFD loops, a missing object, and the header
//! cap. Every failure is a typed error, never a panic and never an
//! allocation sized from the file.

use nodedb_raster::tiff::error::TiffFormatError;
use nodedb_raster::{
    ByteRange, RangeRead, RasterError, RasterLimits, TiffOpener, open_all, open_with,
};

use crate::files::fixture_bytes;
use crate::fixtures::LIMITS;
use crate::image::decode_everything;

/// One fixture per codec and container feature.
const CODEC_FILES: [&str; 12] = [
    "m-none-p1-strip-classic-contig-be.tif",
    "m-lzw-p2-tile-big-separate-le.tif",
    "m-deflate-p3-tile-classic-contig-be.tif",
    "m-packbits-p1-tile-classic-separate-le.tif",
    "m-zstd-p2-strip-big-contig-be.tif",
    "l-lerc-pixel-tile.tif",
    "l-lerc_deflate-band-strip.tif",
    "l-lerc_zstd-mask.tif",
    "j-ycbcr-tile.tif",
    "j-gray-strip.tif",
    "cog-mask.tif",
    "gdal-metadata.tif",
];

/// Decode cap for hostile input: small, so no header can make one block
/// a large allocation.
const HOSTILE: RasterLimits = RasterLimits::new(1 << 20);

#[test]
fn every_truncation_of_every_codec_is_a_typed_error_or_a_whole_image() {
    for file in CODEC_FILES {
        let bytes = fixture_bytes(file);
        for len in 0..bytes.len() {
            let result = decode_everything(&bytes[..len], HOSTILE);
            if len < 64 {
                assert!(result.is_err(), "{file}: a {len}-byte prefix decoded");
            }
        }
    }
}

#[test]
fn corrupt_bytes_of_every_codec_never_panic() {
    for file in CODEC_FILES {
        let bytes = fixture_bytes(file);
        for position in 0..bytes.len() {
            for flip in [0xFF_u8, 0x10] {
                let mut corrupt = bytes.clone();
                corrupt[position] ^= flip;
                // Ok or a typed error are both fine. A panic fails the test.
                let _ = decode_everything(&corrupt, HOSTILE);
            }
        }
    }
}

/// The byte offset of `tag`'s entry in IFD 0 of a little-endian classic
/// TIFF.
fn entry_at(bytes: &[u8], tag: u16) -> usize {
    let ifd = u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes")) as usize;
    let count = u16::from_le_bytes([bytes[ifd], bytes[ifd + 1]]) as usize;
    (0..count)
        .map(|i| ifd + 2 + i * 12)
        .find(|at| u16::from_le_bytes([bytes[*at], bytes[*at + 1]]) == tag)
        .unwrap_or_else(|| panic!("tag {tag}"))
}

/// Write `value` as the inline LONG value of `tag`.
fn patch_long(bytes: &mut [u8], tag: u16, value: u32) {
    let at = entry_at(bytes, tag);
    bytes[at + 2..at + 4].copy_from_slice(&4u16.to_le_bytes());
    bytes[at + 4..at + 8].copy_from_slice(&1u32.to_le_bytes());
    bytes[at + 8..at + 12].copy_from_slice(&value.to_le_bytes());
}

/// A one-band little-endian classic tiled file: one tile per block, so
/// the block index of a 16x16 single-tile image is inline.
fn single_tile() -> Vec<u8> {
    fixture_bytes("t-uint8.tif")
}

#[test]
fn an_absurd_byte_count_is_refused_before_any_fetch() {
    let mut bytes = single_tile();
    // Rewrite the image as one 16x16 tile with a 4 GiB byte count.
    patch_long(&mut bytes, 256, 16);
    patch_long(&mut bytes, 257, 16);
    patch_long(&mut bytes, 324, 8);
    patch_long(&mut bytes, 325, u32::MAX);
    let reader = open_all(&TiffOpener::new(), &bytes, HOSTILE).expect("header is valid");
    assert!(matches!(
        nodedb_raster::RasterReader::block_ranges(&reader, 0),
        Err(RasterError::BlockTooLarge {
            needed_bytes,
            ..
        }) if needed_bytes == u64::from(u32::MAX)
    ));
}

#[test]
fn an_offset_past_the_end_is_a_truncation() {
    let mut bytes = single_tile();
    patch_long(&mut bytes, 256, 16);
    patch_long(&mut bytes, 257, 16);
    patch_long(&mut bytes, 324, 0x7FFF_0000);
    patch_long(&mut bytes, 325, 100);
    assert!(matches!(
        decode_everything(&bytes, HOSTILE),
        Err(RasterError::Malformed(TiffFormatError::Truncated { .. }))
    ));
}

#[test]
fn a_huge_image_without_its_block_index_is_refused() {
    let mut bytes = single_tile();
    patch_long(&mut bytes, 256, u32::MAX);
    assert!(matches!(
        open_all(&TiffOpener::new(), &bytes, HOSTILE),
        Err(RasterError::Malformed(TiffFormatError::TagCount { .. }))
    ));
}

#[test]
fn an_absurd_entry_count_is_refused() {
    let mut bytes = single_tile();
    let ifd = u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes")) as usize;
    bytes[ifd..ifd + 2].copy_from_slice(&u16::MAX.to_le_bytes());
    assert!(matches!(
        open_all(&TiffOpener::new(), &bytes, HOSTILE),
        Err(RasterError::Malformed(
            TiffFormatError::TooManyEntries { .. }
        ))
    ));
}

#[test]
fn an_ifd_loop_is_refused() {
    let mut bytes = single_tile();
    let ifd = u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes")) as usize;
    let count = u16::from_le_bytes([bytes[ifd], bytes[ifd + 1]]) as usize;
    let next = ifd + 2 + count * 12;
    let link = u32::try_from(ifd).expect("small file").to_le_bytes();
    bytes[next..next + 4].copy_from_slice(&link);
    assert!(matches!(
        open_all(&TiffOpener::new(), &bytes, HOSTILE),
        Err(RasterError::Malformed(TiffFormatError::IfdLoop { .. }))
    ));
}

#[test]
fn a_tag_value_over_the_cap_is_refused() {
    let mut bytes = single_tile();
    // A TileOffsets count of 64 Mi LONGs: 256 MiB, over the header cap.
    let at = entry_at(&bytes, 324);
    bytes[at + 4..at + 8].copy_from_slice(&(64 * 1024 * 1024u32).to_le_bytes());
    assert!(matches!(
        open_all(&TiffOpener::new(), &bytes, HOSTILE),
        Err(RasterError::TagValueTooLarge { tag: 324, .. })
    ));
}

#[test]
fn the_header_cap_bounds_what_an_open_caches() {
    let bytes = fixture_bytes("landsat-c2.tif");
    let tight = LIMITS.with_header_bytes(1000);
    assert!(matches!(
        open_all(&TiffOpener::new(), &bytes, tight),
        Err(RasterError::HeaderTooLarge { limit_bytes: 1000 })
    ));
}

/// A source whose object does not exist.
struct Missing;

impl RangeRead for Missing {
    fn read_range(&mut self, _range: ByteRange) -> Result<Option<Vec<u8>>, RasterError> {
        Ok(None)
    }
}

#[test]
fn a_missing_object_is_a_typed_error() {
    assert!(matches!(
        open_with(&TiffOpener::new(), &mut Missing, LIMITS),
        Err(RasterError::SourceMissing { object: 0 })
    ));
}
