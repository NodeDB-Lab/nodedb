//! TIFF open and block decode over arbitrary bytes, through the sans-IO
//! reader and its synchronous driver.
//!
//! Every outcome must be `Ok` or a typed `RasterError`. A decoded block
//! must hold exactly one window of samples per band, and a mask of one
//! flag per window pixel.

use nodedb_raster::{RasterLimits, RasterReader, TiffOpener, open_all, read_block_all};

/// Per-block decode cap. Small enough that a hostile header cannot make
/// one block a large allocation.
const BLOCK_BYTES: usize = 4 * 1024 * 1024;
/// Header cap: the IFDs and block index one open may cache.
const HEADER_BYTES: usize = 4 * 1024 * 1024;
/// Blocks decoded per image, so a huge declared grid stays fast.
const MAX_BLOCKS: u32 = 64;

pub fn run(data: &[u8]) {
    // The full-resolution image and the first overview.
    for image in 0..2 {
        decode_image(data, image);
    }
}

fn decode_image(file: &[u8], image: u32) {
    let opener = TiffOpener::new().with_image(image);
    let limits = RasterLimits::new(BLOCK_BYTES).with_header_bytes(HEADER_BYTES);
    let Ok(reader) = open_all(&opener, file, limits) else {
        return;
    };
    let header = reader.header();
    let _ = header.crs.resolve(&nodedb_crs::CrsContext::builtin_only());
    let bands = header.band_count();
    for index in 0..reader.block_count().min(MAX_BLOCKS) {
        let Ok(block) = read_block_all(&reader, index, file) else {
            continue;
        };
        assert_eq!(block.bands.len(), bands, "band count of block {index}");
        for (samples, band) in block.bands.iter().zip(&header.bands) {
            assert_eq!(samples.len(), block.window.pixels(), "block {index}");
            assert_eq!(samples.sample_type(), band.sample_type, "block {index}");
        }
        assert_eq!(block.masks.len(), bands, "masks of block {index}");
        for mask in block.masks.iter().flatten() {
            assert_eq!(mask.len(), block.window.pixels(), "mask of block {index}");
        }
    }
}
