// SPDX-License-Identifier: Apache-2.0

//! JPEG (code 7): one baseline or progressive JPEG stream per chunk.
//!
//! A TIFF can keep the quantisation and Huffman tables of every chunk in
//! one `JPEGTables` stream. Its tables are spliced in front of the chunk's
//! own markers. A three-component YCbCr stream decodes to RGB.

use zune_jpeg::JpegDecoder;
use zune_jpeg::zune_core::bytestream::ZCursor;
use zune_jpeg::zune_core::colorspace::ColorSpace;
use zune_jpeg::zune_core::options::DecoderOptions;

use crate::codec::error::CodecError;

/// The `SOI` marker that opens a JPEG stream.
const SOI: [u8; 2] = [0xFF, 0xD8];
/// The `EOI` marker that closes a JPEG stream.
const EOI: [u8; 2] = [0xFF, 0xD9];

/// One JPEG chunk and the frame it must decode to.
#[derive(Debug, Clone, Copy)]
pub struct JpegChunk<'a> {
    pub data: &'a [u8],
    /// The `JPEGTables` stream, when the file has one.
    pub tables: Option<&'a [u8]>,
    pub width: u32,
    pub height: u32,
    /// 1 (gray) or 3 (RGB, or YCbCr decoded to RGB).
    pub components: usize,
}

/// Decode `chunk` to pixel-interleaved 8-bit samples.
pub fn decode(chunk: JpegChunk<'_>) -> Result<Vec<u8>, CodecError> {
    let shape_error = |width, height, components| CodecError::JpegShape {
        width,
        height,
        components,
        expected_width: chunk.width,
        expected_height: chunk.height,
        expected_components: chunk.components,
    };
    let out_space = match chunk.components {
        1 => ColorSpace::Luma,
        3 => ColorSpace::RGB,
        _ => return Err(shape_error(0, 0, 0)),
    };
    let stream = splice_tables(chunk.tables, chunk.data);
    let options = DecoderOptions::default()
        .set_max_width(chunk.width as usize)
        .set_max_height(chunk.height as usize)
        .jpeg_set_out_colorspace(out_space);
    let mut decoder = JpegDecoder::new_with_options(ZCursor::new(stream.as_slice()), options);
    decoder.decode_headers().map_err(CodecError::Jpeg)?;
    let Some(info) = decoder.info() else {
        return Err(shape_error(0, 0, 0));
    };
    let (width, height) = (u32::from(info.width), u32::from(info.height));
    let components = usize::from(info.components);
    if width != chunk.width || height != chunk.height || components != chunk.components {
        return Err(shape_error(width, height, components));
    }
    let len = (width as usize)
        .checked_mul(height as usize)
        .and_then(|pixels| pixels.checked_mul(components))
        .ok_or(shape_error(width, height, components))?;
    let mut out = vec![0u8; len];
    decoder.decode_into(&mut out).map_err(CodecError::Jpeg)?;
    Ok(out)
}

/// The chunk stream with the tables spliced in: the tables without their
/// `EOI`, then the chunk without its `SOI`.
fn splice_tables(tables: Option<&[u8]>, data: &[u8]) -> Vec<u8> {
    let Some(tables) = tables else {
        return data.to_vec();
    };
    let (Some(table_body), Some(chunk_body)) = (tables.strip_suffix(&EOI), data.strip_prefix(&SOI))
    else {
        return data.to_vec();
    };
    if !table_body.starts_with(&SOI) {
        return data.to_vec();
    }
    let mut stream = Vec::with_capacity(table_body.len() + chunk_body.len());
    stream.extend_from_slice(table_body);
    stream.extend_from_slice(chunk_body);
    stream
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_are_spliced_between_the_markers() {
        let tables = [0xFF, 0xD8, 0xFF, 0xDB, 1, 0xFF, 0xD9];
        let data = [0xFF, 0xD8, 0xFF, 0xDA, 2, 0xFF, 0xD9];
        assert_eq!(
            splice_tables(Some(&tables), &data),
            vec![0xFF, 0xD8, 0xFF, 0xDB, 1, 0xFF, 0xDA, 2, 0xFF, 0xD9]
        );
        assert_eq!(splice_tables(None, &data), data.to_vec());
        assert_eq!(splice_tables(Some(&[1, 2]), &data), data.to_vec());
    }

    #[test]
    fn garbage_and_bad_shapes_are_typed_errors() {
        let chunk = JpegChunk {
            data: b"not a jpeg",
            tables: None,
            width: 8,
            height: 8,
            components: 1,
        };
        assert!(matches!(decode(chunk), Err(CodecError::Jpeg(_))));
        let four = JpegChunk {
            components: 4,
            ..chunk
        };
        assert!(matches!(decode(four), Err(CodecError::JpegShape { .. })));
    }
}
