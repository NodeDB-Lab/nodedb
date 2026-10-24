// SPDX-License-Identifier: Apache-2.0

//! [`Image`]: the sans-IO reader driven over in-memory bytes, and
//! whole-image readback.

use nodedb_crs::CrsContext;
use nodedb_raster::{
    GeoKeyDirectory, GeoTransform, RasterBlock, RasterError, RasterLayout, RasterLimits,
    RasterReader, SampleType, TiffOpener, TiffReader, open_all, read_block_all,
};
use nodedb_types::geometry::Srid;

use crate::fixtures::LIMITS;

/// An opened image and the bytes it reads its blocks from.
pub struct Image {
    pub reader: TiffReader,
    pub bytes: Vec<u8>,
}

impl Image {
    pub fn layout(&self) -> &RasterLayout {
        self.reader.layout()
    }

    pub fn width(&self) -> u32 {
        self.reader.header().width
    }

    pub fn height(&self) -> u32 {
        self.reader.header().height
    }

    pub fn band_count(&self) -> u16 {
        self.layout().bands
    }

    pub fn sample_type(&self) -> SampleType {
        self.layout().sample_type
    }

    pub fn block_count(&self) -> u32 {
        self.reader.block_count()
    }

    pub fn read_block(&self, index: u32) -> Result<RasterBlock, RasterError> {
        read_block_all(&self.reader, index, &self.bytes)
    }

    pub fn transform(&self) -> Option<GeoTransform> {
        self.reader.header().transform
    }

    pub fn srid(&self, ctx: &CrsContext<'_>) -> Result<Option<Srid>, RasterError> {
        self.reader.header().crs.resolve(ctx)
    }

    /// The nodata of band 0: `GDAL_NODATA` in these fixtures.
    pub fn nodata(&self) -> Option<f64> {
        self.reader
            .header()
            .bands
            .first()
            .and_then(|band| band.nodata)
    }

    pub fn geokeys(&self) -> Option<&GeoKeyDirectory> {
        self.reader.geokeys()
    }
}

/// Open an owned copy of `bytes` under [`LIMITS`], so callers can pass a temporary.
pub fn open(bytes: &[u8]) -> Result<Image, RasterError> {
    open_with_limits(bytes, LIMITS)
}

/// Open an owned copy of `bytes` under `limits`.
pub fn open_with_limits(bytes: &[u8], limits: RasterLimits) -> Result<Image, RasterError> {
    let reader = open_all(&TiffOpener::new(), bytes, limits)?;
    Ok(Image {
        reader,
        bytes: bytes.to_vec(),
    })
}

/// Decode every block and reassemble one row-major `f64` plane per band.
pub fn read_all(image: &Image) -> Vec<Vec<f64>> {
    let width = image.width() as usize;
    let pixels = width * image.height() as usize;
    let mut out = vec![vec![f64::NAN; pixels]; usize::from(image.band_count())];
    for index in 0..image.block_count() {
        let block = image.read_block(index).expect("block decodes");
        let window = &block.window;
        assert_eq!(block.bands.len(), out.len(), "band count of block {index}");
        for (band, samples) in block.bands.iter().enumerate() {
            assert_eq!(samples.len(), window.pixels(), "samples in block {index}");
            assert_eq!(samples.sample_type(), image.sample_type());
            for r in 0..window.height as usize {
                for c in 0..window.width as usize {
                    let row = window.row_off as usize + r;
                    let col = window.col_off as usize + c;
                    out[band][row * width + col] = samples
                        .value_f64(r * window.width as usize + c)
                        .expect("sample in window");
                }
            }
        }
    }
    out
}

/// Open `bytes` with `limits` and decode every block; the first error.
pub fn decode_everything(bytes: &[u8], limits: RasterLimits) -> Result<(), RasterError> {
    let image = open_with_limits(bytes, limits)?;
    image.srid(&CrsContext::builtin_only())?;
    for index in 0..image.block_count() {
        image.read_block(index)?;
    }
    Ok(())
}
