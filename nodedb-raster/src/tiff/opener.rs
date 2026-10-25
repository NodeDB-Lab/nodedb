// SPDX-License-Identifier: Apache-2.0

//! `TiffOpener`: the sans-IO open of a TIFF or BigTIFF image.
//!
//! The first round asks for the first 64 KiB: the header and, in a
//! cloud-optimised GeoTIFF, every IFD. Later rounds ask for IFDs and tag
//! values that lie further. The opener walks the whole IFD chain, picks
//! the full-resolution image or an overview, and the mask IFD of the same
//! size and subfile kind. Georeference and band metadata come from the
//! first image; an overview's geotransform is scaled to its size.

use nodedb_crs::GeoTransform;

use crate::error::RasterError;
use crate::header::RasterHeader;
use crate::limits::RasterLimits;
use crate::reader::traits::{Opened, RasterOpener};
use crate::source::cache::RangeCache;
use crate::source::range::ObjectId;
use crate::tiff::byte_order::ByteOrder;
use crate::tiff::chain::{IfdSummary, read_ifd, walk};
use crate::tiff::error::TiffFormatError;
use crate::tiff::fetch::Fetcher;
use crate::tiff::file_header::{MAX_HEADER_LEN, parse_header};
use crate::tiff::geo::GeoInfo;
use crate::tiff::ifd::Ifd;
use crate::tiff::image::TiffImage;
use crate::tiff::mask::MaskImage;
use crate::tiff::reader::TiffReader;
use crate::tiff::tags::{GEO_TAGS, IMAGE_TAGS};
use crate::tiff::values::{TagValues, prefetch};

/// Opens one image of a TIFF object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TiffOpener {
    object: ObjectId,
    image: u32,
}

impl Default for TiffOpener {
    fn default() -> Self {
        Self::new()
    }
}

impl TiffOpener {
    /// The full-resolution image of object 0.
    pub const fn new() -> Self {
        Self {
            object: ObjectId(0),
            image: 0,
        }
    }

    /// Read `object` instead of object 0.
    pub const fn with_object(self, object: ObjectId) -> Self {
        Self {
            object,
            image: self.image,
        }
    }

    /// Open image `image`: 0 is full resolution, `n` the n-th overview.
    pub const fn with_image(self, image: u32) -> Self {
        Self {
            object: self.object,
            image,
        }
    }
}

impl RasterOpener for TiffOpener {
    type Reader = TiffReader;

    fn open(
        &self,
        cache: &RangeCache,
        limits: RasterLimits,
    ) -> Result<Opened<TiffReader>, RasterError> {
        let mut fetcher = Fetcher::new(cache, self.object);
        let Some(head) = fetcher.prefix(0, MAX_HEADER_LEN)? else {
            return Ok(Opened::Need(fetcher.into_need()));
        };
        let file = parse_header(head)?;
        let format = file.format;
        let Some(chain) = walk(&mut fetcher, format, file.first_ifd, limits.header_bytes)? else {
            return Ok(Opened::Need(fetcher.into_need()));
        };
        let (first, selected, mask) = pick(&chain, self.image)?;
        let first_ifd = read_ifd(&mut fetcher, format, first.offset)?
            .ok_or(RasterError::OpenStalled { rounds: 0 })?;
        let selected_ifd = read_ifd(&mut fetcher, format, selected.offset)?
            .ok_or(RasterError::OpenStalled { rounds: 0 })?;
        let mask_ifd = match mask {
            Some(summary) => Some(
                read_ifd(&mut fetcher, format, summary.offset)?
                    .ok_or(RasterError::OpenStalled { rounds: 0 })?,
            ),
            None => None,
        };
        prefetch(
            &selected_ifd,
            &IMAGE_TAGS,
            &mut fetcher,
            limits.header_bytes,
        )?;
        prefetch(&first_ifd, &GEO_TAGS, &mut fetcher, limits.header_bytes)?;
        if let Some(ifd) = &mask_ifd {
            prefetch(ifd, &IMAGE_TAGS, &mut fetcher, limits.header_bytes)?;
        }
        if fetcher.pending() {
            return Ok(Opened::Need(fetcher.into_need()));
        }
        let read = Reading {
            cache,
            object: self.object,
            order: format.order,
            header_bytes: limits.header_bytes,
        };
        let image = TiffImage::read(&read.values(&selected_ifd), limits)?;
        let geo = GeoInfo::read(&read.values(&first_ifd))?;
        let mask = mask_ifd
            .as_ref()
            .map(|ifd| MaskImage::read(&read.values(ifd), limits))
            .transpose()?;
        let transform = geo.transform.map(|gt| {
            let mut terms = gt.0;
            let sx = first.width as f64 / f64::from(image.layout.width);
            let sy = first.height as f64 / f64::from(image.layout.height);
            terms[1] *= sx;
            terms[2] *= sy;
            terms[4] *= sx;
            terms[5] *= sy;
            GeoTransform(terms)
        });
        let header = RasterHeader {
            width: image.layout.width,
            height: image.layout.height,
            outer: Vec::new(),
            bands: geo.bands(image.layout.bands, image.layout.sample_type),
            transform,
            crs: geo.crs.clone(),
        };
        Ok(Opened::Ready(TiffReader {
            object: self.object,
            header,
            image,
            mask,
            geokeys: geo.geokeys,
            limits,
        }))
    }
}

/// What every tag read of one open round shares.
struct Reading<'a> {
    cache: &'a RangeCache,
    object: ObjectId,
    order: ByteOrder,
    header_bytes: usize,
}

impl<'a> Reading<'a> {
    fn values<'b>(&self, ifd: &'b Ifd) -> TagValues<'b>
    where
        'a: 'b,
    {
        TagValues {
            ifd,
            cache: self.cache,
            object: self.object,
            order: self.order,
            header_bytes: self.header_bytes,
        }
    }
}

/// The first image, the selected image, and the selected image's mask.
fn pick(
    chain: &[IfdSummary],
    image: u32,
) -> Result<(IfdSummary, IfdSummary, Option<IfdSummary>), RasterError> {
    let mut images = chain.iter().filter(|ifd| !ifd.mask);
    let first = *images.next().ok_or(TiffFormatError::NoImage)?;
    if first.reduced {
        return Err(RasterError::ReducedResolutionFirstImage);
    }
    let count = u32::try_from(chain.iter().filter(|ifd| !ifd.mask).count()).unwrap_or(u32::MAX);
    let selected = *chain
        .iter()
        .filter(|ifd| !ifd.mask)
        .nth(image as usize)
        .ok_or(RasterError::ImageOutOfRange {
            index: image,
            count,
        })?;
    let mask = chain
        .iter()
        .find(|ifd| {
            ifd.mask
                && ifd.width == selected.width
                && ifd.height == selected.height
                && ifd.reduced == selected.reduced
        })
        .copied();
    Ok((first, selected, mask))
}
