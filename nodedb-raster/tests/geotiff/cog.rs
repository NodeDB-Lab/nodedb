// SPDX-License-Identifier: Apache-2.0

//! Cloud-optimised GeoTIFFs: one-round opens, overviews, internal masks,
//! band metadata, and the Landsat Collection 2 band shape.

use nodedb_crs::CrsContext;
use nodedb_raster::{
    ByteRange, CrsRef, ObjectId, RangeRead, RasterError, RasterReader, TiffOpener, open_with,
};

use crate::files::{fixture, fixture_bytes, open_fixture};
use crate::fixtures::LIMITS;

/// A slice source that records every range it serves.
struct Counting<'a> {
    bytes: &'a [u8],
    reads: Vec<ByteRange>,
}

impl RangeRead for Counting<'_> {
    fn read_range(&mut self, range: ByteRange) -> Result<Option<Vec<u8>>, RasterError> {
        self.reads.push(range);
        let len = self.bytes.len() as u64;
        let (start, end) = (range.start.min(len) as usize, range.end.min(len) as usize);
        Ok(Some(self.bytes[start..end.max(start)].to_vec()))
    }
}

#[test]
fn a_cog_opens_in_one_round_from_its_first_64_kib() {
    for file in ["cog-mask.tif", "landsat-c2.tif"] {
        let bytes = fixture_bytes(file);
        let mut source = Counting {
            bytes: &bytes,
            reads: Vec::new(),
        };
        for image in [0, 1] {
            source.reads.clear();
            let opener = TiffOpener::new().with_image(image);
            open_with(&opener, &mut source, LIMITS).expect("opens");
            assert_eq!(
                source.reads,
                vec![ByteRange::new(ObjectId(0), 0, 64 * 1024)],
                "{file} image {image}"
            );
        }
    }
}

#[test]
fn overviews_scale_the_geotransform_and_keep_the_crs() {
    let full = open_fixture(&fixture("cog-mask-0")).0;
    let half = open_fixture(&fixture("cog-mask-1")).0;
    let quarter = open_fixture(&fixture("cog-mask-2")).0;
    assert_eq!(
        full.header().transform.map(|gt| gt.0),
        Some([600_000.0, 30.0, 0.0, 450_000.0, 0.0, -30.0])
    );
    assert_eq!(
        half.header().transform.map(|gt| gt.0),
        Some([600_000.0, 60.0, 0.0, 450_000.0, 0.0, -60.0])
    );
    assert_eq!(
        quarter.header().transform.map(|gt| gt.0),
        Some([600_000.0, 120.0, 0.0, 450_000.0, 0.0, -120.0])
    );
    for reader in [&full, &half, &quarter] {
        assert_eq!(reader.header().crs, CrsRef::Epsg(32648));
        assert!(reader.has_mask());
    }
}

#[test]
fn an_image_past_the_last_overview_is_a_typed_error() {
    let bytes = fixture_bytes("cog-mask.tif");
    let opener = TiffOpener::new().with_image(3);
    assert!(matches!(
        nodedb_raster::open_all(&opener, &bytes, LIMITS),
        Err(RasterError::ImageOutOfRange { index: 3, count: 3 })
    ));
}

#[test]
fn mask_ranges_follow_the_data_ranges() {
    let (reader, _) = open_fixture(&fixture("cog-mask-0"));
    let ranges = reader.block_ranges(0).expect("ranges");
    // One data tile and the one mask tile of the same 16x16 block.
    assert_eq!(ranges.len(), 2);
    assert!(
        ranges
            .iter()
            .all(|r| r.object == ObjectId(0) && !r.is_empty())
    );
    assert_ne!(ranges[0], ranges[1]);
}

#[test]
fn the_landsat_band_reads_its_georeference_and_metadata() {
    let (reader, _) = open_fixture(&fixture("landsat-c2"));
    let header = reader.header();
    assert_eq!(
        header.transform.map(|gt| gt.0),
        Some([399_960.0, 30.0, 0.0, 5_000_040.0, 0.0, -30.0])
    );
    assert_eq!(
        header
            .crs
            .resolve(&CrsContext::builtin_only())
            .expect("CRS maps"),
        Some(nodedb_types::geometry::Srid::new(32648).expect("srid"))
    );
    let band = &header.bands[0];
    assert_eq!(band.nodata, Some(0.0));
    assert_eq!(band.scale, Some(2.75e-05));
    assert_eq!(band.offset, Some(-0.2));
    assert_eq!(band.name.as_deref(), Some("SR_B4"));
    assert_eq!(reader.layout().block_width, 256);
    assert_eq!(reader.block_count(), 4);
    assert!(!reader.has_mask());
}

#[test]
fn per_band_gdal_metadata_reaches_the_band_list() {
    let (reader, _) = open_fixture(&fixture("gdal-metadata"));
    let bands = &reader.header().bands;
    assert_eq!(bands.len(), 3);
    assert_eq!(bands[0].name.as_deref(), Some("temperature"));
    assert_eq!(bands[0].scale, Some(0.01));
    assert_eq!(bands[0].offset, Some(273.15));
    assert_eq!(bands[0].units.as_deref(), Some("K"));
    assert_eq!(
        bands[0].nodata,
        Some(-1.0),
        "GDAL_NODATA applies to every band"
    );
    assert_eq!(bands[1].nodata, Some(-9999.0), "a band item overrides it");
    assert_eq!(bands[1].scale, None);
    assert_eq!(bands[2].valid_range, Some((0.0, 1000.0)));
    assert_eq!(bands[2].offset, Some(-10.0));
    assert_eq!(bands[2].units.as_deref(), Some("m"));
}
