// SPDX-License-Identifier: BUSL-1.1

//! The SQLSTATE of each raster load error.
//!
//! - A malformed or corrupt file is `22P04` (bad copy file format). A
//!   user-defined CRS whose GeoKeys do not form a valid definition is one,
//!   and so is a corrupt compressed block.
//! - A valid file that uses a feature the reader lacks is `0A000`. A CRS
//!   that is undefined, or a user-defined CRS no built-in CRS matches, is
//!   one.
//! - A block or header over its cap is `54000`.
//! - A read error outside the file's bytes is `58030`.
//! - A fetch failure reports the class [`FetchError::sqlstate`] names.
//! - A plan refusal reports the class [`RasterLoadError::sqlstate`] names.

use nodedb_raster::RasterError;
use nodedb_raster_load::RasterLoadError;

use super::super::decode::RowError;
use super::driver::DriveError;
use super::fetch::FetchError;

impl From<RasterLoadError> for RowError {
    fn from(e: RasterLoadError) -> Self {
        RowError::new(e.row(), e.sqlstate(), e.to_string())
    }
}

impl From<FetchError> for RowError {
    fn from(e: FetchError) -> Self {
        RowError::new(0, e.sqlstate(), e.to_string())
    }
}

/// `e`, met reading the sources `names` under a `cap`-byte decode cap, as a
/// refusal of the whole input.
pub(super) fn drive_refusal(e: DriveError, names: &[String], cap: usize) -> RowError {
    match e {
        DriveError::Raster { index, error } => {
            let name = names.get(index).map_or("?", String::as_str);
            raster_refusal(&error, name, cap)
        }
        DriveError::Fetch(error) => error.into(),
        DriveError::Task(detail) => {
            RowError::new(0, "XX000", format!("raster decode task failed: {detail}"))
        }
    }
}

/// `e`, met reading `name` under a `cap`-byte decode cap, as a refusal of
/// the whole input.
pub(super) fn raster_refusal(e: &RasterError, name: &str, cap: usize) -> RowError {
    let sqlstate = sqlstate(e);
    let message = if sqlstate == "54000" {
        format!(
            "'{name}': {e}; the decode cap is {cap} bytes; \
             retile the file with `gdal_translate -co TILED=YES`"
        )
    } else {
        format!("'{name}': {e}")
    };
    RowError::new(0, sqlstate, message)
}

fn sqlstate(e: &RasterError) -> &'static str {
    match e {
        RasterError::Malformed(_)
        | RasterError::Codec(_)
        | RasterError::GeoKeys(_)
        | RasterError::GeoTagLength { .. }
        | RasterError::PixelScaleWithoutTiepoint
        | RasterError::DegenerateTransform { .. }
        | RasterError::MissingCrsKey { .. }
        | RasterError::UserCrsParse(_)
        | RasterError::InvalidNodata { .. }
        | RasterError::InvalidMetadata { .. }
        | RasterError::BlockOutOfRange { .. }
        | RasterError::BlockBuffers { .. }
        | RasterError::ShortBlock { .. }
        | RasterError::SourceMissing { .. }
        | RasterError::RangeOverrun { .. }
        | RasterError::OpenStalled { .. } => "22P04",
        RasterError::UnsupportedTiff(_)
        | RasterError::UnsupportedCompression { .. }
        | RasterError::UnsupportedSampleType { .. }
        | RasterError::UnsupportedPhotometric { .. }
        | RasterError::ReducedResolutionFirstImage
        | RasterError::ImageOutOfRange { .. }
        | RasterError::GroundControlPoints
        | RasterError::NonAffineTransformation
        | RasterError::UnsupportedModelType { .. }
        | RasterError::UnknownCrs(_)
        | RasterError::UnsupportedUserCrs { .. }
        | RasterError::UnmatchedUserCrs { .. }
        | RasterError::AmbiguousUserCrs { .. } => "0A000",
        RasterError::BlockTooLarge { .. }
        | RasterError::HeaderTooLarge { .. }
        | RasterError::TagValueTooLarge { .. } => "54000",
        RasterError::Io(_) => "58030",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_class_maps_to_its_sqlstate() {
        let cases = [
            (RasterError::PixelScaleWithoutTiepoint, "22P04"),
            (RasterError::UnsupportedCompression { code: 7 }, "0A000"),
            (RasterError::GroundControlPoints, "0A000"),
            (
                RasterError::BlockTooLarge {
                    needed_bytes: 1 << 30,
                    limit_bytes: 1 << 26,
                },
                "54000",
            ),
            (RasterError::Io(std::io::Error::other("disk gone")), "58030"),
        ];
        for (error, code) in cases {
            let refusal = raster_refusal(&error, "/data/scene.tif", 1 << 26);
            assert_eq!(refusal.sqlstate, code, "{error}");
            assert_eq!(refusal.row, 0);
        }
    }

    #[test]
    fn an_oversized_block_names_the_retile_command() {
        let error = RasterError::BlockTooLarge {
            needed_bytes: 8192,
            limit_bytes: 4096,
        };
        let refusal = raster_refusal(&error, "/data/scene.tif", 4096);
        assert!(
            refusal.message.contains("gdal_translate -co TILED=YES"),
            "{}",
            refusal.message
        );
    }

    #[test]
    fn a_plan_refusal_keeps_its_row_and_class() {
        let row: RowError = RasterLoadError::NodataInNotNull {
            row: 9,
            attr: "red".into(),
            band: 1,
            pixel_row: 0,
            pixel_col: 8,
        }
        .into();
        assert_eq!((row.row, row.sqlstate), (9, "23502"));
    }
}
