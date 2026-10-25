// SPDX-License-Identifier: BUSL-1.1

//! The statement's GeoTIFF options in the form the load planner reads.

use nodedb_raster_load::{BandTarget, LoadOptions, PixelWindow};
use nodedb_sql::ddl_ast::statement::{CopyBand, CopyRasterOptions};

/// The planning options `options` carries. The transport options
/// (`CREDENTIAL`, `REQUESTER_PAYS`, `OVERVIEW`) and `ADOPT_METADATA` stay
/// with the server.
pub(crate) fn load_options(options: &CopyRasterOptions) -> LoadOptions {
    LoadOptions {
        bands: options.bands.as_ref().map(|bands| {
            bands
                .iter()
                .map(|band| match band {
                    CopyBand::Attr(name) => BandTarget::Attr(name.clone()),
                    CopyBand::Skip => BandTarget::Skip,
                })
                .collect()
        }),
        nodata: options.nodata,
        time_ms: options.time_ms,
        window: options.window.map(|w| PixelWindow {
            col0: w.x0,
            row0: w.y0,
            col1: w.x1,
            row1: w.y1,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_sql::ddl_ast::statement::CopyWindow;

    #[test]
    fn planning_options_carry_over() {
        let options = CopyRasterOptions {
            bands: Some(vec![CopyBand::Attr("red".into()), CopyBand::Skip]),
            nodata: Some(-9999.0),
            time_ms: Some(1_700_000_000_000),
            window: Some(CopyWindow {
                x0: 1,
                y0: 2,
                x1: 3,
                y1: 4,
            }),
            ..Default::default()
        };
        assert_eq!(
            load_options(&options),
            LoadOptions {
                bands: Some(vec![BandTarget::Attr("red".into()), BandTarget::Skip]),
                nodata: Some(-9999.0),
                time_ms: Some(1_700_000_000_000),
                window: Some(PixelWindow {
                    col0: 1,
                    row0: 2,
                    col1: 3,
                    row1: 4,
                }),
            }
        );
        assert_eq!(
            load_options(&CopyRasterOptions::default()),
            LoadOptions::default()
        );
    }
}
