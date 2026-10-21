// SPDX-License-Identifier: BUSL-1.1

//! `WINDOW (x0, y0, x1, y1)`, `OVERVIEW n` and `ADOPT_METADATA true` on a
//! GeoTIFF load.
//!
//! A window loads its pixels only, and its footprint alone must fit the dim
//! domains. An overview loads into an array on its coarser grid: the file
//! geotransform scales with the overview. Adoption writes the files' band
//! metadata into an attr that has none, and never into one that has some.

use std::collections::HashMap;

use crate::harness::TestServer;

use super::array_geotiff::files::{expected, fixture};
use super::array_geotiff::fixture::{sample, scene_file};
use super::array_geotiff::support::{
    PIXEL_GEOREF, SCENE_ATTRS, aggregate, copy_geotiff, create, pixel_array, pixel_cells,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_window_loads_its_pixels_only() {
    let server = TestServer::start().await;
    let (_file, path) = scene_file();
    create(
        &server,
        &pixel_array("win", "0..191", SCENE_ATTRS, PIXEL_GEOREF),
    )
    .await;
    let loaded = copy_geotiff(&server, "win", &path, ", WINDOW (10, 20, 70, 30)")
        .await
        .unwrap_or_else(|e| panic!("COPY win: {e:?}"));
    assert_eq!(loaded, 60 * 10);
    let cells = pixel_cells(&server, "win", (0, 191), (0, 255)).await;
    assert_eq!(cells.len(), 600);
    assert!(
        cells
            .keys()
            .all(|&(r, c)| (20..30).contains(&r) && (10..70).contains(&c))
    );
    assert_eq!(cells.get(&(25, 40)), Some(&sample(1, 25, 40).to_string()));

    // A window inside rows 0..99 loads into an array whose row domain ends
    // at 99, where the whole file does not fit.
    create(
        &server,
        &pixel_array("win_short", "0..99", SCENE_ATTRS, PIXEL_GEOREF),
    )
    .await;
    let loaded = copy_geotiff(&server, "win_short", &path, ", WINDOW (0, 0, 256, 100)")
        .await
        .unwrap_or_else(|e| panic!("COPY win_short: {e:?}"));
    assert_eq!(loaded, 256 * 100, "the nodata block starts at row 100");

    let err = copy_geotiff(&server, "win", &path, ", WINDOW (0, 0, 300, 10)")
        .await
        .expect_err("past the image");
    assert_eq!(err.code, "22023", "{err:?}");
}

/// A pixel-mode array over overview `level` of the scene: the grid at
/// `30 * 2^level` m.
fn overview_array(name: &str, level: u32) -> String {
    let scale = 30 << level;
    let (rows, cols) = (192 >> level, 256 >> level);
    format!(
        "CREATE ARRAY {name} DIMS (row INT64 [0..{}], col INT64 [0..{}]) \
         ATTRS (b1 UINT16) TILE_EXTENTS (64, 64) \
         GEOREF (SRID 32647, X col, Y row, TRANSFORM (600000, {scale}, 0, 700000, 0, -{scale}))",
        rows - 1,
        cols - 1
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_overview_loads_on_its_own_grid() {
    let server = TestServer::start().await;
    let path = fixture("scene-overview.tif");
    for level in [1u32, 2] {
        let name = format!("ov{level}");
        create(&server, &overview_array(&name, level)).await;
        let loaded = copy_geotiff(&server, &name, &path, &format!(", OVERVIEW {level}"))
            .await
            .unwrap_or_else(|e| panic!("COPY {name}: {e:?}"));
        let (w, h) = (
            expected(&format!("overview{level}_width")),
            expected(&format!("overview{level}_height")),
        );
        assert_eq!(loaded, w * h, "{name}");
        assert_eq!(
            aggregate(&server, &name, "b1", "sum", "").await,
            expected(&format!("overview{level}_sum")) as f64,
            "{name}"
        );
        let first = pixel_cells(&server, &name, (0, 0), (0, 0)).await;
        assert_eq!(
            first.get(&(0, 0)),
            Some(&expected(&format!("overview{level}_first")).to_string())
        );
    }
    // The full-resolution grid does not align with an overview's.
    create(&server, &overview_array("ov_wrong", 1)).await;
    let err = copy_geotiff(&server, "ov_wrong", &path, "")
        .await
        .expect_err("30 m into a 60 m array");
    assert_eq!(err.code, "22023", "{err:?}");
    let err = copy_geotiff(&server, "ov_wrong", &path, ", OVERVIEW 9")
        .await
        .expect_err("no such overview");
    assert_eq!(err.code, "0A000", "{err:?}");
}

/// DESCRIBE rows of `name`, keyed by field, valued by type.
async fn describe(server: &TestServer, name: &str) -> HashMap<String, String> {
    server
        .query_named_rows(&format!("DESCRIBE {name}"))
        .await
        .unwrap_or_else(|e| panic!("DESCRIBE {name}: {e}"))
        .into_iter()
        .map(|row| {
            let field = row.get("field").cloned().unwrap_or_default();
            let ty = row.get("type").cloned().unwrap_or_default();
            (field, ty)
        })
        .collect()
}

fn small_array(name: &str, attrs: &str) -> String {
    format!(
        "CREATE ARRAY {name} DIMS (row INT64 [0..63], col INT64 [0..63]) \
         ATTRS ({attrs}) TILE_EXTENTS (64, 64) {PIXEL_GEOREF}"
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adopt_metadata_fills_only_attrs_without_metadata() {
    let server = TestServer::start().await;
    let path = fixture("adopt-metadata.tif");

    create(&server, &small_array("adopt_on", "red UINT16")).await;
    let loaded = copy_geotiff(&server, "adopt_on", &path, ", ADOPT_METADATA true")
        .await
        .unwrap_or_else(|e| panic!("COPY adopt_on: {e:?}"));
    assert_eq!(loaded, 64 * 64 - 8, "the file nodata 0 skips 8 pixels");
    let red = describe(&server, "adopt_on").await["red"].clone();
    for part in [
        "NODATA 0.0",
        "SCALE 2.75e-5",
        "OFFSET -0.2",
        "UNITS 'reflectance'",
    ] {
        assert!(red.contains(part), "{red}");
    }

    create(&server, &small_array("adopt_off", "red UINT16")).await;
    copy_geotiff(&server, "adopt_off", &path, "")
        .await
        .unwrap_or_else(|e| panic!("COPY adopt_off: {e:?}"));
    assert_eq!(describe(&server, "adopt_off").await["red"], "UINT16");

    create(&server, &small_array("adopt_kept", "red UINT16 SCALE 1.0")).await;
    copy_geotiff(&server, "adopt_kept", &path, ", ADOPT_METADATA true")
        .await
        .unwrap_or_else(|e| panic!("COPY adopt_kept: {e:?}"));
    let kept = describe(&server, "adopt_kept").await["red"].clone();
    assert!(
        kept.contains("SCALE 1.0") && !kept.contains("UNITS"),
        "{kept}"
    );
}
