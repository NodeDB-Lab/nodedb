// SPDX-License-Identifier: BUSL-1.1

//! `COPY <array> FROM '<path>.tif'` and attr `NODATA` metadata.
//!
//! Nodata precedence: the COPY `NODATA` option, then the file's
//! `GDAL_NODATA`, then each attr's `NODATA`. The first two apply to every
//! band. Attr metadata gives each band the nodata of its attr, so a pixel is
//! skipped only when every loaded band holds its own nodata.

use crate::harness::TestServer;

use super::array_geotiff::fixture::{
    HEIGHT, KEPT_PIXELS, WIDTH, sample, scene_file, scene_file_without_nodata,
};
use super::array_geotiff::support::{
    PIXEL_GEOREF, aggregate, copy_geotiff, create, pixel_array, pixel_cells,
};

const ALL_PIXELS: u64 = (WIDTH * HEIGHT) as u64;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn attr_nodata_applies_when_the_file_has_none() {
    let server = TestServer::start().await;
    let (_file, path) = scene_file_without_nodata();

    // Every band's attr has NODATA 0: the zero block loads no cell.
    create(
        &server,
        &pixel_array(
            "meta_all",
            "0..191",
            "b1 UINT16 NODATA 0, b2 UINT16 NODATA 0, b3 UINT16 NODATA 0",
            PIXEL_GEOREF,
        ),
    )
    .await;
    let loaded = copy_geotiff(&server, "meta_all", &path, "")
        .await
        .unwrap_or_else(|e| panic!("COPY meta_all: {e:?}"));
    assert_eq!(loaded, KEPT_PIXELS);

    // Only b1 has NODATA 0: the zero block loads, b1 NULL there.
    create(
        &server,
        &pixel_array(
            "meta_b1",
            "0..191",
            "b1 UINT16 NODATA 0, b2 UINT16, b3 UINT16",
            PIXEL_GEOREF,
        ),
    )
    .await;
    let loaded = copy_geotiff(&server, "meta_b1", &path, "")
        .await
        .unwrap_or_else(|e| panic!("COPY meta_b1: {e:?}"));
    assert_eq!(loaded, ALL_PIXELS);
    assert_eq!(
        aggregate(&server, "meta_b1", "b1", "count", "").await,
        KEPT_PIXELS as f64
    );
    assert_eq!(
        aggregate(&server, "meta_b1", "b2", "count", "").await,
        ALL_PIXELS as f64
    );
    let block = pixel_cells(&server, "meta_b1", (100, 100), (120, 120)).await;
    assert_eq!(block.get(&(100, 120)).map(String::as_str), Some(""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_copy_option_and_the_file_tag_take_precedence() {
    let server = TestServer::start().await;

    // The file's GDAL_NODATA 0 wins over attr NODATA 1000: the zero block
    // is skipped, and b1 samples of 1000 load.
    let (_tagged, tagged_path) = scene_file();
    create(
        &server,
        &pixel_array(
            "meta_file",
            "0..191",
            "b1 UINT16 NODATA 1000, b2 UINT16, b3 UINT16",
            PIXEL_GEOREF,
        ),
    )
    .await;
    let loaded = copy_geotiff(&server, "meta_file", &tagged_path, "")
        .await
        .unwrap_or_else(|e| panic!("COPY meta_file: {e:?}"));
    assert_eq!(loaded, KEPT_PIXELS);
    let origin = pixel_cells(&server, "meta_file", (0, 0), (0, 0)).await;
    assert_eq!(
        origin.get(&(0, 0)).map(String::as_str),
        Some(sample(1, 0, 0).to_string().as_str())
    );

    // The COPY option wins over attr NODATA 0 on a file with no tag.
    let (_bare, bare_path) = scene_file_without_nodata();
    create(
        &server,
        &pixel_array("meta_opt", "0..191", "b1 UINT16 NODATA 0", PIXEL_GEOREF),
    )
    .await;
    let loaded = copy_geotiff(
        &server,
        "meta_opt",
        &bare_path,
        ", BANDS (b1, SKIP, SKIP), NODATA 1000",
    )
    .await
    .unwrap_or_else(|e| panic!("COPY meta_opt: {e:?}"));
    let skipped = (0..HEIGHT)
        .flat_map(|r| (0..WIDTH).map(move |c| (r, c)))
        .filter(|&(r, c)| sample(1, r, c) == 1000)
        .count() as u64;
    assert_eq!(loaded, ALL_PIXELS - skipped, "zeros load, 1000s do not");
}
