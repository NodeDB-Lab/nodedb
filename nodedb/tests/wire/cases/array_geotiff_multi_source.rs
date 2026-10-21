// SPDX-License-Identifier: BUSL-1.1

//! `COPY <array> FROM ('<b4>', '<b5>', '<qa>') WITH (FORMAT geotiff, BANDS (...))`:
//! one single-band file per attr, read in lockstep so every cell carries
//! every attr.
//!
//! B4 and B5 hold nodata 0 in one 10 x 10 block; QA has no nodata. So every
//! pixel loads, and red and nir are NULL in the block. Sources on another
//! grid or block layout are refused before any row loads.

use crate::harness::TestServer;

use super::array_geotiff::files::{band, fixture, fixture_bytes, qa};
use super::array_geotiff::fixture::{HEIGHT, WIDTH, in_nodata_block};
use super::array_geotiff::http_server::serve;
use super::array_geotiff::support::{PIXEL_GEOREF, aggregate, copy_sql, create, pixel_array};

const ALL_PIXELS: u64 = (WIDTH * HEIGHT) as u64;
const BAND_ATTRS: &str = "red UINT16, nir UINT16, qa UINT16";

/// `red`, `nir` and `qa` of cell `(r, c)` as text. NULL reads as "".
async fn cell(server: &TestServer, array: &str, r: u32, c: u32) -> (String, String, String) {
    let sql = format!(
        "SELECT * FROM ARRAY_SLICE('{array}', '{{\"row\":[{r},{r}],\"col\":[{c},{c}]}}', '*', 10)"
    );
    let rows = server
        .query_named_rows(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let row = rows.first().unwrap_or_else(|| panic!("no cell ({r}, {c})"));
    let text = |k: &str| row.get(k).cloned().unwrap_or_default();
    (text("red"), text("nir"), text("qa"))
}

/// The sum of band `k` over every pixel outside the nodata block.
fn band_sum(k: u32) -> f64 {
    let mut sum = 0u64;
    for r in 0..HEIGHT {
        for c in 0..WIDTH {
            sum += u64::from(band(k, r, c));
        }
    }
    sum as f64
}

async fn assert_lockstep_load(server: &TestServer, array: &str) {
    assert_eq!(
        aggregate(server, array, "qa", "count", "").await,
        ALL_PIXELS as f64
    );
    assert_eq!(
        aggregate(server, array, "red", "count", "").await,
        (ALL_PIXELS - 100) as f64
    );
    assert_eq!(
        aggregate(server, array, "red", "sum", "").await,
        band_sum(1)
    );
    assert_eq!(
        aggregate(server, array, "nir", "sum", "").await,
        band_sum(2)
    );
    let (red, nir, q) = cell(server, array, 5, 7).await;
    assert_eq!(
        (red.as_str(), nir.as_str(), q.as_str()),
        (
            band(1, 5, 7).to_string().as_str(),
            band(2, 5, 7).to_string().as_str(),
            qa(5, 7).to_string().as_str()
        )
    );
    assert!(in_nodata_block(100, 120));
    let (red, nir, q) = cell(server, array, 100, 120).await;
    assert_eq!((red.as_str(), nir.as_str()), ("", ""), "nodata block");
    assert_eq!(q, qa(100, 120).to_string());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_single_band_files_fill_three_attrs_of_each_cell() {
    let server = TestServer::start().await;
    create(
        &server,
        &pixel_array("lsat", "0..191", BAND_ATTRS, PIXEL_GEOREF),
    )
    .await;
    let sql = format!(
        "COPY lsat FROM ('{}', '{}', '{}') WITH (FORMAT geotiff, BANDS (red, nir, qa))",
        fixture("band-b4.tif"),
        fixture("band-b5.tif"),
        fixture("band-qa.tif")
    );
    let loaded = copy_sql(&server, &sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert_eq!(loaded, ALL_PIXELS);
    assert_lockstep_load(&server, "lsat").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_over_http_loads_in_lockstep_with_server_files() {
    let server = TestServer::start().await;
    let (addr, served) = serve(vec![("qa.tif", fixture_bytes("band-qa.tif"))], None).await;
    create(
        &server,
        &pixel_array("lsat_http", "0..191", BAND_ATTRS, PIXEL_GEOREF),
    )
    .await;
    let sql = format!(
        "COPY lsat_http FROM ('{}', '{}', 'http://localhost:{}/qa.tif') \
         WITH (FORMAT geotiff, BANDS (red, nir, qa))",
        fixture("band-b4.tif"),
        fixture("band-b5.tif"),
        addr.port()
    );
    let loaded = copy_sql(&server, &sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert_eq!(loaded, ALL_PIXELS);
    assert!(served.requests.load(std::sync::atomic::Ordering::SeqCst) > 0);
    assert_lockstep_load(&server, "lsat_http").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sources_on_another_grid_or_layout_are_refused() {
    let server = TestServer::start().await;
    for (array, other, needle) in [
        ("mm_tiles", "band-b5-tiles128.tif", "block"),
        ("mm_shift", "band-b5-shifted.tif", "geotransform"),
    ] {
        create(
            &server,
            &pixel_array(array, "0..191", BAND_ATTRS, PIXEL_GEOREF),
        )
        .await;
        let sql = format!(
            "COPY {array} FROM ('{}', '{}') WITH (FORMAT geotiff, BANDS (red, nir))",
            fixture("band-b4.tif"),
            fixture(other)
        );
        let err = copy_sql(&server, &sql).await.expect_err("refused");
        assert_eq!(err.code, "22023", "{array}: {err:?}");
        assert!(err.message.contains(needle), "{array}: {err:?}");
        assert!(err.message.contains(other), "{array}: {err:?}");
        assert_eq!(aggregate(&server, array, "red", "count", "").await, 0.0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_missing_url_and_an_ip_literal_are_refused() {
    let server = TestServer::start().await;
    create(
        &server,
        &pixel_array("lsat_404", "0..191", BAND_ATTRS, PIXEL_GEOREF),
    )
    .await;
    let (addr, _served) = serve(Vec::new(), None).await;
    let sql = format!(
        "COPY lsat_404 FROM 'http://localhost:{}/missing.tif' WITH (FORMAT geotiff)",
        addr.port()
    );
    let err = copy_sql(&server, &sql).await.expect_err("refused");
    assert_eq!(err.code, "58030", "{err:?}");
    let sql = format!(
        "COPY lsat_404 FROM 'http://10.0.0.1:{}/a.tif' WITH (FORMAT geotiff)",
        addr.port()
    );
    let err = copy_sql(&server, &sql)
        .await
        .expect_err("an IP literal is refused");
    assert_eq!(err.code, "42501", "{err:?}");
}
