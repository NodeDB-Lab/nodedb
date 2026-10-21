// SPDX-License-Identifier: Apache-2.0

//! GDM2020 (EPSG:12287) and its state Cassini grids (EPSG:25314 to 25322)
//! against the JUPEM worked example.
//!
//! Source: JUPEM, "Technical Guide to the Coordinate Conversion, Datum
//! Transformation and Map Projection", circular KPUP 3/2021 (Lampiran A),
//! §4.6 test example (i), "GDM2020 to Geocentric Cassini (GDM2020)",
//! Kedah/Perlis.

use nodedb_crs::{CrsContext, CrsKind, DatumShift, Srid};

/// Degrees from degrees, minutes and seconds.
fn dms(d: f64, m: f64, s: f64) -> f64 {
    d + m / 60.0 + s / 3600.0
}

/// Latitude of the worked-example point, N 6° 08′ 22.98682″.
fn example_lat() -> f64 {
    dms(6.0, 8.0, 22.986_82)
}

/// Longitude of the worked-example point, E 100° 23′ 6.56827″.
fn example_lon() -> f64 {
    dms(100.0, 23.0, 6.568_27)
}

/// Northing the guide prints, metres.
const JUPEM_NORTHING_M: f64 = 19_364.381;

/// Easting from PROJ 9.8.1 `+proj=cass` with the EPSG 12320 origin on
/// GRS 1980, metres. The guide prints −27805.019 m, exactly 1.000 m off
/// this value: its GDM2000 (Rev 2009) and GDM2000 examples on the same page
/// agree with PROJ to the millimetre.
const PROJ_EASTING_M: f64 = -27_806.019_141_437_544;

/// The guide prints millimetres.
const HALF_MM: f64 = 5e-4;

#[test]
fn kedah_perlis_grid_matches_the_jupem_example() {
    let ctx = CrsContext::builtin_only();
    let gdm2020 = Srid::new(12287).unwrap();
    let kedah = Srid::new(25320).unwrap();
    let t = ctx.transformer(gdm2020, kedah).unwrap();
    let [e, n, _] = t.apply([example_lon(), example_lat(), 0.0]).unwrap();
    assert!((n - JUPEM_NORTHING_M).abs() < HALF_MM, "northing {n}");
    assert!((e - PROJ_EASTING_M).abs() < 1e-3, "easting {e}");
    let back = ctx.transformer(kedah, gdm2020).unwrap();
    let [lon, lat, _] = back.apply([e, n, 0.0]).unwrap();
    assert!((lon - example_lon()).abs() < 1e-9 && (lat - example_lat()).abs() < 1e-9);
}

/// Each grid origin maps to (0, 0): JUPEM KPUP 3/2021 Table 7 gives the
/// GDM2020 origins with Cassini northing and easting 0.000.
#[test]
fn every_state_origin_maps_to_zero() {
    let ctx = CrsContext::builtin_only();
    let gdm2020 = Srid::new(12287).unwrap();
    // (EPSG code, Table 7 latitude, longitude as degrees, minutes, seconds)
    let origins = [
        (25314, (2.0, 2.0, 33.195_81), (103.0, 33.0, 39.851_96)),
        (25315, (2.0, 42.0, 43.629_44), (101.0, 56.0, 22.944_46)),
        (25316, (3.0, 42.0, 38.687_85), (102.0, 26.0, 4.622_19)),
        (25317, (3.0, 40.0, 48.373_10), (101.0, 30.0, 24.499_70)),
        (25318, (4.0, 56.0, 44.967_15), (102.0, 53.0, 37.018_99)),
        (25319, (5.0, 25.0, 15.199_41), (100.0, 20.0, 40.772_28)),
        (25320, (5.0, 57.0, 52.817_46), (100.0, 38.0, 10.949_96)),
        (25321, (4.0, 51.0, 32.640_21), (100.0, 48.0, 55.483_63)),
        (25322, (5.0, 53.0, 37.075_11), (102.0, 10.0, 32.258_23)),
    ];
    for (code, lat, lon) in origins {
        let grid = Srid::new(code).unwrap();
        let t = ctx.transformer(gdm2020, grid).unwrap();
        let [e, n, _] = t
            .apply([dms(lon.0, lon.1, lon.2), dms(lat.0, lat.1, lat.2), 0.0])
            .unwrap();
        assert!(e.abs() < HALF_MM && n.abs() < HALF_MM, "{code}: ({e}, {n})");
    }
}

/// EPSG transformation 12291 GDM2020 to WGS 84 (1) is a null geocentric
/// translation with an accuracy of 1 m.
#[test]
fn gdm2020_to_wgs84_is_the_one_metre_null_shift() {
    let ctx = CrsContext::builtin_only();
    let def = ctx.resolve(Srid::new(12287).unwrap()).unwrap();
    assert_eq!(def.kind, CrsKind::Geographic);
    assert_eq!(def.datum.epsg_code, Some(1471));
    let DatumShift::Helmert(h) = def.datum.to_wgs84 else {
        panic!("expected a Helmert shift");
    };
    assert!(h.is_zero());
    assert_eq!(h.accuracy_m, Some(1.0));
}
