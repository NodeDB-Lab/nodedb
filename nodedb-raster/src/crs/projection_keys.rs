// SPDX-License-Identifier: Apache-2.0

//! The projection of a user-defined GeoTIFF CRS, as PROJ parameters. Keys
//! map the way GDAL maps them, with the same fallbacks between natural
//! origin, false origin and centre keys.

use super::units::{AngleUnit, epsg_code, push, unsupported};
use crate::error::RasterError;
use crate::tiff::geokeys::{
    GEOG_ANGULAR_UNIT_SIZE, GEOG_AZIMUTH_UNITS, GeoKeyDirectory, PROJ_AZIMUTH_ANGLE,
    PROJ_CENTER_EASTING, PROJ_CENTER_LAT, PROJ_CENTER_LONG, PROJ_CENTER_NORTHING,
    PROJ_FALSE_EASTING, PROJ_FALSE_NORTHING, PROJ_FALSE_ORIGIN_EASTING, PROJ_FALSE_ORIGIN_LAT,
    PROJ_FALSE_ORIGIN_LONG, PROJ_FALSE_ORIGIN_NORTHING, PROJ_METHOD, PROJ_NAT_ORIGIN_LAT,
    PROJ_NAT_ORIGIN_LONG, PROJ_RECTIFIED_GRID_ANGLE, PROJ_SCALE_AT_CENTER,
    PROJ_SCALE_AT_NAT_ORIGIN, PROJ_STD_PARALLEL_1, PROJ_STD_PARALLEL_2,
    PROJ_STRAIGHT_VERT_POLE_LONG, PROJECTION,
};

/// EPSG conversion codes of UTM zones: north `16000 + zone`, south
/// `16100 + zone`.
const UTM_NORTH_BASE: u16 = 16_000;
const UTM_SOUTH_BASE: u16 = 16_100;
const UTM_ZONES: u16 = 60;

/// GeoTIFF projection method codes (`ProjMethodGeoKey`).
mod method {
    pub const TRANSVERSE_MERCATOR: u16 = 1;
    pub const OBLIQUE_MERCATOR: u16 = 3;
    pub const MERCATOR: u16 = 7;
    pub const LCC_2SP: u16 = 8;
    pub const LCC_1SP: u16 = 9;
    pub const LAEA: u16 = 10;
    pub const ALBERS: u16 = 11;
    pub const AZIMUTHAL_EQUIDISTANT: u16 = 12;
    pub const STEREOGRAPHIC: u16 = 14;
    pub const POLAR_STEREOGRAPHIC: u16 = 15;
    pub const OBLIQUE_STEREOGRAPHIC: u16 = 16;
    pub const EQUIRECTANGULAR: u16 = 17;
    pub const CASSINI_SOLDNER: u16 = 18;
    pub const POLYCONIC: u16 = 22;
    pub const SINUSOIDAL: u16 = 24;
    pub const NEW_ZEALAND_MAP_GRID: u16 = 26;
    pub const TM_SOUTH_ORIENTATED: u16 = 27;
    pub const CYLINDRICAL_EQUAL_AREA: u16 = 28;
}

/// Reads projection parameters in their units.
struct Params<'a> {
    keys: &'a GeoKeyDirectory,
    angle: AngleUnit,
    azimuth: AngleUnit,
    metres: f64,
}

impl Params<'_> {
    /// The first of `keys` present, as degrees.
    fn angle(&self, keys: &[u16]) -> Option<f64> {
        self.first(keys).map(|v| self.angle.degrees(v))
    }

    /// The first of `keys` present, as metres.
    fn length(&self, keys: &[u16]) -> f64 {
        self.first(keys).map_or(0.0, |v| v * self.metres)
    }

    fn first(&self, keys: &[u16]) -> Option<f64> {
        keys.iter().find_map(|k| self.keys.double(*k))
    }

    fn scale(&self) -> f64 {
        self.first(&[PROJ_SCALE_AT_NAT_ORIGIN, PROJ_SCALE_AT_CENTER])
            .unwrap_or(1.0)
    }

    fn azimuth(&self, key: u16) -> Option<f64> {
        self.keys.double(key).map(|v| self.azimuth.degrees(v))
    }
}

const NAT_LAT: [u16; 3] = [PROJ_NAT_ORIGIN_LAT, PROJ_FALSE_ORIGIN_LAT, PROJ_CENTER_LAT];
const NAT_LONG: [u16; 3] = [
    PROJ_NAT_ORIGIN_LONG,
    PROJ_FALSE_ORIGIN_LONG,
    PROJ_CENTER_LONG,
];
const CENTER_LAT: [u16; 3] = [PROJ_CENTER_LAT, PROJ_NAT_ORIGIN_LAT, PROJ_FALSE_ORIGIN_LAT];
const CENTER_LONG: [u16; 3] = [
    PROJ_CENTER_LONG,
    PROJ_NAT_ORIGIN_LONG,
    PROJ_FALSE_ORIGIN_LONG,
];
const ORIGIN_LAT: [u16; 3] = [PROJ_FALSE_ORIGIN_LAT, PROJ_NAT_ORIGIN_LAT, PROJ_CENTER_LAT];
const ORIGIN_LONG: [u16; 3] = [
    PROJ_FALSE_ORIGIN_LONG,
    PROJ_NAT_ORIGIN_LONG,
    PROJ_CENTER_LONG,
];
const EASTING: [u16; 3] = [
    PROJ_FALSE_EASTING,
    PROJ_FALSE_ORIGIN_EASTING,
    PROJ_CENTER_EASTING,
];
const NORTHING: [u16; 3] = [
    PROJ_FALSE_NORTHING,
    PROJ_FALSE_ORIGIN_NORTHING,
    PROJ_CENTER_NORTHING,
];

/// Write the PROJ projection parameters of `keys` into `out`. False
/// eastings and northings convert from the projected unit to metres.
pub(super) fn projection(
    keys: &GeoKeyDirectory,
    angle: AngleUnit,
    metres: f64,
    out: &mut String,
) -> Result<(), RasterError> {
    if let Some(code) = epsg_code(keys.short(PROJECTION)) {
        return utm_conversion(code, out);
    }
    let code = keys.short(PROJ_METHOD).ok_or_else(|| {
        unsupported(format!(
            "GeoKey {PROJECTION} is user-defined but GeoKey {PROJ_METHOD} is absent"
        ))
    })?;
    let p = Params {
        keys,
        angle,
        azimuth: AngleUnit::of(keys, GEOG_AZIMUTH_UNITS, GEOG_ANGULAR_UNIT_SIZE)?,
        metres,
    };
    let deg = |v: Option<f64>| v.unwrap_or(0.0);
    let x0 = p.length(&EASTING);
    let y0 = p.length(&NORTHING);
    let k = p.scale();
    match code {
        method::TRANSVERSE_MERCATOR | method::TM_SOUTH_ORIENTATED => {
            push(
                out,
                format_args!(
                    "+proj=tmerc +lat_0={} +lon_0={} +k={k}",
                    deg(p.angle(&NAT_LAT)),
                    deg(p.angle(&NAT_LONG))
                ),
            );
            if code == method::TM_SOUTH_ORIENTATED {
                out.push_str(" +axis=wsu");
            }
        }
        method::OBLIQUE_MERCATOR => {
            let alpha = deg(p.azimuth(PROJ_AZIMUTH_ANGLE));
            let gamma = p.azimuth(PROJ_RECTIFIED_GRID_ANGLE).unwrap_or(alpha);
            push(
                out,
                format_args!(
                    "+proj=omerc +no_uoff +lat_0={} +lonc={} +alpha={alpha} +gamma={gamma} +k={k}",
                    deg(p.angle(&CENTER_LAT)),
                    deg(p.angle(&CENTER_LONG))
                ),
            );
        }
        method::MERCATOR => {
            push(
                out,
                format_args!("+proj=merc +lon_0={}", deg(p.angle(&NAT_LONG))),
            );
            match p.angle(&[PROJ_STD_PARALLEL_1]).filter(|v| *v != 0.0) {
                Some(lat_ts) => push(out, format_args!(" +lat_ts={lat_ts}")),
                None => push(out, format_args!(" +k={k}")),
            }
        }
        method::LCC_2SP => push(
            out,
            format_args!(
                "+proj=lcc +lat_0={} +lon_0={} +lat_1={} +lat_2={}",
                deg(p.angle(&ORIGIN_LAT)),
                deg(p.angle(&ORIGIN_LONG)),
                deg(p.angle(&[PROJ_STD_PARALLEL_1])),
                deg(p.angle(&[PROJ_STD_PARALLEL_2]))
            ),
        ),
        method::LCC_1SP => {
            let lat = deg(p.angle(&NAT_LAT));
            push(
                out,
                format_args!(
                    "+proj=lcc +lat_1={lat} +lat_0={lat} +lon_0={} +k_0={k}",
                    deg(p.angle(&NAT_LONG))
                ),
            );
        }
        method::LAEA | method::AZIMUTHAL_EQUIDISTANT => {
            let name = if code == method::LAEA { "laea" } else { "aeqd" };
            push(
                out,
                format_args!(
                    "+proj={name} +lat_0={} +lon_0={}",
                    deg(p.angle(&CENTER_LAT)),
                    deg(p.angle(&CENTER_LONG))
                ),
            );
        }
        method::ALBERS => push(
            out,
            format_args!(
                "+proj=aea +lat_1={} +lat_2={} +lat_0={} +lon_0={}",
                deg(p.angle(&[PROJ_STD_PARALLEL_1])),
                deg(p.angle(&[PROJ_STD_PARALLEL_2])),
                deg(p.angle(&ORIGIN_LAT)),
                deg(p.angle(&ORIGIN_LONG))
            ),
        ),
        method::STEREOGRAPHIC | method::OBLIQUE_STEREOGRAPHIC => {
            let name = if code == method::STEREOGRAPHIC {
                "stere"
            } else {
                "sterea"
            };
            push(
                out,
                format_args!(
                    "+proj={name} +lat_0={} +lon_0={} +k={k}",
                    deg(p.angle(&CENTER_LAT)),
                    deg(p.angle(&CENTER_LONG))
                ),
            );
        }
        method::POLAR_STEREOGRAPHIC => {
            let lat_ts = deg(p.angle(&NAT_LAT));
            let pole = if lat_ts < 0.0 { -90.0 } else { 90.0 };
            let lon = p
                .angle(&[PROJ_STRAIGHT_VERT_POLE_LONG])
                .or_else(|| p.angle(&NAT_LONG));
            push(
                out,
                format_args!(
                    "+proj=stere +lat_0={pole} +lat_ts={lat_ts} +lon_0={} +k={k}",
                    deg(lon)
                ),
            );
        }
        method::EQUIRECTANGULAR => {
            let lat_ts = p
                .angle(&[PROJ_STD_PARALLEL_1])
                .or_else(|| p.angle(&[PROJ_CENTER_LAT]));
            push(
                out,
                format_args!(
                    "+proj=eqc +lat_ts={} +lon_0={}",
                    deg(lat_ts),
                    deg(p.angle(&CENTER_LONG))
                ),
            );
        }
        method::CASSINI_SOLDNER | method::POLYCONIC | method::NEW_ZEALAND_MAP_GRID => {
            let name = match code {
                method::CASSINI_SOLDNER => "cass",
                method::POLYCONIC => "poly",
                _ => "nzmg",
            };
            push(
                out,
                format_args!(
                    "+proj={name} +lat_0={} +lon_0={}",
                    deg(p.angle(&NAT_LAT)),
                    deg(p.angle(&NAT_LONG))
                ),
            );
        }
        method::SINUSOIDAL => push(
            out,
            format_args!("+proj=sinu +lon_0={}", deg(p.angle(&CENTER_LONG))),
        ),
        method::CYLINDRICAL_EQUAL_AREA => push(
            out,
            format_args!(
                "+proj=cea +lat_ts={} +lon_0={}",
                deg(p.angle(&[PROJ_STD_PARALLEL_1])),
                deg(p.angle(&NAT_LONG))
            ),
        ),
        other => {
            return Err(unsupported(format!(
                "projection method {other} in GeoKey {PROJ_METHOD} is not supported"
            )));
        }
    }
    push(out, format_args!(" +x_0={x0} +y_0={y0}"));
    Ok(())
}

/// The UTM zone of EPSG conversion `code`, as PROJ parameters.
fn utm_conversion(code: u16, out: &mut String) -> Result<(), RasterError> {
    let (zone, south) = match code {
        c if (UTM_NORTH_BASE + 1..=UTM_NORTH_BASE + UTM_ZONES).contains(&c) => {
            (c - UTM_NORTH_BASE, false)
        }
        c if (UTM_SOUTH_BASE + 1..=UTM_SOUTH_BASE + UTM_ZONES).contains(&c) => {
            (c - UTM_SOUTH_BASE, true)
        }
        other => {
            return Err(unsupported(format!(
                "conversion {other} in GeoKey {PROJECTION} is not supported; \
                 supported: UTM zones (16001..16060, 16101..16160) and user-defined (32767)"
            )));
        }
    };
    push(out, format_args!("+proj=utm +zone={zone}"));
    if south {
        out.push_str(" +south");
    }
    Ok(())
}
