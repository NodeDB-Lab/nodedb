// SPDX-License-Identifier: Apache-2.0

//! A user-defined GeoTIFF CRS (GeoKey value 32767), decoded into a
//! [`CrsDef`] from its parameter keys (OGC GeoTIFF 1.1, Requirements
//! Classes GeodeticCRS and ProjectedCRS).
//!
//! The projection keys map to PROJ parameters, then the PROJ-string parser
//! builds the definition. The datum comes from the geodetic CRS code, the
//! datum code, or the ellipsoid keys.

use nodedb_crs::registry::geographic_crs_on_datum;
use nodedb_crs::{CrsContext, CrsDef, Datum, DatumShift, Ellipsoid, parse_proj_string};
use nodedb_types::geometry::Srid;

use super::projection_keys::projection;
use super::units::{AngleUnit, USER_DEFINED, epsg_code, linear_unit, push, unsupported};
use crate::error::RasterError;
use crate::tiff::geokeys::{
    ELLIPSOID, GEODETIC_DATUM, GEOG_ANGULAR_UNIT_SIZE, GEOG_ANGULAR_UNITS, GEOGRAPHIC_TYPE,
    GeoKeyDirectory, INV_FLATTENING, PRIME_MERIDIAN, PRIME_MERIDIAN_LONGITUDE, SEMI_MAJOR_AXIS,
    SEMI_MINOR_AXIS,
};

/// The SRID a decoded definition holds until it matches a built-in CRS:
/// the first code of the user range. The definition is never stored under
/// it.
const UNASSIGNED: Srid = Srid::known(Srid::USER_FIRST);

/// Name of every decoded definition.
const NAME: &str = "GeoTIFF user-defined CRS";

/// EPSG prime meridian code of Greenwich.
const GREENWICH: u16 = 8901;

/// Decode the user-defined CRS of `keys`: projected when `projected`, else
/// geographic. An EPSG geodetic CRS code resolves through `ctx`.
pub fn decode_user_crs(
    keys: &GeoKeyDirectory,
    projected: bool,
    ctx: &CrsContext<'_>,
) -> Result<CrsDef, RasterError> {
    let angle = AngleUnit::of(keys, GEOG_ANGULAR_UNITS, GEOG_ANGULAR_UNIT_SIZE)?;
    let datum = geodetic_datum(keys, angle, ctx)?;
    let mut proj = String::new();
    if projected {
        let unit = linear_unit(keys)?;
        projection(keys, angle, unit, &mut proj)?;
        push(&mut proj, format_args!(" +to_meter={unit}"));
    } else {
        proj.push_str("+proj=longlat");
    }
    write_ellipsoid(&mut proj, &datum.ellipsoid);
    let mut def = parse_proj_string(UNASSIGNED, NAME, &proj)?;
    def.datum = datum;
    Ok(def)
}

fn write_ellipsoid(out: &mut String, e: &Ellipsoid) {
    if e.is_sphere() {
        push(out, format_args!(" +R={}", e.a));
    } else {
        push(out, format_args!(" +a={} +rf={}", e.a, e.inv_f));
    }
}

/// The datum: from an EPSG geodetic CRS code, else an EPSG datum code,
/// else the ellipsoid and prime meridian keys with no known shift.
fn geodetic_datum(
    keys: &GeoKeyDirectory,
    angle: AngleUnit,
    ctx: &CrsContext<'_>,
) -> Result<Datum, RasterError> {
    if let Some(code) = epsg_code(keys.short(GEOGRAPHIC_TYPE)) {
        let srid = ctx.resolve_u32(u32::from(code))?;
        return Ok(ctx.resolve(srid)?.datum);
    }
    if let Some(code) = epsg_code(keys.short(GEODETIC_DATUM)) {
        return datum_by_code(code, ctx);
    }
    Ok(Datum {
        ellipsoid: ellipsoid(keys)?,
        prime_meridian_deg: prime_meridian(keys, angle)?,
        to_wgs84: DatumShift::Identity,
        epsg_code: None,
    })
}

/// The datum of the lowest-coded built-in geographic CRS on EPSG datum
/// `code`.
fn datum_by_code(code: u16, ctx: &CrsContext<'_>) -> Result<Datum, RasterError> {
    let srid = geographic_crs_on_datum(u32::from(code)).ok_or_else(|| {
        unsupported(format!(
            "datum {code} in GeoKey {GEODETIC_DATUM} has no built-in geographic CRS"
        ))
    })?;
    Ok(ctx.resolve(srid)?.datum)
}

/// The ellipsoid: an EPSG code from the table, else the axis keys.
fn ellipsoid(keys: &GeoKeyDirectory) -> Result<Ellipsoid, RasterError> {
    if let Some(code) = epsg_code(keys.short(ELLIPSOID)) {
        return Ellipsoid::by_epsg_code(code).ok_or_else(|| {
            unsupported(format!(
                "ellipsoid {code} in GeoKey {ELLIPSOID} is not supported"
            ))
        });
    }
    let a = keys
        .double(SEMI_MAJOR_AXIS)
        .filter(|a| a.is_finite() && *a > 0.0)
        .ok_or_else(|| {
            unsupported(format!(
                "GeoKey {SEMI_MAJOR_AXIS} is absent or not positive"
            ))
        })?;
    if let Some(rf) = keys.double(INV_FLATTENING) {
        return Ok(if rf == 0.0 {
            Ellipsoid::sphere(a)
        } else {
            Ellipsoid { a, inv_f: rf }
        });
    }
    match keys.double(SEMI_MINOR_AXIS) {
        Some(b) if b == a => Ok(Ellipsoid::sphere(a)),
        Some(b) if b.is_finite() && b > 0.0 && b < a => Ok(Ellipsoid {
            a,
            inv_f: a / (a - b),
        }),
        _ => Err(unsupported(format!(
            "GeoKey {SEMI_MAJOR_AXIS} needs GeoKey {INV_FLATTENING} or a smaller GeoKey {SEMI_MINOR_AXIS}"
        ))),
    }
}

/// The prime meridian, degrees east of Greenwich.
fn prime_meridian(keys: &GeoKeyDirectory, angle: AngleUnit) -> Result<f64, RasterError> {
    match keys.short(PRIME_MERIDIAN) {
        None | Some(GREENWICH) => Ok(0.0),
        Some(USER_DEFINED) => Ok(keys
            .double(PRIME_MERIDIAN_LONGITUDE)
            .map_or(0.0, |v| angle.degrees(v))),
        Some(code) => Err(unsupported(format!(
            "prime meridian {code} in GeoKey {PRIME_MERIDIAN} is not supported; \
             supported: Greenwich (8901), user-defined (32767)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiff::geokeys::{
        PROJ_FALSE_EASTING, PROJ_LINEAR_UNITS, PROJ_METHOD, PROJ_NAT_ORIGIN_LONG, PROJECTION,
    };

    const DOUBLE_PARAMS_TAG: u16 = 34736;

    fn directory(shorts: &[(u16, u16)], doubles: &[(u16, f64)]) -> GeoKeyDirectory {
        let mut all: Vec<(u16, u16, u16)> = shorts.iter().map(|(k, v)| (*k, 0, *v)).collect();
        let mut values = Vec::new();
        for (key, value) in doubles {
            let offset = u16::try_from(values.len()).expect("few doubles");
            values.push(*value);
            all.push((*key, DOUBLE_PARAMS_TAG, offset));
        }
        all.sort_by_key(|(k, _, _)| *k);
        let count = u16::try_from(all.len()).expect("few keys");
        let mut dir = vec![1, 1, 0, count];
        for (key, location, value) in all {
            dir.extend([key, location, 1, value]);
        }
        GeoKeyDirectory::parse(&dir, Some(&values), None).expect("valid directory")
    }

    #[test]
    fn feet_convert_false_easting_to_metres() {
        let dir = directory(
            &[
                (GEOGRAPHIC_TYPE, 4326),
                (PROJECTION, USER_DEFINED),
                (PROJ_METHOD, 1),
                (PROJ_LINEAR_UNITS, 9002),
            ],
            &[(PROJ_FALSE_EASTING, 1000.0), (PROJ_NAT_ORIGIN_LONG, 111.0)],
        );
        let def = decode_user_crs(&dir, true, &CrsContext::builtin_only()).unwrap();
        let text = def.to_proj_string().unwrap();
        assert!(text.contains("+x_0=304.8"), "{text}");
        assert!(text.contains("+units=ft"), "{text}");
    }

    #[test]
    fn ellipsoid_from_axes() {
        let dir = directory(
            &[
                (GEOGRAPHIC_TYPE, USER_DEFINED),
                (GEODETIC_DATUM, USER_DEFINED),
            ],
            &[
                (SEMI_MAJOR_AXIS, 6_378_137.0),
                (INV_FLATTENING, 298.257_222_101),
            ],
        );
        let def = decode_user_crs(&dir, false, &CrsContext::builtin_only()).unwrap();
        assert!(def.is_geographic());
        assert_eq!(def.datum.ellipsoid, Ellipsoid::GRS80);
    }

    #[test]
    fn unknown_method_is_refused() {
        let dir = directory(
            &[
                (GEOGRAPHIC_TYPE, 4326),
                (PROJECTION, USER_DEFINED),
                (PROJ_METHOD, 19),
            ],
            &[],
        );
        assert!(matches!(
            decode_user_crs(&dir, true, &CrsContext::builtin_only()),
            Err(RasterError::UnsupportedUserCrs { .. })
        ));
    }
}
