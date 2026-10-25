// SPDX-License-Identifier: Apache-2.0

//! The CRS the GeoKey directory names, and its SRID.
//!
//! A projected model (1) reads `ProjectedCRSGeoKey` (3072), a geographic
//! model (2) reads `GeodeticCRSGeoKey` (2048). With no model key, 3072 wins
//! over 2048. An EPSG code resolves through the [`CrsContext`]. A
//! user-defined CRS (32767) decodes from its parameter keys and maps to the
//! registered CRS with the same projection, ellipsoid, unit and datum: a
//! live user-defined CRS of the context, or a built-in CRS.

use nodedb_crs::CrsContext;
use nodedb_types::geometry::Srid;

use super::registry_match::match_registered;
use super::units::{UNDEFINED, USER_DEFINED};
use super::user_defined::decode_user_crs;
use crate::error::RasterError;
use crate::header::{CrsRef, UserDefinedCrs};
use crate::tiff::geokeys::{GEOGRAPHIC_TYPE, GT_MODEL_TYPE, GeoKeyDirectory, PROJECTED_CS_TYPE};

const MODEL_PROJECTED: u16 = 1;
const MODEL_GEOGRAPHIC: u16 = 2;

/// The CRS the GeoKeys name. A model type or CRS key the directory cannot
/// name a CRS with is an error here; codes resolve later.
pub fn crs_ref_from_geokeys(keys: Option<&GeoKeyDirectory>) -> Result<CrsRef, RasterError> {
    let Some(keys) = keys else {
        return Ok(CrsRef::None);
    };
    let key = match keys.short(GT_MODEL_TYPE) {
        Some(MODEL_PROJECTED) => PROJECTED_CS_TYPE,
        Some(MODEL_GEOGRAPHIC) => GEOGRAPHIC_TYPE,
        Some(model) => return Err(RasterError::UnsupportedModelType { model }),
        None if keys.short(PROJECTED_CS_TYPE).is_some() => PROJECTED_CS_TYPE,
        None if keys.short(GEOGRAPHIC_TYPE).is_some() => GEOGRAPHIC_TYPE,
        None => return Ok(CrsRef::None),
    };
    let model = keys.short(GT_MODEL_TYPE).unwrap_or(UNDEFINED);
    match keys.short(key) {
        None | Some(UNDEFINED) => Err(RasterError::MissingCrsKey { model, key }),
        Some(USER_DEFINED) => Ok(CrsRef::UserDefined(Box::new(UserDefinedCrs {
            keys: keys.clone(),
            projected: key == PROJECTED_CS_TYPE,
        }))),
        Some(code) => Ok(CrsRef::Epsg(u32::from(code))),
    }
}

/// The SRID `crs` resolves to in `ctx`, or `None` when it names no CRS.
pub fn resolve_crs(crs: &CrsRef, ctx: &CrsContext<'_>) -> Result<Option<Srid>, RasterError> {
    match crs {
        CrsRef::None => Ok(None),
        CrsRef::Epsg(code) => Ok(Some(ctx.resolve_u32(*code)?)),
        CrsRef::UserDefined(user) => {
            let def = decode_user_crs(&user.keys, user.projected, ctx)?;
            match_registered(&def, ctx).map(Some)
        }
    }
}

/// The SRID the GeoKeys name, or `None` when they name no CRS.
pub fn srid_from_geokeys(
    keys: Option<&GeoKeyDirectory>,
    ctx: &CrsContext<'_>,
) -> Result<Option<Srid>, RasterError> {
    resolve_crs(&crs_ref_from_geokeys(keys)?, ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiff::geokeys::{
        ELLIPSOID, GEODETIC_DATUM, INV_FLATTENING, PROJ_CENTER_LONG, PROJ_FALSE_EASTING,
        PROJ_FALSE_NORTHING, PROJ_LINEAR_UNITS, PROJ_METHOD, PROJ_NAT_ORIGIN_LAT,
        PROJ_NAT_ORIGIN_LONG, PROJ_SCALE_AT_NAT_ORIGIN, PROJECTION, SEMI_MAJOR_AXIS,
        SEMI_MINOR_AXIS,
    };
    use nodedb_crs::UnknownSrid;

    const DOUBLE_PARAMS_TAG: u16 = 34736;

    fn srid(code: u32) -> Srid {
        Srid::new(code).unwrap()
    }

    fn ctx() -> CrsContext<'static> {
        CrsContext::builtin_only()
    }

    fn keys(entries: &[(u16, u16)]) -> GeoKeyDirectory {
        with_doubles(entries, &[])
    }

    /// A directory of inline short keys and double keys, sorted by key.
    fn with_doubles(shorts: &[(u16, u16)], doubles: &[(u16, f64)]) -> GeoKeyDirectory {
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
    fn no_directory_is_no_crs() {
        assert_eq!(srid_from_geokeys(None, &ctx()).ok(), Some(None));
    }

    #[test]
    fn projected_model_reads_3072() {
        let dir = keys(&[(GT_MODEL_TYPE, 1), (PROJECTED_CS_TYPE, 32650)]);
        assert_eq!(
            srid_from_geokeys(Some(&dir), &ctx()).ok(),
            Some(Some(srid(32650)))
        );
    }

    #[test]
    fn geographic_model_reads_2048() {
        let dir = keys(&[(GT_MODEL_TYPE, 2), (GEOGRAPHIC_TYPE, 4742)]);
        assert_eq!(
            srid_from_geokeys(Some(&dir), &ctx()).ok(),
            Some(Some(srid(4742)))
        );
    }

    #[test]
    fn model_key_picks_the_crs_key() {
        let dir = keys(&[
            (GT_MODEL_TYPE, 2),
            (GEOGRAPHIC_TYPE, 4326),
            (PROJECTED_CS_TYPE, 32648),
        ]);
        assert_eq!(
            srid_from_geokeys(Some(&dir), &ctx()).ok(),
            Some(Some(Srid::WGS84))
        );
    }

    #[test]
    fn without_a_model_key_3072_wins() {
        let dir = keys(&[(GEOGRAPHIC_TYPE, 4326), (PROJECTED_CS_TYPE, 3375)]);
        assert_eq!(
            srid_from_geokeys(Some(&dir), &ctx()).ok(),
            Some(Some(srid(3375)))
        );
        let none = keys(&[(1025, 1)]);
        assert_eq!(srid_from_geokeys(Some(&none), &ctx()).ok(), Some(None));
    }

    #[test]
    fn unmappable_crs_are_typed_errors() {
        let geocentric = keys(&[(GT_MODEL_TYPE, 3)]);
        assert!(matches!(
            srid_from_geokeys(Some(&geocentric), &ctx()),
            Err(RasterError::UnsupportedModelType { model: 3 })
        ));
        let missing = keys(&[(GT_MODEL_TYPE, 1)]);
        assert!(matches!(
            srid_from_geokeys(Some(&missing), &ctx()),
            Err(RasterError::MissingCrsKey {
                model: 1,
                key: 3072
            })
        ));
        let undefined = keys(&[(GT_MODEL_TYPE, 1), (PROJECTED_CS_TYPE, 999)]);
        assert!(matches!(
            srid_from_geokeys(Some(&undefined), &ctx()),
            Err(RasterError::UnknownCrs(UnknownSrid::Undefined {
                code: 999
            }))
        ));
    }

    /// The MODIS sinusoidal grid as GDAL writes it: user-defined projection,
    /// datum and ellipsoid on the 6371007.181 m sphere.
    #[test]
    fn modis_sinusoidal_maps_to_its_nodedb_code() {
        let dir = with_doubles(
            &[
                (GT_MODEL_TYPE, 1),
                (GEOGRAPHIC_TYPE, USER_DEFINED),
                (GEODETIC_DATUM, USER_DEFINED),
                (ELLIPSOID, USER_DEFINED),
                (PROJECTED_CS_TYPE, USER_DEFINED),
                (PROJECTION, USER_DEFINED),
                (PROJ_METHOD, 24),
                (PROJ_LINEAR_UNITS, 9001),
            ],
            &[
                (SEMI_MAJOR_AXIS, 6_371_007.181),
                (SEMI_MINOR_AXIS, 6_371_007.181),
                (PROJ_FALSE_EASTING, 0.0),
                (PROJ_FALSE_NORTHING, 0.0),
                (PROJ_CENTER_LONG, 0.0),
            ],
        );
        assert_eq!(
            srid_from_geokeys(Some(&dir), &ctx()).ok(),
            Some(Some(Srid::MODIS_SINUSOIDAL))
        );
    }

    /// UTM zone 49N on WGS 84, spelled out as Transverse Mercator
    /// parameters over the EPSG:4326 geographic CRS.
    #[test]
    fn user_defined_utm_49n_maps_to_32649() {
        let dir = with_doubles(
            &[
                (GT_MODEL_TYPE, 1),
                (GEOGRAPHIC_TYPE, 4326),
                (PROJECTED_CS_TYPE, USER_DEFINED),
                (PROJECTION, USER_DEFINED),
                (PROJ_METHOD, 1),
                (PROJ_LINEAR_UNITS, 9001),
            ],
            &[
                (PROJ_NAT_ORIGIN_LONG, 111.0),
                (PROJ_NAT_ORIGIN_LAT, 0.0),
                (PROJ_FALSE_EASTING, 500_000.0),
                (PROJ_FALSE_NORTHING, 0.0),
                (PROJ_SCALE_AT_NAT_ORIGIN, 0.9996),
            ],
        );
        assert_eq!(
            srid_from_geokeys(Some(&dir), &ctx()).ok(),
            Some(Some(srid(32649)))
        );
    }

    /// UTM zone 49N parameters as GeoKeys, over `datum_keys`.
    fn utm_49n_over(datum_keys: &[(u16, u16)]) -> GeoKeyDirectory {
        let mut shorts = vec![
            (GT_MODEL_TYPE, 1),
            (GEOGRAPHIC_TYPE, USER_DEFINED),
            (PROJECTED_CS_TYPE, USER_DEFINED),
            (PROJECTION, USER_DEFINED),
            (PROJ_METHOD, 1),
            (PROJ_LINEAR_UNITS, 9001),
        ];
        shorts.extend_from_slice(datum_keys);
        with_doubles(
            &shorts,
            &[
                (PROJ_NAT_ORIGIN_LONG, 111.0),
                (PROJ_NAT_ORIGIN_LAT, 0.0),
                (PROJ_FALSE_EASTING, 500_000.0),
                (PROJ_FALSE_NORTHING, 0.0),
                (PROJ_SCALE_AT_NAT_ORIGIN, 0.9996),
            ],
        )
    }

    /// The WGS 84 datum code picks WGS 84 / UTM zone 49N, never VN-2000 /
    /// UTM zone 49N (3406) on the same ellipsoid.
    #[test]
    fn utm_49n_on_the_wgs84_datum_maps_to_32649() {
        let dir = utm_49n_over(&[(GEODETIC_DATUM, 6326)]);
        assert_eq!(
            srid_from_geokeys(Some(&dir), &ctx()).ok(),
            Some(Some(srid(32649)))
        );
    }

    /// Only the WGS 84 ellipsoid (7030) is known. SRGI2013 (9479), DGN95
    /// (23869) and WGS 84 (32649) all put UTM zone 49N on that ellipsoid
    /// with a null shift, so the file names no single datum and the match
    /// is refused. VN-2000 (3406) carries a shift and is never accepted.
    #[test]
    fn utm_49n_on_the_wgs84_ellipsoid_alone_is_ambiguous() {
        let dir = utm_49n_over(&[(GEODETIC_DATUM, USER_DEFINED), (ELLIPSOID, 7030)]);
        let Err(RasterError::AmbiguousUserCrs {
            proj,
            first,
            second,
        }) = srid_from_geokeys(Some(&dir), &ctx())
        else {
            panic!("expected an ambiguous user CRS");
        };
        assert!(proj.contains("+proj=tmerc"), "{proj}");
        for code in [first.code(), second.code()] {
            assert_ne!(code, 3406);
            assert!([9479, 23869, 32649].contains(&code), "{code}");
        }
    }

    /// A geographic CRS on the WGS 84 ellipsoid alone is refused, never
    /// mapped to Moznet (4130): Moznet, Hartebeesthoek94 (4148), WGS 84
    /// (4326) and others share that ellipsoid with a null shift.
    #[test]
    fn geographic_wgs84_ellipsoid_alone_is_ambiguous() {
        let dir = keys(&[
            (GT_MODEL_TYPE, 2),
            (GEOGRAPHIC_TYPE, USER_DEFINED),
            (GEODETIC_DATUM, USER_DEFINED),
            (ELLIPSOID, 7030),
        ]);
        match srid_from_geokeys(Some(&dir), &ctx()) {
            Err(RasterError::AmbiguousUserCrs { first, second, .. }) => {
                assert_eq!((first.code(), second.code()), (4130, 4148));
            }
            other => panic!("expected an ambiguous user CRS, got {other:?}"),
        }
        let on_datum = keys(&[
            (GT_MODEL_TYPE, 2),
            (GEOGRAPHIC_TYPE, USER_DEFINED),
            (GEODETIC_DATUM, 6326),
        ]);
        assert_eq!(
            srid_from_geokeys(Some(&on_datum), &ctx()).ok(),
            Some(Some(Srid::WGS84))
        );
    }

    /// A user-defined GeoKey CRS that equals a live user-defined CRS of the
    /// context maps to that code.
    #[test]
    fn user_defined_crs_maps_to_a_custom_definition() {
        let custom = srid(910_777);
        let mut set = nodedb_crs::CustomCrsSet::new();
        set.define(
            nodedb_crs::parse_proj_string(
                custom,
                "custom sinusoidal",
                "+proj=sinu +lon_0=17 +R=6371000 +units=m",
            )
            .unwrap(),
        )
        .unwrap();
        let dir = with_doubles(
            &[
                (GT_MODEL_TYPE, 1),
                (GEOGRAPHIC_TYPE, USER_DEFINED),
                (GEODETIC_DATUM, USER_DEFINED),
                (ELLIPSOID, USER_DEFINED),
                (PROJECTED_CS_TYPE, USER_DEFINED),
                (PROJECTION, USER_DEFINED),
                (PROJ_METHOD, 24),
            ],
            &[
                (SEMI_MAJOR_AXIS, 6_371_000.0),
                (INV_FLATTENING, 0.0),
                (PROJ_CENTER_LONG, 17.0),
            ],
        );
        assert_eq!(
            srid_from_geokeys(Some(&dir), &CrsContext::new(&set)).ok(),
            Some(Some(custom))
        );
    }

    /// The same zone through the EPSG conversion code 16049.
    #[test]
    fn utm_conversion_code_maps_to_32649() {
        let dir = keys(&[
            (GT_MODEL_TYPE, 1),
            (GEOGRAPHIC_TYPE, 4326),
            (PROJECTED_CS_TYPE, USER_DEFINED),
            (PROJECTION, 16049),
        ]);
        assert_eq!(
            srid_from_geokeys(Some(&dir), &ctx()).ok(),
            Some(Some(srid(32649)))
        );
    }

    /// A user-defined CRS with no built-in twin names its PROJ text.
    #[test]
    fn unmatched_user_crs_names_its_proj_text() {
        let dir = with_doubles(
            &[
                (GT_MODEL_TYPE, 1),
                (GEOGRAPHIC_TYPE, USER_DEFINED),
                (GEODETIC_DATUM, USER_DEFINED),
                (ELLIPSOID, USER_DEFINED),
                (PROJECTED_CS_TYPE, USER_DEFINED),
                (PROJECTION, USER_DEFINED),
                (PROJ_METHOD, 24),
            ],
            &[
                (SEMI_MAJOR_AXIS, 6_371_000.0),
                (INV_FLATTENING, 0.0),
                (PROJ_CENTER_LONG, 17.0),
            ],
        );
        let Err(RasterError::UnmatchedUserCrs { proj }) = srid_from_geokeys(Some(&dir), &ctx())
        else {
            panic!("expected an unmatched user CRS");
        };
        assert!(proj.contains("+proj=sinu"), "{proj}");
        assert!(proj.contains("+lon_0=17"), "{proj}");
        assert!(proj.contains("+R=6371000"), "{proj}");
    }
}
