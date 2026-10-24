// SPDX-License-Identifier: Apache-2.0

//! Coordinate transformation of whole geometries between SRIDs.

use nodedb_crs::{CoordinateEpoch, CrsContext, CrsError};
use nodedb_types::geometry::{Position, Srid, SridGeometry};

/// Transform every coordinate of `geom` into `target`, resolving both SRIDs
/// through `ctx`.
///
/// A geometry already in `target` comes back unchanged once `ctx` confirms
/// the SRID is defined. Z is ellipsoidal height in metres and goes through
/// the datum shift with x and y. M passes through. Any coordinate the
/// target projection cannot map fails the whole call with
/// [`CrsError::OutOfDomain`].
///
/// A time-dependent datum shift runs at its parameter reference epoch. Use
/// [`transform_geometry_at`] for coordinates at another epoch.
pub fn transform_geometry(
    geom: &SridGeometry,
    target: Srid,
    ctx: &CrsContext<'_>,
) -> Result<SridGeometry, CrsError> {
    transform(geom, target, None, ctx)
}

/// [`transform_geometry`] for coordinates that refer to coordinate epoch
/// `epoch`: every time-dependent datum shift takes its parameters at that
/// epoch. Static shifts give the same result as [`transform_geometry`].
pub fn transform_geometry_at(
    geom: &SridGeometry,
    target: Srid,
    epoch: CoordinateEpoch,
    ctx: &CrsContext<'_>,
) -> Result<SridGeometry, CrsError> {
    transform(geom, target, Some(epoch), ctx)
}

fn transform(
    geom: &SridGeometry,
    target: Srid,
    epoch: Option<CoordinateEpoch>,
    ctx: &CrsContext<'_>,
) -> Result<SridGeometry, CrsError> {
    let transformer = ctx.transformer(geom.srid, target)?;
    if geom.srid == target {
        return Ok(geom.clone());
    }
    let mapped = geom.try_map_positions(|p| {
        let xyz = [p.x, p.y, p.z.unwrap_or(0.0)];
        let [x, y, z] = match epoch {
            None => transformer.apply(xyz)?,
            Some(epoch) => transformer.apply_at(xyz, epoch)?,
        };
        Ok::<_, CrsError>(Position {
            x,
            y,
            z: p.z.map(|_| z),
            m: p.m,
        })
    })?;
    Ok(mapped.with_srid(target))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_crs::UnknownSrid;
    use nodedb_types::geometry::Geometry;

    /// Largest round-trip error accepted, metres.
    const ROUND_TRIP_TOLERANCE_M: f64 = 1e-3;

    /// Metres per degree of latitude, rounded up. Converts a metre tolerance
    /// to a conservative degree tolerance.
    const METRES_PER_DEGREE: f64 = 111_320.0;

    fn srid(code: u32) -> Srid {
        Srid::new(code).unwrap()
    }

    fn point(srid: Srid, x: f64, y: f64) -> SridGeometry {
        SridGeometry::new(srid, Geometry::point(x, y))
    }

    fn coords(g: &SridGeometry) -> [f64; 2] {
        match g.geometry() {
            Geometry::Point { coordinates } => *coordinates,
            _ => panic!("expected a point"),
        }
    }

    #[test]
    fn wgs84_round_trip_through_projected_srids() {
        let ctx = CrsContext::builtin_only();
        let inputs = [[101.7117, 3.1579], [103.5, 1.5]];
        for code in [32647, 32648, 3375, 3857, 6933] {
            for input in inputs {
                let src = point(Srid::WGS84, input[0], input[1]);
                let projected = transform_geometry(&src, srid(code), &ctx).unwrap();
                assert_eq!(projected.srid, srid(code));
                let back = transform_geometry(&projected, Srid::WGS84, &ctx).unwrap();
                let [lon, lat] = coords(&back);
                let tol = ROUND_TRIP_TOLERANCE_M / METRES_PER_DEGREE;
                assert!(
                    (lon - input[0]).abs() < tol && (lat - input[1]).abs() < tol,
                    "{code}: {input:?} -> {:?}",
                    [lon, lat]
                );
            }
        }
    }

    /// A forward then inverse UTM transform returns the input within 1e-8°.
    #[test]
    fn utm47n_round_trip_is_tight() {
        let ctx = CrsContext::builtin_only();
        let src = point(Srid::WGS84, 101.7117, 3.1579);
        let utm = transform_geometry(&src, srid(32647), &ctx).unwrap();
        let back = transform_geometry(&utm, Srid::WGS84, &ctx).unwrap();
        let [lon, lat] = coords(&back);
        assert!((lon - 101.7117).abs() < 1e-8 && (lat - 3.1579).abs() < 1e-8);
    }

    /// PROJ 9.8.1 reference: EPSG:4326 (102.2375, 3.975) to EPSG:32648.
    #[test]
    fn utm48n_matches_proj() {
        let ctx = CrsContext::builtin_only();
        let src = point(Srid::WGS84, 102.2375, 3.975);
        let [x, y] = coords(&transform_geometry(&src, srid(32648), &ctx).unwrap());
        assert!(
            (x - 193_218.924_909_861_76).abs() < ROUND_TRIP_TOLERANCE_M,
            "{x}"
        );
        assert!(
            (y - 439_876.984_345_265_6).abs() < ROUND_TRIP_TOLERANCE_M,
            "{y}"
        );
    }

    #[test]
    fn same_srid_is_identity() {
        let ctx = CrsContext::builtin_only();
        let src = point(srid(32648), 1.0, 2.0);
        assert_eq!(transform_geometry(&src, srid(32648), &ctx), Ok(src));
    }

    #[test]
    fn undefined_srid_fails() {
        let ctx = CrsContext::builtin_only();
        let src = point(Srid::WGS84, 101.0, 3.0);
        assert_eq!(
            transform_geometry(&src, srid(999), &ctx),
            Err(CrsError::Unknown(UnknownSrid::Undefined { code: 999 }))
        );
    }

    #[test]
    fn every_coordinate_is_mapped() {
        let ctx = CrsContext::builtin_only();
        let src = SridGeometry::wgs84(Geometry::polygon(vec![vec![
            [101.0, 3.0],
            [102.0, 3.0],
            [102.0, 4.0],
            [101.0, 3.0],
        ]]));
        let out = transform_geometry(&src, srid(32647), &ctx).unwrap();
        let Geometry::Polygon { coordinates } = out.geometry() else {
            panic!("shape changed");
        };
        assert_eq!(coordinates[0].len(), 4);
        assert!(coordinates[0].iter().all(|c| c[0] > 100_000.0));
    }

    /// Z is ellipsoidal height through the datum shift. M passes through.
    #[test]
    fn z_goes_through_the_transform_and_m_passes_through() {
        use nodedb_types::geometry::Ordinates;
        let ctx = CrsContext::builtin_only();
        let src = SridGeometry::with_ordinates(
            Srid::WGS84,
            Geometry::point(101.7, 3.1),
            Ordinates::new(Some(vec![100.0]), Some(vec![7.0])),
        )
        .unwrap();
        let utm = transform_geometry(&src, srid(32647), &ctx).unwrap();
        assert_eq!(utm.ordinates().z(), Some(&[100.0][..]));
        assert_eq!(utm.ordinates().m(), Some(&[7.0][..]));
        // GDM2000 is GRS80 with a zero shift: height moves by under 1 mm.
        let gdm = transform_geometry(&src, srid(4742), &ctx).unwrap();
        let z = gdm.ordinates().z().unwrap()[0];
        assert!((z - 100.0).abs() < 1e-3, "{z}");
    }

    /// A static shift gives the same result at any epoch.
    #[test]
    fn epoch_leaves_static_shifts_unchanged() {
        let ctx = CrsContext::builtin_only();
        let src = point(srid(4742), 101.7117, 3.1579);
        let epoch = CoordinateEpoch::new(2024.5).unwrap();
        assert_eq!(
            transform_geometry_at(&src, srid(32647), epoch, &ctx),
            transform_geometry(&src, srid(32647), &ctx)
        );
        assert_eq!(
            transform_geometry_at(&src, srid(4742), epoch, &ctx),
            Ok(src)
        );
    }

    #[test]
    fn out_of_domain_fails_the_call() {
        let ctx = CrsContext::builtin_only();
        let src = point(Srid::WGS84, 179.0, 3.0);
        assert!(matches!(
            transform_geometry(&src, srid(32648), &ctx),
            Err(CrsError::OutOfDomain { .. })
        ));
    }
}
