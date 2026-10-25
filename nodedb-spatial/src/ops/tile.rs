// SPDX-License-Identifier: Apache-2.0

//! `ST_TileEnvelope` and `ST_ShiftLongitude`.

use nodedb_types::BoundingBox;
use nodedb_types::geometry::{Geometry, SridGeometry};

/// Half the side of the EPSG:3857 square, metres: π × 6378137.
pub const WEB_MERCATOR_HALF_SIDE: f64 = 20_037_508.342_789_244;

/// Largest zoom `ST_TileEnvelope` accepts, as PostGIS limits it.
pub const MAX_TILE_ZOOM: u32 = 31;

/// Why a tile address is out of range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TileError {
    #[error("tile zoom {zoom} is outside 0..={max}", max = MAX_TILE_ZOOM)]
    Zoom { zoom: i64 },
    #[error("tile column {x} is outside 0..{count} at zoom {zoom}")]
    Column { x: i64, zoom: u32, count: u64 },
    #[error("tile row {y} is outside 0..{count} at zoom {zoom}")]
    Row { y: i64, zoom: u32, count: u64 },
    #[error("tile bounds have zero or negative extent")]
    Bounds,
}

/// The square EPSG:3857 bounds `ST_TileEnvelope` divides by default.
pub fn web_mercator_bounds() -> BoundingBox {
    BoundingBox::new(
        -WEB_MERCATOR_HALF_SIDE,
        -WEB_MERCATOR_HALF_SIDE,
        WEB_MERCATOR_HALF_SIDE,
        WEB_MERCATOR_HALF_SIDE,
    )
}

/// `ST_TileEnvelope(z, x, y [, bounds])`: the polygon of tile (`x`, `y`)
/// at zoom `z` of the XYZ scheme over `bounds`. Row 0 is the top (north)
/// row.
pub fn tile_envelope(z: i64, x: i64, y: i64, bounds: &BoundingBox) -> Result<Geometry, TileError> {
    let zoom = u32::try_from(z)
        .ok()
        .filter(|z| *z <= MAX_TILE_ZOOM)
        .ok_or(TileError::Zoom { zoom: z })?;
    let count = 1u64 << zoom;
    let in_range = |v: i64| u64::try_from(v).is_ok_and(|v| v < count);
    if !in_range(x) {
        return Err(TileError::Column { x, zoom, count });
    }
    if !in_range(y) {
        return Err(TileError::Row { y, zoom, count });
    }
    let width = bounds.max_lng - bounds.min_lng;
    let height = bounds.max_lat - bounds.min_lat;
    if !(width > 0.0 && height > 0.0) {
        return Err(TileError::Bounds);
    }
    let n = count as f64;
    let (tw, th) = (width / n, height / n);
    let min_x = bounds.min_lng + x as f64 * tw;
    let max_y = bounds.max_lat - y as f64 * th;
    let (max_x, min_y) = (min_x + tw, max_y - th);
    Ok(Geometry::polygon(vec![vec![
        [min_x, min_y],
        [min_x, max_y],
        [max_x, max_y],
        [max_x, min_y],
        [min_x, min_y],
    ]]))
}

/// `ST_ShiftLongitude(geom)`: longitudes below 0 gain 360°, longitudes
/// above 180° lose 360°, so a geometry across the antimeridian reads in
/// 0..360 and back. Z and M are kept.
pub fn shift_longitude(g: &SridGeometry) -> SridGeometry {
    let shifted: Result<SridGeometry, std::convert::Infallible> = g.try_map_positions(|mut p| {
        if p.x < 0.0 {
            p.x += 360.0;
        } else if p.x > 180.0 {
            p.x -= 360.0;
        }
        Ok(p)
    });
    match shifted {
        Ok(g) => g,
        Err(never) => match never {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::Srid;

    #[test]
    fn zoom_zero_is_the_whole_square() {
        let g = tile_envelope(0, 0, 0, &web_mercator_bounds()).unwrap();
        let Geometry::Polygon { coordinates } = g else {
            panic!();
        };
        assert_eq!(
            coordinates[0][0],
            [-WEB_MERCATOR_HALF_SIDE, -WEB_MERCATOR_HALF_SIDE]
        );
        assert_eq!(
            coordinates[0][2],
            [WEB_MERCATOR_HALF_SIDE, WEB_MERCATOR_HALF_SIDE]
        );
    }

    /// PostGIS: ST_TileEnvelope(2, 1, 1) is
    /// POLYGON((-10018754.17 0,-10018754.17 10018754.17,0 10018754.17,0 0,-10018754.17 0)).
    #[test]
    fn zoom_two_tile_matches_postgis() {
        let g = tile_envelope(2, 1, 1, &web_mercator_bounds()).unwrap();
        let Geometry::Polygon { coordinates } = g else {
            panic!();
        };
        let half = WEB_MERCATOR_HALF_SIDE / 2.0;
        assert_eq!(coordinates[0][0], [-half, 0.0]);
        assert_eq!(coordinates[0][2], [0.0, half]);
    }

    #[test]
    fn out_of_range_addresses_fail() {
        let b = web_mercator_bounds();
        assert!(matches!(
            tile_envelope(32, 0, 0, &b),
            Err(TileError::Zoom { .. })
        ));
        assert!(matches!(
            tile_envelope(1, 2, 0, &b),
            Err(TileError::Column { .. })
        ));
        assert!(matches!(
            tile_envelope(1, 0, -1, &b),
            Err(TileError::Row { .. })
        ));
    }

    #[test]
    fn shift_longitude_wraps_both_ways() {
        let g = SridGeometry::new(
            Srid::WGS84,
            Geometry::line_string(vec![[-179.0, 0.0], [179.0, 0.0], [181.0, 1.0]]),
        );
        let s = shift_longitude(&g);
        assert_eq!(
            s.geometry(),
            &Geometry::line_string(vec![[181.0, 0.0], [179.0, 0.0], [-179.0, 1.0]])
        );
    }
}
