// SPDX-License-Identifier: Apache-2.0

//! Spherical geo math on (longitude, latitude) degrees.

/// Mean Earth radius in metres (IUGG R1), the sphere every haversine and
/// spherical-excess measure uses.
pub const EARTH_RADIUS_M: f64 = 6_371_000.0;

/// Haversine distance between two points in meters.
///
/// Input: (lng1, lat1) and (lng2, lat2) in degrees.
pub fn haversine_distance(lng1: f64, lat1: f64, lng2: f64, lat2: f64) -> f64 {
    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();

    let a = (dlat / 2.0).sin().powi(2) + lat1_r.cos() * lat2_r.cos() * (dlng / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_M * c
}

/// Haversine bearing from point A to point B in degrees (0-360).
pub fn haversine_bearing(lng1: f64, lat1: f64, lng2: f64, lat2: f64) -> f64 {
    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();
    let dlng = (lng2 - lng1).to_radians();

    let y = dlng.sin() * lat2_r.cos();
    let x = lat1_r.cos() * lat2_r.sin() - lat1_r.sin() * lat2_r.cos() * dlng.cos();
    let bearing = y.atan2(x).to_degrees();
    (bearing + 360.0) % 360.0
}

/// Check if a point is inside a polygon (ray casting algorithm).
///
/// Planar in its inputs, so it serves geographic and projected coordinates.
pub fn point_in_polygon(lng: f64, lat: f64, ring: &[[f64; 2]]) -> bool {
    let mut inside = false;
    let n = ring.len();
    let mut j = n.wrapping_sub(1);
    for i in 0..n {
        let yi = ring[i][1];
        let yj = ring[j][1];
        if ((yi > lat) != (yj > lat))
            && (lng < (ring[j][0] - ring[i][0]) * (lat - yi) / (yj - yi) + ring[i][0])
        {
            inside = !inside;
        }
        j = i;
    }
    inside
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn haversine_nyc_to_london() {
        // NYC: -74.006, 40.7128 → London: -0.1278, 51.5074
        let d = haversine_distance(-74.006, 40.7128, -0.1278, 51.5074);
        // ~5,570 km
        assert!((d - 5_570_000.0).abs() < 50_000.0, "got {d}m");
    }

    #[test]
    fn haversine_same_point() {
        let d = haversine_distance(0.0, 0.0, 0.0, 0.0);
        assert!(d.abs() < 1e-6);
    }

    #[test]
    fn bearing_north() {
        let b = haversine_bearing(0.0, 0.0, 0.0, 1.0);
        assert!((b - 0.0).abs() < 1.0, "expected ~0, got {b}");
    }

    #[test]
    fn bearing_east() {
        let b = haversine_bearing(0.0, 0.0, 1.0, 0.0);
        assert!((b - 90.0).abs() < 1.0, "expected ~90, got {b}");
    }

    #[test]
    fn point_in_polygon_inside() {
        let ring = vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ];
        assert!(point_in_polygon(5.0, 5.0, &ring));
        assert!(!point_in_polygon(15.0, 5.0, &ring));
    }
}
