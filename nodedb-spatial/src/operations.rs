// SPDX-License-Identifier: Apache-2.0

//! `ST_Envelope`: the bounding box of a geometry as a geometry.

use nodedb_types::geometry::Geometry;
use nodedb_types::{BboxSpace, geometry_bbox_in};

/// `ST_Envelope(geom)`: the bounding box in `space` as a polygon. A box of
/// zero width and height is a point, a box of zero width or height a line,
/// as PostGIS returns them. EMPTY stays EMPTY.
pub fn st_envelope_in(geom: &Geometry, space: BboxSpace) -> Geometry {
    if geom.is_empty() {
        return geom.clone();
    }
    let bb = geometry_bbox_in(geom, space);
    let (lo, hi) = ([bb.min_lng, bb.min_lat], [bb.max_lng, bb.max_lat]);
    match (lo[0] == hi[0], lo[1] == hi[1]) {
        (true, true) => Geometry::point(lo[0], lo[1]),
        (true, false) | (false, true) => Geometry::line_string(vec![lo, hi]),
        (false, false) => Geometry::polygon(vec![vec![lo, [hi[0], lo[1]], hi, [lo[0], hi[1]], lo]]),
    }
}

/// [`st_envelope_in`] in geographic space.
pub fn st_envelope(geom: &Geometry) -> Geometry {
    st_envelope_in(geom, BboxSpace::Geographic)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn polygon_envelope_is_its_box() {
        let poly = Geometry::polygon(vec![vec![[1.0, 2.0], [5.0, 3.0], [3.0, 8.0], [1.0, 2.0]]]);
        assert_eq!(
            st_envelope(&poly),
            Geometry::polygon(vec![vec![
                [1.0, 2.0],
                [5.0, 2.0],
                [5.0, 8.0],
                [1.0, 8.0],
                [1.0, 2.0]
            ]])
        );
    }

    #[test]
    fn degenerate_envelopes_drop_dimension() {
        assert_eq!(
            st_envelope(&Geometry::point(5.0, 10.0)),
            Geometry::point(5.0, 10.0)
        );
        let flat = Geometry::line_string(vec![[0.0, 1.0], [4.0, 1.0]]);
        assert_eq!(
            st_envelope(&flat),
            Geometry::line_string(vec![[0.0, 1.0], [4.0, 1.0]])
        );
        assert!(st_envelope(&Geometry::empty_point()).is_empty());
    }
}
