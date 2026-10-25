// SPDX-License-Identifier: Apache-2.0

//! A geometry broken into the pieces distance algorithms measure between:
//! edges, vertices and polygons. An EMPTY geometry has no pieces.

use nodedb_types::geometry::{Geometry, point_in_polygon};

use crate::predicates::edge::{point_on_ring_boundary, ring_edges};

/// The pieces of a geometry a distance is measured between.
#[derive(Default)]
pub(crate) struct Parts<'a> {
    /// Every edge, with a lone point as a zero-length edge.
    pub(crate) segments: Vec<([f64; 2], [f64; 2])>,
    /// Every vertex.
    pub(crate) vertices: Vec<[f64; 2]>,
    /// Every polygon, as its rings.
    pub(crate) polygons: Vec<&'a [Vec<[f64; 2]>]>,
}

impl<'a> Parts<'a> {
    pub(crate) fn of(geom: &'a Geometry) -> Self {
        let mut parts = Parts::default();
        parts.add(geom);
        parts
    }

    fn add(&mut self, geom: &'a Geometry) {
        match geom {
            Geometry::Point { .. } if geom.is_empty() => {}
            Geometry::Point { coordinates } => self.add_point(*coordinates),
            Geometry::MultiPoint { coordinates } => {
                coordinates.iter().for_each(|c| self.add_point(*c));
            }
            Geometry::LineString { coordinates } => self.add_path(coordinates),
            Geometry::MultiLineString { coordinates } => {
                coordinates.iter().for_each(|line| self.add_path(line));
            }
            Geometry::Polygon { coordinates } => self.add_polygon(coordinates),
            Geometry::MultiPolygon { coordinates } => {
                coordinates.iter().for_each(|rings| self.add_polygon(rings));
            }
            Geometry::GeometryCollection { geometries } => {
                geometries.iter().for_each(|g| self.add(g));
            }
        }
    }

    fn add_point(&mut self, c: [f64; 2]) {
        self.segments.push((c, c));
        self.vertices.push(c);
    }

    fn add_path(&mut self, path: &[[f64; 2]]) {
        match path {
            [] => {}
            [only] => self.add_point(*only),
            _ => {
                self.segments.extend(path.windows(2).map(|w| (w[0], w[1])));
                self.vertices.extend_from_slice(path);
            }
        }
    }

    fn add_polygon(&mut self, rings: &'a [Vec<[f64; 2]>]) {
        for ring in rings {
            self.segments.extend(ring_edges(ring));
            self.vertices.extend_from_slice(ring);
        }
        self.polygons.push(rings);
    }

    /// Whether any polygon of `self` covers any vertex of `other`.
    pub(crate) fn covers_any_vertex_of(&self, other: &Parts<'_>) -> bool {
        self.polygons
            .iter()
            .any(|rings| other.vertices.iter().any(|v| polygon_covers(rings, *v)))
    }
}

/// Whether `pt` lies inside or on a polygon, outside every hole interior.
pub(crate) fn polygon_covers(rings: &[Vec<[f64; 2]>], pt: [f64; 2]) -> bool {
    let Some(exterior) = rings.first() else {
        return false;
    };
    if !point_in_polygon(pt[0], pt[1], exterior) && !point_on_ring_boundary(pt, exterior) {
        return false;
    }
    !rings[1..]
        .iter()
        .any(|hole| point_in_polygon(pt[0], pt[1], hole) && !point_on_ring_boundary(pt, hole))
}
