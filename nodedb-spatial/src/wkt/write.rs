// SPDX-License-Identifier: Apache-2.0

//! WKT and EWKT writer.
//!
//! A geometry with Z or M writes its dimension tag after every type keyword
//! (`POINT Z (1 2 3)`), as PostGIS `ST_AsText` does. An EMPTY geometry
//! writes `KIND [tag] EMPTY`.

use std::fmt::Write as _;

use nodedb_types::geometry::{Dimension, Geometry, Ordinates, Srid, SridGeometry};

/// WKT of `g`, Z and M included. WKT has no SRID slot.
pub fn geometry_to_wkt(g: &SridGeometry) -> String {
    let mut out = String::new();
    let mut writer = Writer {
        out: &mut out,
        ordinates: g.ordinates(),
        dimension: g.dimension(),
        index: 0,
    };
    writer.geometry(g.geometry());
    out
}

/// EWKT of `g`: `SRID=<code>;` then WKT, as PostGIS `ST_AsEWKT` writes it.
pub fn geometry_to_ewkt(g: &SridGeometry) -> String {
    format!("SRID={};{}", g.srid.code(), geometry_to_wkt(g))
}

/// WKT of an XY shape.
pub fn shape_to_wkt(geometry: &Geometry) -> String {
    geometry_to_wkt(&SridGeometry::new(Srid::WGS84, geometry.clone()))
}

struct Writer<'a> {
    out: &'a mut String,
    ordinates: &'a Ordinates,
    dimension: Dimension,
    /// Index of the next coordinate in traversal order.
    index: usize,
}

impl Writer<'_> {
    fn keyword(&mut self, geometry: &Geometry) {
        self.out.push_str(geometry.kind().wkt_keyword());
        let tag = self.dimension.wkt_tag();
        if !tag.is_empty() {
            self.out.push(' ');
            self.out.push_str(tag);
            self.out.push(' ');
        }
    }

    fn empty(&mut self) {
        if !self.out.ends_with(' ') {
            self.out.push(' ');
        }
        self.out.push_str("EMPTY");
    }

    fn geometry(&mut self, geometry: &Geometry) {
        self.keyword(geometry);
        if geometry.is_empty() && !matches!(geometry, Geometry::GeometryCollection { .. }) {
            self.empty();
            // An EMPTY polygon or multi may still hold empty rings.
            self.index += geometry.coord_count();
            return;
        }
        match geometry {
            Geometry::Point { coordinates } => {
                self.out.push('(');
                self.coord(*coordinates);
                self.out.push(')');
            }
            Geometry::LineString { coordinates } => self.path(coordinates),
            Geometry::Polygon { coordinates } | Geometry::MultiLineString { coordinates } => {
                self.paths(coordinates)
            }
            Geometry::MultiPoint { coordinates } => {
                self.out.push('(');
                for (i, c) in coordinates.iter().enumerate() {
                    if i > 0 {
                        self.out.push(',');
                    }
                    self.out.push('(');
                    self.coord(*c);
                    self.out.push(')');
                }
                self.out.push(')');
            }
            Geometry::MultiPolygon { coordinates } => {
                self.out.push('(');
                for (i, polygon) in coordinates.iter().enumerate() {
                    if i > 0 {
                        self.out.push(',');
                    }
                    self.paths(polygon);
                }
                self.out.push(')');
            }
            Geometry::GeometryCollection { geometries } => {
                if geometries.is_empty() {
                    self.empty();
                    return;
                }
                self.out.push('(');
                for (i, member) in geometries.iter().enumerate() {
                    if i > 0 {
                        self.out.push(',');
                    }
                    self.geometry(member);
                }
                self.out.push(')');
            }
        }
    }

    fn paths(&mut self, rings: &[Vec<[f64; 2]>]) {
        self.out.push('(');
        for (i, ring) in rings.iter().enumerate() {
            if i > 0 {
                self.out.push(',');
            }
            self.path(ring);
        }
        self.out.push(')');
    }

    fn path(&mut self, coords: &[[f64; 2]]) {
        self.out.push('(');
        for (i, c) in coords.iter().enumerate() {
            if i > 0 {
                self.out.push(',');
            }
            self.coord(*c);
        }
        self.out.push(')');
    }

    fn coord(&mut self, c: [f64; 2]) {
        let (z, m) = self.ordinates.at(self.index);
        self.index += 1;
        let _ = write!(self.out, "{} {}", c[0], c[1]);
        if let Some(z) = z {
            let _ = write!(self.out, " {z}");
        }
        if let Some(m) = m {
            let _ = write!(self.out, " {m}");
        }
    }
}
