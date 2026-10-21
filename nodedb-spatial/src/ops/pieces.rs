// SPDX-License-Identifier: Apache-2.0

//! A geometry split into points, lines and polygons whose coordinates
//! carry their Z and M, and the way back.

use nodedb_types::geometry::{
    Dimension, Geometry, OrdinateError, OrdinatesBuilder, Position, Srid, SridGeometry,
};

/// One component with its positions.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Piece {
    Point(Position),
    Line(Vec<Position>),
    Polygon(Vec<Vec<Position>>),
}

/// Every component of `g` in traversal order. EMPTY components are skipped.
pub(crate) fn pieces(g: &SridGeometry) -> Vec<Piece> {
    let positions: Vec<Position> = g.positions().collect();
    let mut cursor = 0usize;
    let mut out = Vec::new();
    walk(g.geometry(), &positions, &mut cursor, &mut out);
    out
}

fn take(positions: &[Position], cursor: &mut usize, n: usize) -> Vec<Position> {
    let end = (*cursor + n).min(positions.len());
    let slice = positions[*cursor..end].to_vec();
    *cursor = end;
    slice
}

fn walk(geom: &Geometry, positions: &[Position], cursor: &mut usize, out: &mut Vec<Piece>) {
    match geom {
        Geometry::Point { .. } if geom.is_empty() => {}
        Geometry::Point { .. } => {
            out.extend(take(positions, cursor, 1).into_iter().map(Piece::Point))
        }
        Geometry::MultiPoint { coordinates } => out.extend(
            take(positions, cursor, coordinates.len())
                .into_iter()
                .map(Piece::Point),
        ),
        Geometry::LineString { coordinates } => {
            out.push(Piece::Line(take(positions, cursor, coordinates.len())));
        }
        Geometry::MultiLineString { coordinates } => {
            for line in coordinates {
                out.push(Piece::Line(take(positions, cursor, line.len())));
            }
        }
        Geometry::Polygon { coordinates } => out.push(Piece::Polygon(
            coordinates
                .iter()
                .map(|ring| take(positions, cursor, ring.len()))
                .collect(),
        )),
        Geometry::MultiPolygon { coordinates } => {
            for polygon in coordinates {
                out.push(Piece::Polygon(
                    polygon
                        .iter()
                        .map(|ring| take(positions, cursor, ring.len()))
                        .collect(),
                ));
            }
        }
        Geometry::GeometryCollection { geometries } => {
            for member in geometries {
                walk(member, positions, cursor, out);
            }
        }
    }
}

/// Builds a geometry of one dimension from positions.
pub(crate) struct Assembler {
    dimension: Dimension,
    ordinates: OrdinatesBuilder,
}

impl Assembler {
    pub(crate) fn new(dimension: Dimension) -> Self {
        Self {
            dimension,
            ordinates: OrdinatesBuilder::with_dimension(dimension),
        }
    }

    /// The XY of `p`, recording its Z and M under the fixed dimension.
    pub(crate) fn coord(&mut self, p: &Position) -> Result<[f64; 2], OrdinateError> {
        let z = self.dimension.has_z().then(|| p.z.unwrap_or(0.0));
        let m = self.dimension.has_m().then(|| p.m.unwrap_or(0.0));
        self.ordinates.push(z, m)?;
        Ok([p.x, p.y])
    }

    pub(crate) fn path(&mut self, ps: &[Position]) -> Result<Vec<[f64; 2]>, OrdinateError> {
        ps.iter().map(|p| self.coord(p)).collect()
    }

    pub(crate) fn finish(
        self,
        srid: Srid,
        geometry: Geometry,
    ) -> Result<SridGeometry, OrdinateError> {
        SridGeometry::with_ordinates(srid, geometry, self.ordinates.finish())
    }
}

/// Points as a single point, a multipoint, or `MULTIPOINT EMPTY`, in
/// `dimension`.
pub(crate) fn multipoint(
    srid: Srid,
    dimension: Dimension,
    points: &[Position],
) -> Result<SridGeometry, OrdinateError> {
    let mut asm = Assembler::new(dimension);
    let coords = asm.path(points)?;
    asm.finish(
        srid,
        Geometry::MultiPoint {
            coordinates: coords,
        },
    )
}

/// Lines as a multilinestring (possibly EMPTY) in `dimension`.
pub(crate) fn multiline(
    srid: Srid,
    dimension: Dimension,
    lines: &[Vec<Position>],
) -> Result<SridGeometry, OrdinateError> {
    let mut asm = Assembler::new(dimension);
    let coords = lines
        .iter()
        .map(|l| asm.path(l))
        .collect::<Result<_, _>>()?;
    asm.finish(
        srid,
        Geometry::MultiLineString {
            coordinates: coords,
        },
    )
}

/// Linear interpolation between two positions at fraction `t`. Z and M
/// interpolate where both ends carry them.
pub(crate) fn lerp(a: &Position, b: &Position, t: f64) -> Position {
    let mix = |u: Option<f64>, v: Option<f64>| u.zip(v).map(|(u, v)| u + (v - u) * t);
    Position {
        x: a.x + (b.x - a.x) * t,
        y: a.y + (b.y - a.y) * t,
        z: mix(a.z, b.z),
        m: mix(a.m, b.m),
    }
}
