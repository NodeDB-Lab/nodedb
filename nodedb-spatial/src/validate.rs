// SPDX-License-Identifier: Apache-2.0

//! Geometry validity per OGC Simple Features, as `ST_IsValid` defines it.
//!
//! - every coordinate finite;
//! - a LineString has at least 2 points;
//! - a ring is closed and has at least 4 points;
//! - a ring does not cross itself;
//! - a hole lies inside its shell, and rings meet at points only;
//! - MultiPolygon members neither overlap nor share an edge.
//!
//! Ring direction does not affect validity. EMPTY is valid.

use geo::Validation;
use geo::algorithm::validation::{InvalidMultiPolygon, InvalidPolygon, RingRole};
use nodedb_types::geometry::Geometry;

use crate::ops::convert::polygon;

/// Where in a geometry a problem lies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    /// Index path of collection members, outermost first.
    pub members: Vec<usize>,
    /// Polygon index within a MultiPolygon.
    pub polygon: Option<usize>,
    /// Ring index within a polygon, 0 the shell.
    pub ring: Option<usize>,
}

impl Location {
    fn root() -> Self {
        Self {
            members: Vec::new(),
            polygon: None,
            ring: None,
        }
    }

    fn member(&self, index: usize) -> Self {
        let mut members = self.members.clone();
        members.push(index);
        Self {
            members,
            polygon: None,
            ring: None,
        }
    }

    fn polygon(&self, index: usize) -> Self {
        Self {
            polygon: Some(index),
            ..self.clone()
        }
    }

    fn ring(&self, index: usize) -> Self {
        Self {
            ring: Some(index),
            ..self.clone()
        }
    }
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        for m in &self.members {
            parts.push(format!("member {m}"));
        }
        if let Some(p) = self.polygon {
            parts.push(format!("polygon {p}"));
        }
        match self.ring {
            Some(0) => parts.push("shell".to_string()),
            Some(r) => parts.push(format!("hole {r}")),
            None => {}
        }
        if parts.is_empty() {
            f.write_str("geometry")
        } else {
            f.write_str(&parts.join(" "))
        }
    }
}

/// Why a geometry is invalid.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ValidityReason {
    #[error("{location}: coordinate {index} is not finite")]
    NonFinite { location: Location, index: usize },
    #[error("{location}: {found} points, a LineString needs at least 2")]
    LineTooShort { location: Location, found: usize },
    #[error("{location}: {found} points, a ring needs at least 4")]
    RingTooShort { location: Location, found: usize },
    #[error("{location}: ring is not closed")]
    RingNotClosed { location: Location },
    #[error("{location}: ring crosses itself")]
    SelfIntersection { location: Location },
    #[error("{location}: hole lies outside the shell")]
    HoleOutsideShell { location: Location },
    #[error("{location}: rings {first} and {second} share a line")]
    RingsTouchOnLine {
        location: Location,
        first: usize,
        second: usize,
    },
    #[error("{location}: rings {first} and {second} overlap")]
    RingsOverlap {
        location: Location,
        first: usize,
        second: usize,
    },
    #[error("{location}: polygons {first} and {second} overlap")]
    PolygonsOverlap {
        location: Location,
        first: usize,
        second: usize,
    },
    #[error("{location}: polygons {first} and {second} share a line")]
    PolygonsTouchOnLine {
        location: Location,
        first: usize,
        second: usize,
    },
}

/// Every reason `geom` is invalid. Empty when valid.
pub fn validate_geometry(geom: &Geometry) -> Vec<ValidityReason> {
    let mut reasons = Vec::new();
    check(geom, &Location::root(), &mut reasons);
    reasons
}

/// Whether `geom` is valid.
pub fn is_valid(geom: &Geometry) -> bool {
    validate_geometry(geom).is_empty()
}

fn check(geom: &Geometry, at: &Location, out: &mut Vec<ValidityReason>) {
    match geom {
        g if g.is_empty() => {}
        Geometry::Point { coordinates } => finite(&[*coordinates], at, out),
        Geometry::MultiPoint { coordinates } => finite(coordinates, at, out),
        Geometry::LineString { coordinates } => line(coordinates, at, out),
        Geometry::MultiLineString { coordinates } => {
            for (i, l) in coordinates.iter().enumerate() {
                line(l, &at.polygon(i), out);
            }
        }
        Geometry::Polygon { coordinates } => {
            if rings_ok(coordinates, at, out) {
                polygon_topology(coordinates, at, out);
            }
        }
        Geometry::MultiPolygon { coordinates } => {
            let mut all_ok = true;
            for (i, rings) in coordinates.iter().enumerate() {
                let here = at.polygon(i);
                if rings_ok(rings, &here, out) {
                    polygon_topology(rings, &here, out);
                } else {
                    all_ok = false;
                }
            }
            if all_ok {
                multipolygon_topology(coordinates, at, out);
            }
        }
        Geometry::GeometryCollection { geometries } => {
            for (i, member) in geometries.iter().enumerate() {
                check(member, &at.member(i), out);
            }
        }
    }
}

fn finite(coords: &[[f64; 2]], at: &Location, out: &mut Vec<ValidityReason>) {
    if let Some(index) = coords
        .iter()
        .position(|c| !(c[0].is_finite() && c[1].is_finite()))
    {
        out.push(ValidityReason::NonFinite {
            location: at.clone(),
            index,
        });
    }
}

fn line(coords: &[[f64; 2]], at: &Location, out: &mut Vec<ValidityReason>) {
    finite(coords, at, out);
    if coords.len() < 2 {
        out.push(ValidityReason::LineTooShort {
            location: at.clone(),
            found: coords.len(),
        });
    }
}

/// The structural checks `geo` cannot see once it closes rings. `true`
/// when every ring passes them.
fn rings_ok(rings: &[Vec<[f64; 2]>], at: &Location, out: &mut Vec<ValidityReason>) -> bool {
    let before = out.len();
    for (r, ring) in rings.iter().enumerate() {
        let here = at.ring(r);
        finite(ring, &here, out);
        if ring.len() < 4 {
            out.push(ValidityReason::RingTooShort {
                location: here,
                found: ring.len(),
            });
            continue;
        }
        if ring.first() != ring.last() {
            out.push(ValidityReason::RingNotClosed { location: here });
        }
    }
    out.len() == before
}

fn role_index(role: &RingRole) -> usize {
    match role {
        RingRole::Exterior => 0,
        RingRole::Interior(i) => i + 1,
    }
}

fn polygon_reason(e: &InvalidPolygon, at: &Location) -> ValidityReason {
    match e {
        InvalidPolygon::TooFewPointsInRing(role) => ValidityReason::RingTooShort {
            location: at.ring(role_index(role)),
            found: 3,
        },
        InvalidPolygon::SelfIntersection(role) => ValidityReason::SelfIntersection {
            location: at.ring(role_index(role)),
        },
        InvalidPolygon::NonFiniteCoord(role, index) => ValidityReason::NonFinite {
            location: at.ring(role_index(role)),
            index: index.0,
        },
        InvalidPolygon::InteriorRingNotContainedInExteriorRing(role) => {
            ValidityReason::HoleOutsideShell {
                location: at.ring(role_index(role)),
            }
        }
        InvalidPolygon::IntersectingRingsOnALine(a, b) => ValidityReason::RingsTouchOnLine {
            location: at.clone(),
            first: role_index(a),
            second: role_index(b),
        },
        InvalidPolygon::IntersectingRingsOnAnArea(a, b) => ValidityReason::RingsOverlap {
            location: at.clone(),
            first: role_index(a),
            second: role_index(b),
        },
    }
}

fn polygon_topology(rings: &[Vec<[f64; 2]>], at: &Location, out: &mut Vec<ValidityReason>) {
    out.extend(
        polygon(rings)
            .validation_errors()
            .iter()
            .map(|e| polygon_reason(e, at)),
    );
}

fn multipolygon_topology(
    polygons: &[Vec<Vec<[f64; 2]>>],
    at: &Location,
    out: &mut Vec<ValidityReason>,
) {
    let mp = geo::MultiPolygon::new(polygons.iter().map(|p| polygon(p)).collect());
    for e in mp.validation_errors() {
        match e {
            // Each member is checked on its own above.
            InvalidMultiPolygon::InvalidPolygon(..) => {}
            InvalidMultiPolygon::ElementsOverlaps(a, b) => {
                out.push(ValidityReason::PolygonsOverlap {
                    location: at.clone(),
                    first: a.0,
                    second: b.0,
                });
            }
            InvalidMultiPolygon::ElementsTouchOnALine(a, b) => {
                out.push(ValidityReason::PolygonsTouchOnLine {
                    location: at.clone(),
                    first: a.0,
                    second: b.0,
                });
            }
        }
    }
}

/// `ST_IsValidReason`: `"Valid Geometry"` or the first reason, as PostGIS
/// words the valid case.
pub fn validity_reason(geom: &Geometry) -> String {
    validate_geometry(geom)
        .first()
        .map_or_else(|| "Valid Geometry".to_string(), ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn square(clockwise: bool) -> Vec<[f64; 2]> {
        let mut ring = vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ];
        if clockwise {
            ring.reverse();
        }
        ring
    }

    #[test]
    fn valid_polygon_either_direction() {
        assert!(is_valid(&Geometry::polygon(vec![square(false)])));
        assert!(is_valid(&Geometry::polygon(vec![square(true)])));
    }

    #[test]
    fn unclosed_and_short_rings() {
        let unclosed = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
        ]]);
        assert!(matches!(
            validate_geometry(&unclosed).as_slice(),
            [ValidityReason::RingNotClosed { .. }]
        ));
        let short = Geometry::polygon(vec![vec![[0.0, 0.0], [10.0, 0.0], [10.0, 10.0]]]);
        assert!(matches!(
            validate_geometry(&short).as_slice(),
            [ValidityReason::RingTooShort { found: 3, .. }]
        ));
    }

    #[test]
    fn bowtie_self_intersects() {
        let bowtie = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [10.0, 10.0],
            [10.0, 0.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ]]);
        let reasons = validate_geometry(&bowtie);
        assert!(
            reasons
                .iter()
                .any(|r| matches!(r, ValidityReason::SelfIntersection { .. })),
            "{reasons:?}"
        );
        assert!(validity_reason(&bowtie).contains("crosses itself"));
    }

    #[test]
    fn hole_outside_shell() {
        let g = Geometry::polygon(vec![
            square(false),
            vec![
                [20.0, 20.0],
                [21.0, 20.0],
                [21.0, 21.0],
                [20.0, 21.0],
                [20.0, 20.0],
            ],
        ]);
        assert!(
            validate_geometry(&g)
                .iter()
                .any(|r| matches!(r, ValidityReason::HoleOutsideShell { .. }))
        );
    }

    #[test]
    fn overlapping_multipolygon_members() {
        let shifted: Vec<[f64; 2]> = square(false).iter().map(|c| [c[0] + 5.0, c[1]]).collect();
        let g = Geometry::MultiPolygon {
            coordinates: vec![vec![square(false)], vec![shifted]],
        };
        assert!(
            validate_geometry(&g)
                .iter()
                .any(|r| matches!(r, ValidityReason::PolygonsOverlap { .. }))
        );
    }

    #[test]
    fn lines_points_and_empty() {
        assert!(is_valid(&Geometry::line_string(vec![
            [0.0, 0.0],
            [1.0, 1.0]
        ])));
        assert!(!is_valid(&Geometry::LineString {
            coordinates: vec![[0.0, 0.0]]
        }));
        assert!(is_valid(&Geometry::point(0.0, 0.0)));
        assert!(!is_valid(&Geometry::Point {
            coordinates: [f64::INFINITY, 0.0]
        }));
        assert!(is_valid(&Geometry::empty_point()));
        assert!(is_valid(&Geometry::empty(
            nodedb_types::geometry::GeometryKind::Polygon
        )));
        assert_eq!(
            validity_reason(&Geometry::point(0.0, 0.0)),
            "Valid Geometry"
        );
    }

    #[test]
    fn collection_reports_the_member() {
        let gc = Geometry::GeometryCollection {
            geometries: vec![
                Geometry::point(0.0, 0.0),
                Geometry::LineString {
                    coordinates: vec![[0.0, 0.0]],
                },
            ],
        };
        let reasons = validate_geometry(&gc);
        assert_eq!(reasons.len(), 1);
        assert!(
            reasons[0].to_string().starts_with("member 1"),
            "{}",
            reasons[0]
        );
    }
}
