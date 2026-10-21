// SPDX-License-Identifier: Apache-2.0

//! WKT and EWKT reader.
//!
//! A type keyword takes an optional dimension tag, spaced (`POINT Z`) or
//! attached (`POINTZ`): `Z`, `M` or `ZM`. With a tag every coordinate holds
//! exactly the numbers the tag names. Without one, 2 numbers are XY, 3 are
//! XYZ and 4 are XYZM, and one geometry never mixes counts. `EMPTY` replaces
//! the parenthesised body of any type.

use nodedb_types::geometry::{
    Dimension, Geometry, GeometryKind, OrdinatesBuilder, Srid, SridGeometry,
};

use super::error::{WktError, WktErrorKind};

/// Nesting limit for `GEOMETRYCOLLECTION`, the same bound the WKB reader
/// keeps.
const MAX_WKT_DEPTH: usize = 64;

/// EWKT text split into its optional SRID code and its geometry.
#[derive(Debug, Clone, PartialEq)]
pub struct Ewkt {
    /// The code after `SRID=`, unresolved. `None` when the text has no
    /// prefix.
    pub srid: Option<i64>,
    /// The geometry, tagged WGS 84. The caller retags it once `srid`
    /// resolves.
    pub geometry: SridGeometry,
}

/// Parse WKT. WKT carries no SRID, so the result is tagged WGS 84.
pub fn geometry_from_wkt(input: &str) -> Result<SridGeometry, WktError> {
    let mut parser = Parser::new(input, 0);
    let geometry = parser.document()?;
    Ok(geometry)
}

/// Parse EWKT: an optional `SRID=<code>;` prefix, then WKT.
///
/// The prefix matches case-insensitively. The code is ASCII digits only.
pub fn geometry_from_ewkt(input: &str) -> Result<Ewkt, WktError> {
    let leading = input.len() - input.trim_start().len();
    let s = &input[leading..];
    let Some(rest) = strip_prefix_ci(s, "SRID=") else {
        return geometry_from_wkt(input).map(|geometry| Ewkt {
            srid: None,
            geometry,
        });
    };
    let bad_prefix = || WktError {
        offset: leading,
        kind: WktErrorKind::BadSridPrefix {
            text: s.chars().take(24).collect::<String>().into(),
        },
    };
    let (code, wkt) = rest.split_once(';').ok_or_else(bad_prefix)?;
    let digits = code.trim();
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad_prefix());
    }
    let srid = digits.parse::<i64>().map_err(|_| bad_prefix())?;
    let wkt_offset = input.len() - wkt.len();
    let geometry = Parser::new(wkt, wkt_offset).document()?;
    Ok(Ewkt {
        srid: Some(srid),
        geometry,
    })
}

/// Case-insensitive prefix strip of an ASCII `prefix`. Compares bytes, so a
/// multi-byte character at the cut point never panics.
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let head = s.as_bytes().get(..prefix.len())?;
    if head.eq_ignore_ascii_case(prefix.as_bytes()) {
        s.get(prefix.len()..)
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Token<'a> {
    Word(&'a str),
    Number(&'a str),
    Open,
    Close,
    Comma,
}

impl Token<'_> {
    fn text(&self) -> Box<str> {
        match self {
            Token::Word(s) | Token::Number(s) => (*s).into(),
            Token::Open => "(".into(),
            Token::Close => ")".into(),
            Token::Comma => ",".into(),
        }
    }
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
    /// Offset of `input` within the caller's text, for error reports.
    base: usize,
    ordinates: OrdinatesBuilder,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str, base: usize) -> Self {
        Self {
            input,
            pos: 0,
            base,
            ordinates: OrdinatesBuilder::default(),
        }
    }

    fn error(&self, offset: usize, kind: WktErrorKind) -> WktError {
        WktError {
            offset: self.base + offset,
            kind,
        }
    }

    /// The whole input as one geometry.
    fn document(&mut self) -> Result<SridGeometry, WktError> {
        let geometry = self.geometry(None, MAX_WKT_DEPTH)?;
        self.skip_space();
        if self.pos < self.input.len() {
            let text = self.input[self.pos..].chars().take(24).collect::<String>();
            return Err(self.error(self.pos, WktErrorKind::Trailing { text: text.into() }));
        }
        let ordinates = std::mem::take(&mut self.ordinates).finish();
        SridGeometry::with_ordinates(Srid::WGS84, geometry, ordinates)
            .map_err(|e| self.error(0, e.into()))
    }

    fn skip_space(&mut self) {
        let rest = &self.input[self.pos..];
        self.pos += rest.len() - rest.trim_start().len();
    }

    /// The next token and its start offset, without consuming it.
    fn peek(&mut self) -> Option<(Token<'a>, usize, usize)> {
        self.skip_space();
        let start = self.pos;
        let rest = &self.input[start..];
        let first = rest.chars().next()?;
        let (token, len) = match first {
            '(' => (Token::Open, 1),
            ')' => (Token::Close, 1),
            ',' => (Token::Comma, 1),
            c if c.is_ascii_alphabetic() => {
                let len = rest
                    .find(|c: char| !c.is_ascii_alphanumeric())
                    .unwrap_or(rest.len());
                (Token::Word(&rest[..len]), len)
            }
            _ => {
                let len = rest
                    .find(|c: char| c.is_whitespace() || matches!(c, '(' | ')' | ','))
                    .unwrap_or(rest.len());
                (Token::Number(&rest[..len]), len.max(first.len_utf8()))
            }
        };
        Some((token, start, len))
    }

    fn next(&mut self, expected: &'static str) -> Result<(Token<'a>, usize), WktError> {
        match self.peek() {
            Some((token, start, len)) => {
                self.pos = start + len;
                Ok((token, start))
            }
            None => Err(self.error(self.pos, WktErrorKind::UnexpectedEnd { expected })),
        }
    }

    fn expect(&mut self, want: Token<'static>, expected: &'static str) -> Result<(), WktError> {
        let (token, start) = self.next(expected)?;
        if token == want {
            Ok(())
        } else {
            Err(self.error(
                start,
                WktErrorKind::UnexpectedToken {
                    found: token.text(),
                    expected,
                },
            ))
        }
    }

    /// `KIND [Z|M|ZM] (EMPTY | body)`. `inherited` is the enclosing
    /// collection's declared dimension.
    fn geometry(
        &mut self,
        inherited: Option<Dimension>,
        depth: usize,
    ) -> Result<Geometry, WktError> {
        if depth == 0 {
            return Err(self.error(self.pos, WktErrorKind::TooDeep { max: MAX_WKT_DEPTH }));
        }
        let (token, start) = self.next("a geometry type")?;
        let Token::Word(word) = token else {
            return Err(self.error(
                start,
                WktErrorKind::UnexpectedToken {
                    found: token.text(),
                    expected: "a geometry type",
                },
            ));
        };
        let (kind, mut tag) =
            split_keyword(word).ok_or_else(|| self.error(start, unknown_type(word)))?;
        if tag.is_none()
            && let Some((Token::Word(next), at, len)) = self.peek()
            && let Some(dimension) = dimension_tag(next)
        {
            self.pos = at + len;
            tag = Some(dimension);
        }
        let declared = match (inherited, tag) {
            (Some(parent), Some(member)) if parent != member => {
                return Err(self.error(start, WktErrorKind::MemberDimension { parent, member }));
            }
            (parent, member) => member.or(parent),
        };
        if let Some(dimension) = declared {
            self.declare(dimension, start)?;
        }
        if let Some((Token::Word(w), at, len)) = self.peek()
            && w.eq_ignore_ascii_case("EMPTY")
        {
            self.pos = at + len;
            return Ok(Geometry::empty(kind));
        }
        self.body(kind, declared, depth)
    }

    /// Fix the dimension of the whole geometry. A declared tag that differs
    /// from coordinates already read is a mixed-dimension error.
    fn declare(&mut self, dimension: Dimension, at: usize) -> Result<(), WktError> {
        match self.ordinates.dimension() {
            None => {
                self.ordinates = OrdinatesBuilder::with_dimension(dimension);
                Ok(())
            }
            Some(existing) if existing == dimension => Ok(()),
            Some(existing) => Err(self.error(
                at,
                WktErrorKind::MemberDimension {
                    parent: existing,
                    member: dimension,
                },
            )),
        }
    }

    fn body(
        &mut self,
        kind: GeometryKind,
        declared: Option<Dimension>,
        depth: usize,
    ) -> Result<Geometry, WktError> {
        Ok(match kind {
            GeometryKind::Point => {
                self.expect(Token::Open, "'('")?;
                let c = self.coord(declared)?;
                self.expect(Token::Close, "')'")?;
                Geometry::Point { coordinates: c }
            }
            GeometryKind::LineString => Geometry::LineString {
                coordinates: self.coord_list(declared)?,
            },
            GeometryKind::Polygon => Geometry::Polygon {
                coordinates: self.ring_list(declared)?,
            },
            GeometryKind::MultiPoint => Geometry::MultiPoint {
                coordinates: self.multipoint(declared)?,
            },
            GeometryKind::MultiLineString => Geometry::MultiLineString {
                coordinates: self.ring_list(declared)?,
            },
            GeometryKind::MultiPolygon => {
                self.expect(Token::Open, "'('")?;
                let mut polygons = vec![self.member_rings(declared, "MULTIPOLYGON")?];
                while self.comma_or_close()? {
                    polygons.push(self.member_rings(declared, "MULTIPOLYGON")?);
                }
                Geometry::MultiPolygon {
                    coordinates: polygons,
                }
            }
            GeometryKind::GeometryCollection => {
                self.expect(Token::Open, "'('")?;
                let mut geometries = vec![self.geometry(declared, depth - 1)?];
                while self.comma_or_close()? {
                    geometries.push(self.geometry(declared, depth - 1)?);
                }
                Geometry::GeometryCollection { geometries }
            }
        })
    }

    /// After a list item: `true` on `,`, `false` on `)`.
    fn comma_or_close(&mut self) -> Result<bool, WktError> {
        let (token, start) = self.next("',' or ')'")?;
        match token {
            Token::Comma => Ok(true),
            Token::Close => Ok(false),
            other => Err(self.error(
                start,
                WktErrorKind::UnexpectedToken {
                    found: other.text(),
                    expected: "',' or ')'",
                },
            )),
        }
    }

    /// `(c, c, ...)`.
    fn coord_list(&mut self, declared: Option<Dimension>) -> Result<Vec<[f64; 2]>, WktError> {
        self.expect(Token::Open, "'('")?;
        let mut coords = vec![self.coord(declared)?];
        while self.comma_or_close()? {
            coords.push(self.coord(declared)?);
        }
        Ok(coords)
    }

    /// `((c, ...), (c, ...))`.
    fn ring_list(&mut self, declared: Option<Dimension>) -> Result<Vec<Vec<[f64; 2]>>, WktError> {
        self.expect(Token::Open, "'('")?;
        let mut rings = vec![self.coord_list(declared)?];
        while self.comma_or_close()? {
            rings.push(self.coord_list(declared)?);
        }
        Ok(rings)
    }

    /// One polygon of a MULTIPOLYGON. `EMPTY` here is refused.
    fn member_rings(
        &mut self,
        declared: Option<Dimension>,
        kind: &'static str,
    ) -> Result<Vec<Vec<[f64; 2]>>, WktError> {
        self.refuse_empty_member(kind)?;
        self.ring_list(declared)
    }

    fn refuse_empty_member(&mut self, kind: &'static str) -> Result<(), WktError> {
        if let Some((Token::Word(w), start, _)) = self.peek()
            && w.eq_ignore_ascii_case("EMPTY")
        {
            return Err(self.error(start, WktErrorKind::EmptyMember { kind }));
        }
        Ok(())
    }

    /// `(c, c)` or `((c), (c))`.
    fn multipoint(&mut self, declared: Option<Dimension>) -> Result<Vec<[f64; 2]>, WktError> {
        self.expect(Token::Open, "'('")?;
        let mut points = Vec::new();
        loop {
            self.refuse_empty_member("MULTIPOINT")?;
            let wrapped = matches!(self.peek(), Some((Token::Open, _, _)));
            if wrapped {
                self.expect(Token::Open, "'('")?;
            }
            points.push(self.coord(declared)?);
            if wrapped {
                self.expect(Token::Close, "')'")?;
            }
            if !self.comma_or_close()? {
                return Ok(points);
            }
        }
    }

    /// One coordinate: 2 to 4 numbers.
    fn coord(&mut self, declared: Option<Dimension>) -> Result<[f64; 2], WktError> {
        let start = self.peek().map_or(self.pos, |(_, s, _)| s);
        let mut numbers = Vec::with_capacity(4);
        while let Some((Token::Number(text), at, len)) = self.peek() {
            let value = text
                .parse::<f64>()
                .ok()
                .filter(|v| v.is_finite())
                .ok_or_else(|| self.error(at, WktErrorKind::BadNumber { text: text.into() }))?;
            numbers.push(value);
            self.pos = at + len;
            if numbers.len() > 4 {
                break;
            }
        }
        let dimension = match declared {
            Some(d) => {
                if numbers.len() != usize::from(d.coord_dim()) {
                    return Err(self.error(
                        start,
                        WktErrorKind::CoordinateCount {
                            found: numbers.len(),
                            expected: d.coord_dim(),
                            dimension: d,
                        },
                    ));
                }
                d
            }
            None => match numbers.len() {
                2 => Dimension::Xy,
                3 => Dimension::Xyz,
                4 => Dimension::Xyzm,
                found => {
                    return Err(self.error(start, WktErrorKind::UntaggedCount { found }));
                }
            },
        };
        let z = dimension.has_z().then(|| numbers[2]);
        let m = dimension
            .has_m()
            .then(|| numbers[if dimension.has_z() { 3 } else { 2 }]);
        self.ordinates
            .push(z, m)
            .map_err(|e| self.error(start, e.into()))?;
        Ok([numbers[0], numbers[1]])
    }
}

fn unknown_type(word: &str) -> WktErrorKind {
    WktErrorKind::UnknownType { name: word.into() }
}

/// Type keywords, longest first so `MULTIPOINT` never reads as `POINT`.
const KEYWORDS: [GeometryKind; 7] = [
    GeometryKind::GeometryCollection,
    GeometryKind::MultiLineString,
    GeometryKind::MultiPolygon,
    GeometryKind::LineString,
    GeometryKind::MultiPoint,
    GeometryKind::Polygon,
    GeometryKind::Point,
];

/// A type keyword with an optional attached dimension tag (`POINTZ`).
fn split_keyword(word: &str) -> Option<(GeometryKind, Option<Dimension>)> {
    KEYWORDS.iter().find_map(|kind| {
        let rest = strip_prefix_ci(word, kind.wkt_keyword())?;
        if rest.is_empty() {
            Some((*kind, None))
        } else {
            dimension_tag(rest).map(|d| (*kind, Some(d)))
        }
    })
}

fn dimension_tag(word: &str) -> Option<Dimension> {
    if word.eq_ignore_ascii_case("Z") {
        Some(Dimension::Xyz)
    } else if word.eq_ignore_ascii_case("M") {
        Some(Dimension::Xym)
    } else if word.eq_ignore_ascii_case("ZM") {
        Some(Dimension::Xyzm)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::super::write::{geometry_to_ewkt, geometry_to_wkt};
    use super::*;
    use nodedb_types::geometry::{OrdinateError, Ordinates};

    fn shape(text: &str) -> Geometry {
        geometry_from_wkt(text).unwrap().geometry().clone()
    }

    fn round_trip(g: SridGeometry) {
        let text = geometry_to_wkt(&g);
        assert_eq!(geometry_from_wkt(&text), Ok(g), "{text}");
    }

    #[test]
    fn xy_shapes_round_trip() {
        for g in [
            Geometry::point(-73.9857, 40.7484),
            Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0], [2.0, 0.0]]),
            Geometry::polygon(vec![
                vec![
                    [0.0, 0.0],
                    [10.0, 0.0],
                    [10.0, 10.0],
                    [0.0, 10.0],
                    [0.0, 0.0],
                ],
                vec![[2.0, 2.0], [8.0, 2.0], [8.0, 8.0], [2.0, 8.0], [2.0, 2.0]],
            ]),
            Geometry::MultiPoint {
                coordinates: vec![[1.0, 2.0], [3.0, 4.0]],
            },
            Geometry::MultiLineString {
                coordinates: vec![vec![[0.0, 0.0], [1.0, 1.0]], vec![[2.0, 2.0], [3.0, 3.0]]],
            },
            Geometry::MultiPolygon {
                coordinates: vec![
                    vec![vec![[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]],
                    vec![vec![[5.0, 5.0], [6.0, 5.0], [6.0, 6.0], [5.0, 5.0]]],
                ],
            },
            Geometry::GeometryCollection {
                geometries: vec![
                    Geometry::point(1.0, 2.0),
                    Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]),
                ],
            },
        ] {
            round_trip(SridGeometry::wgs84(g));
        }
    }

    #[test]
    fn three_numbers_are_z_not_dropped() {
        let g = geometry_from_wkt("POINT(1 2 3)").unwrap();
        assert_eq!(g.dimension(), Dimension::Xyz);
        assert_eq!(g.ordinates().z(), Some(&[3.0][..]));
        let zm = geometry_from_wkt("POINT(1 2 3 4)").unwrap();
        assert_eq!(zm.dimension(), Dimension::Xyzm);
        assert_eq!(zm.ordinates().m(), Some(&[4.0][..]));
    }

    #[test]
    fn every_tag_spelling_parses() {
        for (text, dim) in [
            ("POINT Z (1 2 3)", Dimension::Xyz),
            ("POINTZ(1 2 3)", Dimension::Xyz),
            ("point z(1 2 3)", Dimension::Xyz),
            ("POINT M (1 2 3)", Dimension::Xym),
            ("POINTM(1 2 3)", Dimension::Xym),
            ("POINT ZM (1 2 3 4)", Dimension::Xyzm),
            ("POINTZM(1 2 3 4)", Dimension::Xyzm),
            ("LINESTRING Z (0 0 1, 1 1 2)", Dimension::Xyz),
            ("MULTIPOINT M ((0 0 1), (1 1 2))", Dimension::Xym),
            (
                "POLYGON ZM ((0 0 0 0, 1 0 0 0, 1 1 0 0, 0 0 0 0))",
                Dimension::Xyzm,
            ),
        ] {
            let g = geometry_from_wkt(text).unwrap_or_else(|e| panic!("{text}: {e}"));
            assert_eq!(g.dimension(), dim, "{text}");
        }
        let m = geometry_from_wkt("POINT M (1 2 9)").unwrap();
        assert_eq!(m.ordinates().m(), Some(&[9.0][..]));
        assert_eq!(m.ordinates().z(), None);
    }

    #[test]
    fn mismatched_counts_fail() {
        for text in [
            "POINT Z (1 2)",
            "POINT M (1 2 3 4)",
            "POINT ZM (1 2 3)",
            "POINT(1)",
            "POINT(1 2 3 4 5)",
        ] {
            assert!(geometry_from_wkt(text).is_err(), "{text}");
        }
        let mixed = geometry_from_wkt("LINESTRING(0 0, 1 1 1)").unwrap_err();
        assert!(
            matches!(
                mixed.kind,
                WktErrorKind::Ordinates(OrdinateError::MixedDimension { .. })
            ),
            "{mixed}"
        );
    }

    #[test]
    fn collection_members_share_the_dimension() {
        let g = geometry_from_wkt("GEOMETRYCOLLECTION Z (POINT Z (1 2 3), POINT(4 5 6))").unwrap();
        assert_eq!(g.ordinates().z(), Some(&[3.0, 6.0][..]));
        assert!(geometry_from_wkt("GEOMETRYCOLLECTION Z (POINT M (1 2 3))").is_err());
        assert!(geometry_from_wkt("GEOMETRYCOLLECTION(POINT Z (1 2 3), POINT(1 2))").is_err());
        let m = geometry_from_wkt("GEOMETRYCOLLECTION M (POINT(1 2 3))").unwrap();
        assert_eq!(m.ordinates().m(), Some(&[3.0][..]));
    }

    #[test]
    fn z_m_and_zm_round_trip() {
        let line = Geometry::line_string(vec![[0.0, 0.0], [1.5, -2.25]]);
        for ordinates in [
            Ordinates::new(Some(vec![1.0, 2.0]), None),
            Ordinates::new(None, Some(vec![3.0, 4.0])),
            Ordinates::new(Some(vec![1.0, 2.0]), Some(vec![3.0, 4.0])),
        ] {
            round_trip(SridGeometry::with_ordinates(Srid::WGS84, line.clone(), ordinates).unwrap());
        }
        let gc = SridGeometry::with_ordinates(
            Srid::WGS84,
            Geometry::GeometryCollection {
                geometries: vec![Geometry::point(1.0, 1.0), Geometry::empty_point()],
            },
            Ordinates::new(Some(vec![7.0]), None),
        )
        .unwrap();
        let text = geometry_to_wkt(&gc);
        assert_eq!(text, "GEOMETRYCOLLECTION Z (POINT Z (1 1 7),POINT Z EMPTY)");
        let back = geometry_from_wkt(&text).unwrap();
        assert_eq!(back.ordinates().z(), Some(&[7.0][..]));
    }

    #[test]
    fn empty_of_every_kind() {
        for (text, kind) in [
            ("POINT EMPTY", GeometryKind::Point),
            ("LINESTRING EMPTY", GeometryKind::LineString),
            ("POLYGON EMPTY", GeometryKind::Polygon),
            ("MULTIPOINT EMPTY", GeometryKind::MultiPoint),
            ("MULTILINESTRING EMPTY", GeometryKind::MultiLineString),
            ("MULTIPOLYGON EMPTY", GeometryKind::MultiPolygon),
            ("GEOMETRYCOLLECTION EMPTY", GeometryKind::GeometryCollection),
        ] {
            let g = geometry_from_wkt(text).unwrap();
            assert!(g.is_empty(), "{text}");
            assert_eq!(g.geometry().kind(), kind);
            assert_eq!(geometry_to_wkt(&g), text);
        }
        let z = geometry_from_wkt("POINT Z EMPTY").unwrap();
        assert_eq!(z.dimension(), Dimension::Xyz);
        assert_eq!(geometry_to_wkt(&z), "POINT Z EMPTY");
    }

    #[test]
    fn postgis_text_form() {
        let g = geometry_from_wkt("LINESTRING (0 0, 1 1)").unwrap();
        assert_eq!(geometry_to_wkt(&g), "LINESTRING(0 0,1 1)");
        let z = geometry_from_wkt("POINT(1 2 3)").unwrap();
        assert_eq!(geometry_to_wkt(&z), "POINT Z (1 2 3)");
    }

    #[test]
    fn case_insensitive_and_multipoint_forms() {
        assert_eq!(shape("point(5 10)"), Geometry::point(5.0, 10.0));
        assert_eq!(
            shape("MULTIPOINT(1 2, 3 4)"),
            shape("MULTIPOINT((1 2), (3 4))")
        );
    }

    #[test]
    fn ewkt_prefix_is_case_insensitive() {
        let square = Geometry::polygon(vec![vec![[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]]);
        for text in [
            "SRID=4326;POLYGON((0 0, 1 0, 1 1, 0 0))",
            "srid=4326;polygon((0 0, 1 0, 1 1, 0 0))",
            "  SrId= 4326 ;POLYGON((0 0, 1 0, 1 1, 0 0))",
        ] {
            let parsed = geometry_from_ewkt(text).unwrap();
            assert_eq!(parsed.srid, Some(4326), "{text}");
            assert_eq!(parsed.geometry.geometry(), &square, "{text}");
        }
        let g = SridGeometry::new(Srid::new(32648).unwrap(), Geometry::point(1.0, 2.0));
        assert_eq!(geometry_to_ewkt(&g), "SRID=32648;POINT(1 2)");
    }

    #[test]
    fn ewkt_without_prefix_has_no_srid() {
        let parsed = geometry_from_ewkt("POINT(5 10)").unwrap();
        assert_eq!(parsed.srid, None);
        assert_eq!(parsed.geometry.geometry(), &Geometry::point(5.0, 10.0));
    }

    #[test]
    fn malformed_text_is_an_error() {
        for text in [
            "SRID=;POINT(1 2)",
            "SRID=43x6;POINT(1 2)",
            "SRID=-4326;POINT(1 2)",
            "SRID=4326 POINT(1 2)",
            "SRID=4326;GARBAGE",
            "SRÍD=4326;POINT(1 2)",
        ] {
            assert!(geometry_from_ewkt(text).is_err(), "{text}");
        }
        for text in [
            "",
            "GARBAGE(1 2)",
            "POINT(abc def)",
            "POIÑT(1 2)",
            "é",
            "POINT(1 2",
            "POINT(1 2) trailing",
            "POINT(nan 2)",
            "MULTIPOINT(EMPTY, (1 2))",
        ] {
            assert!(geometry_from_wkt(text).is_err(), "{text}");
        }
    }

    #[test]
    fn error_reports_the_offset() {
        let e = geometry_from_wkt("POINT(1 x)").unwrap_err();
        assert_eq!(e.offset, 6);
    }

    #[test]
    fn collection_depth_is_bounded() {
        let mut text = String::new();
        for _ in 0..70 {
            text.push_str("GEOMETRYCOLLECTION(");
        }
        text.push_str("POINT(1 2)");
        for _ in 0..70 {
            text.push(')');
        }
        assert!(matches!(
            geometry_from_wkt(&text).map_err(|e| e.kind),
            Err(WktErrorKind::TooDeep { .. })
        ));
    }
}
