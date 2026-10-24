// SPDX-License-Identifier: Apache-2.0

//! Description of one geospatial SQL function: every spelling that reaches it,
//! the shape of its argument list, and the kind of value it returns.

/// The argument list a geo function accepts.
///
/// This is the only place arity is declared. `nodedb-sql` maps each shape to
/// its `ArgTypeSpec` slice exhaustively, so a new shape cannot be introduced
/// without the planner being taught the types that go with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoArgShape {
    /// `(geom)`
    Geometry1,
    /// `(geom1, geom2)`
    Geometry2,
    /// `(geom1, geom2, distance_m)`
    Geometry2Distance,
    /// `(geom, srid)`
    GeometrySrid,
    /// `(geom, srid [, epoch])`: the epoch is a decimal year.
    GeometrySridEpoch,
    /// `(geom, distance_m [, segments])`
    GeometryBuffer,
    /// `(lng, lat)`
    LngLat,
    /// `(lng, lat [, precision])`
    LngLatPrecision,
    /// `(lat, lng, resolution)` — note the reversed leading pair relative to
    /// [`GeoArgShape::LngLatPrecision`]; the H3 surface names latitude first.
    LatLngResolution,
    /// `(text)`
    Text1,
    /// `(wkt [, srid])`
    TextSrid,
    /// `(wkb [, srid])`
    BytesSrid,
    /// `(lng1, lat1, lng2, lat2)`
    Haversine4,
    /// `(lng, lat, radius_m [, segments])`
    Circle,
    /// `(min_lng, min_lat, max_lng, max_lat)`
    Bbox,
    /// `(point, point, ...)`
    PointsVariadic,
    /// `(ring, ring, ...)`
    RingsVariadic,
    /// `(x, y [, z [, m]])`
    Xyzm,
    /// `(x, y, m)`
    Xym,
    /// `(geom, number)`
    GeometryNumber,
    /// `(geom [, number])`
    GeometryNumberOpt,
    /// `(geom [, number [, number]])`
    GeometryNumbers2Opt,
    /// `(geom, number [, number])`
    GeometryNumberNumberOpt,
    /// `(geom, number, number [, number])`
    GeometryRange,
    /// `(geom [, text])`
    GeometryTextOpt,
    /// `(geom1, geom2 [, text])`
    Geometry2TextOpt,
    /// `(z, x, y [, bounds])`
    TileEnvelope,
}

impl GeoArgShape {
    /// Minimum and maximum accepted argument counts.
    pub const fn arity(self) -> (usize, usize) {
        match self {
            Self::Geometry1 | Self::Text1 => (1, 1),
            Self::TextSrid | Self::BytesSrid | Self::GeometryNumberOpt | Self::GeometryTextOpt => {
                (1, 2)
            }
            Self::GeometryNumbers2Opt => (1, 3),
            Self::Geometry2 | Self::LngLat | Self::GeometrySrid | Self::GeometryNumber => (2, 2),
            Self::GeometryBuffer
            | Self::GeometrySridEpoch
            | Self::LngLatPrecision
            | Self::GeometryNumberNumberOpt
            | Self::Geometry2TextOpt => (2, 3),
            Self::Xyzm => (2, 4),
            Self::Geometry2Distance | Self::LatLngResolution | Self::Xym => (3, 3),
            Self::Circle | Self::GeometryRange | Self::TileEnvelope => (3, 4),
            Self::Haversine4 | Self::Bbox => (4, 4),
            Self::PointsVariadic => (2, MAX_VARIADIC),
            Self::RingsVariadic => (1, MAX_VARIADIC),
        }
    }
}

impl GeoArgShape {
    /// The zero-based position of the SRID argument, for a shape that has
    /// one: `(geom, srid)`, `(geom, srid [, epoch])`, `(wkt [, srid])` and
    /// `(wkb [, srid])`.
    pub const fn srid_position(self) -> Option<usize> {
        match self {
            Self::GeometrySrid | Self::GeometrySridEpoch | Self::TextSrid | Self::BytesSrid => {
                Some(1)
            }
            Self::Geometry1
            | Self::Geometry2
            | Self::Geometry2Distance
            | Self::GeometryBuffer
            | Self::LngLat
            | Self::LngLatPrecision
            | Self::LatLngResolution
            | Self::Text1
            | Self::Haversine4
            | Self::Circle
            | Self::Bbox
            | Self::PointsVariadic
            | Self::RingsVariadic
            | Self::Xyzm
            | Self::Xym
            | Self::GeometryNumber
            | Self::GeometryNumberOpt
            | Self::GeometryNumbers2Opt
            | Self::GeometryNumberNumberOpt
            | Self::GeometryRange
            | Self::GeometryTextOpt
            | Self::Geometry2TextOpt
            | Self::TileEnvelope => None,
        }
    }
}

/// Upper bound on a variadic geo constructor's argument count. Matches the
/// registry's existing cap for `geo_line` / `geo_polygon`.
pub const MAX_VARIADIC: usize = 255;

/// The kind of value a geo function produces.
///
/// `nodedb-sql` maps this to a `ColumnType` for plan-time typing. `Object`
/// covers the decoded-bounds shapes (geohash / H3 cell), which have no scalar
/// column type and are typed as unknown at plan time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoReturn {
    Bool,
    Float,
    Int,
    Text,
    Bytes,
    Geometry,
    Object,
    Array,
    /// The type depends on the argument count (`ST_Relate`: text with two
    /// arguments, boolean with a pattern). Typed at runtime.
    ByArity,
}

/// One geospatial SQL function.
#[derive(Debug, Clone, Copy)]
pub struct GeoFunctionSpec {
    /// Dispatch key. The evaluator matches on exactly this string, and every
    /// other spelling in `aliases` resolves to it before dispatch.
    pub canonical: &'static str,
    /// Additional accepted spellings — the internal `geo_*` names that predate
    /// the standard ones, plus legacy synonyms. Never includes `canonical`.
    pub aliases: &'static [&'static str],
    pub args: GeoArgShape,
    pub returns: GeoReturn,
}

impl GeoFunctionSpec {
    /// Every spelling that resolves to this function, canonical first.
    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_ {
        std::iter::once(self.canonical).chain(self.aliases.iter().copied())
    }

    /// Whether `name` (already lowercased) refers to this function.
    pub fn matches(&self, name: &str) -> bool {
        self.canonical == name || self.aliases.contains(&name)
    }
}

/// Compact constructor for the catalog table.
pub(super) const fn f(
    canonical: &'static str,
    aliases: &'static [&'static str],
    args: GeoArgShape,
    returns: GeoReturn,
) -> GeoFunctionSpec {
    GeoFunctionSpec {
        canonical,
        aliases,
        args,
        returns,
    }
}
