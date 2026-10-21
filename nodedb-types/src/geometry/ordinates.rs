// SPDX-License-Identifier: Apache-2.0

//! Z and M ordinates carried beside a 2D [`Geometry`](super::Geometry).
//!
//! A shape holds XY only. Z (height) and M (measure) live in flat arrays,
//! one value per XY coordinate in the traversal order of
//! [`Geometry::coords`](super::Geometry::coords).

/// Which ordinates every coordinate of a geometry carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dimension {
    Xy,
    Xyz,
    Xym,
    Xyzm,
}

impl Dimension {
    /// The dimension with Z when `z`, M when `m`.
    pub fn from_flags(z: bool, m: bool) -> Self {
        match (z, m) {
            (false, false) => Dimension::Xy,
            (true, false) => Dimension::Xyz,
            (false, true) => Dimension::Xym,
            (true, true) => Dimension::Xyzm,
        }
    }

    pub fn has_z(self) -> bool {
        matches!(self, Dimension::Xyz | Dimension::Xyzm)
    }

    pub fn has_m(self) -> bool {
        matches!(self, Dimension::Xym | Dimension::Xyzm)
    }

    /// Number of ordinates per coordinate: 2, 3 or 4 (`ST_CoordDim`).
    pub fn coord_dim(self) -> u8 {
        2 + u8::from(self.has_z()) + u8::from(self.has_m())
    }

    /// The PostGIS `ST_Zmflag` code: 0 XY, 1 XYM, 2 XYZ, 3 XYZM.
    pub fn zm_flag(self) -> u8 {
        u8::from(self.has_m()) + 2 * u8::from(self.has_z())
    }

    /// The WKT dimension tag: empty, `Z`, `M` or `ZM`.
    pub fn wkt_tag(self) -> &'static str {
        match self {
            Dimension::Xy => "",
            Dimension::Xyz => "Z",
            Dimension::Xym => "M",
            Dimension::Xyzm => "ZM",
        }
    }
}

impl std::fmt::Display for Dimension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Dimension::Xy => "XY",
            Dimension::Xyz => "XYZ",
            Dimension::Xym => "XYM",
            Dimension::Xyzm => "XYZM",
        })
    }
}

/// One coordinate with its optional Z and M.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Position {
    pub x: f64,
    pub y: f64,
    pub z: Option<f64>,
    pub m: Option<f64>,
}

impl Position {
    /// An XY position.
    pub fn xy(x: f64, y: f64) -> Self {
        Self {
            x,
            y,
            z: None,
            m: None,
        }
    }

    pub fn dimension(&self) -> Dimension {
        Dimension::from_flags(self.z.is_some(), self.m.is_some())
    }
}

/// The Z or M ordinate, for error reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ordinate {
    Z,
    M,
}

impl std::fmt::Display for Ordinate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Ordinate::Z => "Z",
            Ordinate::M => "M",
        })
    }
}

/// Why a set of ordinates does not fit a geometry.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum OrdinateError {
    #[error("{ordinate} has {actual} values for {expected} coordinates")]
    LengthMismatch {
        ordinate: Ordinate,
        expected: usize,
        actual: usize,
    },
    #[error("{ordinate} value of coordinate {index} is not finite")]
    NonFinite { ordinate: Ordinate, index: usize },
    #[error("coordinate {index} is {found}, earlier coordinates are {expected}")]
    MixedDimension {
        index: usize,
        expected: Dimension,
        found: Dimension,
    },
}

/// Z and M values, one per XY coordinate, or absent.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Ordinates {
    z: Option<Box<[f64]>>,
    m: Option<Box<[f64]>>,
}

impl Ordinates {
    /// No Z, no M.
    pub fn xy() -> Self {
        Self::default()
    }

    /// Z and M arrays as given. [`super::SridGeometry::with_ordinates`]
    /// checks them against the shape.
    pub fn new(z: Option<Vec<f64>>, m: Option<Vec<f64>>) -> Self {
        Self {
            z: z.map(Vec::into_boxed_slice),
            m: m.map(Vec::into_boxed_slice),
        }
    }

    pub fn z(&self) -> Option<&[f64]> {
        self.z.as_deref()
    }

    pub fn m(&self) -> Option<&[f64]> {
        self.m.as_deref()
    }

    pub fn dimension(&self) -> Dimension {
        Dimension::from_flags(self.z.is_some(), self.m.is_some())
    }

    pub fn is_xy(&self) -> bool {
        self.z.is_none() && self.m.is_none()
    }

    /// Z and M as owned vectors.
    pub fn into_parts(self) -> (Option<Vec<f64>>, Option<Vec<f64>>) {
        (self.z.map(Vec::from), self.m.map(Vec::from))
    }

    /// Check one value per coordinate and finite values.
    pub fn check(&self, coord_count: usize) -> Result<(), OrdinateError> {
        check_one(self.z(), Ordinate::Z, coord_count)?;
        check_one(self.m(), Ordinate::M, coord_count)
    }

    /// The Z and M of coordinate `index`.
    pub fn at(&self, index: usize) -> (Option<f64>, Option<f64>) {
        (
            self.z().and_then(|z| z.get(index).copied()),
            self.m().and_then(|m| m.get(index).copied()),
        )
    }
}

fn check_one(
    values: Option<&[f64]>,
    ordinate: Ordinate,
    expected: usize,
) -> Result<(), OrdinateError> {
    let Some(values) = values else {
        return Ok(());
    };
    if values.len() != expected {
        return Err(OrdinateError::LengthMismatch {
            ordinate,
            expected,
            actual: values.len(),
        });
    }
    match values.iter().position(|v| !v.is_finite()) {
        Some(index) => Err(OrdinateError::NonFinite { ordinate, index }),
        None => Ok(()),
    }
}

/// Collects Z and M while a parser walks coordinates in traversal order.
///
/// The first coordinate fixes the dimension. A later coordinate with a
/// different one fails with [`OrdinateError::MixedDimension`].
#[derive(Debug, Clone, Default)]
pub struct OrdinatesBuilder {
    dimension: Option<Dimension>,
    count: usize,
    z: Vec<f64>,
    m: Vec<f64>,
}

impl OrdinatesBuilder {
    /// A builder whose dimension is fixed up front, as a WKT or WKB type
    /// tag declares it.
    pub fn with_dimension(dimension: Dimension) -> Self {
        Self {
            dimension: Some(dimension),
            ..Self::default()
        }
    }

    /// The dimension fixed so far.
    pub fn dimension(&self) -> Option<Dimension> {
        self.dimension
    }

    /// Record the Z and M of the next coordinate.
    pub fn push(&mut self, z: Option<f64>, m: Option<f64>) -> Result<(), OrdinateError> {
        let found = Dimension::from_flags(z.is_some(), m.is_some());
        let expected = *self.dimension.get_or_insert(found);
        if expected != found {
            return Err(OrdinateError::MixedDimension {
                index: self.count,
                expected,
                found,
            });
        }
        if let Some(z) = z {
            self.z.push(z);
        }
        if let Some(m) = m {
            self.m.push(m);
        }
        self.count += 1;
        Ok(())
    }

    /// The collected ordinates. A builder with no coordinates and no fixed
    /// dimension gives XY.
    pub fn finish(self) -> Ordinates {
        let dimension = self.dimension.unwrap_or(Dimension::Xy);
        Ordinates::new(
            dimension.has_z().then_some(self.z),
            dimension.has_m().then_some(self.m),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dimension_codes_match_postgis() {
        assert_eq!(Dimension::Xy.zm_flag(), 0);
        assert_eq!(Dimension::Xym.zm_flag(), 1);
        assert_eq!(Dimension::Xyz.zm_flag(), 2);
        assert_eq!(Dimension::Xyzm.zm_flag(), 3);
        assert_eq!(Dimension::Xyzm.coord_dim(), 4);
        assert_eq!(Dimension::Xym.coord_dim(), 3);
    }

    #[test]
    fn check_rejects_wrong_length_and_non_finite() {
        let o = Ordinates::new(Some(vec![1.0, 2.0]), None);
        assert!(o.check(2).is_ok());
        assert_eq!(
            o.check(3),
            Err(OrdinateError::LengthMismatch {
                ordinate: Ordinate::Z,
                expected: 3,
                actual: 2
            })
        );
        let nan = Ordinates::new(None, Some(vec![f64::NAN]));
        assert_eq!(
            nan.check(1),
            Err(OrdinateError::NonFinite {
                ordinate: Ordinate::M,
                index: 0
            })
        );
    }

    #[test]
    fn builder_refuses_mixed_dimensions() {
        let mut b = OrdinatesBuilder::default();
        b.push(Some(1.0), None).unwrap();
        assert_eq!(
            b.push(None, None),
            Err(OrdinateError::MixedDimension {
                index: 1,
                expected: Dimension::Xyz,
                found: Dimension::Xy
            })
        );
    }

    #[test]
    fn builder_with_fixed_dimension_and_no_coords_keeps_it() {
        let o = OrdinatesBuilder::with_dimension(Dimension::Xyzm).finish();
        assert_eq!(o.dimension(), Dimension::Xyzm);
        assert_eq!(o.z(), Some(&[][..]));
    }
}
