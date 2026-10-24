// SPDX-License-Identifier: Apache-2.0

//! Match a decoded CRS definition to the registered CRS it equals: a live
//! user-defined CRS of the context, or a built-in CRS.
//!
//! Two definitions have one shape when their projections have the same
//! method and parameters, their ellipsoids the same axes, their prime
//! meridians and axis orders agree, and their linear units are equal.
//!
//! The datum decides among candidates of one shape:
//! - A decoded datum with an EPSG code accepts only candidates on that
//!   datum code.
//! - A decoded datum without one accepts candidates where
//!   [`Datum::same_datum`] holds: same ellipsoid and a shift with the same
//!   effect. A GeoTIFF that names only an ellipsoid carries no shift, so it
//!   accepts every candidate with a null shift on that ellipsoid.
//! - When the accepted candidates sit on more than one EPSG datum, the file
//!   does not say which one it means. The match is refused as ambiguous, so
//!   the stored SRID never names a datum the file does not.
//!
//! Among accepted candidates on one datum the lowest code wins.

use nodedb_crs::registry::builtin_codes;
use nodedb_crs::{
    CrsContext, CrsDef, CrsKind, CustomCrsState, Datum, Ellipsoid, LinearUnit, ProjectionDef,
};
use nodedb_types::geometry::Srid;

use crate::error::RasterError;

/// Relative tolerance of a projection parameter, an axis or a unit.
const RELATIVE_TOLERANCE: f64 = 1e-9;

/// The registered SRID `def` equals.
///
/// Fails with [`RasterError::UnmatchedUserCrs`] when no candidate has the
/// shape and datum of `def`, and with [`RasterError::AmbiguousUserCrs`]
/// when the accepted candidates sit on two EPSG datums. Both name the PROJ
/// text of `def`.
pub fn match_registered(def: &CrsDef, ctx: &CrsContext<'_>) -> Result<Srid, RasterError> {
    let custom: Vec<Srid> = ctx
        .custom_set()
        .entries()
        .iter()
        .filter_map(|entry| match &entry.state {
            CustomCrsState::Live(live) if live.kind == def.kind => Some(entry.srid),
            CustomCrsState::Live(_) | CustomCrsState::Dropped => None,
        })
        .collect();
    let mut chosen: Option<(Srid, Option<u32>)> = None;
    for srid in builtin_codes(def.kind).chain(custom) {
        // A fork per candidate keeps the memo from growing over the whole
        // table.
        let Ok(candidate) = ctx.fork().resolve(srid) else {
            continue;
        };
        if !same_shape(def, &candidate) || !datum_accepts(&def.datum, &candidate.datum) {
            continue;
        }
        let datum_code = candidate.datum.epsg_code;
        chosen = match chosen {
            None => Some((srid, datum_code)),
            Some((first, first_code)) if first_code != datum_code => {
                return Err(RasterError::AmbiguousUserCrs {
                    proj: def.to_proj_string().unwrap_or_else(|_| def.to_wkt2()),
                    first: first.min(srid),
                    second: first.max(srid),
                });
            }
            Some((first, first_code)) => Some((first.min(srid), first_code)),
        };
    }
    chosen
        .map(|(srid, _)| srid)
        .ok_or_else(|| RasterError::UnmatchedUserCrs {
            proj: def.to_proj_string().unwrap_or_else(|_| def.to_wkt2()),
        })
}

/// Whether a candidate datum is one the decoded datum accepts.
fn datum_accepts(decoded: &Datum, candidate: &Datum) -> bool {
    match decoded.epsg_code {
        Some(code) => candidate.epsg_code == Some(code),
        None => decoded.same_datum(candidate),
    }
}

/// Whether `a` and `b` have equal projections, ellipsoids, prime
/// meridians, axis orders and units.
fn same_shape(a: &CrsDef, b: &CrsDef) -> bool {
    a.kind == b.kind
        && a.axis == b.axis
        && close(a.datum.prime_meridian_deg, b.datum.prime_meridian_deg)
        && same_ellipsoid(&a.datum.ellipsoid, &b.datum.ellipsoid)
        && same_unit(&a.unit, &b.unit, a.kind)
        && same_projection(a.projection.as_ref(), b.projection.as_ref())
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= RELATIVE_TOLERANCE * a.abs().max(b.abs()).max(1.0)
}

fn same_ellipsoid(a: &Ellipsoid, b: &Ellipsoid) -> bool {
    close(a.a, b.a) && (a.is_sphere() == b.is_sphere()) && close(a.inv_f, b.inv_f)
}

/// Geographic coordinates are degrees whatever the unit field holds.
fn same_unit(a: &LinearUnit, b: &LinearUnit, kind: CrsKind) -> bool {
    match kind {
        CrsKind::Geographic => true,
        CrsKind::Projected => close(a.metres_per_unit, b.metres_per_unit),
    }
}

/// Same method and every parameter within tolerance. The parameters are
/// compared through their PROJ text, so every method compares one way.
fn same_projection(a: Option<&ProjectionDef>, b: Option<&ProjectionDef>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            if core::mem::discriminant(a) != core::mem::discriminant(b) {
                return false;
            }
            let (mut ta, mut tb) = (String::new(), String::new());
            a.write_proj(&mut ta);
            b.write_proj(&mut tb);
            same_tokens(&ta, &tb)
        }
        (Some(_), None) | (None, Some(_)) => false,
    }
}

/// Whether two PROJ parameter lists hold the same keys in the same order,
/// with numeric values within tolerance and other values equal.
fn same_tokens(a: &str, b: &str) -> bool {
    let mut ia = a.split_whitespace();
    let mut ib = b.split_whitespace();
    loop {
        match (ia.next(), ib.next()) {
            (None, None) => return true,
            (Some(x), Some(y)) => {
                let (kx, vx) = x.split_once('=').unwrap_or((x, ""));
                let (ky, vy) = y.split_once('=').unwrap_or((y, ""));
                if kx != ky {
                    return false;
                }
                let equal = match (vx.parse::<f64>(), vy.parse::<f64>()) {
                    (Ok(nx), Ok(ny)) => close(nx, ny),
                    _ => vx == vy,
                };
                if !equal {
                    return false;
                }
            }
            (Some(_), None) | (None, Some(_)) => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_crs::{CustomCrsSet, parse_proj_string};

    #[test]
    fn a_builtin_matches_itself() {
        let ctx = CrsContext::builtin_only();
        let srid = Srid::new(32650).unwrap();
        let def = ctx.resolve(srid).unwrap();
        assert_eq!(match_registered(&def, &ctx).ok(), Some(srid));
    }

    /// A definition with no built-in twin matches the user-defined CRS of
    /// the context that equals it.
    #[test]
    fn a_custom_definition_matches_its_code() {
        let custom = Srid::new(910_042).unwrap();
        let proj = "+proj=tmerc +lat_0=0 +lon_0=101.25 +k=0.99991 +x_0=123456 +y_0=0 \
                    +ellps=GRS80 +units=m";
        let mut set = CustomCrsSet::new();
        set.define(parse_proj_string(custom, "custom tm", proj).unwrap())
            .unwrap();
        let ctx = CrsContext::new(&set);
        let decoded = parse_proj_string(Srid::new(910_000).unwrap(), "decoded", proj).unwrap();
        assert_eq!(match_registered(&decoded, &ctx).ok(), Some(custom));
        let builtin_only = CrsContext::builtin_only();
        assert!(matches!(
            match_registered(&decoded, &builtin_only),
            Err(RasterError::UnmatchedUserCrs { .. })
        ));
    }

    #[test]
    fn tokens_compare_numbers_with_tolerance() {
        assert!(same_tokens("+proj=sinu +lon_0=0", "+proj=sinu +lon_0=0.0"));
        assert!(same_tokens("+k=0.9996", "+k=0.99960000000001"));
        assert!(!same_tokens("+k=0.9996", "+k=0.9997"));
        assert!(!same_tokens("+proj=sinu", "+proj=tmerc"));
    }
}
