// SPDX-License-Identifier: Apache-2.0

//! One entry point for every textual and binary geometry form.
//!
//! Text starting with `{` is GeoJSON. Text of hex digits only is (E)WKB in
//! hex, as PostGIS prints it. Any other text is EWKT or WKT. Bytes are WKB
//! or EWKB.
//!
//! An input parse resolves the SRID through a resolver, so an undefined
//! SRID is refused. A stored parse checks the SRID's syntax only.

use nodedb_crs::{SridResolver, UnknownSrid};
use nodedb_types::geometry::{Srid, SridGeometry, decode_stored_geojson, parse_geojson};

use crate::error::SpatialError;
use crate::wkb::geometry_from_wkb;
use crate::wkt::geometry_from_ewkt;

/// How a parsed SRID code becomes an [`Srid`].
#[derive(Clone, Copy)]
pub enum SridCheck<'a> {
    /// Resolve through a resolver: an undefined code fails.
    Resolve(&'a dyn SridResolver),
    /// Check the code range only, for stored data.
    Syntax,
}

impl SridCheck<'_> {
    fn code(&self, code: i64) -> Result<Srid, UnknownSrid> {
        let in_range = u32::try_from(code)
            .ok()
            .and_then(|c| Srid::new(c).ok())
            .ok_or(UnknownSrid::OutOfRange { code })?;
        match self {
            SridCheck::Resolve(ctx) => ctx.resolve_u32(in_range.code()),
            SridCheck::Syntax => Ok(in_range),
        }
    }
}

/// Parse a geometry text in any accepted form.
pub fn geometry_from_text(text: &str, check: SridCheck<'_>) -> Result<SridGeometry, SpatialError> {
    let trimmed = text.trim();
    if trimmed.starts_with('{') {
        return Ok(match check {
            SridCheck::Resolve(ctx) => parse_geojson(trimmed, ctx)?,
            SridCheck::Syntax => decode_stored_geojson(trimmed.as_bytes())?,
        });
    }
    if is_hex(trimmed) {
        let bytes = decode_hex(trimmed);
        return geometry_from_bytes(&bytes, check);
    }
    let ewkt = geometry_from_ewkt(trimmed)?;
    Ok(match ewkt.srid {
        Some(code) => ewkt.geometry.with_srid(check.code(code)?),
        None => ewkt.geometry,
    })
}

/// Parse WKB or EWKB bytes. ISO WKB is WGS 84.
pub fn geometry_from_bytes(
    bytes: &[u8],
    check: SridCheck<'_>,
) -> Result<SridGeometry, SpatialError> {
    let wkb = geometry_from_wkb(bytes)?;
    Ok(match wkb.srid {
        Some(code) => wkb.geometry.with_srid(check.code(i64::from(code))?),
        None => wkb.geometry,
    })
}

/// Whether `text` is an even-length run of hex digits long enough to hold
/// a WKB header.
fn is_hex(text: &str) -> bool {
    text.len() >= 10 && text.len().is_multiple_of(2) && text.bytes().all(|b| b.is_ascii_hexdigit())
}

fn decode_hex(text: &str) -> Vec<u8> {
    text.as_bytes()
        .chunks(2)
        .map(|pair| (hex_value(pair[0]) << 4) | hex_value(pair[1]))
        .collect()
}

/// Value of one ASCII hex digit. [`is_hex`] admits only hex digits.
fn hex_value(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        _ => b - b'A' + 10,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::Geometry;

    fn ctx() -> nodedb_crs::CrsContext<'static> {
        nodedb_crs::CrsContext::builtin_only()
    }

    #[test]
    fn every_text_form_parses() {
        let ctx = ctx();
        let check = SridCheck::Resolve(&ctx);
        let want = SridGeometry::wgs84(Geometry::point(1.0, 2.0));
        assert_eq!(geometry_from_text("POINT(1 2)", check), Ok(want.clone()));
        assert_eq!(
            geometry_from_text(r#"{"type":"Point","coordinates":[1,2]}"#, check),
            Ok(want.clone())
        );
        assert_eq!(
            geometry_from_text("0101000000000000000000F03F0000000000000040", check),
            Ok(want)
        );
        let ewkt = geometry_from_text("SRID=32648;POINT(1 2)", check).unwrap();
        assert_eq!(ewkt.srid.code(), 32648);
    }

    #[test]
    fn undefined_srid_fails_only_on_input() {
        let ctx = ctx();
        assert!(matches!(
            geometry_from_text("SRID=999;POINT(1 2)", SridCheck::Resolve(&ctx)),
            Err(SpatialError::Srid(UnknownSrid::Undefined { code: 999 }))
        ));
        assert_eq!(
            geometry_from_text("SRID=999;POINT(1 2)", SridCheck::Syntax).map(|g| g.srid.code()),
            Ok(999)
        );
    }

    #[test]
    fn malformed_text_is_an_error() {
        assert!(geometry_from_text("POINT(", SridCheck::Syntax).is_err());
        assert!(geometry_from_text("not a geometry", SridCheck::Syntax).is_err());
    }
}
