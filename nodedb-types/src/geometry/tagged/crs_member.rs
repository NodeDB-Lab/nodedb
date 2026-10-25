// SPDX-License-Identifier: Apache-2.0

//! The GeoJSON 2008 `crs` member.
//!
//! GeoJSON (RFC 7946) fixes the reference system to WGS 84. A geometry in any
//! other SRID carries the 2008 GeoJSON `crs` member:
//! `"crs":{"type":"name","properties":{"name":"EPSG:<code>"}}`, or
//! `NODEDB:<code>` for a NodeDB or user-defined code. A WGS 84 geometry
//! writes no `crs` member, so its text stays plain RFC 7946.
//! Input also accepts the legacy linked form GDAL and old PostGIS write:
//! `"crs":{"type":"EPSG","properties":{"code":<code>}}`.
//!
//! Input paths resolve a `crs` member through a [`SridResolver`], so an
//! undefined SRID is refused. Stored geometry decodes with a syntax check
//! only, so a row written under a CRS that is later dropped still decodes.

use nodedb_srid::{SridResolver, UnknownSrid, code_from_name};
use serde::{Deserialize, Serialize};

use super::super::srid::Srid;
use super::geojson_read::GeoJsonError;

/// A GeoJSON 2008 `crs` member: the named form, or the legacy `EPSG` form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrsMember {
    #[serde(rename = "type")]
    pub kind: String,
    pub properties: CrsProperties,
}

/// The `properties` of a `crs` member. The named form sets `name`, the
/// legacy `EPSG` form sets `code`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct CrsProperties {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<CrsCode>,
}

/// The `code` of a legacy `EPSG` member: a number, or a decimal string.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CrsCode {
    Number(i64),
    Text(String),
}

/// The `type` value of a named `crs` member.
const CRS_KIND_NAME: &str = "name";

/// The `type` value of a legacy `EPSG` `crs` member.
const CRS_KIND_EPSG: &str = "EPSG";

/// What a `crs` member refers to.
enum CrsRef<'a> {
    Name(&'a str),
    Code(i64),
}

impl CrsMember {
    pub(super) fn for_srid(srid: Srid) -> Self {
        Self {
            kind: CRS_KIND_NAME.to_string(),
            properties: CrsProperties {
                name: Some(srid.crs_name()),
                code: None,
            },
        }
    }

    /// The name or code the member refers to.
    fn reference(&self) -> Result<CrsRef<'_>, UnknownSrid> {
        let invalid = |reason: &str| UnknownSrid::InvalidCrsMember {
            reason: reason.into(),
        };
        if self.kind.eq_ignore_ascii_case(CRS_KIND_NAME) {
            let name = self
                .properties
                .name
                .as_deref()
                .ok_or_else(|| invalid("type 'name' has no properties.name"))?;
            return Ok(CrsRef::Name(name.trim()));
        }
        if self.kind.eq_ignore_ascii_case(CRS_KIND_EPSG) {
            let code = match &self.properties.code {
                Some(CrsCode::Number(n)) => *n,
                Some(CrsCode::Text(t)) => t
                    .trim()
                    .parse::<i64>()
                    .map_err(|_| invalid("type 'EPSG' has a non-integer properties.code"))?,
                None => return Err(invalid("type 'EPSG' has no properties.code")),
            };
            return Ok(CrsRef::Code(code));
        }
        Err(UnknownSrid::InvalidCrsMember {
            reason: format!("type '{}'", self.kind).into(),
        })
    }
}

/// `code` as an SRID, checking the range only.
fn srid_in_range(code: i64) -> Result<Srid, UnknownSrid> {
    u32::try_from(code)
        .ok()
        .and_then(|c| Srid::new(c).ok())
        .ok_or(UnknownSrid::OutOfRange { code })
}

/// Resolve an optional `crs` member to an SRID `ctx` defines. No member
/// means WGS 84.
///
/// Names take every form [`SridResolver::resolve_name`] accepts: `EPSG:<n>`,
/// OGC URNs and URLs, the OGC CRS84 names and `SR-ORG:6842`. A legacy
/// `EPSG` member resolves its code.
pub fn srid_from_crs(
    crs: Option<&CrsMember>,
    ctx: &dyn SridResolver,
) -> Result<Srid, GeoJsonError> {
    let Some(member) = crs else {
        return Ok(Srid::WGS84);
    };
    let srid = match member.reference()? {
        CrsRef::Name(name) => ctx.resolve_name(name)?,
        CrsRef::Code(code) => {
            let code = u32::try_from(code).map_err(|_| UnknownSrid::OutOfRange { code })?;
            ctx.resolve_u32(code)?
        }
    };
    Ok(srid)
}

/// The SRID an optional `crs` member names, checking the member form and
/// the code range only.
pub(super) fn stored_srid_from_crs(crs: Option<&CrsMember>) -> Result<Srid, GeoJsonError> {
    let Some(member) = crs else {
        return Ok(Srid::WGS84);
    };
    let code = match member.reference()? {
        CrsRef::Name(name) => {
            code_from_name(name).ok_or_else(|| UnknownSrid::UnknownName { name: name.into() })?
        }
        CrsRef::Code(code) => code,
    };
    Ok(srid_in_range(code)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn srid(code: u32) -> Srid {
        Srid::new(code).unwrap()
    }

    fn crs(name: &str) -> CrsMember {
        CrsMember {
            kind: "name".into(),
            properties: CrsProperties {
                name: Some(name.into()),
                code: None,
            },
        }
    }

    fn ctx() -> nodedb_crs::CrsContext<'static> {
        nodedb_crs::CrsContext::builtin_only()
    }

    #[test]
    fn every_accepted_crs_spelling_resolves() {
        let ctx = ctx();
        assert_eq!(srid_from_crs(Some(&crs("EPSG:3375")), &ctx), Ok(srid(3375)));
        assert_eq!(
            srid_from_crs(Some(&crs("urn:ogc:def:crs:EPSG::3376")), &ctx),
            Ok(srid(3376))
        );
        assert_eq!(
            srid_from_crs(Some(&crs("urn:ogc:def:crs:OGC:1.3:CRS84")), &ctx),
            Ok(Srid::WGS84)
        );
        assert_eq!(
            srid_from_crs(Some(&crs("epsg:32647")), &ctx),
            Ok(srid(32647))
        );
        assert_eq!(
            srid_from_crs(Some(&crs("SR-ORG:6842")), &ctx),
            Ok(Srid::MODIS_SINUSOIDAL)
        );
    }

    #[test]
    fn undefined_and_unknown_crs_names_fail() {
        let ctx = ctx();
        assert_eq!(
            srid_from_crs(Some(&crs("EPSG:999")), &ctx),
            Err(GeoJsonError::Srid(UnknownSrid::Undefined { code: 999 }))
        );
        assert!(matches!(
            srid_from_crs(Some(&crs("urn:x:y")), &ctx),
            Err(GeoJsonError::Srid(UnknownSrid::UnknownName { .. }))
        ));
        let linked = CrsMember {
            kind: "link".into(),
            properties: CrsProperties {
                name: Some("x".into()),
                code: None,
            },
        };
        assert!(matches!(
            srid_from_crs(Some(&linked), &ctx),
            Err(GeoJsonError::Srid(UnknownSrid::InvalidCrsMember { .. }))
        ));
    }
}
