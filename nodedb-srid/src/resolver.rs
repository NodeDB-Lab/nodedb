// SPDX-License-Identifier: Apache-2.0

//! The resolver a crate that carries SRIDs checks them against.

use crate::error::UnknownSrid;
use crate::kind::CrsKind;
use crate::name::code_from_name;
use crate::srid::Srid;

/// Answers whether an SRID code or a CRS name is defined, and the kind of
/// a defined CRS.
///
/// `nodedb-crs` implements it for its per-statement CRS context, so a crate
/// that depends on this crate alone checks SRIDs without linking the EPSG
/// table.
pub trait SridResolver {
    /// The SRID of `code`, when a CRS with that code is defined.
    fn resolve_u32(&self, code: u32) -> Result<Srid, UnknownSrid>;

    /// Whether `srid` is geographic or projected.
    fn kind(&self, srid: Srid) -> Result<CrsKind, UnknownSrid>;

    /// The SRID a CRS name refers to: every form [`code_from_name`]
    /// accepts, checked with [`SridResolver::resolve_u32`].
    fn resolve_name(&self, name: &str) -> Result<Srid, UnknownSrid> {
        let code =
            code_from_name(name).ok_or_else(|| UnknownSrid::UnknownName { name: name.into() })?;
        let code = u32::try_from(code).map_err(|_| UnknownSrid::OutOfRange { code })?;
        self.resolve_u32(code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Defines EPSG:4326 only.
    struct OnlyWgs84;

    impl SridResolver for OnlyWgs84 {
        fn resolve_u32(&self, code: u32) -> Result<Srid, UnknownSrid> {
            let srid = Srid::new(code).map_err(|_| UnknownSrid::OutOfRange {
                code: i64::from(code),
            })?;
            if srid == Srid::WGS84 {
                Ok(srid)
            } else {
                Err(UnknownSrid::Undefined { code })
            }
        }

        fn kind(&self, _srid: Srid) -> Result<CrsKind, UnknownSrid> {
            Ok(CrsKind::Geographic)
        }
    }

    #[test]
    fn names_resolve_through_the_code_check() {
        assert_eq!(OnlyWgs84.resolve_name("EPSG:4326"), Ok(Srid::WGS84));
        assert_eq!(OnlyWgs84.resolve_name("CRS84"), Ok(Srid::WGS84));
        assert_eq!(
            OnlyWgs84.resolve_name("EPSG:3857"),
            Err(UnknownSrid::Undefined { code: 3857 })
        );
        assert!(matches!(
            OnlyWgs84.resolve_name("ESRI:102100"),
            Err(UnknownSrid::UnknownName { .. })
        ));
        assert_eq!(
            OnlyWgs84.resolve_name("EPSG:99999999999"),
            Err(UnknownSrid::OutOfRange {
                code: 99_999_999_999
            })
        );
    }
}
