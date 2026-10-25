// SPDX-License-Identifier: Apache-2.0

//! Spatial reference system identifiers.

use core::num::NonZeroU32;

use serde::{Deserialize, Serialize};

/// First code of the NodeDB built-in range.
const NODEDB_FIRST: u32 = 990_000;

/// Last code of the NodeDB built-in range.
const NODEDB_LAST: u32 = 990_999;

/// First code above the EPSG range.
const EPSG_END: u32 = 900_000;

/// A spatial reference system identifier.
///
/// The code is checked for syntax only: `1..=MAX_CODE`. Whether a CRS with
/// that code exists is a registry question, answered by a
/// [`SridResolver`](crate::SridResolver). Serialized as its numeric code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(into = "u32", try_from = "u32")]
pub struct Srid(NonZeroU32);

/// Who assigns an SRID code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SridAuthority {
    /// EPSG, codes below 900 000.
    Epsg,
    /// NodeDB built-ins, codes 990 000 to 990 999.
    NodeDb,
    /// User-defined CRSs, codes 910 000 to 989 999.
    User,
    /// Codes no authority assigns: 900 000 to 909 999 and 991 000 to
    /// `Srid::MAX_CODE`.
    Reserved,
}

/// A code outside `1..=Srid::MAX_CODE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("SRID code {code} is outside 1..={max}", max = Srid::MAX_CODE)]
pub struct InvalidSridCode {
    pub code: u32,
}

/// A code outside the user-defined range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "SRID {code} is outside the user range {first}..={last}",
    first = Srid::USER_FIRST,
    last = Srid::USER_LAST
)]
pub struct NotUserSrid {
    pub code: u64,
}

impl Srid {
    /// Largest accepted code.
    pub const MAX_CODE: u32 = 998_999;

    /// First code of the user-defined range.
    pub const USER_FIRST: u32 = 910_000;

    /// Last code of the user-defined range.
    pub const USER_LAST: u32 = 989_999;

    /// EPSG:4326, WGS 84 geographic.
    pub const WGS84: Srid = Srid::known(4326);

    /// EPSG:3857, WGS 84 / Pseudo-Mercator, the web map tile grid.
    pub const WEB_MERCATOR: Srid = Srid::known(3857);

    /// NodeDB 990001, the MODIS sinusoidal grid (SR-ORG:6842).
    pub const MODIS_SINUSOIDAL: Srid = Srid::known(990_001);

    /// NodeDB 990002, the Himawari geostationary view.
    pub const HIMAWARI: Srid = Srid::known(990_002);

    /// Check `code` is in `1..=MAX_CODE`.
    pub const fn new(code: u32) -> Result<Srid, InvalidSridCode> {
        if code > Self::MAX_CODE {
            return Err(InvalidSridCode { code });
        }
        match NonZeroU32::new(code) {
            Some(n) => Ok(Srid(n)),
            None => Err(InvalidSridCode { code }),
        }
    }

    /// Build an SRID from a code known valid at compile time. Use it in a
    /// `const` item only: an invalid code fails the build there.
    pub const fn known(code: u32) -> Srid {
        match Srid::new(code) {
            Ok(srid) => srid,
            Err(_) => panic!("SRID constant out of range"),
        }
    }

    /// Check `code` is in the user-defined range. It takes `u64` so a SQL
    /// literal checks without a narrowing step.
    pub const fn new_user(code: u64) -> Result<Srid, NotUserSrid> {
        if code < Self::USER_FIRST as u64 || code > Self::USER_LAST as u64 {
            return Err(NotUserSrid { code });
        }
        match Srid::new(code as u32) {
            Ok(srid) => Ok(srid),
            Err(_) => Err(NotUserSrid { code }),
        }
    }

    /// The numeric code.
    pub const fn code(self) -> u32 {
        self.0.get()
    }

    /// The CRS name of this code: `EPSG:<code>` for an EPSG code,
    /// `NODEDB:<code>` for every other.
    pub fn crs_name(self) -> String {
        match self.authority() {
            SridAuthority::Epsg => format!("EPSG:{}", self.code()),
            SridAuthority::NodeDb | SridAuthority::User | SridAuthority::Reserved => {
                format!("{}:{}", crate::name::NODEDB_AUTHORITY, self.code())
            }
        }
    }

    /// The authority that assigns this code.
    pub const fn authority(self) -> SridAuthority {
        match self.code() {
            0..EPSG_END => SridAuthority::Epsg,
            Self::USER_FIRST..=Self::USER_LAST => SridAuthority::User,
            NODEDB_FIRST..=NODEDB_LAST => SridAuthority::NodeDb,
            _ => SridAuthority::Reserved,
        }
    }
}

impl Default for Srid {
    fn default() -> Self {
        Srid::WGS84
    }
}

impl From<Srid> for u32 {
    fn from(srid: Srid) -> u32 {
        srid.code()
    }
}

impl TryFrom<u32> for Srid {
    type Error = InvalidSridCode;

    fn try_from(code: u32) -> Result<Srid, InvalidSridCode> {
        Srid::new(code)
    }
}

impl core::fmt::Display for Srid {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.code())
    }
}

impl zerompk::ToMessagePack for Srid {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_u32(self.code())
    }
}

impl<'a> zerompk::FromMessagePack<'a> for Srid {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let code = reader.read_u32()?;
        Srid::new(code).map_err(|e| zerompk::Error::UnknownKey(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_is_one_to_max() {
        assert!(Srid::new(0).is_err());
        assert_eq!(Srid::new(1).map(Srid::code), Ok(1));
        assert_eq!(
            Srid::new(Srid::MAX_CODE).map(Srid::code),
            Ok(Srid::MAX_CODE)
        );
        assert_eq!(
            Srid::new(Srid::MAX_CODE + 1),
            Err(InvalidSridCode {
                code: Srid::MAX_CODE + 1
            })
        );
    }

    #[test]
    fn user_codes_are_the_user_range() {
        assert_eq!(
            Srid::new_user(u64::from(Srid::USER_FIRST)).map(Srid::code),
            Ok(Srid::USER_FIRST)
        );
        assert_eq!(
            Srid::new_user(u64::from(Srid::USER_LAST)).map(Srid::code),
            Ok(Srid::USER_LAST)
        );
        for code in [0, 4326, 909_999, 990_001, 999_999, u64::from(u32::MAX) + 1] {
            assert_eq!(Srid::new_user(code), Err(NotUserSrid { code }));
        }
        assert_eq!(
            NotUserSrid { code: 4326 }.to_string(),
            "SRID 4326 is outside the user range 910000..=989999"
        );
    }

    #[test]
    fn authority_by_range() {
        let auth = |c: u32| Srid::new(c).map(Srid::authority);
        assert_eq!(auth(4326), Ok(SridAuthority::Epsg));
        assert_eq!(auth(899_999), Ok(SridAuthority::Epsg));
        assert_eq!(auth(900_000), Ok(SridAuthority::Reserved));
        assert_eq!(auth(909_999), Ok(SridAuthority::Reserved));
        assert_eq!(auth(910_000), Ok(SridAuthority::User));
        assert_eq!(auth(989_999), Ok(SridAuthority::User));
        assert_eq!(auth(990_000), Ok(SridAuthority::NodeDb));
        assert_eq!(auth(990_999), Ok(SridAuthority::NodeDb));
        assert_eq!(auth(991_000), Ok(SridAuthority::Reserved));
        assert_eq!(auth(Srid::MAX_CODE), Ok(SridAuthority::Reserved));
    }

    #[test]
    fn crs_names_follow_the_authority() {
        assert_eq!(Srid::WGS84.crs_name(), "EPSG:4326");
        assert_eq!(Srid::MODIS_SINUSOIDAL.crs_name(), "NODEDB:990001");
        assert_eq!(Srid::new(910_001).unwrap().crs_name(), "NODEDB:910001");
    }

    #[test]
    fn constants() {
        assert_eq!(Srid::WGS84.code(), 4326);
        assert_eq!(Srid::default(), Srid::WGS84);
        assert_eq!(Srid::MODIS_SINUSOIDAL.authority(), SridAuthority::NodeDb);
    }

    #[test]
    fn msgpack_round_trip_and_rejects_zero() {
        let srid = Srid::new(32648).unwrap();
        let bytes = zerompk::to_msgpack_vec(&srid).unwrap();
        assert_eq!(zerompk::from_msgpack::<Srid>(&bytes).unwrap(), srid);
        let zero = zerompk::to_msgpack_vec(&0u32).unwrap();
        assert!(zerompk::from_msgpack::<Srid>(&zero).is_err());
    }
}
