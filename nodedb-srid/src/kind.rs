// SPDX-License-Identifier: Apache-2.0

//! Whether a CRS is geographic or projected.

use serde::{Deserialize, Serialize};

/// Whether coordinates are angles or lengths.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum CrsKind {
    /// x is longitude and y is latitude, degrees.
    Geographic,
    /// x is easting and y is northing, in the CRS's linear unit.
    Projected,
}

impl CrsKind {
    /// The lower-case name: `geographic` or `projected`.
    pub const fn as_str(self) -> &'static str {
        match self {
            CrsKind::Geographic => "geographic",
            CrsKind::Projected => "projected",
        }
    }

    /// The kind `name` spells, as [`CrsKind::as_str`] writes it.
    pub fn from_name(name: &str) -> Option<CrsKind> {
        match name {
            "geographic" => Some(CrsKind::Geographic),
            "projected" => Some(CrsKind::Projected),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip() {
        for kind in [CrsKind::Geographic, CrsKind::Projected] {
            assert_eq!(CrsKind::from_name(kind.as_str()), Some(kind));
        }
        assert_eq!(CrsKind::from_name("Geographic"), None);
    }
}
