// SPDX-License-Identifier: Apache-2.0

//! Area of use of a CRS.

use serde::{Deserialize, Serialize};

use crate::error::CrsParseError;

/// Geographic bounds where a CRS is valid, WGS 84 degrees. `west > east`
/// marks an area crossing the anti-meridian.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct AreaOfUse {
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
}

impl AreaOfUse {
    /// Parse `west,south,east,north`. An empty string is no area.
    pub fn parse(text: &str) -> Result<Option<AreaOfUse>, CrsParseError> {
        if text.is_empty() {
            return Ok(None);
        }
        let bad = || CrsParseError::invalid("area", text);
        let mut parts = text.split(',').map(|p| p.trim().parse::<f64>());
        let mut next = || parts.next().ok_or_else(bad)?.map_err(|_| bad());
        let area = AreaOfUse {
            west: next()?,
            south: next()?,
            east: next()?,
            north: next()?,
        };
        if parts.next().is_some() {
            return Err(bad());
        }
        Ok(Some(area))
    }

    /// Whether `[lon, lat]` WGS 84 degrees lies inside the area.
    pub fn contains(&self, lon: f64, lat: f64) -> bool {
        let in_lat = (self.south..=self.north).contains(&lat);
        let in_lon = if self.west <= self.east {
            (self.west..=self.east).contains(&lon)
        } else {
            lon >= self.west || lon <= self.east
        };
        in_lat && in_lon
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_four_numbers() {
        let area = AreaOfUse::parse("102,0,108,84").unwrap().unwrap();
        assert_eq!(area.west, 102.0);
        assert_eq!(area.north, 84.0);
        assert!(area.contains(105.0, 3.0));
        assert!(!area.contains(101.0, 3.0));
    }

    #[test]
    fn empty_is_none_and_bad_is_error() {
        assert_eq!(AreaOfUse::parse(""), Ok(None));
        assert!(AreaOfUse::parse("1,2,3").is_err());
        assert!(AreaOfUse::parse("1,2,3,4,5").is_err());
        assert!(AreaOfUse::parse("1,x,3,4").is_err());
    }

    #[test]
    fn anti_meridian_area() {
        let area = AreaOfUse::parse("170,-10,-170,10").unwrap().unwrap();
        assert!(area.contains(175.0, 0.0));
        assert!(area.contains(-175.0, 0.0));
        assert!(!area.contains(0.0, 0.0));
    }
}
