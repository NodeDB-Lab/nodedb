// SPDX-License-Identifier: Apache-2.0

//! Tokenizing a PROJ string into `+key[=value]` parameters.

use crate::error::CrsParseError;

/// Format name reported by [`CrsParseError::Malformed`].
pub const PROJ_FORMAT: &str = "PROJ";

/// Every key the parser understands. Any other key is refused.
const KNOWN_KEYS: &[&str] = &[
    "proj",
    "zone",
    "south",
    "lat_0",
    "lon_0",
    "lat_1",
    "lat_2",
    "lat_ts",
    "lonc",
    "alpha",
    "gamma",
    "k",
    "k_0",
    "x_0",
    "y_0",
    "h",
    "ellps",
    "a",
    "b",
    "rf",
    "f",
    "R",
    "datum",
    "towgs84",
    "pm",
    "units",
    "to_meter",
    "axis",
    "no_defs",
    "type",
    "no_uoff",
    "no_off",
    "no_rot",
    "azi",
    "sweep",
    "czech",
    "guam",
    "hyperbolic",
];

/// One `+key[=value]` token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Param<'a> {
    key: &'a str,
    value: Option<&'a str>,
}

/// The parameters of one PROJ string. The first occurrence of a key wins,
/// as in PROJ.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjParams<'a> {
    params: Vec<Param<'a>>,
}

impl<'a> ProjParams<'a> {
    /// Split `text` into parameters and refuse unknown keys.
    pub fn tokenize(text: &'a str) -> Result<Self, CrsParseError> {
        let mut params = Vec::new();
        let mut rest = text;
        loop {
            let trimmed = rest.trim_start();
            if trimmed.is_empty() {
                break;
            }
            let offset = text.len() - trimmed.len();
            let end = trimmed.find(char::is_whitespace).unwrap_or(trimmed.len());
            let token = &trimmed[..end];
            rest = &trimmed[end..];
            let body = token
                .strip_prefix('+')
                .filter(|b| !b.is_empty() && !b.starts_with('='))
                .ok_or(CrsParseError::Malformed {
                    format: PROJ_FORMAT,
                    offset,
                })?;
            let (key, value) = match body.split_once('=') {
                Some((k, v)) => (k, Some(v)),
                None => (body, None),
            };
            if !KNOWN_KEYS.contains(&key) {
                return Err(CrsParseError::UnknownParameter { key: key.into() });
            }
            params.push(Param { key, value });
        }
        Ok(Self { params })
    }

    fn find(&self, key: &str) -> Option<&Param<'a>> {
        self.params.iter().find(|p| p.key == key)
    }

    /// Whether `key` is present, with or without a value.
    pub fn has(&self, key: &str) -> bool {
        self.find(key).is_some()
    }

    /// The text value of `key`. A bare flag has no value and reads as
    /// `InvalidValue`.
    pub fn text(&self, key: &str) -> Result<Option<&'a str>, CrsParseError> {
        match self.find(key) {
            None => Ok(None),
            Some(Param { value: Some(v), .. }) => Ok(Some(*v)),
            Some(Param { value: None, .. }) => Err(CrsParseError::invalid(key, "")),
        }
    }

    /// The numeric value of `key`.
    pub fn number(&self, key: &str) -> Result<Option<f64>, CrsParseError> {
        match self.text(key)? {
            None => Ok(None),
            Some(v) => parse_number(key, v).map(Some),
        }
    }

    /// The numeric value of `key`, or `default` when absent.
    pub fn number_or(&self, key: &str, default: f64) -> Result<f64, CrsParseError> {
        Ok(self.number(key)?.unwrap_or(default))
    }

    /// The first present key among `keys`, as a number.
    pub fn number_any(&self, keys: &[&str]) -> Result<Option<f64>, CrsParseError> {
        for key in keys {
            if let Some(v) = self.number(key)? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }
}

/// Parse a finite decimal number, or a fraction `a/b` as PROJ accepts for
/// `+to_meter`.
pub fn parse_number(key: &str, text: &str) -> Result<f64, CrsParseError> {
    let bad = || CrsParseError::invalid(key, text);
    let value = match text.split_once('/') {
        Some((num, den)) => {
            let num: f64 = num.parse().map_err(|_| bad())?;
            let den: f64 = den.parse().map_err(|_| bad())?;
            num / den
        }
        None => text.parse().map_err(|_| bad())?,
    };
    if value.is_finite() {
        Ok(value)
    } else {
        Err(bad())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_flags_and_values() {
        let p = ProjParams::tokenize("  +proj=utm +zone=48 +south +no_defs").unwrap();
        assert_eq!(p.text("proj"), Ok(Some("utm")));
        assert_eq!(p.number("zone"), Ok(Some(48.0)));
        assert!(p.has("south"));
        assert!(!p.has("lat_0"));
        assert!(p.text("south").is_err());
    }

    #[test]
    fn first_occurrence_wins() {
        let p = ProjParams::tokenize("+proj=tmerc +k=1 +k=2").unwrap();
        assert_eq!(p.number("k"), Ok(Some(1.0)));
    }

    #[test]
    fn refuses_unknown_and_malformed() {
        assert_eq!(
            ProjParams::tokenize("+proj=tmerc +wat=1"),
            Err(CrsParseError::UnknownParameter { key: "wat".into() })
        );
        assert_eq!(
            ProjParams::tokenize("+proj=tmerc lat_0=1"),
            Err(CrsParseError::Malformed {
                format: PROJ_FORMAT,
                offset: 12
            })
        );
        assert!(ProjParams::tokenize("+ +proj=tmerc").is_err());
    }

    #[test]
    fn numbers_and_fractions() {
        assert_eq!(parse_number("to_meter", "1200/3937"), Ok(1200.0 / 3937.0));
        assert!(parse_number("k", "abc").is_err());
        assert!(parse_number("k", "inf").is_err());
    }
}
