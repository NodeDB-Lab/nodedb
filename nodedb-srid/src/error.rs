// SPDX-License-Identifier: Apache-2.0

//! Errors for SRID resolution and CRS definition parsing.

use crate::srid::Srid;

/// An SRID that does not resolve to a usable CRS.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum UnknownSrid {
    /// The code is outside `1..=Srid::MAX_CODE`.
    #[error("SRID {code} is outside 1..={max}", max = Srid::MAX_CODE)]
    OutOfRange { code: i64 },
    /// No built-in or user-defined CRS has this code.
    #[error("SRID {code} is not defined")]
    Undefined { code: u32 },
    /// A user-defined CRS with this code was dropped.
    #[error("SRID {code} was dropped")]
    Dropped { code: u32 },
    /// The CRS name matches no supported naming form.
    #[error("unknown CRS name '{name}'")]
    UnknownName { name: Box<str> },
    /// A GeoJSON `crs` member that is neither the named form nor the
    /// legacy `EPSG` form.
    #[error(
        "invalid GeoJSON crs member: {reason}; supported: \
         {{\"type\":\"name\",\"properties\":{{\"name\":\"EPSG:<code>\"}}}} or \
         {{\"type\":\"EPSG\",\"properties\":{{\"code\":<code>}}}}"
    )]
    InvalidCrsMember { reason: Box<str> },
    /// The CRS exists but its definition uses something this build does
    /// not implement.
    #[error("SRID {code} is not supported: {cause}")]
    Unsupported { code: u32, cause: CrsParseError },
}

/// A CRS definition text that does not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CrsParseError {
    #[error("unknown parameter '+{key}'")]
    UnknownParameter { key: Box<str> },
    #[error("projection method '{method}' is not implemented; implemented: {implemented}")]
    UnsupportedMethod {
        method: Box<str>,
        implemented: &'static str,
    },
    #[error("invalid value '{value}' for '+{key}'")]
    InvalidValue { key: Box<str>, value: Box<str> },
    #[error("method '{method}' needs '+{key}'")]
    Missing {
        key: &'static str,
        method: &'static str,
    },
    #[error("malformed {format} text at byte {offset}")]
    Malformed { format: &'static str, offset: usize },
    /// Well-formed text whose content is not a CRS this build reads.
    #[error("invalid {format} definition: {detail}")]
    Invalid {
        format: &'static str,
        detail: Box<str>,
    },
}

impl CrsParseError {
    /// An [`CrsParseError::Invalid`] in `format` explaining `detail`.
    pub fn content(format: &'static str, detail: impl Into<Box<str>>) -> Self {
        CrsParseError::Invalid {
            format,
            detail: detail.into(),
        }
    }

    /// An [`CrsParseError::InvalidValue`] naming `key` and `value`.
    pub fn invalid(key: &str, value: &str) -> Self {
        CrsParseError::InvalidValue {
            key: key.into(),
            value: value.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_name_the_input() {
        let err = UnknownSrid::Undefined { code: 2154 };
        assert!(err.to_string().contains("2154"));
        let err = CrsParseError::invalid("zone", "99");
        assert_eq!(err.to_string(), "invalid value '99' for '+zone'");
        let err = UnknownSrid::InvalidCrsMember {
            reason: "type 'link'".into(),
        };
        assert!(err.to_string().contains("type 'link'"), "{err}");
    }
}
