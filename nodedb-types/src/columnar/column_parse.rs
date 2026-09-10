// SPDX-License-Identifier: Apache-2.0

//! [`std::fmt::Display`] and [`std::str::FromStr`] for [`ColumnType`], plus
//! [`ColumnTypeParseError`].

use std::fmt;
use std::str::FromStr;

use super::column_type::ColumnType;

/// Every declared spelling that resolves to [`ColumnType::Int64`].
///
/// nodedb stores every integer as a full `i64`, so all of these collapse to
/// one storage variant. The declared width travels separately as an
/// [`IntWidth`](super::IntWidth), which narrows the advertised wire OID and
/// bounds writes. [`IntWidth::from_declared_type`](super::IntWidth::from_declared_type)
/// must recognize every spelling listed here — a spelling it does not know
/// advertises OID 20 (`bigint`) whatever the author declared.
pub const DECLARED_INT_KEYWORDS: [&str; 8] = [
    "BIGINT", "INT64", "INTEGER", "INT", "INT4", "INT8", "SMALLINT", "INT2",
];

/// Every declared spelling that resolves to [`ColumnType::Float64`].
///
/// The float counterpart of [`DECLARED_INT_KEYWORDS`]: nodedb stores every
/// float as a full `f64`, and
/// [`FloatWidth::from_declared_type`](super::FloatWidth::from_declared_type)
/// carries the declared width that narrows the advertised wire OID. It must
/// recognize every spelling listed here.
pub const DECLARED_FLOAT_KEYWORDS: [&str; 8] = [
    "FLOAT64",
    "DOUBLE",
    "DOUBLE PRECISION",
    "FLOAT8",
    "REAL",
    "FLOAT4",
    "FLOAT32",
    "FLOAT",
];

/// Error from parsing a column type string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ColumnTypeParseError {
    #[error("unknown column type: '{0}'")]
    Unknown(String),
    #[error("'DATETIME' is not a valid type — use 'TIMESTAMP' instead")]
    UseTimestamp,
    #[error("invalid VECTOR dimension: '{0}' (must be a positive integer)")]
    InvalidVectorDim(String),
    #[error(
        "invalid DECIMAL/NUMERIC params: '{0}' (expected DECIMAL(precision, scale) with precision 1-38 and scale <= precision)"
    )]
    InvalidDecimalParams(String),
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int64 => f.write_str("BIGINT"),
            Self::Float64 => f.write_str("FLOAT64"),
            Self::String => f.write_str("TEXT"),
            Self::Bool => f.write_str("BOOL"),
            Self::Bytes => f.write_str("BYTES"),
            Self::Timestamp => f.write_str("TIMESTAMP"),
            Self::Timestamptz => f.write_str("TIMESTAMPTZ"),
            Self::SystemTimestamp => f.write_str("SYSTEM_TIMESTAMP"),
            Self::Decimal { precision, scale } => write!(f, "DECIMAL({precision},{scale})"),
            Self::Geometry => f.write_str("GEOMETRY"),
            Self::Vector(dim) => write!(f, "VECTOR({dim})"),
            Self::SparseVector => f.write_str("SPARSEVECTOR"),
            Self::Uuid => f.write_str("UUID"),
            Self::Json => f.write_str("JSON"),
            Self::Ulid => f.write_str("ULID"),
            Self::Duration => f.write_str("DURATION"),
            Self::Array => f.write_str("ARRAY"),
            Self::Set => f.write_str("SET"),
            Self::Regex => f.write_str("REGEX"),
            Self::Range => f.write_str("RANGE"),
            Self::Record => f.write_str("RECORD"),
        }
    }
}

impl FromStr for ColumnType {
    type Err = ColumnTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let upper = s.trim().to_uppercase();

        // NUMERIC(p,s) / DECIMAL(p,s) special case.
        if upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL") {
            let base = if upper.starts_with("NUMERIC") {
                "NUMERIC"
            } else {
                "DECIMAL"
            };
            let rest = upper[base.len()..].trim();
            if rest.is_empty() {
                return Ok(Self::Decimal {
                    precision: 38,
                    scale: 10,
                });
            }
            if rest.starts_with('(') && rest.ends_with(')') {
                let inner = &rest[1..rest.len() - 1];
                let parts: Vec<&str> = inner.splitn(2, ',').collect();
                let precision: u8 = parts[0]
                    .trim()
                    .parse()
                    .map_err(|_| ColumnTypeParseError::InvalidDecimalParams(rest.to_string()))?;
                let scale: u8 = parts
                    .get(1)
                    .map(|p| p.trim())
                    .unwrap_or("0")
                    .parse()
                    .map_err(|_| ColumnTypeParseError::InvalidDecimalParams(rest.to_string()))?;
                if precision == 0 || precision > 38 {
                    return Err(ColumnTypeParseError::InvalidDecimalParams(format!(
                        "precision {precision} out of range 1-38"
                    )));
                }
                if scale > precision {
                    return Err(ColumnTypeParseError::InvalidDecimalParams(format!(
                        "scale {scale} must be <= precision {precision}"
                    )));
                }
                return Ok(Self::Decimal { precision, scale });
            }
            return Err(ColumnTypeParseError::InvalidDecimalParams(rest.to_string()));
        }

        // VECTOR(N) special case.
        if upper.starts_with("VECTOR") {
            let inner = upper
                .trim_start_matches("VECTOR")
                .trim()
                .trim_start_matches('(')
                .trim_end_matches(')')
                .trim();
            if inner.is_empty() {
                return Err(ColumnTypeParseError::InvalidVectorDim("empty".into()));
            }
            let dim: u32 = inner
                .parse()
                .map_err(|_| ColumnTypeParseError::InvalidVectorDim(inner.into()))?;
            if dim == 0 {
                return Err(ColumnTypeParseError::InvalidVectorDim("0".into()));
            }
            return Ok(Self::Vector(dim));
        }

        match upper.as_str() {
            // Every PostgreSQL wire-width integer keyword collapses to the
            // one `Int64` storage variant; `DECLARED_INT_KEYWORDS` lists them
            // and carries the width contract.
            keyword if DECLARED_INT_KEYWORDS.contains(&keyword) => Ok(Self::Int64),
            // The float counterpart, listed by `DECLARED_FLOAT_KEYWORDS`.
            keyword if DECLARED_FLOAT_KEYWORDS.contains(&keyword) => Ok(Self::Float64),
            "TEXT" | "STRING" | "VARCHAR" => Ok(Self::String),
            "BOOL" | "BOOLEAN" => Ok(Self::Bool),
            "BYTES" | "BYTEA" | "BLOB" => Ok(Self::Bytes),
            "TIMESTAMP" => Ok(Self::Timestamp),
            "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => Ok(Self::Timestamptz),
            "SYSTEM_TIMESTAMP" | "SYSTEMTIMESTAMP" => Ok(Self::SystemTimestamp),
            "GEOMETRY" => Ok(Self::Geometry),
            // Dimensionless: an exact keyword with no `(N)`. Placed as an exact
            // match rather than a `starts_with` guard because "SPARSEVECTOR"
            // does not prefix-collide with the `starts_with("VECTOR")` branch
            // above ("SPARSEVECTOR" starts with "SPARSE", not "VECTOR").
            "SPARSEVECTOR" => Ok(Self::SparseVector),
            "UUID" => Ok(Self::Uuid),
            "JSON" | "JSONB" => Ok(Self::Json),
            "ULID" => Ok(Self::Ulid),
            "DURATION" => Ok(Self::Duration),
            "ARRAY" => Ok(Self::Array),
            "SET" => Ok(Self::Set),
            "REGEX" => Ok(Self::Regex),
            "RANGE" => Ok(Self::Range),
            "RECORD" => Ok(Self::Record),
            "DATETIME" => Err(ColumnTypeParseError::UseTimestamp),
            other => Err(ColumnTypeParseError::Unknown(other.to_string())),
        }
    }
}

impl ColumnType {
    /// Resolve a declared DDL type string to a column type.
    ///
    /// This is the single answer to "which [`ColumnType`] does this declared
    /// string denote", shared by both planes. The catalog records the raw DDL
    /// text that followed the column name, so an entry reads `INT DEFAULT 5`
    /// or `DECIMAL(10, 2) NOT NULL`, not `INT`. This resolves the leading type
    /// token, so a trailing modifier never changes the answer.
    ///
    /// `None` means the token names no known type. A caller that needs a
    /// fallback picks its own — this reports the absence rather than guessing.
    ///
    /// A multi-word spelling resolves from its first word: `DOUBLE PRECISION`
    /// is [`ColumnType::Float64`], and `TIMESTAMP WITH TIME ZONE` reaching
    /// here as catalog text resolves to [`ColumnType::Timestamp`]. Pass such a
    /// spelling to [`str::parse`] instead to resolve it whole.
    pub fn from_declared_type(declared: &str) -> Option<Self> {
        bare_declared_token(declared).parse().ok()
    }
}

/// The leading type token of a declared DDL type string.
///
/// The cut is the first whitespace outside parentheses, so a parameter list
/// keeps its internal spaces and `DECIMAL(10, 2) NOT NULL` yields
/// `DECIMAL(10, 2)`. A trailing comma left by a column-list split is dropped.
fn bare_declared_token(declared: &str) -> &str {
    let trimmed = declared.trim_start();
    let mut depth = 0usize;
    let mut end = trimmed.len();
    for (index, ch) in trimmed.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ if depth == 0 && ch.is_whitespace() => {
                end = index;
                break;
            }
            _ => {}
        }
    }
    trimmed.get(..end).unwrap_or(trimmed).trim_end_matches(',')
}

#[cfg(test)]
mod tests {
    use super::super::{FloatWidth, IntWidth};
    use super::*;

    /// Every spelling the parser resolves to `Int64` must also resolve to a
    /// declared [`IntWidth`]. A spelling one side knows and the other does not
    /// advertises the wrong `RowDescription` OID for the column.
    ///
    /// The list is the one the parser itself matches on, so adding a spelling
    /// there extends this test rather than leaving it behind.
    #[test]
    fn every_declared_int_keyword_resolves_to_int64_and_a_width() {
        for keyword in DECLARED_INT_KEYWORDS {
            assert_eq!(
                keyword.parse::<ColumnType>(),
                Ok(ColumnType::Int64),
                "{keyword} must resolve to Int64"
            );
            assert!(
                IntWidth::from_declared_type(keyword).is_some(),
                "{keyword} must resolve to a declared IntWidth"
            );
        }
    }

    /// The float counterpart of
    /// [`every_declared_int_keyword_resolves_to_int64_and_a_width`].
    #[test]
    fn every_declared_float_keyword_resolves_to_float64_and_a_width() {
        for keyword in DECLARED_FLOAT_KEYWORDS {
            assert_eq!(
                keyword.parse::<ColumnType>(),
                Ok(ColumnType::Float64),
                "{keyword} must resolve to Float64"
            );
            assert!(
                FloatWidth::from_declared_type(keyword).is_some(),
                "{keyword} must resolve to a declared FloatWidth"
            );
        }
    }

    #[test]
    fn declared_type_ignores_trailing_modifiers() {
        assert_eq!(
            ColumnType::from_declared_type("INT DEFAULT 5"),
            Some(ColumnType::Int64)
        );
        assert_eq!(
            ColumnType::from_declared_type("TEXT NOT NULL PRIMARY KEY"),
            Some(ColumnType::String)
        );
        assert_eq!(
            ColumnType::from_declared_type("timestamp time_key"),
            Some(ColumnType::Timestamp)
        );
        assert_eq!(
            ColumnType::from_declared_type("BIGINT,"),
            Some(ColumnType::Int64)
        );
    }

    /// A parameter list keeps its internal spaces: cutting at the first
    /// whitespace would leave `DECIMAL(10,` and lose the type.
    #[test]
    fn declared_type_keeps_a_spaced_parameter_list() {
        assert_eq!(
            ColumnType::from_declared_type("DECIMAL(10, 2) NOT NULL"),
            Some(ColumnType::Decimal {
                precision: 10,
                scale: 2
            })
        );
        assert_eq!(
            ColumnType::from_declared_type("VECTOR(768)"),
            Some(ColumnType::Vector(768))
        );
    }

    #[test]
    fn declared_type_reports_an_unknown_token_as_none() {
        assert_eq!(ColumnType::from_declared_type("SOMETHING_ELSE"), None);
        assert_eq!(ColumnType::from_declared_type(""), None);
        assert_eq!(ColumnType::from_declared_type("   "), None);
    }

    /// `Timestamp` and `Timestamptz` are instants; `SystemTimestamp` is
    /// engine-assigned and is not.
    #[test]
    fn only_timestamp_types_are_instants() {
        assert!(ColumnType::Timestamp.is_instant());
        assert!(ColumnType::Timestamptz.is_instant());
        assert!(!ColumnType::SystemTimestamp.is_instant());
        assert!(!ColumnType::Int64.is_instant());
        assert!(!ColumnType::String.is_instant());
    }
}
