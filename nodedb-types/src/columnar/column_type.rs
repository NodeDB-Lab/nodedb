//! ColumnType and ColumnDef — the atomic building blocks of typed schemas.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::value::Value;

/// Typed column definition for strict document and columnar collections.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[serde(tag = "type", content = "params")]
pub enum ColumnType {
    Int64,
    Float64,
    String,
    Bool,
    Bytes,
    Timestamp,
    Decimal,
    Geometry,
    /// Fixed-dimension float32 vector.
    Vector(u32),
    Uuid,
    /// Arbitrary nested data stored as inline MessagePack.
    /// Variable-length. Accepts any Value type.
    Json,
    /// ULID: 16-byte Crockford Base32-encoded sortable ID.
    Ulid,
    /// Duration: signed microsecond precision (i64 internally).
    Duration,
    /// Ordered array of values. Variable-length, inline MessagePack.
    Array,
    /// Ordered set (auto-deduplicated). Variable-length, inline MessagePack.
    Set,
    /// Compiled regex pattern. Stored as string internally.
    Regex,
    /// Bounded range of values. Variable-length, inline MessagePack.
    Range,
    /// Typed reference to another record (`table:id`). Variable-length, inline MessagePack.
    Record,
}

impl ColumnType {
    /// Whether this type has a fixed byte size in Binary Tuple layout.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            Self::Int64 | Self::Float64 | Self::Timestamp | Self::Duration => Some(8),
            Self::Bool => Some(1),
            Self::Decimal | Self::Uuid | Self::Ulid => Some(16),
            Self::Vector(dim) => Some(*dim as usize * 4),
            Self::String
            | Self::Bytes
            | Self::Geometry
            | Self::Json
            | Self::Array
            | Self::Set
            | Self::Regex
            | Self::Range
            | Self::Record => None,
        }
    }

    /// Whether this type is variable-length (requires offset table entry).
    pub fn is_variable_length(&self) -> bool {
        self.fixed_size().is_none()
    }

    /// Whether a `Value` is compatible with this column type.
    ///
    /// Accepts both native Value types (e.g., `Value::DateTime` for Timestamp)
    /// AND coercion sources from SQL input (e.g., `Value::String` for Timestamp).
    /// Null is accepted for any type — nullability is enforced at schema level.
    pub fn accepts(&self, value: &Value) -> bool {
        matches!(
            (self, value),
            (Self::Int64, Value::Integer(_))
                | (Self::Float64, Value::Float(_) | Value::Integer(_))
                | (Self::String, Value::String(_))
                | (Self::Bool, Value::Bool(_))
                | (Self::Bytes, Value::Bytes(_))
                | (
                    Self::Timestamp,
                    Value::DateTime(_) | Value::Integer(_) | Value::String(_)
                )
                | (
                    Self::Decimal,
                    Value::Decimal(_) | Value::String(_) | Value::Float(_) | Value::Integer(_)
                )
                | (Self::Geometry, Value::Geometry(_) | Value::String(_))
                | (Self::Vector(_), Value::Array(_) | Value::Bytes(_))
                | (Self::Uuid, Value::Uuid(_) | Value::String(_))
                | (Self::Ulid, Value::Ulid(_) | Value::String(_))
                | (
                    Self::Duration,
                    Value::Duration(_) | Value::Integer(_) | Value::String(_)
                )
                | (Self::Array, Value::Array(_))
                | (Self::Set, Value::Set(_) | Value::Array(_))
                | (Self::Regex, Value::Regex(_) | Value::String(_))
                | (Self::Range, Value::Range { .. })
                | (Self::Record, Value::Record { .. } | Value::String(_))
                | (Self::Json, _)
                | (_, Value::Null)
        )
    }
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
            Self::Decimal => f.write_str("DECIMAL"),
            Self::Geometry => f.write_str("GEOMETRY"),
            Self::Vector(dim) => write!(f, "VECTOR({dim})"),
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
            "BIGINT" | "INT64" | "INTEGER" | "INT" => Ok(Self::Int64),
            "FLOAT64" | "DOUBLE" | "REAL" | "FLOAT" => Ok(Self::Float64),
            "TEXT" | "STRING" | "VARCHAR" => Ok(Self::String),
            "BOOL" | "BOOLEAN" => Ok(Self::Bool),
            "BYTES" | "BYTEA" | "BLOB" => Ok(Self::Bytes),
            "TIMESTAMP" | "TIMESTAMPTZ" => Ok(Self::Timestamp),
            "DECIMAL" | "NUMERIC" => Ok(Self::Decimal),
            "GEOMETRY" => Ok(Self::Geometry),
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

/// Error from parsing a column type string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ColumnTypeParseError {
    #[error("unknown column type: '{0}'")]
    Unknown(String),
    #[error("'DATETIME' is not a valid type — use 'TIMESTAMP' instead")]
    UseTimestamp,
    #[error("invalid VECTOR dimension: '{0}' (must be a positive integer)")]
    InvalidVectorDim(String),
}

/// Column-level modifiers that designate special engine roles.
///
/// These tell the engine which column serves a specialized purpose.
/// Extensible for future column roles (e.g., `PartitionKey`, `SortKey`).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
#[repr(u8)]
pub enum ColumnModifier {
    /// This column is the time-partitioning key (timeseries profile).
    /// Exactly one required for timeseries collections.
    TimeKey = 0,
    /// This column has an automatic R-tree spatial index (spatial profile).
    /// Exactly one required for spatial collections.
    SpatialIndex = 1,
}

/// A single column definition in a strict document or columnar schema.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ColumnDef {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
    pub default: Option<String>,
    pub primary_key: bool,
    /// Column-level modifiers (TIME_KEY, SPATIAL_INDEX, etc.).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub modifiers: Vec<ColumnModifier>,
    /// GENERATED ALWAYS AS expression (serialized SqlExpr JSON).
    /// When set, this column is computed at write time, not supplied by the user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_expr: Option<String>,
    /// Column names this generated column depends on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub generated_deps: Vec<String>,
    /// Schema version at which this column was added. Original columns have
    /// version 1 (the default). Columns added via `ALTER ADD COLUMN` record
    /// the schema version after the bump so the reader can build a physical
    /// sub-schema for tuples written under older versions.
    #[serde(default = "default_added_at_version")]
    pub added_at_version: u16,
}

fn default_added_at_version() -> u16 {
    1
}

impl ColumnDef {
    pub fn required(name: impl Into<String>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            nullable: false,
            default: None,
            primary_key: false,
            modifiers: Vec::new(),
            generated_expr: None,
            generated_deps: Vec::new(),
            added_at_version: 1,
        }
    }

    pub fn nullable(name: impl Into<String>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            nullable: true,
            default: None,
            primary_key: false,
            modifiers: Vec::new(),
            generated_expr: None,
            generated_deps: Vec::new(),
            added_at_version: 1,
        }
    }

    pub fn with_primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false;
        self
    }

    /// Check if this column has the TIME_KEY modifier.
    pub fn is_time_key(&self) -> bool {
        self.modifiers.contains(&ColumnModifier::TimeKey)
    }

    /// Check if this column has the SPATIAL_INDEX modifier.
    pub fn is_spatial_index(&self) -> bool {
        self.modifiers.contains(&ColumnModifier::SpatialIndex)
    }

    pub fn with_default(mut self, expr: impl Into<String>) -> Self {
        self.default = Some(expr.into());
        self
    }
}

impl fmt::Display for ColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.column_type)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        if self.primary_key {
            write!(f, " PRIMARY KEY")?;
        }
        if let Some(ref d) = self.default {
            write!(f, " DEFAULT {d}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_canonical() {
        assert_eq!("BIGINT".parse::<ColumnType>().unwrap(), ColumnType::Int64);
        assert_eq!(
            "FLOAT64".parse::<ColumnType>().unwrap(),
            ColumnType::Float64
        );
        assert_eq!("TEXT".parse::<ColumnType>().unwrap(), ColumnType::String);
        assert_eq!("BOOL".parse::<ColumnType>().unwrap(), ColumnType::Bool);
        assert_eq!(
            "TIMESTAMP".parse::<ColumnType>().unwrap(),
            ColumnType::Timestamp
        );
        assert_eq!(
            "GEOMETRY".parse::<ColumnType>().unwrap(),
            ColumnType::Geometry
        );
        assert_eq!("UUID".parse::<ColumnType>().unwrap(), ColumnType::Uuid);
    }

    #[test]
    fn parse_vector() {
        assert_eq!(
            "VECTOR(768)".parse::<ColumnType>().unwrap(),
            ColumnType::Vector(768)
        );
        assert!("VECTOR(0)".parse::<ColumnType>().is_err());
    }

    #[test]
    fn display_roundtrip() {
        for ct in [
            ColumnType::Int64,
            ColumnType::Float64,
            ColumnType::String,
            ColumnType::Vector(768),
        ] {
            let s = ct.to_string();
            let parsed: ColumnType = s.parse().unwrap();
            assert_eq!(parsed, ct);
        }
    }

    #[test]
    fn accepts_native_values() {
        assert!(ColumnType::Int64.accepts(&Value::Integer(42)));
        assert!(ColumnType::Float64.accepts(&Value::Float(42.0)));
        assert!(ColumnType::Float64.accepts(&Value::Integer(42))); // coercion
        assert!(ColumnType::String.accepts(&Value::String("x".into())));
        assert!(ColumnType::Bool.accepts(&Value::Bool(true)));
        assert!(ColumnType::Bytes.accepts(&Value::Bytes(vec![1])));
        assert!(
            ColumnType::Uuid.accepts(&Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()))
        );
        assert!(ColumnType::Decimal.accepts(&Value::Decimal(rust_decimal::Decimal::ZERO)));

        // Null accepted for any type.
        assert!(ColumnType::Int64.accepts(&Value::Null));

        // Wrong types rejected.
        assert!(!ColumnType::Int64.accepts(&Value::String("x".into())));
        assert!(!ColumnType::Bool.accepts(&Value::Integer(1)));
    }

    #[test]
    fn accepts_coercion_sources() {
        // SQL input coercion: strings for Timestamp, Uuid, Geometry, Decimal.
        assert!(ColumnType::Timestamp.accepts(&Value::String("2024-01-01".into())));
        assert!(ColumnType::Timestamp.accepts(&Value::Integer(1_700_000_000)));
        assert!(ColumnType::Uuid.accepts(&Value::String(
            "550e8400-e29b-41d4-a716-446655440000".into()
        )));
        assert!(ColumnType::Decimal.accepts(&Value::String("99.95".into())));
        assert!(ColumnType::Decimal.accepts(&Value::Float(99.95)));
        assert!(ColumnType::Geometry.accepts(&Value::String("POINT(0 0)".into())));
    }

    #[test]
    fn column_def_display() {
        let col = ColumnDef::required("id", ColumnType::Int64).with_primary_key();
        assert_eq!(col.to_string(), "id BIGINT NOT NULL PRIMARY KEY");
    }
}
