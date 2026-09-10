// SPDX-License-Identifier: Apache-2.0

//! ColumnType — the atomic value type for typed schemas.

use serde::{Deserialize, Serialize};

use crate::value::Value;

/// Typed column definition for strict document and columnar collections.
///
/// `#[non_exhaustive]` — this enum grows with each type system expansion
/// (e.g. future variants may add `Decimal { precision, scale }` or split
/// `Timestamp`/`TimestampTz`). External exhaustive `match` arms must handle
/// future variants via a typed error arm rather than `_ => unreachable!()`.
#[non_exhaustive]
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
#[serde(tag = "type", content = "params")]
pub enum ColumnType {
    Int64,
    Float64,
    String,
    Bool,
    Bytes,
    /// Naive (no-timezone) timestamp with microsecond precision. OID 1114.
    Timestamp,
    /// UTC (timezone-aware) timestamp with microsecond precision. OID 1184.
    Timestamptz,
    /// System-assigned timestamp (bitemporal `system_from_ms`). Same 8-byte
    /// layout as `Timestamp`, but tagged distinctly so the planner and DDL
    /// layer can reject user-supplied values — the column is populated by the
    /// engine from HLC at commit.
    SystemTimestamp,
    /// Arbitrary-precision decimal with explicit precision and scale.
    ///
    /// `precision`: total significant digits, 1–38. `scale`: digits after the
    /// decimal point, 0–precision. Default when unspecified: `{38, 10}`.
    Decimal {
        precision: u8,
        scale: u8,
    },
    Geometry,
    /// Fixed-dimension float32 vector.
    Vector(u32),
    /// Dimensionless sparse vector (term-id → weight map). Variable-length:
    /// a value is a quoted string literal like `'{12: 0.5, 88: 0.3}'`, stored
    /// as inline UTF-8 bytes and parsed at index-build time. The sparse index
    /// carries no dimension, so unlike `Vector(u32)` this type takes no `(N)`.
    SparseVector,
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
            Self::Int64
            | Self::Float64
            | Self::Timestamp
            | Self::Timestamptz
            | Self::SystemTimestamp
            | Self::Duration => Some(8),
            Self::Bool => Some(1),
            Self::Decimal { .. } | Self::Uuid | Self::Ulid => Some(16),
            Self::Vector(dim) => Some(*dim as usize * 4),
            Self::String
            | Self::Bytes
            | Self::Geometry
            | Self::Json
            | Self::SparseVector
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

    /// Whether a column of this type carries an instant.
    ///
    /// This is the single answer to that question for both planes: the
    /// Control Plane types such a column as a timestamp on the wire, and the
    /// Data Plane scales exactly these columns from the epoch milliseconds
    /// storage holds to the epoch microseconds a client reads.
    ///
    /// `SystemTimestamp` is not an instant. It is engine-assigned from HLC at
    /// commit and the planner types it as text.
    pub const fn is_instant(&self) -> bool {
        matches!(self, Self::Timestamp | Self::Timestamptz)
    }

    /// Return the canonical PostgreSQL type OID for this column type.
    ///
    /// This is the single authoritative mapping between NodeDB `ColumnType`
    /// variants and PostgreSQL wire-protocol OIDs. All pgwire code must derive
    /// OIDs from this method — no local string-matching tables.
    ///
    /// Choices for non-native types:
    /// - `Geometry` → `25` (TEXT): no standard pg geometry OID; PostGIS uses
    ///   its own extension OID which we cannot claim. TEXT lets clients at least
    ///   see the WKT/WKB string.
    /// - `Vector(_)` → `1021` (FLOAT4_ARRAY): closest built-in pg type for a
    ///   fixed-dimension float32 vector; pgvector uses a custom OID, which we
    ///   avoid to stay dependency-free.
    /// - `Array`, `Set`, `Range`, `Record`, `Regex` → `114` (JSON): these are
    ///   variable-length MessagePack-encoded structures; JSON is the safest
    ///   generic text OID for clients that need to read the value as a string.
    pub fn to_pg_oid(&self) -> u32 {
        match self {
            Self::Bool => 16,
            Self::Bytes => 17,
            Self::Int64 => 20,
            Self::Float64 => 701,
            Self::String => 25,
            Self::Timestamp | Self::SystemTimestamp => 1114,
            Self::Timestamptz => 1184,
            Self::Decimal { .. } => 1700,
            Self::Uuid | Self::Ulid => 2950,
            Self::Json => 3802,
            Self::Duration => 1186,
            // No standard built-in OID for geometry; TEXT lets clients read WKT.
            Self::Geometry => 25,
            // FLOAT4_ARRAY (1021) is the closest built-in for fixed float32 vectors.
            Self::Vector(_) => 1021,
            // Variable-length structured types: expose as JSONB so clients can
            // parse the serialized representation. `SparseVector` groups here —
            // its `'{id: weight}'` literal reads naturally as a JSON object.
            Self::Array
            | Self::Set
            | Self::Range
            | Self::Record
            | Self::Regex
            | Self::SparseVector => 3802,
        }
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
                    Value::NaiveDateTime(_) | Value::Integer(_) | Value::String(_)
                )
                | (
                    Self::Timestamptz,
                    Value::DateTime(_) | Value::Integer(_) | Value::String(_)
                )
                | (
                    Self::SystemTimestamp,
                    Value::DateTime(_) | Value::Integer(_)
                )
                | (
                    Self::Decimal { .. },
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
                | (Self::SparseVector, Value::String(_) | Value::Bytes(_))
                | (Self::Json, _)
                | (_, Value::Null)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodedb_types_datetime_epoch() -> crate::datetime::NdbDateTime {
        crate::datetime::NdbDateTime::from_micros(0)
    }

    #[test]
    fn to_pg_oid_stable() {
        assert_eq!(ColumnType::Bool.to_pg_oid(), 16);
        assert_eq!(ColumnType::Bytes.to_pg_oid(), 17);
        assert_eq!(ColumnType::Int64.to_pg_oid(), 20);
        assert_eq!(ColumnType::String.to_pg_oid(), 25);
        assert_eq!(ColumnType::Float64.to_pg_oid(), 701);
        assert_eq!(ColumnType::Timestamp.to_pg_oid(), 1114);
        assert_eq!(ColumnType::Timestamptz.to_pg_oid(), 1184);
        assert_eq!(ColumnType::SystemTimestamp.to_pg_oid(), 1114);
        assert_eq!(ColumnType::Duration.to_pg_oid(), 1186);
        assert_eq!(
            ColumnType::Decimal {
                precision: 38,
                scale: 10
            }
            .to_pg_oid(),
            1700
        );
        assert_eq!(ColumnType::Uuid.to_pg_oid(), 2950);
        assert_eq!(ColumnType::Ulid.to_pg_oid(), 2950);
        assert_eq!(ColumnType::Json.to_pg_oid(), 3802);
        assert_eq!(ColumnType::Geometry.to_pg_oid(), 25);
        assert_eq!(ColumnType::Vector(768).to_pg_oid(), 1021);
        assert_eq!(ColumnType::Array.to_pg_oid(), 3802);
        assert_eq!(ColumnType::Set.to_pg_oid(), 3802);
        assert_eq!(ColumnType::Range.to_pg_oid(), 3802);
        assert_eq!(ColumnType::Record.to_pg_oid(), 3802);
        assert_eq!(ColumnType::Regex.to_pg_oid(), 3802);
    }

    #[test]
    fn parse_system_timestamp() {
        assert_eq!(
            "SYSTEM_TIMESTAMP".parse::<ColumnType>().unwrap(),
            ColumnType::SystemTimestamp
        );
        assert_eq!(
            "SystemTimestamp".parse::<ColumnType>().unwrap(),
            ColumnType::SystemTimestamp
        );
        assert_eq!(ColumnType::SystemTimestamp.fixed_size(), Some(8));
        assert!(!ColumnType::SystemTimestamp.is_variable_length());
        assert_eq!(ColumnType::SystemTimestamp.to_string(), "SYSTEM_TIMESTAMP");
        assert!(!ColumnType::SystemTimestamp.accepts(&Value::String("2024-01-01".into())));
        assert!(ColumnType::SystemTimestamp.accepts(&Value::Integer(1_700_000_000)));
    }

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
            "TIMESTAMPTZ".parse::<ColumnType>().unwrap(),
            ColumnType::Timestamptz
        );
        assert_eq!(
            "TIMESTAMP WITH TIME ZONE".parse::<ColumnType>().unwrap(),
            ColumnType::Timestamptz
        );
        assert_eq!(
            "GEOMETRY".parse::<ColumnType>().unwrap(),
            ColumnType::Geometry
        );
        assert_eq!("UUID".parse::<ColumnType>().unwrap(), ColumnType::Uuid);
    }

    /// `INT4`/`INT8`/`SMALLINT`/`INT2` are wire-width DDL keywords: strict/kv
    /// `CREATE COLLECTION` must accept them as valid PostgreSQL integer
    /// aliases, not reject them as unknown column types. They all
    /// map to the same `Int64` storage variant as `BIGINT`/`INTEGER`/`INT` —
    /// nodedb's columnar/strict/kv storage always keeps integers as a full
    /// i64; only the wire (`DdlColType`) layer narrows the advertised OID.
    #[test]
    fn parse_int_width_aliases_map_to_int64() {
        assert_eq!("INT4".parse::<ColumnType>().unwrap(), ColumnType::Int64);
        assert_eq!("INT8".parse::<ColumnType>().unwrap(), ColumnType::Int64);
        assert_eq!("SMALLINT".parse::<ColumnType>().unwrap(), ColumnType::Int64);
        assert_eq!("INT2".parse::<ColumnType>().unwrap(), ColumnType::Int64);
    }

    /// The float analogue of [`parse_int_width_aliases_map_to_int64`]:
    /// `FLOAT4`/`FLOAT8`/`FLOAT32`/`DOUBLE PRECISION` are valid PostgreSQL
    /// float aliases that were rejected as unknown column types. They all map
    /// to the same `Float64` storage variant — nodedb always keeps floats as a
    /// full f64; only the wire (`DdlColType`) layer narrows the advertised OID
    /// via `FloatWidth`.
    ///
    /// Every spelling `FloatWidth::from_declared_type` recognizes must appear
    /// here, or DDL accepts a width the wire layer honours and vice versa.
    #[test]
    fn parse_float_width_aliases_map_to_float64() {
        for declared in [
            "FLOAT4",
            "FLOAT8",
            "FLOAT32",
            "FLOAT64",
            "REAL",
            "DOUBLE",
            "DOUBLE PRECISION",
            "FLOAT",
        ] {
            assert_eq!(
                declared.parse::<ColumnType>().unwrap(),
                ColumnType::Float64,
                "{declared} must resolve to the single Float64 storage variant"
            );
            assert!(
                super::super::FloatWidth::from_declared_type(declared).is_some(),
                "{declared} must also resolve to a declared FloatWidth"
            );
        }
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
    fn sparsevector_is_variable_length_string_type() {
        // Dimensionless: parses from the bare keyword and round-trips via Display.
        let parsed = "SPARSEVECTOR".parse::<ColumnType>().unwrap();
        assert_eq!(parsed, ColumnType::SparseVector);
        assert_eq!(ColumnType::SparseVector.to_string(), "SPARSEVECTOR");
        assert_eq!(
            ColumnType::SparseVector
                .to_string()
                .parse::<ColumnType>()
                .unwrap(),
            ColumnType::SparseVector
        );

        // Variable-length (no fixed byte size), JSONB pg oid like the other
        // variable-length structured types.
        assert_eq!(ColumnType::SparseVector.fixed_size(), None);
        assert!(ColumnType::SparseVector.is_variable_length());
        assert_eq!(ColumnType::SparseVector.to_pg_oid(), 3802);

        // Accepts the quoted string literal (and raw bytes); rejects numerics.
        assert!(ColumnType::SparseVector.accepts(&Value::String("{3:0.5}".into())));
        assert!(ColumnType::SparseVector.accepts(&Value::Bytes(vec![1, 2, 3])));
        assert!(ColumnType::SparseVector.accepts(&Value::Null));
        assert!(!ColumnType::SparseVector.accepts(&Value::Integer(1)));
    }

    #[test]
    fn display_roundtrip() {
        for ct in [
            ColumnType::Int64,
            ColumnType::Float64,
            ColumnType::String,
            ColumnType::Timestamp,
            ColumnType::Timestamptz,
            ColumnType::Vector(768),
            ColumnType::Decimal {
                precision: 10,
                scale: 2,
            },
            ColumnType::Decimal {
                precision: 38,
                scale: 10,
            },
        ] {
            let s = ct.to_string();
            let parsed: ColumnType = s.parse().unwrap();
            assert_eq!(parsed, ct);
        }
    }

    #[test]
    fn decimal_parse_with_params() {
        assert_eq!(
            "NUMERIC(10,2)".parse::<ColumnType>().unwrap(),
            ColumnType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(
            "DECIMAL(38,10)".parse::<ColumnType>().unwrap(),
            ColumnType::Decimal {
                precision: 38,
                scale: 10
            }
        );
        assert_eq!(
            "NUMERIC".parse::<ColumnType>().unwrap(),
            ColumnType::Decimal {
                precision: 38,
                scale: 10
            }
        );
        assert_eq!(
            "DECIMAL".parse::<ColumnType>().unwrap(),
            ColumnType::Decimal {
                precision: 38,
                scale: 10
            }
        );
    }

    #[test]
    fn decimal_parse_invalid() {
        assert!("DECIMAL(5,6)".parse::<ColumnType>().is_err());
        assert!("DECIMAL(0,0)".parse::<ColumnType>().is_err());
        assert!("DECIMAL(39,0)".parse::<ColumnType>().is_err());
    }

    #[test]
    fn decimal_fixed_size() {
        assert_eq!(
            ColumnType::Decimal {
                precision: 10,
                scale: 2
            }
            .fixed_size(),
            Some(16)
        );
    }

    #[test]
    fn decimal_to_pg_oid_is_1700() {
        assert_eq!(
            ColumnType::Decimal {
                precision: 10,
                scale: 2
            }
            .to_pg_oid(),
            1700
        );
    }

    #[test]
    fn accepts_native_values() {
        assert!(ColumnType::Int64.accepts(&Value::Integer(42)));
        assert!(ColumnType::Float64.accepts(&Value::Float(42.0)));
        assert!(ColumnType::Float64.accepts(&Value::Integer(42)));
        assert!(ColumnType::String.accepts(&Value::String("x".into())));
        assert!(ColumnType::Bool.accepts(&Value::Bool(true)));
        assert!(ColumnType::Bytes.accepts(&Value::Bytes(vec![1])));
        assert!(
            ColumnType::Uuid.accepts(&Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()))
        );
        assert!(
            ColumnType::Decimal {
                precision: 38,
                scale: 10
            }
            .accepts(&Value::Decimal(rust_decimal::Decimal::ZERO))
        );

        let naive = Value::NaiveDateTime(nodedb_types_datetime_epoch());
        let tz = Value::DateTime(nodedb_types_datetime_epoch());
        assert!(ColumnType::Timestamp.accepts(&naive));
        assert!(!ColumnType::Timestamp.accepts(&tz));
        assert!(ColumnType::Timestamptz.accepts(&tz));
        assert!(!ColumnType::Timestamptz.accepts(&naive));

        assert!(ColumnType::Int64.accepts(&Value::Null));
        assert!(!ColumnType::Int64.accepts(&Value::String("x".into())));
        assert!(!ColumnType::Bool.accepts(&Value::Integer(1)));
    }

    #[test]
    fn accepts_coercion_sources() {
        assert!(ColumnType::Timestamp.accepts(&Value::String("2024-01-01".into())));
        assert!(ColumnType::Timestamp.accepts(&Value::Integer(1_700_000_000)));
        assert!(ColumnType::Timestamptz.accepts(&Value::String("2024-01-01T00:00:00Z".into())));
        assert!(ColumnType::Timestamptz.accepts(&Value::Integer(1_700_000_000)));
        assert!(ColumnType::Uuid.accepts(&Value::String(
            "550e8400-e29b-41d4-a716-446655440000".into()
        )));
        assert!(
            ColumnType::Decimal {
                precision: 10,
                scale: 2
            }
            .accepts(&Value::String("99.95".into()))
        );
        assert!(
            ColumnType::Decimal {
                precision: 10,
                scale: 2
            }
            .accepts(&Value::Float(99.95))
        );
        assert!(ColumnType::Geometry.accepts(&Value::String("POINT(0 0)".into())));
    }

    #[test]
    fn column_def_display() {
        use super::super::column_def::ColumnDef;
        let col = ColumnDef::required("id", ColumnType::Int64).with_primary_key();
        assert_eq!(col.to_string(), "id BIGINT NOT NULL PRIMARY KEY");
    }
}
