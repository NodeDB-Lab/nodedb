// SPDX-License-Identifier: Apache-2.0

//! The target type of a runtime `CAST`, and its on-wire form.
//!
//! A type without a payload encodes as its tag, an unsigned integer. Tags
//! are stable and MUST NOT be renumbered: Int=0, Float=1, String=2, Bool=3,
//! Decimal=4, Uuid=5, Timestamp=6, Timestamptz=7, Duration=8, Int2=9, Int4=10.
//! `Decimal` encodes as the array `[4, precision | nil, scale | nil]`.

use nodedb_types::columnar::DecimalTypmod;

/// Target types for CAST.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CastType {
    /// `BIGINT` / `INT8`.
    Int,
    Float,
    String,
    Bool,
    /// Exact `DECIMAL` / `NUMERIC`, fitted to the declared `(p,s)` when one
    /// is given.
    Decimal(Option<DecimalTypmod>),
    /// `UUID`.
    Uuid,
    /// `TIMESTAMP` (without time zone).
    Timestamp,
    /// `TIMESTAMPTZ` / `TIMESTAMP WITH TIME ZONE`.
    Timestamptz,
    /// `DURATION`.
    Duration,
    /// `SMALLINT` / `INT2`.
    Int2,
    /// `INTEGER` / `INT` / `INT4`.
    Int4,
}

const TAG_INT: u64 = 0;
const TAG_FLOAT: u64 = 1;
const TAG_STRING: u64 = 2;
const TAG_BOOL: u64 = 3;
const TAG_DECIMAL: u64 = 4;
const TAG_UUID: u64 = 5;
const TAG_TIMESTAMP: u64 = 6;
const TAG_TIMESTAMPTZ: u64 = 7;
const TAG_DURATION: u64 = 8;
const TAG_INT2: u64 = 9;
const TAG_INT4: u64 = 10;

impl CastType {
    /// The cast type a SQL type name names, as the planner prints it:
    /// `BIGINT`, `NUMERIC(10,2)`, `TIMESTAMP WITH TIME ZONE`, any case.
    /// `None` for a name no runtime cast covers, and for a `DECIMAL(p,s)`
    /// whose precision or scale is not a valid typmod.
    pub fn from_type_name(name: &str) -> Option<Self> {
        let upper = name.trim().to_ascii_uppercase();
        let (base, args) = match upper.split_once('(') {
            Some((base, rest)) => (base.trim(), rest.split(')').next()),
            None => (upper.as_str(), None),
        };
        if base.starts_with("TIMESTAMP") {
            let zoned = base == "TIMESTAMPTZ"
                || (upper.contains("WITH TIME ZONE") && !upper.contains("WITHOUT TIME ZONE"));
            return Some(if zoned {
                Self::Timestamptz
            } else {
                Self::Timestamp
            });
        }
        Some(match base {
            "BIGINT" | "INT8" | "INT64" => Self::Int,
            "INT" | "INTEGER" | "INT4" => Self::Int4,
            "SMALLINT" | "INT2" => Self::Int2,
            "FLOAT" | "FLOAT4" | "FLOAT8" | "FLOAT64" | "DOUBLE" | "DOUBLE PRECISION" | "REAL" => {
                Self::Float
            }
            "TEXT" | "STRING" | "VARCHAR" | "CHAR" | "CHARACTER" | "CHARACTER VARYING" => {
                Self::String
            }
            "BOOL" | "BOOLEAN" => Self::Bool,
            "UUID" => Self::Uuid,
            "DURATION" => Self::Duration,
            "NUMERIC" | "DECIMAL" | "DEC" => Self::Decimal(match args {
                None => None,
                Some(args) => Some(decimal_typmod(args)?),
            }),
            _ => return None,
        })
    }
}

/// The typmod `p` or `p,s` spells. `DECIMAL(p)` has scale 0.
fn decimal_typmod(args: &str) -> Option<DecimalTypmod> {
    let mut parts = args.split(',').map(str::trim);
    let precision = parts.next()?.parse::<i64>().ok()?;
    let scale = match parts.next() {
        Some(scale) => scale.parse::<i64>().ok()?,
        None => 0,
    };
    if parts.next().is_some() {
        return None;
    }
    DecimalTypmod::new(precision, scale).ok()
}

impl zerompk::ToMessagePack for CastType {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        let tag = match self {
            Self::Int => TAG_INT,
            Self::Float => TAG_FLOAT,
            Self::String => TAG_STRING,
            Self::Bool => TAG_BOOL,
            Self::Uuid => TAG_UUID,
            Self::Timestamp => TAG_TIMESTAMP,
            Self::Timestamptz => TAG_TIMESTAMPTZ,
            Self::Duration => TAG_DURATION,
            Self::Int2 => TAG_INT2,
            Self::Int4 => TAG_INT4,
            Self::Decimal(typmod) => {
                writer.write_array_len(3)?;
                writer.write_u64(TAG_DECIMAL)?;
                return match typmod {
                    Some(typmod) => {
                        writer.write_u64(u64::from(typmod.precision()))?;
                        writer.write_u64(u64::from(typmod.scale()))
                    }
                    None => {
                        writer.write_nil()?;
                        writer.write_nil()
                    }
                };
            }
        };
        writer.write_u64(tag)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for CastType {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let marker = reader.peek_marker()?;
        let is_array = (0x90..=0x9f).contains(&marker) || marker == 0xdc || marker == 0xdd;
        if !is_array {
            return match reader.read_u64()? {
                TAG_INT => Ok(Self::Int),
                TAG_FLOAT => Ok(Self::Float),
                TAG_STRING => Ok(Self::String),
                TAG_BOOL => Ok(Self::Bool),
                TAG_UUID => Ok(Self::Uuid),
                TAG_TIMESTAMP => Ok(Self::Timestamp),
                TAG_TIMESTAMPTZ => Ok(Self::Timestamptz),
                TAG_DURATION => Ok(Self::Duration),
                TAG_INT2 => Ok(Self::Int2),
                TAG_INT4 => Ok(Self::Int4),
                other => Err(zerompk::Error::InvalidMarker(
                    u8::try_from(other).unwrap_or(u8::MAX),
                )),
            };
        }
        let len = reader.read_array_len()?;
        if len != 3 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 3,
                actual: len,
            });
        }
        let tag = reader.read_u64()?;
        if tag != TAG_DECIMAL {
            return Err(zerompk::Error::InvalidMarker(
                u8::try_from(tag).unwrap_or(u8::MAX),
            ));
        }
        let precision = Option::<u64>::read(reader)?;
        let scale = Option::<u64>::read(reader)?;
        let typmod = match (precision, scale) {
            (None, None) => None,
            (Some(precision), Some(scale)) => Some(
                i64::try_from(precision)
                    .ok()
                    .zip(i64::try_from(scale).ok())
                    .and_then(|(p, s)| DecimalTypmod::new(p, s).ok())
                    .ok_or(zerompk::Error::InvalidMarker(TAG_DECIMAL as u8))?,
            ),
            _ => return Err(zerompk::Error::InvalidMarker(TAG_DECIMAL as u8)),
        };
        Ok(Self::Decimal(typmod))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(cast: CastType) -> CastType {
        let bytes = zerompk::to_msgpack_vec(&cast).expect("encode");
        zerompk::from_msgpack(&bytes).expect("decode")
    }

    #[test]
    fn every_cast_type_round_trips() {
        let typmod = DecimalTypmod::new(10, 2).expect("valid typmod");
        for cast in [
            CastType::Int,
            CastType::Float,
            CastType::String,
            CastType::Bool,
            CastType::Decimal(None),
            CastType::Decimal(Some(typmod)),
            CastType::Uuid,
            CastType::Timestamp,
            CastType::Timestamptz,
            CastType::Duration,
            CastType::Int2,
            CastType::Int4,
        ] {
            assert_eq!(round_trip(cast), cast);
        }
    }

    #[test]
    fn a_unit_cast_type_encodes_as_its_tag() {
        assert_eq!(
            zerompk::to_msgpack_vec(&CastType::Bool).expect("encode"),
            vec![0x03]
        );
    }

    #[test]
    fn type_names_resolve_to_cast_types() {
        let typmod = DecimalTypmod::new(10, 2).expect("valid typmod");
        assert_eq!(
            CastType::from_type_name("NUMERIC(10,2)"),
            Some(CastType::Decimal(Some(typmod)))
        );
        assert_eq!(
            CastType::from_type_name("decimal"),
            Some(CastType::Decimal(None))
        );
        assert_eq!(CastType::from_type_name("uuid"), Some(CastType::Uuid));
        assert_eq!(
            CastType::from_type_name("TIMESTAMP(3)"),
            Some(CastType::Timestamp)
        );
        assert_eq!(
            CastType::from_type_name("TIMESTAMP WITH TIME ZONE"),
            Some(CastType::Timestamptz)
        );
        assert_eq!(
            CastType::from_type_name("TIMESTAMP WITHOUT TIME ZONE"),
            Some(CastType::Timestamp)
        );
        assert_eq!(
            CastType::from_type_name("timestamptz"),
            Some(CastType::Timestamptz)
        );
        assert_eq!(CastType::from_type_name("bigint"), Some(CastType::Int));
        assert_eq!(CastType::from_type_name("INTEGER"), Some(CastType::Int4));
        assert_eq!(CastType::from_type_name("int"), Some(CastType::Int4));
        assert_eq!(CastType::from_type_name("SMALLINT"), Some(CastType::Int2));
        assert_eq!(CastType::from_type_name("int2"), Some(CastType::Int2));
        assert_eq!(
            zerompk::to_msgpack_vec(&CastType::Int4).expect("encode"),
            vec![0x0a]
        );
        assert_eq!(CastType::from_type_name("NUMERIC(0)"), None);
        assert_eq!(CastType::from_type_name("JSONB"), None);
    }
}
