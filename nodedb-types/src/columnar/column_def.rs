// SPDX-License-Identifier: Apache-2.0

//! [`ColumnDef`] and [`ColumnModifier`] — typed column definitions for strict
//! document and columnar collections.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::column_type::ColumnType;
use super::float_width::FloatWidth;
use super::int_width::IntWidth;

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
///
/// `#[non_exhaustive]` — new fields may be added (e.g. column-level
/// compression hints, foreign-key metadata).
#[non_exhaustive]
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
    /// version 1. Columns added via `ALTER ADD COLUMN` record the schema
    /// version after the bump so the reader can build a physical sub-schema
    /// for tuples written under earlier schema versions.
    pub added_at_version: u32,
    /// The declared width of an `Int64` column. Storage stays 8 bytes.
    /// `SMALLINT` and `INTEGER` bound every stored value and narrow the wire
    /// type. `None` is `BIGINT`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub int_width: Option<IntWidth>,
    /// The declared width of a `Float64` column. Storage stays 8 bytes.
    /// `REAL` refuses a finite value past `f32` and narrows the wire type.
    /// `None` is `DOUBLE PRECISION`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub float_width: Option<FloatWidth>,
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
            int_width: None,
            float_width: None,
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
            int_width: None,
            float_width: None,
        }
    }

    /// Record the numeric width `declared` names. `declared` is the DDL type
    /// text of this column, modifiers included.
    ///
    /// Only an `Int64` column takes an integer width, and only a `Float64`
    /// column takes a float width. Every other column keeps no width.
    pub fn with_declared_width(mut self, declared: &str) -> Self {
        self.int_width = match self.column_type {
            ColumnType::Int64 => IntWidth::from_declared_type(declared),
            _ => None,
        };
        self.float_width = match self.column_type {
            ColumnType::Float64 => FloatWidth::from_declared_type(declared),
            _ => None,
        };
        self
    }

    /// The SQL type name this column declares: its numeric width when one is
    /// set, else the name of its column type. The name parses back to the
    /// same column type and width.
    pub fn declared_type_name(&self) -> String {
        match (self.int_width, self.float_width) {
            (Some(width), _) => width.pg_type_name().to_ascii_uppercase(),
            (None, Some(width)) => width.pg_type_name().to_ascii_uppercase(),
            (None, None) => self.column_type.to_string(),
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
        write!(f, "{} {}", self.name, self.declared_type_name())?;
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
    fn declared_width_lands_only_on_its_numeric_family() {
        let small = ColumnDef::nullable("n", ColumnType::Int64).with_declared_width("SMALLINT");
        assert_eq!(small.int_width, Some(IntWidth::I16));
        assert_eq!(small.float_width, None);

        let real = ColumnDef::nullable("r", ColumnType::Float64).with_declared_width("REAL");
        assert_eq!(real.float_width, Some(FloatWidth::F32));
        assert_eq!(real.int_width, None);

        let text = ColumnDef::nullable("t", ColumnType::String).with_declared_width("SMALLINT");
        assert_eq!((text.int_width, text.float_width), (None, None));
    }

    /// The declared name parses back to the column type the column holds.
    #[test]
    fn declared_type_name_parses_back_to_the_same_column() {
        for declared in ["SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE PRECISION"] {
            let column_type: ColumnType = declared.parse().expect("declared type parses");
            let column = ColumnDef::nullable("c", column_type).with_declared_width(declared);
            assert_eq!(column.declared_type_name(), declared);
            let reparsed = ColumnDef::nullable("c", column_type)
                .with_declared_width(&column.declared_type_name());
            assert_eq!(reparsed, column);
        }
        assert_eq!(
            ColumnDef::nullable("c", ColumnType::Int64).declared_type_name(),
            "BIGINT"
        );
    }
}
