//! Strict document and columnar schemas with shared operations trait.

use serde::{Deserialize, Serialize};

use super::column_type::ColumnDef;
use crate::columnar::ColumnType;

/// Shared schema operations (eliminates duplication between Strict and Columnar).
pub trait SchemaOps {
    fn columns(&self) -> &[ColumnDef];

    fn column_index(&self, name: &str) -> Option<usize> {
        self.columns().iter().position(|c| c.name == name)
    }

    fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns().iter().find(|c| c.name == name)
    }

    fn primary_key_columns(&self) -> Vec<&ColumnDef> {
        self.columns().iter().filter(|c| c.primary_key).collect()
    }

    fn len(&self) -> usize {
        self.columns().len()
    }

    fn is_empty(&self) -> bool {
        self.columns().is_empty()
    }
}

/// Schema for a strict document collection (Binary Tuple serialization).
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
pub struct StrictSchema {
    pub columns: Vec<ColumnDef>,
    pub version: u16,
    /// Columns that were removed via `ALTER DROP COLUMN`. Retained so the
    /// reader can reconstruct the physical layout of tuples written before
    /// the drop.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dropped_columns: Vec<DroppedColumn>,
}

/// Tombstone for a column removed by `ALTER DROP COLUMN`.
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
pub struct DroppedColumn {
    /// The full column definition at time of drop.
    pub def: ColumnDef,
    /// The column's position in the column list before it was removed.
    pub position: usize,
    /// The schema version at which the column was dropped.
    pub dropped_at_version: u16,
}

/// Schema for a columnar collection (compressed segment files).
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
pub struct ColumnarSchema {
    pub columns: Vec<ColumnDef>,
    pub version: u16,
}

/// Schema validation errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    #[error("schema must have at least one column")]
    Empty,
    #[error("duplicate column name: '{0}'")]
    DuplicateColumn(String),
    #[error("VECTOR dimension must be positive, got 0 for column '{0}'")]
    ZeroVectorDim(String),
    #[error("primary key column '{0}' must be NOT NULL")]
    NullablePrimaryKey(String),
}

fn validate_columns(columns: &[ColumnDef]) -> Result<(), SchemaError> {
    if columns.is_empty() {
        return Err(SchemaError::Empty);
    }
    let mut seen = std::collections::HashSet::with_capacity(columns.len());
    for col in columns {
        if !seen.insert(&col.name) {
            return Err(SchemaError::DuplicateColumn(col.name.clone()));
        }
        if col.primary_key && col.nullable {
            return Err(SchemaError::NullablePrimaryKey(col.name.clone()));
        }
        if let ColumnType::Vector(0) = col.column_type {
            return Err(SchemaError::ZeroVectorDim(col.name.clone()));
        }
    }
    Ok(())
}

impl SchemaOps for StrictSchema {
    fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }
}

impl SchemaOps for ColumnarSchema {
    fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }
}

impl StrictSchema {
    pub fn new(columns: Vec<ColumnDef>) -> Result<Self, SchemaError> {
        validate_columns(&columns)?;
        Ok(Self {
            columns,
            version: 1,
            dropped_columns: Vec::new(),
        })
    }

    /// Count of variable-length columns (determines offset table size).
    pub fn variable_column_count(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| c.column_type.is_variable_length())
            .count()
    }

    /// Total fixed-field byte size (for Binary Tuple layout computation).
    pub fn fixed_fields_size(&self) -> usize {
        self.columns
            .iter()
            .filter_map(|c| c.column_type.fixed_size())
            .sum()
    }

    /// Null bitmap size in bytes.
    pub fn null_bitmap_size(&self) -> usize {
        self.columns.len().div_ceil(8)
    }

    /// Build a sub-schema matching the physical layout of tuples written at
    /// the given version. Columns added after `version` are excluded;
    /// columns dropped after `version` are re-inserted at their original
    /// positions.
    pub fn schema_for_version(&self, version: u16) -> StrictSchema {
        // Start with live columns that existed at this version.
        let mut cols: Vec<ColumnDef> = self
            .columns
            .iter()
            .filter(|c| c.added_at_version <= version)
            .cloned()
            .collect();

        // Re-insert dropped columns that were still alive at this version,
        // sorted by position (ascending) so inserts don't shift later indices.
        let mut to_reinsert: Vec<&DroppedColumn> = self
            .dropped_columns
            .iter()
            .filter(|dc| dc.def.added_at_version <= version && dc.dropped_at_version > version)
            .collect();
        to_reinsert.sort_by_key(|dc| dc.position);
        for dc in to_reinsert {
            let pos = dc.position.min(cols.len());
            cols.insert(pos, dc.def.clone());
        }

        StrictSchema {
            version,
            columns: cols,
            dropped_columns: Vec::new(),
        }
    }

    /// Parse a SQL default literal (e.g. `'n/a'`, `0`, `true`) into a `Value`.
    ///
    /// Covers the common cases produced by `ALTER ADD COLUMN ... DEFAULT ...`.
    /// Returns `Value::Null` for expressions that cannot be trivially evaluated
    /// at read time (functions, sub-queries, etc.).
    pub fn parse_default_literal(expr: &str) -> crate::value::Value {
        use crate::value::Value;

        let trimmed = expr.trim();

        // String literals: 'foo'
        if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
            return Value::String(trimmed[1..trimmed.len() - 1].replace("''", "'"));
        }

        // Boolean
        match trimmed.to_uppercase().as_str() {
            "TRUE" => return Value::Bool(true),
            "FALSE" => return Value::Bool(false),
            "NULL" => return Value::Null,
            _ => {}
        }

        // Integer
        if let Ok(i) = trimmed.parse::<i64>() {
            return Value::Integer(i);
        }

        // Float
        if let Ok(f) = trimmed.parse::<f64>() {
            return Value::Float(f);
        }

        Value::Null
    }
}

impl ColumnarSchema {
    pub fn new(columns: Vec<ColumnDef>) -> Result<Self, SchemaError> {
        validate_columns(&columns)?;
        Ok(Self {
            columns,
            version: 1,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::ColumnType;

    #[test]
    fn strict_schema_validation() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("name", ColumnType::String),
        ]);
        assert!(schema.is_ok());
        assert!(StrictSchema::new(vec![]).is_err());
    }

    #[test]
    fn schema_ops_trait() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("name", ColumnType::String),
            ColumnDef::nullable("balance", ColumnType::Decimal),
        ])
        .unwrap();
        assert_eq!(schema.len(), 3);
        assert_eq!(schema.column_index("balance"), Some(2));
        assert!(schema.column("nonexistent").is_none());
        assert_eq!(schema.primary_key_columns().len(), 1);
    }

    #[test]
    fn strict_layout_helpers() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("name", ColumnType::String),
            ColumnDef::nullable("balance", ColumnType::Decimal),
            ColumnDef::nullable("bio", ColumnType::String),
        ])
        .unwrap();
        assert_eq!(schema.null_bitmap_size(), 1);
        assert_eq!(schema.fixed_fields_size(), 8 + 16);
        assert_eq!(schema.variable_column_count(), 2);
    }

    #[test]
    fn columnar_schema_validation() {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("time", ColumnType::Timestamp),
            ColumnDef::nullable("cpu", ColumnType::Float64),
        ]);
        assert!(schema.is_ok());
        assert_eq!(schema.unwrap().len(), 2);
    }

    #[test]
    fn nullable_pk_rejected() {
        let cols = vec![ColumnDef {
            name: "id".into(),
            column_type: ColumnType::Int64,
            nullable: true,
            default: None,
            primary_key: true,
            modifiers: Vec::new(),
            generated_expr: None,
            generated_deps: Vec::new(),
            added_at_version: 1,
        }];
        assert!(matches!(
            StrictSchema::new(cols),
            Err(SchemaError::NullablePrimaryKey(_))
        ));
    }

    #[test]
    fn zero_vector_dim_rejected() {
        let cols = vec![ColumnDef::required("emb", ColumnType::Vector(0))];
        assert!(matches!(
            StrictSchema::new(cols),
            Err(SchemaError::ZeroVectorDim(_))
        ));
    }
}
