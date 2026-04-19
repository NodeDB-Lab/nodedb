//! Schema serialization (V2: includes codec per column).

use nodedb_codec::ColumnCodec;

use super::super::columnar_memtable::{ColumnType, ColumnarSchema};
use super::error::SegmentError;

/// Schema entry for JSON serialization.
#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct SchemaEntry {
    pub(super) name: String,
    #[serde(rename = "type")]
    pub(super) col_type: String,
    /// Codec used for this column. Absent in legacy schemas (defaults to Auto).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) codec: Option<ColumnCodec>,
}

/// Schema JSON format — V2 is an array of objects, V1 is an array of tuples.
#[derive(serde::Deserialize)]
#[serde(untagged)]
pub(super) enum SchemaJson {
    /// V2: array of `{ name, type, codec }` objects.
    V2(Vec<SchemaEntry>),
    /// V1 (legacy): array of `[name, type]` tuples.
    V1(Vec<(String, String)>),
}

pub(super) fn schema_to_json(schema: &ColumnarSchema) -> Vec<SchemaEntry> {
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(i, (name, ty))| {
            let ty_str = match ty {
                ColumnType::Timestamp => "timestamp",
                ColumnType::Float64 => "float64",
                ColumnType::Int64 => "int64",
                ColumnType::Symbol => "symbol",
            };
            let codec = schema.codecs.get(i).copied();
            SchemaEntry {
                name: name.clone(),
                col_type: ty_str.to_string(),
                codec,
            }
        })
        .collect()
}

pub(super) fn schema_from_parsed(json: &SchemaJson) -> Result<ColumnarSchema, SegmentError> {
    match json {
        SchemaJson::V2(entries) => {
            let mut columns = Vec::with_capacity(entries.len());
            let mut codecs = Vec::with_capacity(entries.len());
            let mut timestamp_idx = 0;

            for (i, entry) in entries.iter().enumerate() {
                let ty = parse_column_type(&entry.col_type)?;
                if ty == ColumnType::Timestamp {
                    timestamp_idx = i;
                }
                columns.push((entry.name.clone(), ty));
                codecs.push(entry.codec.unwrap_or(ColumnCodec::Auto));
            }

            Ok(ColumnarSchema {
                columns,
                timestamp_idx,
                codecs,
            })
        }
        SchemaJson::V1(tuples) => {
            let mut columns = Vec::with_capacity(tuples.len());
            let mut timestamp_idx = 0;

            for (i, (name, ty_str)) in tuples.iter().enumerate() {
                let ty = parse_column_type(ty_str)?;
                if ty == ColumnType::Timestamp {
                    timestamp_idx = i;
                }
                columns.push((name.clone(), ty));
            }

            Ok(ColumnarSchema {
                codecs: vec![ColumnCodec::Auto; columns.len()],
                columns,
                timestamp_idx,
            })
        }
    }
}

fn parse_column_type(ty_str: &str) -> Result<ColumnType, SegmentError> {
    match ty_str {
        "timestamp" => Ok(ColumnType::Timestamp),
        "float64" => Ok(ColumnType::Float64),
        "int64" => Ok(ColumnType::Int64),
        "symbol" => Ok(ColumnType::Symbol),
        other => Err(SegmentError::Corrupt(format!(
            "unknown column type: {other}"
        ))),
    }
}
