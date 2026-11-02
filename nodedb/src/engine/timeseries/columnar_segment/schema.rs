// SPDX-License-Identifier: BUSL-1.1

//! Schema serialization (V2: includes codec per column).

use nodedb_codec::ColumnCodec;
use nodedb_types::InstantKind;

use super::super::columnar_memtable::{ColumnType, ColumnarSchema, TimeKind};
use super::error::SegmentError;

/// On-disk type name of a millisecond time column read back as an integer.
const TYPE_TIMESTAMP: &str = "timestamp";
/// On-disk type name of a time column read back as a naive instant.
const TYPE_INSTANT_NAIVE: &str = "instant_naive";
/// On-disk type name of a time column read back as a UTC instant.
const TYPE_INSTANT_UTC: &str = "instant_utc";

/// Schema entry for JSON serialization.
#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct SchemaEntry {
    pub(super) name: String,
    #[serde(rename = "type")]
    pub(super) col_type: String,
    /// Codec used for this column. Absent when the schema records no codec
    /// for the column, which reads as `Auto`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) codec: Option<ColumnCodec>,
    /// Marks the collection's designated time column. A schema may hold more
    /// than one timestamp column — only one of them partitions and orders the
    /// collection, and which one cannot be recovered by scanning types.
    /// The writer omits the key for every column but the designated one.
    /// A schema whose marked column is not a time column reads its first
    /// time column as the designated one.
    #[serde(default, skip_serializing_if = "is_false")]
    pub(super) time_key: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Schema JSON format: an array of `{ name, type, codec, time_key }` objects.
pub(super) type SchemaJson = Vec<SchemaEntry>;

pub(super) fn schema_to_json(schema: &ColumnarSchema) -> Vec<SchemaEntry> {
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(i, (name, ty))| {
            let ty_str = match ty {
                ColumnType::Timestamp(TimeKind::Millis) => TYPE_TIMESTAMP,
                ColumnType::Timestamp(TimeKind::Instant(InstantKind::Naive)) => TYPE_INSTANT_NAIVE,
                ColumnType::Timestamp(TimeKind::Instant(InstantKind::Utc)) => TYPE_INSTANT_UTC,
                ColumnType::Float64 => "float64",
                ColumnType::Int64 => "int64",
                ColumnType::Symbol => "symbol",
                ColumnType::Text => "text",
            };
            let codec = schema.codecs.get(i).copied();
            SchemaEntry {
                name: name.clone(),
                col_type: ty_str.to_string(),
                codec,
                time_key: i == schema.timestamp_idx,
            }
        })
        .collect()
}

/// Position of the first timestamp column. A schema with no marked time
/// column designates it.
fn first_timestamp_idx(columns: &[(String, ColumnType)]) -> usize {
    columns.iter().position(|(_, ty)| ty.is_time()).unwrap_or(0)
}

/// Rebuild a schema from its JSON entries.
pub(super) fn schema_from_parsed(entries: &[SchemaEntry]) -> Result<ColumnarSchema, SegmentError> {
    let mut columns = Vec::with_capacity(entries.len());
    let mut codecs = Vec::with_capacity(entries.len());
    let mut marked_idx = None;

    for (i, entry) in entries.iter().enumerate() {
        let ty = parse_column_type(&entry.col_type)?;
        if entry.time_key && ty.is_time() {
            marked_idx = Some(i);
        }
        columns.push((entry.name.clone(), ty));
        codecs.push(entry.codec.unwrap_or(ColumnCodec::Auto));
    }

    let timestamp_idx = marked_idx.unwrap_or_else(|| first_timestamp_idx(&columns));
    Ok(ColumnarSchema {
        columns,
        timestamp_idx,
        codecs,
    })
}

fn parse_column_type(ty_str: &str) -> Result<ColumnType, SegmentError> {
    match ty_str {
        TYPE_TIMESTAMP => Ok(ColumnType::Timestamp(TimeKind::Millis)),
        TYPE_INSTANT_NAIVE => Ok(ColumnType::Timestamp(TimeKind::Instant(InstantKind::Naive))),
        TYPE_INSTANT_UTC => Ok(ColumnType::Timestamp(TimeKind::Instant(InstantKind::Utc))),
        "float64" => Ok(ColumnType::Float64),
        "int64" => Ok(ColumnType::Int64),
        "symbol" => Ok(ColumnType::Symbol),
        "text" => Ok(ColumnType::Text),
        other => Err(SegmentError::Corrupt(format!(
            "unknown column type: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_roundtrip() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("cpu".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
                ("msg".into(), ColumnType::Text),
            ],
            timestamp_idx: 0,
            codecs: vec![
                ColumnCodec::DoubleDelta,
                ColumnCodec::Gorilla,
                ColumnCodec::Raw,
                ColumnCodec::Raw,
            ],
        };
        let json = sonic_rs::to_vec(&schema_to_json(&schema)).unwrap();
        let parsed: SchemaJson = sonic_rs::from_slice(&json).unwrap();
        let recovered = schema_from_parsed(&parsed).unwrap();

        assert_eq!(recovered.columns, schema.columns);
        assert_eq!(recovered.timestamp_idx, 0);
        assert_eq!(recovered.codecs, schema.codecs);
    }

    /// Every time kind has its own on-disk type name and reads back as itself.
    #[test]
    fn schema_keeps_the_time_kind_of_every_time_column() {
        let schema = ColumnarSchema {
            columns: vec![
                (
                    "ts".into(),
                    ColumnType::Timestamp(TimeKind::Instant(InstantKind::Naive)),
                ),
                (
                    "seen_at".into(),
                    ColumnType::Timestamp(TimeKind::Instant(InstantKind::Utc)),
                ),
                ("_ts_system".into(), ColumnType::Timestamp(TimeKind::Millis)),
            ],
            timestamp_idx: 0,
            codecs: vec![ColumnCodec::Auto; 3],
        };
        let entries = schema_to_json(&schema);
        assert_eq!(entries[0].col_type, TYPE_INSTANT_NAIVE);
        assert_eq!(entries[1].col_type, TYPE_INSTANT_UTC);
        assert_eq!(entries[2].col_type, TYPE_TIMESTAMP);

        let json = sonic_rs::to_vec(&entries).unwrap();
        let parsed: SchemaJson = sonic_rs::from_slice(&json).unwrap();
        let recovered = schema_from_parsed(&parsed).unwrap();
        assert_eq!(recovered.columns, schema.columns);
        assert_eq!(recovered.timestamp_idx, 0);
    }
}
