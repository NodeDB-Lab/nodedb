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
    /// Codec used for this column. Absent in legacy schemas (defaults to Auto).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) codec: Option<ColumnCodec>,
    /// Marks the collection's designated time column. A schema may hold more
    /// than one timestamp column — only one of them partitions and orders the
    /// collection, and which one cannot be recovered by scanning types.
    /// Absent in schemas written before the marker existed; those were always
    /// produced by inference, which places the designated column first.
    #[serde(default, skip_serializing_if = "is_false")]
    pub(super) time_key: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
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
                ColumnType::Timestamp(TimeKind::Millis) => TYPE_TIMESTAMP,
                ColumnType::Timestamp(TimeKind::Instant(InstantKind::Naive)) => TYPE_INSTANT_NAIVE,
                ColumnType::Timestamp(TimeKind::Instant(InstantKind::Utc)) => TYPE_INSTANT_UTC,
                ColumnType::Float64 => "float64",
                ColumnType::Int64 => "int64",
                ColumnType::Symbol => "symbol",
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

/// Position of the first timestamp column — the designation an unmarked
/// (pre-marker) schema carries implicitly, since those were written by
/// inference, which always emits the designated column first.
fn first_timestamp_idx(columns: &[(String, ColumnType)]) -> usize {
    columns.iter().position(|(_, ty)| ty.is_time()).unwrap_or(0)
}

pub(super) fn schema_from_parsed(json: &SchemaJson) -> Result<ColumnarSchema, SegmentError> {
    match json {
        SchemaJson::V2(entries) => {
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
        SchemaJson::V1(tuples) => {
            let mut columns = Vec::with_capacity(tuples.len());

            for (name, ty_str) in tuples.iter() {
                let ty = parse_column_type(ty_str)?;
                columns.push((name.clone(), ty));
            }

            let timestamp_idx = first_timestamp_idx(&columns);
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
        TYPE_TIMESTAMP => Ok(ColumnType::Timestamp(TimeKind::Millis)),
        TYPE_INSTANT_NAIVE => Ok(ColumnType::Timestamp(TimeKind::Instant(InstantKind::Naive))),
        TYPE_INSTANT_UTC => Ok(ColumnType::Timestamp(TimeKind::Instant(InstantKind::Utc))),
        "float64" => Ok(ColumnType::Float64),
        "int64" => Ok(ColumnType::Int64),
        "symbol" => Ok(ColumnType::Symbol),
        other => Err(SegmentError::Corrupt(format!(
            "unknown column type: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_v2_roundtrip() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("cpu".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![
                ColumnCodec::DoubleDelta,
                ColumnCodec::Gorilla,
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

    /// A V1 tuple schema names its time column `timestamp` and reads back as
    /// `Millis`.
    #[test]
    fn v1_schema_time_column_is_millis() {
        let json = br#"[["timestamp","timestamp"],["value","float64"]]"#;
        let parsed: SchemaJson = sonic_rs::from_slice(json).unwrap();
        let recovered = schema_from_parsed(&parsed).unwrap();
        assert_eq!(
            recovered.columns[0].1,
            ColumnType::Timestamp(TimeKind::Millis)
        );
        assert_eq!(recovered.timestamp_idx, 0);
    }
}
