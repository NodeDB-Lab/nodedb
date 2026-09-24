// SPDX-License-Identifier: BUSL-1.1

//! Columnar-schema derivation + evolution for ILP ingest.
//!
//! Splits three concerns out of the ingest loop:
//! 1. `infer_schema` — build an initial schema from the first batch of
//!    ILP lines (timestamp + tags + fields).
//! 2. `evolve_schema` — widen an existing memtable's schema with any
//!    previously-unseen tags/fields in a subsequent batch.
//! 3. `ensure_bitemporal_columns` — append the three reserved bitemporal
//!    Int64 slots (`_ts_system`, `_ts_valid_from`, `_ts_valid_until`) to
//!    a schema that doesn't already carry them.

use nodedb_types::columnar::schema::{TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};

use super::columnar_memtable::{ColumnType, ColumnarMemtable, ColumnarSchema, TimeKind};
use super::ilp::{FieldValue, IlpLine};

/// Infers a columnar schema from a batch of ILP lines.
///
/// Scans all lines to discover tag keys and field keys, then builds
/// a schema: timestamp + tag columns (Symbol) + field columns (typed).
/// The inferred time column is `Millis`: no declaration names it an
/// instant, so it reads back as the integer the line carried.
pub fn infer_schema(lines: &[IlpLine<'_>]) -> ColumnarSchema {
    let mut tag_keys: Vec<String> = Vec::new();
    let mut field_keys: Vec<(String, ColumnType)> = Vec::new();
    let mut seen_tags: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut seen_fields: std::collections::HashSet<String> = std::collections::HashSet::new();

    for line in lines {
        for (key, _) in &line.tags {
            if seen_tags.insert(key.to_string()) {
                tag_keys.push(key.to_string());
            }
        }
        for (key, value) in &line.fields {
            if seen_fields.insert(key.to_string()) {
                let col_type = match value {
                    FieldValue::Float(_) => ColumnType::Float64,
                    FieldValue::Int(_) | FieldValue::UInt(_) => ColumnType::Int64,
                    FieldValue::Str(_) => ColumnType::Symbol,
                    FieldValue::Bool(_) => ColumnType::Float64,
                };
                field_keys.push((key.to_string(), col_type));
            }
        }
    }

    let mut columns = Vec::with_capacity(1 + tag_keys.len() + field_keys.len());
    columns.push((
        "timestamp".to_string(),
        ColumnType::Timestamp(TimeKind::Millis),
    ));
    for tag in &tag_keys {
        columns.push((tag.clone(), ColumnType::Symbol));
    }
    for (field, ty) in &field_keys {
        columns.push((field.clone(), *ty));
    }

    ColumnarSchema {
        timestamp_idx: 0,
        codecs: vec![nodedb_codec::ColumnCodec::Auto; columns.len()],
        columns,
    }
}

/// Inject the three reserved bitemporal Int64 columns (`_ts_system`,
/// `_ts_valid_from`, `_ts_valid_until`) at the end of the schema when
/// they are not already present. Kept at the tail — not the head — so
/// `timestamp_idx = 0` stays valid for the existing time-range pruning
/// path.
pub fn ensure_bitemporal_columns(schema: &mut ColumnarSchema) {
    const RESERVED: [&str; 3] = [TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL];
    let existing: std::collections::HashSet<&str> =
        schema.columns.iter().map(|(n, _)| n.as_str()).collect();
    let missing: Vec<&str> = RESERVED
        .iter()
        .copied()
        .filter(|name| !existing.contains(name))
        .collect();
    if missing.is_empty() {
        return;
    }
    for name in missing {
        schema.columns.push((name.to_string(), ColumnType::Int64));
        schema.codecs.push(nodedb_codec::ColumnCodec::Auto);
    }
}

/// Detect new fields in an ILP batch and expand the memtable schema.
///
/// Scans all lines for tag keys and field keys not present in the current
/// schema. New columns are added with NULL backfill for existing rows.
/// Must be called BEFORE `ingest_batch` so the batch can map values to
/// the expanded schema.
pub fn evolve_schema(memtable: &mut ColumnarMemtable, lines: &[IlpLine<'_>]) {
    for (name, col_type) in new_columns(memtable.schema(), lines) {
        memtable.add_column(name, col_type);
    }
}

/// The columns `evolve_schema` adds to `schema` for `lines`, in the order
/// it adds them. The first line naming a key decides its type.
pub fn new_columns(schema: &ColumnarSchema, lines: &[IlpLine<'_>]) -> Vec<(String, ColumnType)> {
    let existing: std::collections::HashSet<&str> =
        schema.columns.iter().map(|(n, _)| n.as_str()).collect();

    let mut new_columns: Vec<(String, ColumnType)> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for line in lines {
        for (key, _) in &line.tags {
            if !existing.contains(key.as_ref()) && seen.insert(key.to_string()) {
                new_columns.push((key.to_string(), ColumnType::Symbol));
            }
        }
        for (key, value) in &line.fields {
            if !existing.contains(key.as_ref()) && seen.insert(key.to_string()) {
                let col_type = match value {
                    FieldValue::Float(_) => ColumnType::Float64,
                    FieldValue::Int(_) | FieldValue::UInt(_) => ColumnType::Int64,
                    FieldValue::Str(_) => ColumnType::Symbol,
                    FieldValue::Bool(_) => ColumnType::Float64,
                };
                new_columns.push((key.to_string(), col_type));
            }
        }
    }
    new_columns
}
