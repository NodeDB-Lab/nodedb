// SPDX-License-Identifier: BUSL-1.1

pub(crate) use nodedb_types::DEFAULT_IDENTITY_COLUMN;
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

/// Build a `ColumnarSchema` from raw catalog column-type strings.
///
/// `column_schema` is the list of `(column_name, type_str)` pairs from the
/// DDL catalog (`stored.fields`). Unknown type strings are treated as
/// `ColumnType::String` (matching the memtable's existing fallback).
///
/// `identity_column` names the column that carries the row's identity: the
/// DDL-declared `PRIMARY KEY` when one exists, else the engine's resolved
/// primary key. The column of that name is the schema primary key. When no
/// column carries that name, a required `String` column is synthesized under
/// it.
///
/// Returns `None` when `column_schema` is empty, meaning no catalog schema is
/// available, or when the resulting schema fails validation.
///
/// This is the single source of truth for turning a catalog's raw
/// `(name, type_str)` field list into a typed `ColumnarSchema` — shared by
/// the live SQL insert path (via [`build_schema_bytes`]) and
/// `bootstrap::data_plane::load_columnar_schema_seed`, which pre-registers
/// each columnar-family collection's real schema before WAL replay so a
/// fresh `MutationEngine` never falls back to type-lossy inference.
pub(crate) fn build_columnar_schema(
    column_schema: &[(String, String)],
    identity_column: &str,
) -> Option<ColumnarSchema> {
    if column_schema.is_empty() {
        return None;
    }
    let mut cols = Vec::with_capacity(column_schema.len());
    let mut has_id = false;
    for (name, type_str) in column_schema {
        // `type_str` may contain SQL modifiers such as `NOT NULL` or `PRIMARY KEY`
        // (e.g. "BIGINT NOT NULL"). Strip everything after the first token so that
        // `ColumnType::from_str` receives the bare type name (e.g. "BIGINT").
        let bare_type = type_str
            .split_whitespace()
            .next()
            .unwrap_or(type_str.as_str());
        let col_type = bare_type
            .parse::<ColumnType>()
            .unwrap_or(ColumnType::String);
        if name == identity_column {
            has_id = true;
            cols.push(ColumnDef::required(name.clone(), col_type).with_primary_key());
        } else {
            cols.push(ColumnDef::nullable(name.clone(), col_type));
        }
    }
    // No column carries the identity: synthesize it.
    if !has_id {
        cols.insert(
            0,
            ColumnDef::required(identity_column, ColumnType::String).with_primary_key(),
        );
    }
    ColumnarSchema::new(cols).ok()
}

/// Build a `ColumnarSchema` from raw catalog column-type strings, then
/// serialize it as MessagePack for the `ColumnarOp::Insert::schema_bytes` field.
///
/// `identity_column` is forwarded to [`build_columnar_schema`] unchanged.
///
/// Returns an empty `Vec` when `column_schema` is empty or fails validation
/// — see [`build_columnar_schema`] for the typed builder this wraps.
pub(in super::super) fn build_schema_bytes(
    column_schema: &[(String, String)],
    identity_column: &str,
) -> Vec<u8> {
    build_columnar_schema(column_schema, identity_column)
        .map(|schema| zerompk::to_msgpack_vec(&schema).unwrap_or_default())
        .unwrap_or_default()
}
