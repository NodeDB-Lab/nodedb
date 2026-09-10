// SPDX-License-Identifier: BUSL-1.1

use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

/// Build a `ColumnarSchema` from raw catalog column-type strings.
///
/// `column_schema` is the list of `(column_name, type_str)` pairs from the
/// DDL catalog (`stored.fields`). Unknown type strings are treated as
/// `ColumnType::String` (matching the memtable's existing fallback).
///
/// `declared_pk` names the collection's DDL-declared `PRIMARY KEY` column,
/// when one was declared (see [`declared_primary_key_name`]). A column
/// matching that name is the schema primary key. When `declared_pk` is
/// `None`, the legacy `id` / `document_id` name convention applies instead.
///
/// Returns `None` when `column_schema` is empty (no catalog schema available
/// — test fixtures and legacy paths) or the resulting schema fails
/// validation.
///
/// This is the single source of truth for turning a catalog's raw
/// `(name, type_str)` field list into a typed `ColumnarSchema` — shared by
/// the live SQL insert path (via [`build_schema_bytes`]) and
/// `bootstrap::data_plane::load_columnar_schema_seed`, which pre-registers
/// each columnar-family collection's real schema before WAL replay so a
/// fresh `MutationEngine` never falls back to type-lossy inference.
pub(crate) fn build_columnar_schema(
    column_schema: &[(String, String)],
    declared_pk: Option<&str>,
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
        let is_id = match declared_pk {
            Some(pk) => name == pk,
            None => name == "id" || name == "document_id",
        };
        if is_id {
            has_id = true;
            cols.push(ColumnDef::required(name.clone(), col_type).with_primary_key());
        } else {
            cols.push(ColumnDef::nullable(name.clone(), col_type));
        }
    }
    // If no PK column found in stored.fields, inject a synthetic one.
    if !has_id {
        cols.insert(
            0,
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
        );
    }
    ColumnarSchema::new(cols).ok()
}

/// Build a `ColumnarSchema` from raw catalog column-type strings, then
/// serialize it as MessagePack for the `ColumnarOp::Insert::schema_bytes` field.
///
/// `declared_pk` is forwarded to [`build_columnar_schema`] unchanged.
///
/// Returns an empty `Vec` when `column_schema` is empty or fails validation
/// — see [`build_columnar_schema`] for the typed builder this wraps.
pub(crate) fn build_schema_bytes(
    column_schema: &[(String, String)],
    declared_pk: Option<&str>,
) -> Vec<u8> {
    build_columnar_schema(column_schema, declared_pk)
        .map(|schema| zerompk::to_msgpack_vec(&schema).unwrap_or_default())
        .unwrap_or_default()
}
