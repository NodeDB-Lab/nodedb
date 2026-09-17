// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral shaped row set: typed cells keyed by column.

use std::collections::BTreeMap;

use nodedb_types::Value;

/// Protocol-neutral SQL column type, mapped to each entrypoint's own wire
/// type. One variant per pgwire field-builder, so the mapping is lossless.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DdlColType {
    #[default]
    Text,
    Int8,
    Int4,
    Int2,
    Float8,
    Float4,
    Bool,
    Bytea,
    Json,
    Jsonb,
    Timestamp,
    Timestamptz,
    Varchar,
    Float4Array,
    Float8Array,
}

/// One shaped row: typed cells keyed by [`ShapedRows::cell_keys`].
///
/// A `BTreeMap` iterates in sorted key order, which is the order column
/// derivation reads a row's keys in.
pub type ShapedRow = BTreeMap<String, Value>;

/// Protocol-neutral shaped row set: columns + row objects + an optional
/// client-facing notice.
#[derive(Debug, Clone)]
pub struct ShapedRows {
    pub columns: Vec<String>,
    /// Per-column SQL type, parallel to `columns`. Only pgwire consumes this
    /// (RowDescription OIDs); `Text` when the source type is unknown.
    pub column_types: Vec<DdlColType>,
    /// One map per row, keyed by [`ShapedRows::cell_keys`] not `columns` —
    /// SQL output names may repeat and a map can't hold two cells per key.
    pub rows: Vec<ShapedRow>,
    pub notice: Option<String>,
}

impl ShapedRows {
    /// Build a `column_types` vec of `n` `Text` entries, for non-DDL sites
    /// whose consumers ignore column types.
    pub fn text_types(n: usize) -> Vec<DdlColType> {
        vec![DdlColType::Text; n]
    }

    /// A result set from typed rows, with one catalog type per column and no
    /// notice.
    pub fn from_rows(
        columns: Vec<String>,
        column_types: Vec<DdlColType>,
        rows: Vec<ShapedRow>,
    ) -> Self {
        Self {
            columns,
            column_types,
            rows,
            notice: None,
        }
    }

    /// A result set from decoded JSON rows, with one catalog type per column
    /// and no notice. Each JSON cell becomes the matching [`Value`]: a JSON
    /// string is a `Value::String`, never parsed further.
    pub fn from_json_rows(
        columns: Vec<String>,
        column_types: Vec<DdlColType>,
        rows: Vec<serde_json::Map<String, serde_json::Value>>,
    ) -> Self {
        let rows = rows.into_iter().map(json_row_to_shaped).collect();
        Self::from_rows(columns, column_types, rows)
    }

    /// A result set whose every column is `Text`: the shape a DDL or
    /// inspection statement answers with. The type list is sized from
    /// `columns`, so the two cannot disagree.
    pub fn text_rows(
        columns: Vec<String>,
        rows: Vec<serde_json::Map<String, serde_json::Value>>,
    ) -> Self {
        let column_types = Self::text_types(columns.len());
        Self::from_json_rows(columns, column_types, rows)
    }

    /// Attach a client-facing notice.
    pub fn with_notice(mut self, notice: impl Into<String>) -> Self {
        self.notice = Some(notice.into());
        self
    }

    /// Fold another shaped result into this one so N tasks answer with ONE result
    /// set — some drivers reject multiple result sets. Columns are the union of
    /// every contributor's; rows read by key so a missing column encodes NULL.
    pub fn append(&mut self, other: ShapedRows) {
        if self.notice.is_none() {
            self.notice = other.notice;
        }
        if self.columns.is_empty() {
            self.columns = other.columns;
            self.column_types = other.column_types;
            self.rows.extend(other.rows);
            return;
        }
        for (index, name) in other.columns.iter().enumerate() {
            if self.columns.iter().any(|existing| existing == name) {
                continue;
            }
            self.columns.push(name.clone());
            self.column_types
                .push(other.column_types.get(index).copied().unwrap_or_default());
        }
        self.rows.extend(other.rows);
    }

    /// Per-column keys for reading cells out of [`ShapedRows::rows`]. Identical
    /// to `columns` unless names collide, then later duplicates take a `_<n>` suffix
    /// — visible in HTTP JSON, but pgwire/native stay positional.
    pub fn cell_keys(&self) -> Vec<String> {
        crate::control::server::response_shape::project::cell_keys(&self.columns)
    }
}

/// Convert one JSON row map to a typed row, cell by cell.
fn json_row_to_shaped(row: serde_json::Map<String, serde_json::Value>) -> ShapedRow {
    row.into_iter().map(|(k, v)| (k, Value::from(v))).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shaped(columns: &[&str], rows: &[&[(&str, &str)]]) -> ShapedRows {
        ShapedRows {
            columns: columns.iter().map(|c| (*c).to_string()).collect(),
            column_types: ShapedRows::text_types(columns.len()),
            rows: rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|(k, v)| ((*k).to_string(), Value::String((*v).to_string())))
                        .collect()
                })
                .collect(),
            notice: None,
        }
    }

    /// A column only a later contributor carries must survive the fold — a key
    /// absent from `columns` is never read by `cell_keys()`.
    #[test]
    fn append_unions_a_column_only_a_later_row_carries() {
        let mut merged = shaped(&["id", "name"], &[&[("id", "r1"), ("name", "a")]]);
        merged.append(shaped(
            &["id", "name", "extra"],
            &[&[("id", "r2"), ("name", "b"), ("extra", "x")]],
        ));

        assert_eq!(merged.columns, vec!["id", "name", "extra"]);
        assert_eq!(
            merged.column_types.len(),
            merged.columns.len(),
            "column types must stay parallel to columns"
        );

        let keys = merged.cell_keys();
        assert_eq!(keys, vec!["id", "name", "extra"]);
        assert_eq!(
            merged.rows[1].get(keys[2].as_str()),
            Some(&Value::String("x".to_string())),
            "the later row's extra value must be readable through the merged keys"
        );
        assert!(
            !merged.rows[0].contains_key(keys[2].as_str()),
            "the row that lacks the column encodes as NULL, not a shifted cell"
        );
    }

    /// The first contributor's columns keep their positions and newly-seen
    /// columns are appended in first-seen order, so a positional client never
    /// sees a column move between rows.
    #[test]
    fn append_keeps_the_first_contributors_column_order_and_appends_the_rest() {
        let mut merged = shaped(&["b", "a"], &[&[("b", "1"), ("a", "2")]]);
        merged.append(shaped(&["a", "z"], &[&[("a", "3"), ("z", "4")]]));
        merged.append(shaped(&["y", "b"], &[&[("y", "5"), ("b", "6")]]));

        assert_eq!(
            merged.columns,
            vec!["b", "a", "z", "y"],
            "first contributor's positions are fixed; later columns append in \
             first-seen order"
        );
        assert_eq!(merged.rows.len(), 3);
    }

    /// A contributor with no columns at all — a task whose rows were entirely
    /// removed by a read policy, which shapes as `RETURNING *` with an empty
    /// column list — must not fix an empty shape for the statement.
    #[test]
    fn append_adopts_the_shape_of_the_first_contributor_that_has_columns() {
        let mut merged = shaped(&[], &[]);
        merged.append(shaped(&["id"], &[&[("id", "r1")]]));

        assert_eq!(merged.columns, vec!["id"]);
        assert_eq!(merged.rows.len(), 1);
    }

    /// Every JSON cell kind lands as the matching typed cell; a JSON string
    /// stays a string and is never parsed into an instant or a number.
    #[test]
    fn from_json_rows_maps_each_json_kind_to_the_matching_value() {
        let row = match serde_json::json!({
            "n": 7,
            "f": 1.5,
            "s": "2020-03-05T10:00:00.000000Z",
            "z": null,
            "b": true,
            "a": [1, "x"],
            "o": {"k": false},
        }) {
            serde_json::Value::Object(map) => map,
            other => panic!("fixture must be an object, got {other}"),
        };
        let columns: Vec<String> = ["n", "f", "s", "z", "b", "a", "o"]
            .iter()
            .map(|c| (*c).to_string())
            .collect();
        let shaped = ShapedRows::from_json_rows(
            columns.clone(),
            ShapedRows::text_types(columns.len()),
            vec![row],
        );

        let cells = &shaped.rows[0];
        assert_eq!(cells["n"], Value::Integer(7));
        assert_eq!(cells["f"], Value::Float(1.5));
        assert_eq!(
            cells["s"],
            Value::String("2020-03-05T10:00:00.000000Z".to_string())
        );
        assert_eq!(cells["z"], Value::Null);
        assert_eq!(cells["b"], Value::Bool(true));
        assert_eq!(
            cells["a"],
            Value::Array(vec![Value::Integer(1), Value::String("x".to_string())])
        );
        assert_eq!(
            cells["o"],
            Value::Object(std::collections::HashMap::from([(
                "k".to_string(),
                Value::Bool(false)
            )]))
        );
        assert!(shaped.notice.is_none());
    }
}
