// SPDX-License-Identifier: Apache-2.0

//! Decoder for search-hit rows: one `SearchResult` per row, read by column
//! name.
//!
//! Vector and text search return the hit id in the `id` column and the
//! rank value in a named score column (`distance` for vector search,
//! `score` for text search). Other columns (document fields, `_surrogate`)
//! are ignored. Both clients decode through here: the native protocol
//! carries typed cells, while pgwire can carry every cell as text.
//!
//! A row set with rows but without the id or score column is an error, and
//! so is a row whose id or score does not decode. A hit dropped or defaulted
//! here reads, to the caller, as "the search matched less".

use std::collections::HashMap;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::result::SearchResult;
use nodedb_types::value::Value;

/// Output column holding the hit id.
pub(crate) const ID_COLUMN: &str = "id";
/// Output column holding a vector hit's distance.
pub(crate) const DISTANCE_COLUMN: &str = "distance";

/// What a hit row set answers, named in decode errors.
pub(crate) struct HitSource<'a> {
    /// The client operation, e.g. `vector_search`.
    pub op: &'a str,
    pub collection: &'a str,
    /// The column holding the rank value.
    pub score_column: &'a str,
}

/// Decode every row of a search result into a hit, in row order.
///
/// An empty row set is no hits whatever its columns: pgwire reports no
/// column names for a statement that returned no rows.
pub(crate) fn decode_search_hits(
    source: &HitSource<'_>,
    columns: &[String],
    rows: &[Vec<Value>],
) -> NodeDbResult<Vec<SearchResult>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let id_idx = column_index(source, columns, ID_COLUMN)?;
    let score_idx = column_index(source, columns, source.score_column)?;
    rows.iter()
        .enumerate()
        .map(|(row_idx, row)| {
            let id = hit_id(source, row_idx, row.get(id_idx))?;
            let distance = hit_score(source, row_idx, &id, row.get(score_idx))?;
            Ok(SearchResult {
                id,
                node_id: None,
                distance,
                metadata: HashMap::new(),
            })
        })
        .collect()
}

fn column_index(source: &HitSource<'_>, columns: &[String], name: &str) -> NodeDbResult<usize> {
    columns.iter().position(|c| c == name).ok_or_else(|| {
        decode_error(
            source,
            format!("result has no '{name}' column; columns are {columns:?}"),
        )
    })
}

/// A text id, or the integer identity the server reports for a hit bound to
/// no primary key.
fn hit_id(source: &HitSource<'_>, row_idx: usize, cell: Option<&Value>) -> NodeDbResult<String> {
    match cell {
        Some(Value::String(id)) => Ok(id.clone()),
        Some(Value::Integer(id)) => Ok(id.to_string()),
        other => Err(decode_error(
            source,
            format!("row {row_idx} has id {other:?}, expected text or an integer"),
        )),
    }
}

/// A number, or the decimal text pgwire carries for one.
fn hit_score(
    source: &HitSource<'_>,
    row_idx: usize,
    id: &str,
    cell: Option<&Value>,
) -> NodeDbResult<f32> {
    match cell {
        Some(Value::Float(score)) => Ok(*score as f32),
        Some(Value::Integer(score)) => Ok(*score as f32),
        Some(Value::String(text)) => text.parse::<f32>().map_err(|e| {
            decode_error(
                source,
                format!(
                    "row {row_idx} ('{id}') {} '{text}' is not a number: {e}",
                    source.score_column
                ),
            )
        }),
        other => Err(decode_error(
            source,
            format!(
                "row {row_idx} ('{id}') has {} {other:?}, expected a number",
                source.score_column
            ),
        )),
    }
}

fn decode_error(source: &HitSource<'_>, detail: String) -> NodeDbError {
    NodeDbError::serialization(
        "search hit",
        format!("{} '{}': {detail}", source.op, source.collection),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOURCE: HitSource<'static> = HitSource {
        op: "vector_search",
        collection: "c",
        score_column: "distance",
    };

    fn names(columns: &[&str]) -> Vec<String> {
        columns.iter().map(|c| c.to_string()).collect()
    }

    #[test]
    fn hits_decode_by_column_name_in_row_order() {
        let columns = names(&["embedding", "distance", "id"]);
        let rows = vec![
            vec![Value::Null, Value::Float(0.5), Value::String("a".into())],
            vec![Value::Null, Value::Integer(2), Value::String("b".into())],
        ];
        let hits = decode_search_hits(&SOURCE, &columns, &rows).expect("decode");
        let got: Vec<(&str, f32)> = hits.iter().map(|h| (h.id.as_str(), h.distance)).collect();
        assert_eq!(got, vec![("a", 0.5), ("b", 2.0)]);
    }

    #[test]
    fn pgwire_text_cells_decode() {
        let rows = vec![vec![
            Value::String("a".into()),
            Value::String("0.25".into()),
        ]];
        let hits = decode_search_hits(&SOURCE, &names(&["id", "distance"]), &rows).expect("decode");
        assert_eq!(hits[0].distance, 0.25);
    }

    #[test]
    fn an_integer_identity_decodes_as_its_text() {
        let rows = vec![vec![Value::Integer(7), Value::Float(1.0)]];
        let hits = decode_search_hits(&SOURCE, &names(&["id", "distance"]), &rows).expect("decode");
        assert_eq!(hits[0].id, "7");
    }

    #[test]
    fn no_rows_is_no_hits_without_columns() {
        assert!(
            decode_search_hits(&SOURCE, &[], &[])
                .expect("empty")
                .is_empty()
        );
    }

    #[test]
    fn a_missing_column_is_an_error_naming_it() {
        let rows = vec![vec![Value::String("a".into())]];
        let err = decode_search_hits(&SOURCE, &names(&["id"]), &rows).expect_err("no distance");
        assert!(err.to_string().contains("'distance'"), "{err}");
    }

    #[test]
    fn a_null_id_or_bad_score_is_an_error() {
        let columns = names(&["id", "distance"]);
        let null_id = vec![vec![Value::Null, Value::Float(1.0)]];
        assert!(decode_search_hits(&SOURCE, &columns, &null_id).is_err());
        let bad_score = vec![vec![Value::String("a".into()), Value::String("x".into())]];
        assert!(decode_search_hits(&SOURCE, &columns, &bad_score).is_err());
        let null_score = vec![vec![Value::String("a".into()), Value::Null]];
        assert!(decode_search_hits(&SOURCE, &columns, &null_score).is_err());
    }
}
