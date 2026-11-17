// SPDX-License-Identifier: BUSL-1.1

//! The source side of `UPDATE ... FROM` and `MERGE`, built from the rows the
//! Control Plane read and shipped. The Data Plane holds no read path for a
//! source relation, so both statements build their source here and cannot
//! diverge on decode, key equality, or duplicate handling.
//!
//! Join keys compare by [`Value::eq_coerced`], the equality a WHERE `=`
//! uses: `1`, `1.0`, `'1'`, and `'1.0'` all match an integer `1`, and NULL
//! matches nothing. Rows are bucketed by [`Value::eq_coerced_bucket`] and
//! each candidate in a bucket is confirmed with `eq_coerced`.

use std::collections::HashMap;

use nodedb_types::Value;
use nodedb_types::value::coerce::EqCoercedBucket;

use crate::data::executor::doc_format;

/// One shipped source row.
pub(in crate::data::executor) struct SourceRow {
    /// The row's position in the shipped source, as text. Both MERGE passes
    /// build from the same shipped rows, so it names the same row in each.
    pub id: String,
    /// The row's position in the shipped source: its place in source order.
    pub index: usize,
    pub doc: serde_json::Value,
    /// The join column's value. `None` when absent or NULL: the row joins
    /// no target row.
    key: Option<Value>,
}

/// The shipped source rows, indexed by join key.
pub(in crate::data::executor) struct JoinSource {
    rows: Vec<SourceRow>,
    buckets: HashMap<EqCoercedBucket, Vec<usize>>,
}

impl JoinSource {
    /// Decode the shipped rows (standard msgpack maps) and index them by
    /// `join_col`.
    ///
    /// Two rows with equal join keys are an error: each would act on the same
    /// target row, and which one wins would depend on read order. A row that
    /// fails to decode is an error too, since skipping it would silently
    /// leave target rows unjoined.
    pub(in crate::data::executor) fn build(
        statement: &str,
        source_label: &str,
        join_col: &str,
        shipped: &[Vec<u8>],
    ) -> crate::Result<Self> {
        let mut rows = Vec::with_capacity(shipped.len());
        let mut buckets: HashMap<EqCoercedBucket, Vec<usize>> = HashMap::new();
        for (index, bytes) in shipped.iter().enumerate() {
            let doc = doc_format::decode_document(bytes)?;
            let key = doc
                .get(join_col)
                .map(|value| Value::from(value.clone()))
                .filter(|value| !value.is_null());
            if let Some(this_key) = key.as_ref()
                && let Some(bucket) = this_key.eq_coerced_bucket()
            {
                let slot = buckets.entry(bucket).or_default();
                let duplicate = slot
                    .iter()
                    .find_map(|&other| rows_key(&rows, other).filter(|k| k.eq_coerced(this_key)));
                if let Some(other_key) = duplicate {
                    let key = this_key;
                    return Err(crate::Error::BadRequest {
                        detail: format!(
                            "{statement}: more than one row of source '{source_label}' has \
                             {join_col} = '{key}' (also '{other_key}'); each target row must \
                             match at most one source row"
                        ),
                    });
                }
                slot.push(index);
            }
            rows.push(SourceRow {
                id: index.to_string(),
                index,
                doc,
                key,
            });
        }
        Ok(Self { rows, buckets })
    }

    /// The source row whose join key equals `target_key`, by the equality a
    /// WHERE `=` uses. `None` for an absent or NULL target key.
    pub(in crate::data::executor) fn probe(
        &self,
        target_key: Option<&serde_json::Value>,
    ) -> Option<&SourceRow> {
        let target = Value::from(target_key?.clone());
        let bucket = target.eq_coerced_bucket()?;
        self.buckets.get(&bucket)?.iter().find_map(|&index| {
            let row = self.rows.get(index)?;
            row.key
                .as_ref()
                .is_some_and(|key| key.eq_coerced(&target))
                .then_some(row)
        })
    }

    /// Every source row in shipped order, joinable or not.
    pub(in crate::data::executor) fn rows(&self) -> &[SourceRow] {
        &self.rows
    }

    pub(in crate::data::executor) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

fn rows_key(rows: &[SourceRow], index: usize) -> Option<&Value> {
    rows.get(index).and_then(|row| row.key.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: Value, name: &str) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert("id".to_string(), id);
        obj.insert("name".to_string(), Value::String(name.into()));
        nodedb_types::value_to_msgpack(&Value::Object(obj)).expect("encode row")
    }

    fn name_of(source: &JoinSource, key: serde_json::Value) -> Option<String> {
        source
            .probe(Some(&key))
            .and_then(|row| row.doc.get("name"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    }

    #[test]
    fn keys_match_by_where_equality() {
        let rows = vec![
            row(Value::String("k1".into()), "text"),
            row(Value::Integer(2), "two"),
            row(Value::String("3.0".into()), "three"),
        ];
        let source = JoinSource::build("MERGE", "src", "id", &rows).expect("build source");
        assert_eq!(
            name_of(&source, serde_json::json!("k1")).as_deref(),
            Some("text")
        );
        // `2 = '2'` and `2 = 2.0` hold in a WHERE, so they join.
        assert_eq!(
            name_of(&source, serde_json::json!("2")).as_deref(),
            Some("two")
        );
        assert_eq!(
            name_of(&source, serde_json::json!(2.0)).as_deref(),
            Some("two")
        );
        // `'3.0' = 3` holds in a WHERE; it joined nothing as text.
        assert_eq!(
            name_of(&source, serde_json::json!(3)).as_deref(),
            Some("three")
        );
        // `'3.0' = '3'` compares text, which differs, as in a WHERE.
        assert_eq!(name_of(&source, serde_json::json!("3")), None);
        assert_eq!(name_of(&source, serde_json::json!("k2")), None);
    }

    #[test]
    fn a_null_key_joins_nothing_but_stays_a_source_row() {
        let rows = vec![row(Value::Null, "orphan")];
        let source = JoinSource::build("MERGE", "src", "id", &rows).expect("build source");
        assert!(source.probe(Some(&serde_json::Value::Null)).is_none());
        assert!(source.probe(None).is_none());
        assert_eq!(
            source.rows().len(),
            1,
            "a NULL-key row is still a source row"
        );
    }

    #[test]
    fn equal_keys_of_different_types_are_duplicates() {
        let rows = vec![
            row(Value::Integer(1), "int"),
            row(Value::String("1.0".into()), "text"),
        ];
        for statement in ["MERGE", "UPDATE ... FROM"] {
            match JoinSource::build(statement, "src", "id", &rows) {
                Err(crate::Error::BadRequest { detail }) => {
                    assert!(detail.starts_with(statement), "{detail}");
                    assert!(detail.contains("'src'"), "{detail}");
                }
                Err(other) => panic!("expected a duplicate-key error, got {other:?}"),
                Ok(_) => panic!("expected a duplicate-key error, got a source"),
            }
        }
    }

    #[test]
    fn an_undecodable_row_is_an_error() {
        let rows = vec![vec![0xc1]];
        assert!(JoinSource::build("UPDATE ... FROM", "src", "id", &rows).is_err());
    }
}
