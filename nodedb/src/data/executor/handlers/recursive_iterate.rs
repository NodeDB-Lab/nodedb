// SPDX-License-Identifier: BUSL-1.1

//! The fixed-point iteration of a recursive CTE scan, over rows already
//! read and normalized to msgpack maps.
//!
//! The iteration stops at a fixed point (no new rows) or at the statement's
//! LIMIT. Reaching the iteration bound without a fixed point is an error:
//! the rows so far are a prefix of the recursion, never its result.

use std::collections::{HashMap, HashSet};

use nodedb_types::Value;
use nodedb_types::value::coerce::EqCoercedBucket;

use crate::bridge::scan_filter::ScanFilter;

/// One collection row: its storage id and its msgpack map.
pub(super) struct RecursiveRow {
    pub id: String,
    pub body: Vec<u8>,
}

/// The shape of one recursive CTE.
pub(super) struct RecursionSpec<'a> {
    pub base_preds: &'a [ScanFilter],
    pub recursive_preds: &'a [ScanFilter],
    /// `(collection_field, working_table_field)`.
    pub join_link: Option<&'a (String, String)>,
    pub max_iterations: usize,
    pub distinct: bool,
    pub limit: usize,
}

/// Why an iteration stopped without a result.
#[derive(Debug)]
pub(super) enum RecursionError {
    /// A predicate failed to evaluate.
    Eval(nodedb_query::EvalError),
    /// A row failed to decode for its distinct key.
    Decode(String),
    /// `max_iterations` passed with rows still to expand.
    DepthExceeded { max_depth: usize },
}

/// Run the recursion to its fixed point and return the result rows.
pub(super) fn iterate(
    rows: &[RecursiveRow],
    spec: &RecursionSpec<'_>,
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<Vec<Vec<u8>>, RecursionError> {
    let mut results: Vec<Vec<u8>> = Vec::new();
    let mut seen: HashSet<Vec<u8>> = HashSet::new();

    // A filter-only recursion admits each row once, so a base row is
    // recorded under the key the steps test: its storage id without
    // DISTINCT. Otherwise a step re-admits every base row. A linked
    // recursion without DISTINCT keeps every row it reaches.
    let (base_dedup, base_key) = match (spec.join_link, spec.distinct) {
        (_, true) => (true, ""),
        (None, false) => (true, "id"),
        (Some(_), false) => (false, ""),
    };
    for row in rows {
        if matches(spec.base_preds, &row.body, crs)? && admit(base_dedup, &mut seen, row, base_key)?
        {
            results.push(row.body.clone());
        }
    }
    if results.len() >= spec.limit {
        results.truncate(spec.limit);
        return Ok(results);
    }

    match spec.join_link {
        Some((collection_field, working_field)) => {
            // The rows a recursive step can reach, indexed once by their link
            // value: the recursive predicate and the link column do not change
            // between steps.
            let mut reachable = LinkIndex::default();
            for (index, row) in rows.iter().enumerate() {
                if matches(spec.recursive_preds, &row.body, crs)?
                    && let Some(key) = field_value(&row.body, collection_field)?
                {
                    reachable.insert(key, index);
                }
            }
            let mut frontier = results.clone();
            for _ in 0..spec.max_iterations {
                let working: Vec<Value> = frontier
                    .iter()
                    .map(|row| field_value(row, working_field))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .collect();
                if working.is_empty() {
                    return Ok(finish(results, spec.limit));
                }
                let mut step: Vec<usize> = working
                    .iter()
                    .flat_map(|key| reachable.matching(key))
                    .collect();
                // A row two working keys reach is one row of this step, in
                // collection order.
                step.sort_unstable();
                step.dedup();
                let mut new_rows = Vec::new();
                for index in step {
                    let Some(row) = rows.get(index) else {
                        continue;
                    };
                    if admit(spec.distinct, &mut seen, row, "")? {
                        new_rows.push(row.body.clone());
                    }
                }
                if new_rows.is_empty() {
                    return Ok(finish(results, spec.limit));
                }
                results.extend(new_rows.iter().cloned());
                if results.len() >= spec.limit {
                    return Ok(finish(results, spec.limit));
                }
                frontier = new_rows;
            }
        }
        None => {
            // Filter-only recursion: each step admits every collection row
            // the recursive predicate matches and that is not yet a result.
            // Without DISTINCT a row is told apart by its storage id.
            for _ in 0..spec.max_iterations {
                let mut new_rows = Vec::new();
                for row in rows {
                    if matches(spec.recursive_preds, &row.body, crs)?
                        && admit(true, &mut seen, row, if spec.distinct { "" } else { "id" })?
                    {
                        new_rows.push(row.body.clone());
                    }
                }
                if new_rows.is_empty() {
                    return Ok(finish(results, spec.limit));
                }
                results.extend(new_rows);
                if results.len() >= spec.limit {
                    return Ok(finish(results, spec.limit));
                }
            }
        }
    }
    Err(RecursionError::DepthExceeded {
        max_depth: spec.max_iterations,
    })
}

fn finish(mut results: Vec<Vec<u8>>, limit: usize) -> Vec<Vec<u8>> {
    results.truncate(limit);
    results
}

fn matches(
    preds: &[ScanFilter],
    body: &[u8],
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<bool, RecursionError> {
    ScanFilter::all_match_binary(preds, body, crs).map_err(RecursionError::Eval)
}

/// Whether `row` is new. With `distinct` off every row is new. `by` selects
/// the key: the storage id (`"id"`), else the row's whole content.
fn admit(
    distinct: bool,
    seen: &mut HashSet<Vec<u8>>,
    row: &RecursiveRow,
    by: &str,
) -> Result<bool, RecursionError> {
    if !distinct {
        return Ok(true);
    }
    let key = if by == "id" {
        format!("id:{}", row.id).into_bytes()
    } else {
        let value = nodedb_types::object_msgpack::decode_object(&row.body)
            .map_err(|e| RecursionError::Decode(e.to_string()))?;
        zerompk::to_msgpack_vec(&value).map_err(|e| RecursionError::Decode(e.to_string()))?
    };
    Ok(seen.insert(key))
}

/// The value of `field` in a msgpack map row. `None` when absent or NULL.
fn field_value(body: &[u8], field: &str) -> Result<Option<Value>, RecursionError> {
    let view = nodedb_types::object_msgpack::ObjectView::new(body)
        .map_err(|e| RecursionError::Decode(e.to_string()))?;
    view.get(field)
        .map(|value| value.filter(|value| !value.is_null()))
        .map_err(|e| RecursionError::Decode(e.to_string()))
}

/// Collection rows by link value, matched by the equality a WHERE `=` uses.
#[derive(Default)]
struct LinkIndex {
    buckets: HashMap<EqCoercedBucket, Vec<(Value, usize)>>,
}

impl LinkIndex {
    fn insert(&mut self, key: Value, row: usize) {
        if let Some(bucket) = key.eq_coerced_bucket() {
            self.buckets.entry(bucket).or_default().push((key, row));
        }
    }

    /// The rows whose link value equals `key`.
    fn matching<'a>(&'a self, key: &'a Value) -> impl Iterator<Item = usize> + 'a {
        key.eq_coerced_bucket()
            .and_then(|bucket| self.buckets.get(&bucket))
            .into_iter()
            .flatten()
            .filter(move |(value, _)| value.eq_coerced(key))
            .map(|(_, row)| *row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, fields: &[(&str, Value)]) -> RecursiveRow {
        let map = fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        RecursiveRow {
            id: id.to_string(),
            body: zerompk::to_msgpack_vec(&Value::Object(map)).expect("encode row"),
        }
    }

    #[test]
    fn native_link_and_distinct_preserve_rich_values() {
        let id = Value::Decimal("123456789.123456789".parse().expect("decimal"));
        let child_id = Value::Decimal("123456789.123456790".parse().expect("decimal"));
        let rows = vec![
            row(
                "root",
                &[("id", id.clone()), ("bytes", Value::Bytes(vec![0, 255]))],
            ),
            row("child", &[("id", child_id.clone()), ("parent", id.clone())]),
        ];
        let base = [eq_filter("id", id)];
        let link = ("parent".into(), "id".into());
        let spec = RecursionSpec {
            base_preds: &base,
            recursive_preds: &[],
            join_link: Some(&link),
            max_iterations: 5,
            distinct: true,
            limit: usize::MAX,
        };
        let result =
            iterate(&rows, &spec, &nodedb_crs::CrsContext::builtin_only()).expect("iterate");
        assert_eq!(result.len(), 2);
        assert_eq!(field_value(&result[1], "id").expect("id"), Some(child_id));
        assert_eq!(
            field_value(&result[0], "bytes").expect("bytes"),
            Some(Value::Bytes(vec![0, 255]))
        );
        assert!(field_value(&[0x92, 7, 0x81], "id").is_err());
    }

    fn eq_filter(field: &str, value: Value) -> ScanFilter {
        ScanFilter {
            field: field.into(),
            op: crate::bridge::scan_filter::FilterOp::Eq,
            value,
            clauses: Vec::new(),
            expr: None,
        }
    }

    /// A chain past 10,000 rows: every link is followed, never cut at a
    /// row count.
    fn chain(len: i64) -> Vec<RecursiveRow> {
        (0..len)
            .map(|i| {
                let parent = if i == 0 {
                    Value::Null
                } else {
                    Value::Integer(i - 1)
                };
                row(
                    &format!("r{i}"),
                    &[("id", Value::Integer(i)), ("parent", parent)],
                )
            })
            .collect()
    }

    #[test]
    fn a_tree_walk_past_ten_thousand_rows_returns_every_row() {
        let rows = chain(10_500);
        let base = [eq_filter("id", Value::Integer(0))];
        let link = ("parent".to_string(), "id".to_string());
        let spec = RecursionSpec {
            base_preds: &base,
            recursive_preds: &[],
            join_link: Some(&link),
            max_iterations: 20_000,
            distinct: true,
            limit: usize::MAX,
        };
        assert_eq!(
            iterate(&rows, &spec, &nodedb_crs::CrsContext::builtin_only())
                .expect("iterate")
                .len(),
            10_500
        );
    }

    #[test]
    fn the_iteration_bound_fails_loudly() {
        let rows = chain(50);
        let base = [eq_filter("id", Value::Integer(0))];
        let link = ("parent".to_string(), "id".to_string());
        let spec = RecursionSpec {
            base_preds: &base,
            recursive_preds: &[],
            join_link: Some(&link),
            max_iterations: 10,
            distinct: true,
            limit: usize::MAX,
        };
        assert!(matches!(
            iterate(&rows, &spec, &nodedb_crs::CrsContext::builtin_only()),
            Err(RecursionError::DepthExceeded { max_depth: 10 })
        ));
    }

    #[test]
    fn an_explicit_limit_stops_the_recursion() {
        let rows = chain(50);
        let base = [eq_filter("id", Value::Integer(0))];
        let link = ("parent".to_string(), "id".to_string());
        let spec = RecursionSpec {
            base_preds: &base,
            recursive_preds: &[],
            join_link: Some(&link),
            max_iterations: 10,
            distinct: true,
            limit: 5,
        };
        assert_eq!(
            iterate(&rows, &spec, &nodedb_crs::CrsContext::builtin_only())
                .expect("iterate")
                .len(),
            5
        );
    }

    /// A filter-only recursion without DISTINCT returns each base row once:
    /// the step that admits every collection row skips the rows the base
    /// already returned.
    #[test]
    fn a_filter_only_union_all_returns_each_base_row_once() {
        let mut rows = vec![row(
            "root",
            &[
                ("id", Value::String("root".into())),
                ("parent", Value::Null),
            ],
        )];
        for i in 0..3 {
            rows.push(row(
                &format!("c{i}"),
                &[
                    ("id", Value::String(format!("c{i}"))),
                    ("parent", Value::String("root".into())),
                ],
            ));
        }
        let base = [eq_filter("id", Value::String("root".into()))];
        let spec = RecursionSpec {
            base_preds: &base,
            recursive_preds: &[],
            join_link: None,
            max_iterations: 10,
            distinct: false,
            limit: usize::MAX,
        };
        let results =
            iterate(&rows, &spec, &nodedb_crs::CrsContext::builtin_only()).expect("iterate");
        let roots = results
            .iter()
            .filter(|body| field_value(body, "id").unwrap() == Some(Value::String("root".into())))
            .count();
        assert_eq!((results.len(), roots), (4, 1));
    }

    /// A linked recursion without DISTINCT returns the root once and each
    /// child once.
    #[test]
    fn a_linked_union_all_returns_the_root_once() {
        let mut rows = vec![row(
            "root",
            &[
                ("id", Value::String("root".into())),
                ("parent", Value::Null),
            ],
        )];
        for i in 0..3 {
            rows.push(row(
                &format!("c{i}"),
                &[
                    ("id", Value::String(format!("c{i}"))),
                    ("parent", Value::String("root".into())),
                ],
            ));
        }
        let base = [eq_filter("id", Value::String("root".into()))];
        let link = ("parent".to_string(), "id".to_string());
        let spec = RecursionSpec {
            base_preds: &base,
            recursive_preds: &[],
            join_link: Some(&link),
            max_iterations: 10,
            distinct: false,
            limit: usize::MAX,
        };
        assert_eq!(
            iterate(&rows, &spec, &nodedb_crs::CrsContext::builtin_only())
                .expect("iterate")
                .len(),
            4
        );
    }

    /// The link compares by WHERE equality: a text parent `'0'` follows the
    /// integer id `0`.
    #[test]
    fn the_link_matches_by_where_equality() {
        let rows = vec![
            row("a", &[("id", Value::Integer(0))]),
            row(
                "b",
                &[
                    ("id", Value::Integer(1)),
                    ("parent", Value::String("0".into())),
                ],
            ),
        ];
        let base = [eq_filter("id", Value::Integer(0))];
        let link = ("parent".to_string(), "id".to_string());
        let spec = RecursionSpec {
            base_preds: &base,
            recursive_preds: &[],
            join_link: Some(&link),
            max_iterations: 10,
            distinct: true,
            limit: usize::MAX,
        };
        assert_eq!(
            iterate(&rows, &spec, &nodedb_crs::CrsContext::builtin_only())
                .expect("iterate")
                .len(),
            2
        );
    }
}
