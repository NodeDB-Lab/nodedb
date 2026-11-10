// SPDX-License-Identifier: BUSL-1.1

//! Row identity for set-operation merging.
//!
//! A merged row is a flat row body: a plain map or a native object
//! `[7, {...}]`. The caller has already unwrapped any `{id, data}` scan
//! envelope under its plan's row shape.
//!
//! Rows compare by column position. Each branch's positions come from the
//! plan: `QueryOp::SetOp::columns` names the row key of every output column
//! of the branch in SELECT-list order. The value at each position reads
//! typed, plain or native, through the shared canonical value identity
//! (`msgpack_scan::group_key::encode_group_values`): `5`, `5.0` and `5.00`
//! are one value of any numeric type, a string stays a string, and a nested
//! object compares independent of its map order.
//!
//! A `*` expands into its source's declared column order when the source
//! has one: a strict, columnar or timeseries collection, or a derived table
//! or CTE whose body's positions are known
//! (`output_schema::set_op_columns`). Such a branch is positional like any
//! other.
//!
//! A `*` over a schemaless source has no positional order, so its branch
//! declares no positions (an empty column list). When any branch is such a
//! `*`, every branch's rows compare by column name instead, as a set of
//! name/value pairs. This is the only case that leaves position order.

use nodedb_types::{ObjectView, Value};

use crate::util::value_identity::{canonical_text, named_row_identity, ordered};

/// How the rows of one set operation compare.
pub(super) enum RowComparison<'a> {
    /// By position: each branch's output column keys, in branch order.
    Positional(&'a [Vec<String>]),
    /// By column name, because a schemaless `*` branch declares no
    /// positions.
    Named,
}

impl<'a> RowComparison<'a> {
    /// The comparison for branches with these declared columns. `columns`
    /// holds one entry per branch.
    pub(super) fn for_branches(columns: &'a [Vec<String>], branches: usize) -> crate::Result<Self> {
        if columns.len() != branches {
            return Err(crate::Error::Internal {
                detail: format!(
                    "set operation declares columns for {} of its {branches} branches",
                    columns.len()
                ),
            });
        }
        if columns.iter().any(Vec::is_empty) {
            return Ok(Self::Named);
        }
        let mut counts = columns.iter().map(Vec::len);
        let first = counts.next().unwrap_or_default();
        if counts.any(|count| count != first) {
            return Err(crate::Error::BadRequest {
                detail: "each UNION / INTERSECT / EXCEPT query must have the same number of \
                         columns"
                    .to_string(),
            });
        }
        Ok(Self::Positional(columns))
    }

    /// The identity of one row of branch `branch`.
    pub(super) fn identity(&self, branch: usize, row: &[u8]) -> crate::Result<Vec<String>> {
        match self {
            Self::Positional(columns) => {
                let view = ObjectView::new(row).map_err(|e| decode_error(row, e))?;
                let keys = columns.get(branch).ok_or_else(|| crate::Error::Internal {
                    detail: format!("set operation has no columns for branch {branch}"),
                })?;
                keys.iter()
                    .map(|key| {
                        let value = view
                            .get(key)
                            .map_err(|e| decode_error(row, e))?
                            .unwrap_or(Value::Null);
                        canonical_text(&[ordered(value)])
                    })
                    .collect()
            }
            // A set of pairs: the identity holds no column order.
            Self::Named => named_row_identity(row),
        }
    }
}

fn decode_error(row: &[u8], detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Codec {
        detail: format!(
            "set-op merge: a {}-byte row is not a row object: {detail}",
            row.len()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, Value)]) -> Vec<u8> {
        let fields: Vec<(&str, &Value)> = pairs.iter().map(|(k, v)| (*k, v)).collect();
        nodedb_types::ordered_map_to_msgpack(&fields).unwrap()
    }

    fn columns(branches: &[&[&str]]) -> Vec<Vec<String>> {
        branches
            .iter()
            .map(|keys| keys.iter().map(|k| (*k).to_string()).collect())
            .collect()
    }

    /// Positions come from each branch's columns, never from names or map
    /// order: `SELECT b, a` row {b:1,a:2} and `SELECT x, y` row {x:1,y:2}
    /// match, and {x:2,y:1} does not.
    #[test]
    fn rows_compare_by_declared_position() {
        let declared = columns(&[&["b", "a"], &["x", "y"]]);
        let compare = RowComparison::for_branches(&declared, 2).unwrap();
        let left = compare
            .identity(
                0,
                &row(&[("b", Value::Integer(1)), ("a", Value::Integer(2))]),
            )
            .unwrap();
        let same = compare
            .identity(
                1,
                &row(&[("y", Value::Integer(2)), ("x", Value::Integer(1))]),
            )
            .unwrap();
        let swapped = compare
            .identity(
                1,
                &row(&[("x", Value::Integer(2)), ("y", Value::Integer(1))]),
            )
            .unwrap();
        assert_eq!(left, same);
        assert_ne!(left, swapped);
    }

    /// Branches of different column counts are SQL's arity error.
    #[test]
    fn branches_of_different_arity_are_refused() {
        let declared = columns(&[&["a", "b"], &["x"]]);
        assert!(matches!(
            RowComparison::for_branches(&declared, 2),
            Err(crate::Error::BadRequest { .. })
        ));
    }

    /// Typed values compare by canonical identity: a native Decimal 5.0, a
    /// Decimal 5.00, an integer 5 and a float 5 are one value; the string
    /// "5.0" is a different one.
    #[test]
    fn numbers_compare_by_exact_value_and_strings_stay_strings() {
        let declared = columns(&[&["v"]]);
        let compare = RowComparison::for_branches(&declared, 1).unwrap();
        let native = |value: Value| {
            zerompk::to_msgpack_vec(&Value::Object([("v".to_string(), value)].into())).unwrap()
        };
        let five = compare
            .identity(0, &native(Value::Decimal("5.0".parse().unwrap())))
            .unwrap();
        for other in [
            native(Value::Decimal("5.00".parse().unwrap())),
            native(Value::Integer(5)),
            row(&[("v", Value::Float(5.0))]),
        ] {
            assert_eq!(compare.identity(0, &other).unwrap(), five);
        }
        let text = compare
            .identity(0, &row(&[("v", Value::String("5.0".into()))]))
            .unwrap();
        assert_ne!(text, five);
    }

    /// A schemaless `*` branch compares every branch by column name,
    /// whatever the map order or encoding.
    #[test]
    fn star_branches_compare_by_name() {
        let declared = columns(&[&[], &["a", "b"]]);
        let compare = RowComparison::for_branches(&declared, 2).unwrap();
        let typed = Value::Object(
            [
                ("a".to_string(), Value::Integer(1)),
                (
                    "b".to_string(),
                    Value::Object(
                        [
                            ("x".to_string(), Value::Integer(1)),
                            ("y".to_string(), Value::Integer(2)),
                        ]
                        .into(),
                    ),
                ),
            ]
            .into(),
        );
        let native = zerompk::to_msgpack_vec(&typed).unwrap();
        let reordered = nodedb_types::ordered_map_to_msgpack(&[
            ("b", &typed.get("b").cloned().unwrap()),
            ("a", &Value::Integer(1)),
        ])
        .unwrap();
        assert_eq!(
            compare.identity(0, &native).unwrap(),
            compare.identity(1, &reordered).unwrap()
        );
    }

    #[test]
    fn a_malformed_native_row_is_an_error() {
        let declared = columns(&[&["a"]]);
        let compare = RowComparison::for_branches(&declared, 1).unwrap();
        let malformed = [0x92, 0x07, 0x81, 0xa1, b'a', 0x92, 0x63, 0x01];
        assert!(matches!(
            compare.identity(0, &malformed),
            Err(crate::Error::Codec { .. })
        ));
    }
}
