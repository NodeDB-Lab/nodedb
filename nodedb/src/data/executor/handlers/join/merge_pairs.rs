// SPDX-License-Identifier: BUSL-1.1

//! Sort-merge join pairing over canonical match keys.
//!
//! Rows pair exactly when their canonical key identities are equal: the
//! same relation the hash join matches on. A row with a NULL or missing key
//! component pairs with no row. An outer join null-extends it.

use std::cmp::Ordering;

use nodedb_query::EvalError;
use nodedb_query::msgpack_scan::join_match_key;
use nodedb_types::Value;

use super::compare_join_keys;
pub(super) use super::merge_left_only::{CompositeTuples, LeftOnlyMode};
use super::merge_left_only::{LeftOnlyInput, left_only_rows};

/// One row's canonical key values and identity. `None` for a key with a
/// NULL or missing component.
pub(super) type MatchKey = Option<(Vec<Value>, String)>;

/// Each row's match key, read through its object format. A row that is not
/// a decodable object is an error.
pub(super) fn rows_match_keys(
    docs: &[(String, Vec<u8>)],
    keys: &[&str],
) -> Result<Vec<MatchKey>, EvalError> {
    docs.iter()
        .map(|(_, bytes)| join_match_key(bytes, keys))
        .collect()
}

/// Order two present keys: by canonical values in the stored-column sort
/// order, then by identity. `Equal` exactly when the identities are equal.
pub(super) fn compare_present(a: &(Vec<Value>, String), b: &(Vec<Value>, String)) -> Ordering {
    compare_join_keys(&a.0, &b.0).then_with(|| a.1.cmp(&b.1))
}

/// One output row of a sort-merge join, by input row index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Pair {
    Matched(usize, usize),
    LeftOnly(usize),
    RightOnly(usize),
}

/// How a sort-merge join pairs its sides.
pub(super) struct MergeSpec {
    /// Semi or anti: emit qualifying left rows instead of pairs.
    pub(super) left_only: Option<LeftOnlyMode>,
    /// LEFT or FULL: emit unmatched left rows.
    pub(super) is_left: bool,
    /// RIGHT or FULL: emit unmatched right rows.
    pub(super) is_right: bool,
    /// The inputs already arrive in key order.
    pub(super) pre_sorted: bool,
    /// The most pairs to emit.
    pub(super) limit: usize,
    /// Each side's key tuples for a composite `NOT IN`; `None` otherwise.
    pub(super) composite: Option<CompositeTuples>,
}

/// Indices of the rows with a present key, sorted by key unless the input
/// is pre-sorted, and the indices of the rows with a NULL key.
fn split_and_sort(keys: &[MatchKey], pre_sorted: bool) -> (Vec<usize>, Vec<usize>) {
    let (mut present, null): (Vec<usize>, Vec<usize>) =
        (0..keys.len()).partition(|&index| keys[index].is_some());
    if !pre_sorted {
        present.sort_by(|&a, &b| match (&keys[a], &keys[b]) {
            (Some(a), Some(b)) => compare_present(a, b),
            _ => Ordering::Equal,
        });
    }
    (present, null)
}

/// The end of the run of `indices[start..]` whose key equals the key at
/// `indices[start]`.
fn key_group_end(indices: &[usize], keys: &[MatchKey], start: usize) -> usize {
    let Some(Some(pivot)) = indices.get(start).map(|&index| &keys[index]) else {
        return start;
    };
    let mut end = start + 1;
    while let Some(Some(current)) = indices.get(end).map(|&index| &keys[index]) {
        if compare_present(pivot, current).is_ne() {
            break;
        }
        end += 1;
    }
    end
}

/// Pair the rows of both sides by match key, in merge order, then the
/// NULL-key rows an outer join null-extends.
pub(super) fn merge_pairs(left: &[MatchKey], right: &[MatchKey], spec: &MergeSpec) -> Vec<Pair> {
    let (left_indices, left_null) = split_and_sort(left, spec.pre_sorted);
    let (right_indices, right_null) = split_and_sort(right, spec.pre_sorted);
    if let Some(mode) = spec.left_only {
        return left_only_rows(LeftOnlyInput {
            left,
            right,
            left_indices: &left_indices,
            left_null: &left_null,
            right_indices: &right_indices,
            right_has_null: !right_null.is_empty(),
            mode,
            limit: spec.limit,
            composite: spec.composite.as_ref(),
        });
    }
    let mut pairs = Vec::new();
    let mut right_matched = vec![false; right.len()];
    let (mut li, mut ri) = (0usize, 0usize);
    while li < left_indices.len() && ri < right_indices.len() && pairs.len() < spec.limit {
        let (left_idx, right_idx) = (left_indices[li], right_indices[ri]);
        let order = match (&left[left_idx], &right[right_idx]) {
            (Some(a), Some(b)) => compare_present(a, b),
            _ => Ordering::Equal,
        };
        match order {
            Ordering::Less => {
                if spec.is_left {
                    pairs.push(Pair::LeftOnly(left_idx));
                }
                li += 1;
            }
            Ordering::Greater => {
                if spec.is_right {
                    right_matched[right_idx] = true;
                    pairs.push(Pair::RightOnly(right_idx));
                }
                ri += 1;
            }
            Ordering::Equal => {
                let left_end = key_group_end(&left_indices, left, li);
                let right_end = key_group_end(&right_indices, right, ri);
                'outer: for &lj in &left_indices[li..left_end] {
                    for &rj in &right_indices[ri..right_end] {
                        if pairs.len() >= spec.limit {
                            break 'outer;
                        }
                        right_matched[rj] = true;
                        pairs.push(Pair::Matched(lj, rj));
                    }
                }
                li = left_end;
                ri = right_end;
            }
        }
    }
    if spec.is_left {
        for &index in left_indices[li..].iter().chain(&left_null) {
            if pairs.len() >= spec.limit {
                break;
            }
            pairs.push(Pair::LeftOnly(index));
        }
    }
    if spec.is_right {
        for &index in right_indices[ri..].iter().chain(&right_null) {
            if pairs.len() >= spec.limit {
                break;
            }
            if !right_matched[index] {
                pairs.push(Pair::RightOnly(index));
            }
        }
    }
    pairs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn native(value: Value) -> Vec<u8> {
        zerompk::to_msgpack_vec(&Value::Object(
            [("k".to_owned(), value)].into_iter().collect(),
        ))
        .expect("native row")
    }

    fn plain(value: Value) -> Vec<u8> {
        nodedb_types::value_to_msgpack(&Value::Object(
            [("k".to_owned(), value)].into_iter().collect(),
        ))
        .expect("plain row")
    }

    fn keys(rows: Vec<Vec<u8>>) -> Vec<MatchKey> {
        let docs: Vec<(String, Vec<u8>)> =
            rows.into_iter().map(|row| (String::new(), row)).collect();
        rows_match_keys(&docs, &["k"]).expect("keys")
    }

    fn spec(is_left: bool, is_right: bool) -> MergeSpec {
        MergeSpec {
            left_only: None,
            is_left,
            is_right,
            pre_sorted: false,
            limit: usize::MAX,
            composite: None,
        }
    }

    fn sorted(mut pairs: Vec<Pair>) -> Vec<Pair> {
        pairs.sort_by_key(|pair| format!("{pair:?}"));
        pairs
    }

    #[test]
    fn plain_and_native_keys_pair() {
        for value in [
            Value::Integer(9_007_199_254_740_993),
            Value::String("key".into()),
            Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
        ] {
            let left = keys(vec![native(value.clone())]);
            let right = keys(vec![plain(value.clone())]);
            assert_eq!(
                merge_pairs(&left, &right, &spec(false, false)),
                vec![Pair::Matched(0, 0)],
                "{value:?}"
            );
        }
    }

    /// Numbers equal by exact value pair whatever their type, `-0.0` pairs
    /// with `0.0`, and `Float(0.1)` stays apart from `Decimal(0.1)`.
    #[test]
    fn cross_type_numbers_pair_by_exact_value() {
        let left = keys(vec![
            native(Value::Integer(5)),
            native(Value::Float(-0.0)),
            native(Value::Float(0.1)),
            native(Value::Float(1.5)),
        ]);
        let right = keys(vec![
            native(Value::Decimal("5.00".parse().expect("decimal"))),
            native(Value::Float(5.0)),
            native(Value::Float(0.0)),
            native(Value::Decimal("0.1".parse().expect("decimal"))),
            native(Value::Decimal("1.50".parse().expect("decimal"))),
        ]);
        assert_eq!(
            sorted(merge_pairs(&left, &right, &spec(false, false))),
            sorted(vec![
                Pair::Matched(0, 0),
                Pair::Matched(0, 1),
                Pair::Matched(1, 2),
                Pair::Matched(3, 4),
            ])
        );
    }

    /// A NULL or missing key pairs with nothing. An inner join drops the
    /// row, and each outer side null-extends its own NULL-key rows.
    #[test]
    fn null_keys_never_pair() {
        let empty = zerompk::to_msgpack_vec(&Value::Object(Default::default())).expect("empty");
        let left = keys(vec![
            native(Value::Null),
            empty.clone(),
            native(Value::Integer(1)),
        ]);
        let right = keys(vec![plain(Value::Null), empty, plain(Value::Integer(1))]);
        assert_eq!(
            merge_pairs(&left, &right, &spec(false, false)),
            vec![Pair::Matched(2, 2)]
        );
        assert_eq!(
            sorted(merge_pairs(&left, &right, &spec(true, false))),
            sorted(vec![
                Pair::Matched(2, 2),
                Pair::LeftOnly(0),
                Pair::LeftOnly(1)
            ])
        );
        assert_eq!(
            sorted(merge_pairs(&left, &right, &spec(false, true))),
            sorted(vec![
                Pair::Matched(2, 2),
                Pair::RightOnly(0),
                Pair::RightOnly(1)
            ])
        );
        assert_eq!(
            sorted(merge_pairs(&left, &right, &spec(true, true))),
            sorted(vec![
                Pair::Matched(2, 2),
                Pair::LeftOnly(0),
                Pair::LeftOnly(1),
                Pair::RightOnly(0),
                Pair::RightOnly(1),
            ])
        );
    }

    #[test]
    fn composite_key_with_a_null_component_never_pairs() {
        let row = |a: Value, b: Value| {
            zerompk::to_msgpack_vec(&Value::Object(
                [("a".to_owned(), a), ("b".to_owned(), b)]
                    .into_iter()
                    .collect(),
            ))
            .expect("row")
        };
        let docs = vec![(String::new(), row(Value::Integer(1), Value::Null))];
        let left = rows_match_keys(&docs, &["a", "b"]).expect("keys");
        assert_eq!(left, vec![None]);
        assert!(merge_pairs(&left, &left, &spec(false, false)).is_empty());
    }

    #[test]
    fn exact_numeric_order_groups_ties() {
        let left = keys(vec![
            native(Value::Decimal(rust_decimal::Decimal::from_i128_with_scale(
                90071992547409930001,
                4,
            ))),
            native(Value::Integer(9_007_199_254_740_993)),
            native(Value::Float(9_007_199_254_740_992.0)),
            plain(Value::Integer(9_007_199_254_740_993)),
        ]);
        let right = keys(vec![native(Value::Integer(9_007_199_254_740_993))]);
        assert_eq!(
            merge_pairs(&left, &right, &spec(false, false)),
            vec![Pair::Matched(1, 0), Pair::Matched(3, 0)]
        );
        let (order, _) = split_and_sort(&left, false);
        assert_eq!(order, vec![2, 1, 3, 0]);
    }

    fn left_only(mode: LeftOnlyMode) -> MergeSpec {
        MergeSpec {
            left_only: Some(mode),
            ..spec(false, false)
        }
    }

    /// Semi, `NOT EXISTS` and `NOT IN` over the sort-merge path agree with
    /// SQL, NULL keys and an empty subquery included.
    #[test]
    fn semi_and_anti_modes_follow_sql() {
        let outer = keys(vec![
            native(Value::Integer(1)),
            native(Value::Integer(2)),
            native(Value::Null),
        ]);
        let sub = keys(vec![plain(Value::Integer(1))]);
        let null_sub = keys(vec![plain(Value::Integer(1)), plain(Value::Null)]);
        let run = |right: &[MatchKey], mode| merge_pairs(&outer, right, &left_only(mode));
        assert_eq!(run(&sub, LeftOnlyMode::Semi), vec![Pair::LeftOnly(0)]);
        assert_eq!(
            run(&sub, LeftOnlyMode::Anti),
            vec![Pair::LeftOnly(1), Pair::LeftOnly(2)]
        );
        assert_eq!(
            run(&sub, LeftOnlyMode::AntiNullAware),
            vec![Pair::LeftOnly(1)]
        );
        assert!(run(&null_sub, LeftOnlyMode::AntiNullAware).is_empty());
        assert_eq!(
            run(&null_sub, LeftOnlyMode::Anti),
            vec![Pair::LeftOnly(1), Pair::LeftOnly(2)]
        );
        assert_eq!(run(&[], LeftOnlyMode::AntiNullAware).len(), 3);
        assert!(run(&[], LeftOnlyMode::Semi).is_empty());
    }

    /// A composite `NOT IN` in the sort-merge path follows SQL row
    /// comparison, the same as the hash join.
    #[test]
    fn composite_not_in_compares_row_wise() {
        use super::super::null_aware_tuple::KeyTuple;
        use Value::{Integer as I, Null};
        let tuple = |a: Value, b: Value| {
            let row = zerompk::to_msgpack_vec(&Value::Object(
                [("a".to_owned(), a), ("b".to_owned(), b)]
                    .into_iter()
                    .collect(),
            ))
            .expect("row");
            KeyTuple::of_row(&row, &["a", "b"]).expect("tuple")
        };
        let kept = |probe: KeyTuple, build: Vec<KeyTuple>| {
            let spec = MergeSpec {
                composite: Some(CompositeTuples {
                    left: vec![probe],
                    right: build,
                }),
                ..left_only(LeftOnlyMode::AntiNullAware)
            };
            // The match keys play no part in a composite `NOT IN`.
            !merge_pairs(&[None], &[], &spec).is_empty()
        };
        assert!(
            kept(tuple(I(1), I(1)), vec![tuple(I(2), Null)]),
            "(1,1) NOT IN ((2,NULL))"
        );
        assert!(
            !kept(tuple(I(1), I(1)), vec![tuple(I(1), Null)]),
            "(1,1) NOT IN ((1,NULL))"
        );
        assert!(
            kept(tuple(I(1), Null), vec![tuple(I(2), I(2))]),
            "(1,NULL) NOT IN ((2,2))"
        );
        assert!(
            !kept(tuple(I(1), Null), vec![tuple(I(1), I(2))]),
            "(1,NULL) NOT IN ((1,2))"
        );
        assert!(kept(tuple(I(1), Null), Vec::new()), "empty subquery");
    }

    #[test]
    fn join_types_name_their_left_only_modes() {
        assert_eq!(
            LeftOnlyMode::from_join_type("semi"),
            Some(LeftOnlyMode::Semi)
        );
        assert_eq!(
            LeftOnlyMode::from_join_type("anti"),
            Some(LeftOnlyMode::Anti)
        );
        assert_eq!(
            LeftOnlyMode::from_join_type(super::super::hash::NULL_AWARE_ANTI),
            Some(LeftOnlyMode::AntiNullAware)
        );
        assert_eq!(LeftOnlyMode::from_join_type("inner"), None);
    }

    #[test]
    fn limit_bounds_pairs() {
        let left = keys(vec![native(Value::Integer(1)), native(Value::Integer(1))]);
        let right = keys(vec![native(Value::Integer(1))]);
        let mut bounded = spec(false, false);
        bounded.limit = 1;
        assert_eq!(
            merge_pairs(&left, &right, &bounded),
            vec![Pair::Matched(0, 0)]
        );
    }

    /// The hash join and the sort-merge join pair exactly the same rows over
    /// mixed numeric types, signed zeros, NULLs and plain/native rows.
    #[test]
    fn hash_and_sort_merge_agree() {
        let values = [
            Value::Integer(5),
            Value::Float(5.0),
            Value::Decimal("5.00".parse().expect("decimal")),
            Value::Float(-0.0),
            Value::Integer(0),
            Value::Float(0.1),
            Value::Decimal("0.1".parse().expect("decimal")),
            Value::Decimal("1.50".parse().expect("decimal")),
            Value::Float(1.5),
            Value::Null,
            Value::String("5".into()),
        ];
        let left_rows: Vec<(String, Vec<u8>)> = values
            .iter()
            .map(|value| (String::new(), native(value.clone())))
            .collect();
        let right_rows: Vec<(String, Vec<u8>)> = values
            .iter()
            .map(|value| (String::new(), plain(value.clone())))
            .chain(
                values
                    .iter()
                    .map(|value| (String::new(), native(value.clone()))),
            )
            .collect();
        let index = super::super::hash::HashIndex::build(&right_rows, &["k"]).expect("build");
        let mut by_hash = Vec::new();
        for (l, (_, doc)) in left_rows.iter().enumerate() {
            for r in index.probe(doc, &["k"], &right_rows).expect("probe") {
                by_hash.push(Pair::Matched(l, r));
            }
        }
        let left = rows_match_keys(&left_rows, &["k"]).expect("left");
        let right = rows_match_keys(&right_rows, &["k"]).expect("right");
        let by_merge = merge_pairs(&left, &right, &spec(false, false));
        assert_eq!(sorted(by_hash.clone()), sorted(by_merge));
        // Right rows `11..` are the native twins of `values`. A plain row
        // holds a decimal as text, so only the native twins carry decimals.
        let native_right = |position: usize| values.len() + position;
        assert!(by_hash.contains(&Pair::Matched(0, native_right(2))));
        assert!(by_hash.contains(&Pair::Matched(1, native_right(0))));
        assert!(by_hash.contains(&Pair::Matched(3, native_right(4))));
        assert!(by_hash.contains(&Pair::Matched(7, native_right(8))));
        assert!(by_hash.contains(&Pair::Matched(5, 5)));
        assert!(!by_hash.contains(&Pair::Matched(5, native_right(6))));
        assert!(
            !by_hash
                .iter()
                .any(|pair| matches!(pair, Pair::Matched(9, _)))
        );
    }

    #[test]
    fn malformed_rows_return_errors() {
        let docs = vec![(String::new(), vec![0x92, 7, 0x81, 0xa1, b'k'])];
        assert!(rows_match_keys(&docs, &["k"]).is_err());
    }
}
