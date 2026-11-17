// SPDX-License-Identifier: BUSL-1.1

//! Semi and anti sort-merge joins: emit left rows by whether each matches.

use super::merge_pairs::{MatchKey, Pair, compare_present};
use super::null_aware_tuple::{KeyTuple, PossibleMatchIndex};

/// A join that emits only left rows, by whether each has a match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LeftOnlyMode {
    /// `EXISTS` / `IN`: left rows with a match.
    Semi,
    /// `NOT EXISTS`: left rows without a match, NULL keys included.
    Anti,
    /// `NOT IN`: as `Anti`, but a NULL in a non-empty right side rejects
    /// every left row, and a NULL left key is rejected unless the right side
    /// is empty.
    AntiNullAware,
}

impl LeftOnlyMode {
    /// The mode a physical join type names, `None` for a pairing join.
    pub(super) fn from_join_type(join_type: &str) -> Option<Self> {
        match join_type {
            "semi" => Some(Self::Semi),
            "anti" => Some(Self::Anti),
            super::hash::NULL_AWARE_ANTI => Some(Self::AntiNullAware),
            _ => None,
        }
    }
}

/// The sorted sides of a semi or anti sort-merge join.
pub(super) struct LeftOnlyInput<'a> {
    pub(super) left: &'a [MatchKey],
    pub(super) right: &'a [MatchKey],
    pub(super) left_indices: &'a [usize],
    pub(super) left_null: &'a [usize],
    pub(super) right_indices: &'a [usize],
    pub(super) right_has_null: bool,
    pub(super) mode: LeftOnlyMode,
    pub(super) limit: usize,
    /// Each side's key tuples, for a composite `NOT IN`: SQL row comparison
    /// decides it, not the match keys.
    pub(super) composite: Option<&'a CompositeTuples>,
}

/// Both sides' key tuples of a composite `NOT IN`, positionally aligned
/// with the rows.
#[derive(Debug, Default)]
pub(super) struct CompositeTuples {
    pub(super) left: Vec<KeyTuple>,
    pub(super) right: Vec<KeyTuple>,
}

/// Left rows of a composite `NOT IN`, in input order: a row is kept when
/// the right side is empty, or no right tuple possibly equals it under SQL
/// row comparison. One index pass over the right side, then one lookup per
/// NULL pattern per left row.
fn composite_not_in_rows(tuples: &CompositeTuples, limit: usize) -> Vec<Pair> {
    let mut index = PossibleMatchIndex::default();
    for tuple in &tuples.right {
        index.insert(tuple.clone());
    }
    tuples
        .left
        .iter()
        .enumerate()
        .filter(|(_, tuple)| index.is_empty() || !index.possibly_matches(tuple))
        .take(limit)
        .map(|(l, _)| Pair::LeftOnly(l))
        .collect()
}

/// Left rows a semi or anti join emits, in merge order, then NULL-key rows.
pub(super) fn left_only_rows(input: LeftOnlyInput<'_>) -> Vec<Pair> {
    let LeftOnlyInput {
        left,
        right,
        left_indices,
        left_null,
        right_indices,
        right_has_null,
        mode,
        limit,
        composite,
    } = input;
    if mode == LeftOnlyMode::AntiNullAware
        && let Some(tuples) = composite
    {
        return composite_not_in_rows(tuples, limit);
    }
    let right_empty = right.is_empty();
    // `x NOT IN (s)` with a NULL in a non-empty `s` is never true.
    if mode == LeftOnlyMode::AntiNullAware && !right_empty && right_has_null {
        return Vec::new();
    }
    let mut matched = vec![false; left.len()];
    let mut ri = 0usize;
    for &l in left_indices {
        let Some(left_key) = &left[l] else { continue };
        while let Some(Some(right_key)) = right_indices.get(ri).map(|&r| &right[r]) {
            if compare_present(right_key, left_key).is_lt() {
                ri += 1;
            } else {
                break;
            }
        }
        matched[l] = matches!(
            right_indices.get(ri).map(|&r| &right[r]),
            Some(Some(right_key)) if compare_present(left_key, right_key).is_eq()
        );
    }
    let keeps = |l: usize, null_key: bool| match mode {
        LeftOnlyMode::Semi => matched[l],
        LeftOnlyMode::Anti => !matched[l],
        // `NULL NOT IN (s)` is true only over an empty `s`.
        LeftOnlyMode::AntiNullAware => !matched[l] && (!null_key || right_empty),
    };
    left_indices
        .iter()
        .map(|&l| (l, false))
        .chain(left_null.iter().map(|&l| (l, true)))
        .filter(|&(l, null_key)| keeps(l, null_key))
        .take(limit)
        .map(|(l, _)| Pair::LeftOnly(l))
        .collect()
}
