//! Vectorized predicate evaluation on dictionary-encoded columns.
//!
//! Implements late materialization for `DictEncoded` columns: predicates are
//! evaluated on compact integer IDs rather than decompressed strings. This
//! turns `O(N * string_len)` evaluation into `O(dict_size * string_len + N)`
//! — a large win when cardinality is low (the common case for dict-encoded
//! columns).
//!
//! # Short-circuit paths
//!
//! - **eq, not-in-dict**: return all-zero mask immediately (O(1) reject)
//! - **ne, not-in-dict**: return all-ones mask immediately (O(1) accept)
//! - **contains, empty-match-set**: return all-zero mask immediately
//!
//! # Bitmask encoding
//!
//! Results are returned as `Vec<u64>` packed bitmasks: bit *i* is set if row
//! *i* passes the predicate. Use `words_for(row_count)` to size the output.

use std::collections::{HashMap, HashSet};

use crate::memtable::ColumnData;
use crate::reader::DecodedColumn;

/// Unpacked view of a memtable `DictEncoded` column.
///
/// The validity slice is returned via `Cow` — `Borrowed` when the column is
/// nullable (has an explicit bitmap), `Owned(all-true)` when non-nullable.
type MemtableDict<'a> = (
    &'a [u32],
    &'a [String],
    &'a HashMap<String, u32>,
    std::borrow::Cow<'a, [bool]>,
);

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Evaluate an equality predicate on a memtable `DictEncoded` column.
///
/// Returns `Some(mask)` where bit *i* is set if row *i* equals `value` and
/// `valid[i]` is true. Returns `None` if `col` is not `DictEncoded`.
pub fn dict_eval_eq(col: &ColumnData, value: &str, row_count: usize) -> Option<Vec<u64>> {
    let (ids, _, reverse, valid) = unpack_memtable(col)?;
    match reverse.get(value) {
        None => Some(zero_mask(row_count)),
        Some(&target_id) => Some(build_eq_mask(ids, &valid, target_id, row_count)),
    }
}

/// Evaluate a not-equal predicate on a memtable `DictEncoded` column.
///
/// Returns `Some(mask)` where bit *i* is set if row *i* is not equal to
/// `value` (or is NULL, which is treated as not matching the value).
/// Returns `None` if `col` is not `DictEncoded`.
pub fn dict_eval_ne(col: &ColumnData, value: &str, row_count: usize) -> Option<Vec<u64>> {
    let (ids, _, reverse, valid) = unpack_memtable(col)?;
    match reverse.get(value) {
        None => Some(all_valid_mask(&valid, row_count)),
        Some(&target_id) => Some(build_ne_mask(ids, &valid, target_id, row_count)),
    }
}

/// Evaluate a substring-contains predicate on a memtable `DictEncoded` column.
///
/// All dictionary entries that contain `substr` are collected into a set of
/// matching IDs (O(dict_size * string_len)), then rows are filtered by ID
/// membership (O(N)).
///
/// Returns `None` if `col` is not `DictEncoded`.
pub fn dict_eval_contains(col: &ColumnData, substr: &str, row_count: usize) -> Option<Vec<u64>> {
    let (ids, dictionary, _, valid) = unpack_memtable(col)?;
    let matching = matching_ids_contains(dictionary, substr);
    if matching.is_empty() {
        return Some(zero_mask(row_count));
    }
    Some(build_set_mask(ids, &valid, &matching, row_count))
}

/// Evaluate a LIKE predicate on a memtable `DictEncoded` column.
///
/// Supports `%` as wildcard (leading, trailing, both, none). Only simple
/// patterns are supported; complex patterns with mid-string `%` fall back
/// to `None` (caller should decompress and evaluate).
///
/// Returns `None` if `col` is not `DictEncoded` or the pattern is unsupported.
pub fn dict_eval_like(col: &ColumnData, pattern: &str, row_count: usize) -> Option<Vec<u64>> {
    let (ids, dictionary, _, valid) = unpack_memtable(col)?;
    let matching = matching_ids_like(dictionary, pattern)?;
    if matching.is_empty() {
        return Some(zero_mask(row_count));
    }
    Some(build_set_mask(ids, &valid, &matching, row_count))
}

// ---------------------------------------------------------------------------
// Decoded-column variants (from SegmentReader)
// ---------------------------------------------------------------------------

/// Evaluate an equality predicate on a `DecodedColumn::DictEncoded`.
///
/// The `DecodedColumn` variant carries no reverse map, so lookup is a linear
/// scan of the dictionary — acceptable because dictionaries are small.
pub fn decoded_dict_eval_eq(
    col: &DecodedColumn,
    value: &str,
    row_count: usize,
) -> Option<Vec<u64>> {
    let (ids, dictionary, valid) = unpack_decoded(col)?;
    match find_dict_id(dictionary, value) {
        None => Some(zero_mask(row_count)),
        Some(target_id) => Some(build_eq_mask(ids, valid, target_id, row_count)),
    }
}

/// Evaluate a not-equal predicate on a `DecodedColumn::DictEncoded`.
pub fn decoded_dict_eval_ne(
    col: &DecodedColumn,
    value: &str,
    row_count: usize,
) -> Option<Vec<u64>> {
    let (ids, dictionary, valid) = unpack_decoded(col)?;
    match find_dict_id(dictionary, value) {
        None => Some(all_valid_mask(valid, row_count)),
        Some(target_id) => Some(build_ne_mask(ids, valid, target_id, row_count)),
    }
}

/// Evaluate a substring-contains predicate on a `DecodedColumn::DictEncoded`.
pub fn decoded_dict_eval_contains(
    col: &DecodedColumn,
    substr: &str,
    row_count: usize,
) -> Option<Vec<u64>> {
    let (ids, dictionary, valid) = unpack_decoded(col)?;
    let matching = matching_ids_contains(dictionary, substr);
    if matching.is_empty() {
        return Some(zero_mask(row_count));
    }
    Some(build_set_mask(ids, valid, &matching, row_count))
}

/// Evaluate a LIKE predicate on a `DecodedColumn::DictEncoded`.
pub fn decoded_dict_eval_like(
    col: &DecodedColumn,
    pattern: &str,
    row_count: usize,
) -> Option<Vec<u64>> {
    let (ids, dictionary, valid) = unpack_decoded(col)?;
    let matching = matching_ids_like(dictionary, pattern)?;
    if matching.is_empty() {
        return Some(zero_mask(row_count));
    }
    Some(build_set_mask(ids, valid, &matching, row_count))
}

// ---------------------------------------------------------------------------
// Bitmask helpers (public, mirrors nodedb-query simd_filter API surface)
// ---------------------------------------------------------------------------

/// Number of `u64` words needed to hold `row_count` bits.
#[inline]
pub fn words_for(row_count: usize) -> usize {
    row_count.div_ceil(64)
}

/// Bitwise AND of two equal-length bitmasks.
pub fn bitmask_and(a: &[u64], b: &[u64]) -> Vec<u64> {
    let len = a.len().min(b.len());
    let mut out = vec![0u64; len];
    for i in 0..len {
        out[i] = a[i] & b[i];
    }
    out
}

/// All-ones bitmask for `row_count` rows.
pub fn bitmask_all(row_count: usize) -> Vec<u64> {
    let words = words_for(row_count);
    let mut out = vec![u64::MAX; words];
    let tail = row_count % 64;
    if tail > 0 && !out.is_empty() {
        *out.last_mut().expect("non-empty") = (1u64 << tail) - 1;
    }
    out
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Destructure a memtable `ColumnData::DictEncoded`.
fn unpack_memtable(col: &ColumnData) -> Option<MemtableDict<'_>> {
    if let ColumnData::DictEncoded {
        ids,
        dictionary,
        reverse,
        valid,
    } = col
    {
        let validity = match valid {
            Some(v) => std::borrow::Cow::Borrowed(v.as_slice()),
            None => std::borrow::Cow::Owned(vec![true; ids.len()]),
        };
        Some((ids.as_slice(), dictionary.as_slice(), reverse, validity))
    } else {
        None
    }
}

/// Destructure a `DecodedColumn::DictEncoded`.
fn unpack_decoded(col: &DecodedColumn) -> Option<(&[u32], &[String], &[bool])> {
    if let DecodedColumn::DictEncoded {
        ids,
        dictionary,
        valid,
    } = col
    {
        Some((ids.as_slice(), dictionary.as_slice(), valid.as_slice()))
    } else {
        None
    }
}

/// Linear scan to find the ID of `value` in a dictionary.
fn find_dict_id(dictionary: &[String], value: &str) -> Option<u32> {
    dictionary.iter().position(|s| s == value).map(|i| i as u32)
}

/// Collect IDs of all dictionary entries that contain `substr`.
fn matching_ids_contains(dictionary: &[String], substr: &str) -> HashSet<u32> {
    dictionary
        .iter()
        .enumerate()
        .filter(|(_, s)| s.contains(substr))
        .map(|(i, _)| i as u32)
        .collect()
}

/// Collect IDs matching a simple LIKE pattern (leading/trailing `%` only).
///
/// Returns `None` for unsupported patterns (mid-string `%`).
fn matching_ids_like(dictionary: &[String], pattern: &str) -> Option<HashSet<u32>> {
    let matching = match (pattern.starts_with('%'), pattern.ends_with('%')) {
        (true, true) => {
            // %substr%
            let inner = pattern.trim_matches('%');
            if inner.contains('%') {
                return None; // mid-string wildcard
            }
            dictionary
                .iter()
                .enumerate()
                .filter(|(_, s)| s.contains(inner))
                .map(|(i, _)| i as u32)
                .collect()
        }
        (true, false) => {
            // %suffix
            let suffix = &pattern[1..];
            if suffix.contains('%') {
                return None;
            }
            dictionary
                .iter()
                .enumerate()
                .filter(|(_, s)| s.ends_with(suffix))
                .map(|(i, _)| i as u32)
                .collect()
        }
        (false, true) => {
            // prefix%
            let prefix = &pattern[..pattern.len() - 1];
            if prefix.contains('%') {
                return None;
            }
            dictionary
                .iter()
                .enumerate()
                .filter(|(_, s)| s.starts_with(prefix))
                .map(|(i, _)| i as u32)
                .collect()
        }
        (false, false) => {
            // exact match — no wildcards
            if pattern.contains('%') {
                return None;
            }
            dictionary
                .iter()
                .enumerate()
                .filter(|(_, s)| s.as_str() == pattern)
                .map(|(i, _)| i as u32)
                .collect()
        }
    };
    Some(matching)
}

/// Build an equality bitmask: bit i set iff `valid[i] && ids[i] == target_id`.
fn build_eq_mask(ids: &[u32], valid: &[bool], target_id: u32, row_count: usize) -> Vec<u64> {
    let words = words_for(row_count);
    let mut mask = vec![0u64; words];
    let n = row_count.min(ids.len()).min(valid.len());
    for i in 0..n {
        if valid[i] && ids[i] == target_id {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

/// Build a not-equal bitmask: bit i set iff `valid[i] && ids[i] != target_id`.
fn build_ne_mask(ids: &[u32], valid: &[bool], target_id: u32, row_count: usize) -> Vec<u64> {
    let words = words_for(row_count);
    let mut mask = vec![0u64; words];
    let n = row_count.min(ids.len()).min(valid.len());
    for i in 0..n {
        if valid[i] && ids[i] != target_id {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

/// Build a set-membership bitmask: bit i set iff `valid[i] && matching.contains(&ids[i])`.
fn build_set_mask(
    ids: &[u32],
    valid: &[bool],
    matching: &HashSet<u32>,
    row_count: usize,
) -> Vec<u64> {
    let words = words_for(row_count);
    let mut mask = vec![0u64; words];
    let n = row_count.min(ids.len()).min(valid.len());
    for i in 0..n {
        if valid[i] && matching.contains(&ids[i]) {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

/// All-zeros bitmask of the correct size.
#[inline]
fn zero_mask(row_count: usize) -> Vec<u64> {
    vec![0u64; words_for(row_count)]
}

/// Bitmask where bit i is set for all valid (non-null) rows.
fn all_valid_mask(valid: &[bool], row_count: usize) -> Vec<u64> {
    let words = words_for(row_count);
    let mut mask = vec![0u64; words];
    let n = row_count.min(valid.len());
    for i in 0..n {
        if valid[i] {
            mask[i / 64] |= 1u64 << (i % 64);
        }
    }
    mask
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::ColumnData;
    use crate::reader::DecodedColumn;
    use std::collections::HashMap;

    fn make_dict_col(values: &[Option<&str>]) -> ColumnData {
        let mut dictionary: Vec<String> = Vec::new();
        let mut reverse: HashMap<String, u32> = HashMap::new();
        let mut ids: Vec<u32> = Vec::new();
        let mut valid: Vec<bool> = Vec::new();

        for opt in values {
            match opt {
                None => {
                    ids.push(0);
                    valid.push(false);
                }
                Some(s) => {
                    let id = if let Some(&existing) = reverse.get(*s) {
                        existing
                    } else {
                        let new_id = dictionary.len() as u32;
                        dictionary.push(s.to_string());
                        reverse.insert(s.to_string(), new_id);
                        new_id
                    };
                    ids.push(id);
                    valid.push(true);
                }
            }
        }

        ColumnData::DictEncoded {
            ids,
            dictionary,
            reverse,
            valid: Some(valid),
        }
    }

    fn make_decoded_col(values: &[Option<&str>]) -> DecodedColumn {
        let mut dictionary: Vec<String> = Vec::new();
        let mut id_map: HashMap<String, u32> = HashMap::new();
        let mut ids: Vec<u32> = Vec::new();
        let mut valid: Vec<bool> = Vec::new();

        for opt in values {
            match opt {
                None => {
                    ids.push(0);
                    valid.push(false);
                }
                Some(s) => {
                    let id = if let Some(&existing) = id_map.get(*s) {
                        existing
                    } else {
                        let new_id = dictionary.len() as u32;
                        dictionary.push(s.to_string());
                        id_map.insert(s.to_string(), new_id);
                        new_id
                    };
                    ids.push(id);
                    valid.push(true);
                }
            }
        }

        DecodedColumn::DictEncoded {
            ids,
            dictionary,
            valid,
        }
    }

    fn bits(mask: &[u64], row_count: usize) -> Vec<bool> {
        (0..row_count)
            .map(|i| (mask[i / 64] >> (i % 64)) & 1 == 1)
            .collect()
    }

    // ---- eq ----------------------------------------------------------------

    #[test]
    fn dict_eq_match() {
        let col = make_dict_col(&[Some("web"), Some("db"), Some("web"), Some("cache")]);
        let mask = dict_eval_eq(&col, "web", 4).unwrap();
        assert_eq!(bits(&mask, 4), vec![true, false, true, false]);
    }

    #[test]
    fn dict_eq_value_not_in_dict_returns_zero_mask() {
        let col = make_dict_col(&[Some("web"), Some("db")]);
        let mask = dict_eval_eq(&col, "missing", 2).unwrap();
        assert_eq!(bits(&mask, 2), vec![false, false]);
    }

    #[test]
    fn dict_eq_null_rows_excluded() {
        let col = make_dict_col(&[Some("web"), None, Some("web")]);
        let mask = dict_eval_eq(&col, "web", 3).unwrap();
        assert_eq!(bits(&mask, 3), vec![true, false, true]);
    }

    // ---- ne ----------------------------------------------------------------

    #[test]
    fn dict_ne_basic() {
        let col = make_dict_col(&[Some("web"), Some("db"), Some("web")]);
        let mask = dict_eval_ne(&col, "web", 3).unwrap();
        assert_eq!(bits(&mask, 3), vec![false, true, false]);
    }

    #[test]
    fn dict_ne_value_not_in_dict_all_valid_rows_pass() {
        let col = make_dict_col(&[Some("web"), None, Some("db")]);
        let mask = dict_eval_ne(&col, "missing", 3).unwrap();
        // NULL row (index 1) does NOT pass.
        assert_eq!(bits(&mask, 3), vec![true, false, true]);
    }

    // ---- contains ----------------------------------------------------------

    #[test]
    fn dict_contains_basic() {
        let col = make_dict_col(&[Some("web-1"), Some("db-1"), Some("web-2"), Some("cache")]);
        let mask = dict_eval_contains(&col, "web", 4).unwrap();
        assert_eq!(bits(&mask, 4), vec![true, false, true, false]);
    }

    #[test]
    fn dict_contains_no_match_zero_mask() {
        let col = make_dict_col(&[Some("alpha"), Some("beta")]);
        let mask = dict_eval_contains(&col, "gamma", 2).unwrap();
        assert_eq!(bits(&mask, 2), vec![false, false]);
    }

    // ---- like --------------------------------------------------------------

    #[test]
    fn dict_like_prefix_wildcard() {
        let col = make_dict_col(&[Some("web-1"), Some("db-1"), Some("web-2")]);
        let mask = dict_eval_like(&col, "web%", 3).unwrap();
        assert_eq!(bits(&mask, 3), vec![true, false, true]);
    }

    #[test]
    fn dict_like_suffix_wildcard() {
        let col = make_dict_col(&[Some("alpha-web"), Some("beta-db"), Some("gamma-web")]);
        let mask = dict_eval_like(&col, "%web", 3).unwrap();
        assert_eq!(bits(&mask, 3), vec![true, false, true]);
    }

    #[test]
    fn dict_like_both_wildcards() {
        let col = make_dict_col(&[Some("alpha-web-1"), Some("beta-db"), Some("gamma-web-2")]);
        let mask = dict_eval_like(&col, "%web%", 3).unwrap();
        assert_eq!(bits(&mask, 3), vec![true, false, true]);
    }

    #[test]
    fn dict_like_exact_no_wildcards() {
        let col = make_dict_col(&[Some("exact"), Some("other")]);
        let mask = dict_eval_like(&col, "exact", 2).unwrap();
        assert_eq!(bits(&mask, 2), vec![true, false]);
    }

    #[test]
    fn dict_like_unsupported_mid_wildcard_returns_none() {
        let col = make_dict_col(&[Some("abc")]);
        assert!(dict_eval_like(&col, "a%c", 1).is_none());
    }

    // ---- decoded column ----------------------------------------------------

    #[test]
    fn decoded_dict_eq_match() {
        let col = make_decoded_col(&[Some("web"), Some("db"), Some("web")]);
        let mask = decoded_dict_eval_eq(&col, "web", 3).unwrap();
        assert_eq!(bits(&mask, 3), vec![true, false, true]);
    }

    #[test]
    fn decoded_dict_eq_not_in_dict() {
        let col = make_decoded_col(&[Some("web"), Some("db")]);
        let mask = decoded_dict_eval_eq(&col, "missing", 2).unwrap();
        assert_eq!(bits(&mask, 2), vec![false, false]);
    }

    #[test]
    fn decoded_dict_ne_not_in_dict_all_valid_pass() {
        let col = make_decoded_col(&[Some("a"), None, Some("b")]);
        let mask = decoded_dict_eval_ne(&col, "missing", 3).unwrap();
        assert_eq!(bits(&mask, 3), vec![true, false, true]);
    }

    #[test]
    fn decoded_dict_contains() {
        let col = make_decoded_col(&[Some("web-1"), Some("db"), Some("web-2")]);
        let mask = decoded_dict_eval_contains(&col, "web", 3).unwrap();
        assert_eq!(bits(&mask, 3), vec![true, false, true]);
    }

    #[test]
    fn decoded_dict_like() {
        let col = make_decoded_col(&[Some("web-1"), Some("db"), Some("web-2")]);
        let mask = decoded_dict_eval_like(&col, "web%", 3).unwrap();
        assert_eq!(bits(&mask, 3), vec![true, false, true]);
    }

    // ---- short-circuit and bitmask helpers ---------------------------------

    #[test]
    fn bitmask_all_correct_tail_bits() {
        // row_count=65: word0 covers rows 0..63 (all set), word1 covers row 64 only (bit 0).
        let mask = bitmask_all(65);
        assert_eq!(mask.len(), 2);
        assert_eq!(mask[0], u64::MAX);
        assert_eq!(mask[1], 1u64); // only bit 0 set in last word (row 64)

        // row_count=66: word1 covers rows 64 and 65 → bits 0 and 1.
        let mask66 = bitmask_all(66);
        assert_eq!(mask66[1], 0b11u64);
    }

    #[test]
    fn words_for_alignment() {
        assert_eq!(words_for(0), 0);
        assert_eq!(words_for(1), 1);
        assert_eq!(words_for(64), 1);
        assert_eq!(words_for(65), 2);
    }

    #[test]
    fn non_dict_encoded_col_returns_none() {
        let col = ColumnData::Int64 {
            values: vec![1, 2, 3],
            valid: Some(vec![true, true, true]),
        };
        assert!(dict_eval_eq(&col, "x", 3).is_none());
        assert!(dict_eval_ne(&col, "x", 3).is_none());
        assert!(dict_eval_contains(&col, "x", 3).is_none());
        assert!(dict_eval_like(&col, "x%", 3).is_none());
    }
}
