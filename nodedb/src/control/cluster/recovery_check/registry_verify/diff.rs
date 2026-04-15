//! Generic diff helper for registry verifiers.
//!
//! Every verifier produces the same shape: two deterministic
//! key-sorted vectors (expected from redb, actual from memory)
//! and needs to enumerate "only in expected", "only in actual",
//! and "value mismatched". This helper does that once.

use std::cmp::Ordering;

/// Result of a two-sided diff.
#[derive(Debug)]
pub struct DiffResult<K: Clone, V: Clone> {
    /// Keys present in the expected (redb) set but missing in
    /// the actual (in-memory) set.
    pub only_in_expected: Vec<(K, V)>,
    /// Keys present in the actual set but missing in expected.
    pub only_in_actual: Vec<(K, V)>,
    /// Keys present in both but with different values.
    pub mismatched: Vec<(K, V, V)>,
}

impl<K: Clone, V: Clone> Default for DiffResult<K, V> {
    fn default() -> Self {
        Self {
            only_in_expected: Vec::new(),
            only_in_actual: Vec::new(),
            mismatched: Vec::new(),
        }
    }
}

impl<K: Clone, V: Clone> DiffResult<K, V> {
    pub fn is_clean(&self) -> bool {
        self.only_in_expected.is_empty()
            && self.only_in_actual.is_empty()
            && self.mismatched.is_empty()
    }

    pub fn total(&self) -> usize {
        self.only_in_expected.len() + self.only_in_actual.len() + self.mismatched.len()
    }
}

/// Diff two key-sorted vectors by key. Caller guarantees both
/// inputs are pre-sorted ascending by `K`. Linear merge walk.
///
/// `eq_value` decides whether two entries with equal keys are
/// considered equivalent — use `|a, b| a == b` when `V: Eq`,
/// or a custom closure when comparing across type boundaries
/// (e.g. `StoredPermission` vs `Grant`).
pub fn diff_sorted<K, V, F>(expected: &[(K, V)], actual: &[(K, V)], eq_value: F) -> DiffResult<K, V>
where
    K: Clone + Ord,
    V: Clone,
    F: Fn(&V, &V) -> bool,
{
    let mut result = DiffResult::default();
    let (mut i, mut j) = (0usize, 0usize);
    while i < expected.len() && j < actual.len() {
        match expected[i].0.cmp(&actual[j].0) {
            Ordering::Less => {
                result.only_in_expected.push(expected[i].clone());
                i += 1;
            }
            Ordering::Greater => {
                result.only_in_actual.push(actual[j].clone());
                j += 1;
            }
            Ordering::Equal => {
                if !eq_value(&expected[i].1, &actual[j].1) {
                    result.mismatched.push((
                        expected[i].0.clone(),
                        expected[i].1.clone(),
                        actual[j].1.clone(),
                    ));
                }
                i += 1;
                j += 1;
            }
        }
    }
    while i < expected.len() {
        result.only_in_expected.push(expected[i].clone());
        i += 1;
    }
    while j < actual.len() {
        result.only_in_actual.push(actual[j].clone());
        j += 1;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(k: &str, v: &str) -> (String, String) {
        (k.to_string(), v.to_string())
    }

    #[test]
    fn clean_match() {
        let expected = vec![s("a", "1"), s("b", "2")];
        let actual = vec![s("a", "1"), s("b", "2")];
        let d = diff_sorted(&expected, &actual, |a, b| a == b);
        assert!(d.is_clean());
        assert_eq!(d.total(), 0);
    }

    #[test]
    fn only_in_expected() {
        let expected = vec![s("a", "1"), s("b", "2"), s("c", "3")];
        let actual = vec![s("a", "1")];
        let d = diff_sorted(&expected, &actual, |a, b| a == b);
        assert_eq!(d.only_in_expected.len(), 2);
        assert_eq!(d.only_in_actual.len(), 0);
    }

    #[test]
    fn only_in_actual() {
        let expected = vec![s("a", "1")];
        let actual = vec![s("a", "1"), s("b", "2")];
        let d = diff_sorted(&expected, &actual, |a, b| a == b);
        assert_eq!(d.only_in_actual.len(), 1);
        assert_eq!(d.only_in_actual[0].0, "b");
    }

    #[test]
    fn value_mismatch() {
        let expected = vec![s("a", "1"), s("b", "2")];
        let actual = vec![s("a", "1"), s("b", "99")];
        let d = diff_sorted(&expected, &actual, |a, b| a == b);
        assert_eq!(d.mismatched.len(), 1);
        assert_eq!(d.mismatched[0].0, "b");
    }

    #[test]
    fn interleaved_divergence() {
        let expected = vec![s("a", "1"), s("c", "3"), s("e", "5")];
        let actual = vec![s("b", "2"), s("c", "3"), s("d", "4")];
        let d = diff_sorted(&expected, &actual, |a, b| a == b);
        assert_eq!(d.only_in_expected.len(), 2);
        assert_eq!(d.only_in_actual.len(), 2);
        assert!(d.mismatched.is_empty());
    }
}
