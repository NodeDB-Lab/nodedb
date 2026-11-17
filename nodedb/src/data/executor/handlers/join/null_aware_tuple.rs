// SPDX-License-Identifier: BUSL-1.1

//! SQL row comparison for a composite `NOT IN` key.
//!
//! For a probe tuple `p` and a build tuple `b`, `p = b` is TRUE when every
//! component pair is equal, FALSE when some pair of non-NULL components
//! differs, and UNKNOWN otherwise. `p NOT IN (build)` is TRUE only when
//! `p = b` is FALSE for every `b`, or the build side is empty. So `p` is
//! rejected exactly when some `b` agrees with it on every component that is
//! non-NULL in both: `b` *possibly matches* `p`.
//!
//! [`PossibleMatchIndex`] answers that question over a set of build tuples.
//! It groups the tuples by NULL pattern. For a probe with NULL pattern `mp`
//! and a group with pattern `mb`, the compared components are those non-NULL
//! in both (`c = !mb & !mp`):
//! - `c` empty: every tuple of the group possibly matches.
//! - otherwise: the probe is rejected when its projection onto `c` is in the
//!   group's projection set for `c`.
//!
//! Cost: the projection set of one `(mb, c)` pair is built once, on first
//! use, in one pass over that group. A probe then costs one hash lookup per
//! NULL pattern present on the build side. At most `3^k` pairs exist for `k`
//! key columns, and only pairs the probe side asks for are ever built.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use nodedb_query::EvalError;
use nodedb_query::msgpack_scan::group_key::encode_group_values;
use nodedb_query::msgpack_scan::join_key_values;
use nodedb_types::Value;

/// The most key columns a composite `NOT IN` compares: one bit per column.
const MAX_KEY_COLUMNS: usize = 64;

/// One key tuple: the canonical identity of each component, `None` for NULL
/// or missing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct KeyTuple {
    components: Vec<Option<String>>,
}

impl KeyTuple {
    /// The key tuple of `keys` in the object row `doc`. More than
    /// [`MAX_KEY_COLUMNS`] columns is an error.
    pub(super) fn of_row<S: AsRef<str>>(doc: &[u8], keys: &[S]) -> Result<Self, EvalError> {
        if keys.len() > MAX_KEY_COLUMNS {
            return Err(EvalError::ArgumentValue {
                function: "not in",
                position: 1,
                reason: format!(
                    "compares at most {MAX_KEY_COLUMNS} key columns, got {}",
                    keys.len()
                ),
            });
        }
        let components = join_key_values(doc, keys)?
            .into_iter()
            .map(|value| match value {
                Value::Null => Ok(None),
                value => encode_group_values(std::slice::from_ref(&value)).map(Some),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { components })
    }

    /// The bit mask of the NULL components.
    fn null_mask(&self) -> u64 {
        self.components
            .iter()
            .enumerate()
            .filter(|(_, component)| component.is_none())
            .fold(0, |mask, (i, _)| mask | (1 << i))
    }

    /// Whether any component is NULL.
    pub(super) fn has_null(&self) -> bool {
        self.components.iter().any(Option::is_none)
    }

    /// The mask with one bit per component.
    fn full_mask(&self) -> u64 {
        match self.components.len() {
            MAX_KEY_COLUMNS => u64::MAX,
            n => (1 << n) - 1,
        }
    }

    /// The components selected by `mask`, joined in position order. Every
    /// selected component is non-NULL.
    fn projection(&self, mask: u64) -> String {
        let mut out = String::new();
        for (i, component) in self.components.iter().enumerate() {
            if mask & (1 << i) != 0 {
                out.push_str(component.as_deref().unwrap_or_default());
                // A unit separator never occurs in a JSON or hex identity.
                out.push('\x1F');
            }
        }
        out
    }
}

/// Build tuples grouped by NULL pattern, answering whether any possibly
/// matches a probe tuple.
#[derive(Debug, Default)]
pub(super) struct PossibleMatchIndex {
    groups: HashMap<u64, Vec<KeyTuple>>,
    projections: RefCell<HashMap<(u64, u64), HashSet<String>>>,
}

impl PossibleMatchIndex {
    /// Add one build tuple.
    pub(super) fn insert(&mut self, tuple: KeyTuple) {
        self.projections.get_mut().clear();
        self.groups
            .entry(tuple.null_mask())
            .or_default()
            .push(tuple);
    }

    /// Whether the index holds no tuple.
    pub(super) fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Whether some build tuple agrees with `probe` on every component that
    /// is non-NULL in both.
    pub(super) fn possibly_matches(&self, probe: &KeyTuple) -> bool {
        let probe_nulls = probe.null_mask();
        let full = probe.full_mask();
        let mut projections = self.projections.borrow_mut();
        for (&build_nulls, tuples) in &self.groups {
            let compared = full & !build_nulls & !probe_nulls;
            if compared == 0 {
                return true;
            }
            let set = projections
                .entry((build_nulls, compared))
                .or_insert_with(|| tuples.iter().map(|t| t.projection(compared)).collect());
            if set.contains(&probe.projection(compared)) {
                return true;
            }
        }
        false
    }
}

/// NULL-bearing probe tuples awaiting a check against a stream of build
/// tuples, indexed by the projection each is compared on.
#[derive(Debug, Default)]
pub(super) struct PendingNullProbes {
    /// Probe NULL mask → projection onto its non-NULL components → probes.
    by_mask: HashMap<u64, HashMap<String, Vec<usize>>>,
    rejected: Vec<bool>,
}

impl PendingNullProbes {
    /// Hold probe number `index` (its position in the caller's list).
    pub(super) fn hold(&mut self, index: usize, tuple: &KeyTuple) {
        let compared = tuple.full_mask() & !tuple.null_mask();
        self.by_mask
            .entry(tuple.null_mask())
            .or_default()
            .entry(tuple.projection(compared))
            .or_default()
            .push(index);
        if self.rejected.len() <= index {
            self.rejected.resize(index + 1, false);
        }
    }

    /// Reject every held probe a non-NULL build tuple possibly matches.
    pub(super) fn check_build(&mut self, build: &KeyTuple) {
        let full = build.full_mask();
        for (&probe_nulls, by_projection) in &self.by_mask {
            let compared = full & !probe_nulls;
            if let Some(indices) = by_projection.get(&build.projection(compared)) {
                for &i in indices {
                    self.rejected[i] = true;
                }
            }
        }
    }

    /// Whether probe number `index` was rejected.
    pub(super) fn is_rejected(&self, index: usize) -> bool {
        self.rejected.get(index).copied().unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuple(a: Value, b: Value) -> KeyTuple {
        let row = zerompk::to_msgpack_vec(&Value::Object(
            [("a".to_owned(), a), ("b".to_owned(), b)]
                .into_iter()
                .collect(),
        ))
        .expect("row");
        KeyTuple::of_row(&row, &["a", "b"]).expect("tuple")
    }

    fn rejects(probe: KeyTuple, build: Vec<KeyTuple>) -> bool {
        let mut index = PossibleMatchIndex::default();
        for b in build {
            index.insert(b);
        }
        index.possibly_matches(&probe)
    }

    use Value::{Integer as I, Null};

    /// The four composite cases of SQL row comparison.
    #[test]
    fn possible_matches_follow_sql_row_comparison() {
        assert!(
            !rejects(tuple(I(1), I(1)), vec![tuple(I(2), Null)]),
            "(1,1) kept"
        );
        assert!(
            rejects(tuple(I(1), I(1)), vec![tuple(I(1), Null)]),
            "(1,1) dropped"
        );
        assert!(
            !rejects(tuple(I(1), Null), vec![tuple(I(2), I(2))]),
            "(1,NULL) kept"
        );
        assert!(
            rejects(tuple(I(1), Null), vec![tuple(I(1), I(2))]),
            "(1,NULL) dropped"
        );
        assert!(
            rejects(tuple(I(1), I(1)), vec![tuple(I(1), I(1))]),
            "exact match"
        );
        assert!(
            rejects(tuple(I(3), I(4)), vec![tuple(Null, Null)]),
            "all-NULL build"
        );
        assert!(!rejects(tuple(I(3), I(4)), vec![tuple(I(3), I(5))]));
    }

    /// Held NULL-bearing probes are rejected by a streamed build tuple on the
    /// same terms.
    #[test]
    fn pending_probes_match_streamed_build_tuples() {
        let mut pending = PendingNullProbes::default();
        pending.hold(0, &tuple(I(1), Null));
        pending.hold(1, &tuple(I(9), Null));
        pending.hold(2, &tuple(Null, Null));
        pending.check_build(&tuple(I(1), I(2)));
        assert!(pending.is_rejected(0));
        assert!(!pending.is_rejected(1));
        assert!(
            pending.is_rejected(2),
            "an all-NULL probe meets any build row"
        );
    }
}
