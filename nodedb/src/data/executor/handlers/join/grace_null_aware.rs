// SPDX-License-Identifier: BUSL-1.1

//! Whole-build-side facts a partitioned `NOT IN` join needs.
//!
//! A null-aware anti join depends on facts no single partition holds. The
//! grace spiller records them while it partitions the build side, keeps
//! NULL-bearing build rows out of every partition, and filters probe rows by
//! them before they reach a partition.
//!
//! Single-column key:
//! - A NULL key in a non-empty build side admits no probe row.
//! - An empty build side admits every probe row.
//! - Otherwise a probe row with a NULL key is dropped, and the rest run as a
//!   per-partition anti join over non-NULL keys.
//!
//! Composite key, under SQL row comparison (see
//! [`super::null_aware_tuple`]):
//! - An empty build side admits every probe row.
//! - NULL-bearing build tuples stay resident in a [`PossibleMatchIndex`]. A
//!   probe row any of them possibly matches is dropped.
//! - A probe row with no NULL then runs as a per-partition anti join: only
//!   an exact match in its own partition can still reject it.
//! - A NULL-bearing probe row can be possibly matched by a build tuple in any
//!   partition, so it stays resident until the finish, where one pass over
//!   every partition's build rows rejects it or keeps it.
//!
//! Memory: the NULL-bearing rows of both sides stay resident. Every other
//! row spills as usual.

use nodedb_query::msgpack_scan::join_match_identity;

use super::null_aware_tuple::{KeyTuple, PendingNullProbes, PossibleMatchIndex};

/// The build-side facts of one null-aware anti join.
#[derive(Debug, Default)]
pub(super) struct NullAwareFacts {
    build_rows: u64,
    build_has_null: bool,
    /// NULL-bearing build tuples of a composite key.
    null_build: PossibleMatchIndex,
    /// NULL-bearing probe rows of a composite key, awaiting the finish.
    held_probes: Vec<Vec<u8>>,
    pending: PendingNullProbes,
}

impl NullAwareFacts {
    /// Count one build row. `Ok(false)` when its key has a NULL or missing
    /// component: the row joins no partition.
    pub(super) fn record_build(&mut self, value: &[u8], keys: &[String]) -> crate::Result<bool> {
        self.build_rows = self.build_rows.saturating_add(1);
        if keys.len() > 1 {
            let tuple = KeyTuple::of_row(value, keys)?;
            if !tuple.has_null() {
                return Ok(true);
            }
            self.build_has_null = true;
            self.null_build.insert(tuple);
            return Ok(false);
        }
        if join_match_identity(value, keys)?.is_some() {
            return Ok(true);
        }
        self.build_has_null = true;
        Ok(false)
    }

    /// Whether no probe row can be emitted: a single-column key whose
    /// non-empty build side holds a NULL. A composite key compares row-wise,
    /// so a NULL-bearing build tuple rejects only the rows it possibly
    /// matches.
    pub(super) fn rejects_every_probe(&self, keys: &[String]) -> bool {
        keys.len() == 1 && self.build_rows > 0 && self.build_has_null
    }

    /// Whether a probe row goes on to a partition. Call only after every
    /// build row is recorded. A composite NULL-bearing row that survives the
    /// NULL-bearing build tuples is held for [`Self::check_build_row`].
    pub(super) fn admits_probe(&mut self, value: &[u8], keys: &[String]) -> crate::Result<bool> {
        // `p NOT IN (s)` is true over an empty `s`: every partition is
        // empty then, so the row is emitted as unmatched.
        if self.build_rows == 0 {
            return Ok(true);
        }
        if keys.len() > 1 {
            let tuple = KeyTuple::of_row(value, keys)?;
            if self.null_build.possibly_matches(&tuple) {
                return Ok(false);
            }
            if tuple.has_null() {
                self.pending.hold(self.held_probes.len(), &tuple);
                self.held_probes.push(value.to_vec());
                return Ok(false);
            }
            return Ok(true);
        }
        if self.rejects_every_probe(keys) {
            return Ok(false);
        }
        Ok(join_match_identity(value, keys)?.is_some())
    }

    /// Whether NULL-bearing probe rows are held for the finish.
    pub(super) fn holds_probes(&self) -> bool {
        !self.held_probes.is_empty()
    }

    /// Reject every held probe row the non-NULL build row `value` possibly
    /// matches.
    pub(super) fn check_build_row(&mut self, value: &[u8], keys: &[String]) -> crate::Result<()> {
        let tuple = KeyTuple::of_row(value, keys)?;
        self.pending.check_build(&tuple);
        Ok(())
    }

    /// The held probe rows no build row rejected.
    pub(super) fn surviving_probes(self) -> Vec<Vec<u8>> {
        let pending = self.pending;
        self.held_probes
            .into_iter()
            .enumerate()
            .filter(|(i, _)| !pending.is_rejected(*i))
            .map(|(_, row)| row)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Value;

    use super::*;

    fn row(value: Value) -> Vec<u8> {
        zerompk::to_msgpack_vec(&Value::Object(
            [("k".to_owned(), value)].into_iter().collect(),
        ))
        .expect("row")
    }

    fn pair(a: Value, b: Value) -> Vec<u8> {
        zerompk::to_msgpack_vec(&Value::Object(
            [("a".to_owned(), a), ("b".to_owned(), b)]
                .into_iter()
                .collect(),
        ))
        .expect("row")
    }

    #[test]
    fn facts_follow_not_in_rules() {
        let keys = ["k".to_owned()];
        let mut empty = NullAwareFacts::default();
        assert!(!empty.rejects_every_probe(&keys));
        assert!(empty.admits_probe(&row(Value::Null), &keys).expect("probe"));

        let mut plain = NullAwareFacts::default();
        assert!(
            plain
                .record_build(&row(Value::Integer(1)), &keys)
                .expect("build")
        );
        assert!(
            plain
                .admits_probe(&row(Value::Integer(2)), &keys)
                .expect("probe")
        );
        assert!(!plain.admits_probe(&row(Value::Null), &keys).expect("probe"));

        assert!(!plain.record_build(&row(Value::Null), &keys).expect("build"));
        assert!(plain.rejects_every_probe(&keys));
        assert!(
            !plain
                .admits_probe(&row(Value::Integer(2)), &keys)
                .expect("probe")
        );
    }

    /// A composite key holds NULL-bearing probe rows for the finish and
    /// rejects them only on a possible row match.
    #[test]
    fn composite_facts_compare_row_wise() {
        use Value::{Integer as I, Null};
        let keys = ["a".to_owned(), "b".to_owned()];
        let mut facts = NullAwareFacts::default();
        assert!(!facts.record_build(&pair(I(2), Null), &keys).expect("build"));
        assert!(facts.record_build(&pair(I(5), I(6)), &keys).expect("build"));
        assert!(!facts.rejects_every_probe(&keys));
        // (1,1) vs (2,NULL): FALSE, so the row goes on to its partition.
        assert!(facts.admits_probe(&pair(I(1), I(1)), &keys).expect("probe"));
        // (2,7) vs (2,NULL): UNKNOWN, so the row is dropped.
        assert!(!facts.admits_probe(&pair(I(2), I(7)), &keys).expect("probe"));
        // NULL-bearing probes are held; (5,NULL) meets build row (5,6).
        assert!(!facts.admits_probe(&pair(I(5), Null), &keys).expect("probe"));
        assert!(!facts.admits_probe(&pair(I(9), Null), &keys).expect("probe"));
        assert!(facts.holds_probes());
        facts
            .check_build_row(&pair(I(5), I(6)), &keys)
            .expect("check");
        assert_eq!(facts.surviving_probes(), vec![pair(I(9), Null)]);
    }
}
