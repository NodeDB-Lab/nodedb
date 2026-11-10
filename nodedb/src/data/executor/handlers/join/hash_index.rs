// SPDX-License-Identifier: BUSL-1.1

//! Hash join build index: canonical key identity to build row indices.

use std::cell::RefCell;
use std::collections::HashMap;

use nodedb_query::EvalError;

use super::null_aware_tuple::{KeyTuple, PossibleMatchIndex};
use nodedb_query::msgpack_scan::join_match_identity;

/// Build side of hash join: canonical key identity → build doc indices.
///
/// The identity is the GROUP BY key encoding of the canonical key values,
/// read through the row's object format. Plain and native rows, and numbers
/// equal by exact value, share it. Two rows match exactly when their
/// identities are equal, so no candidate needs a second check. A row with a
/// NULL or missing key component is never indexed and never matches.
pub(super) struct HashIndex {
    buckets: HashMap<String, Vec<usize>>,
    /// Build rows indexed or skipped, NULL keys included.
    pub(super) rows: usize,
    /// Whether any build row has a NULL or missing key component.
    has_null_key: bool,
    /// The build key columns, for a composite `NOT IN`'s row comparison.
    keys: Vec<String>,
    /// Every build tuple by NULL pattern, built on the first composite
    /// `NOT IN` probe. `None` until then and for every other join.
    possible: RefCell<Option<PossibleMatchIndex>>,
}

impl HashIndex {
    /// Index `docs` by `keys`. A row that is not a decodable object is an
    /// error. With no keys the index holds nothing: [`HashIndex::probe`]
    /// then answers every build row.
    pub(super) fn build(docs: &[(String, Vec<u8>)], keys: &[&str]) -> Result<Self, EvalError> {
        let mut index = Self {
            buckets: HashMap::new(),
            rows: docs.len(),
            has_null_key: false,
            keys: keys.iter().map(|key| (*key).to_owned()).collect(),
            possible: RefCell::new(None),
        };
        if keys.is_empty() {
            return Ok(index);
        }
        index.buckets.reserve(docs.len());
        for (i, (_, value)) in docs.iter().enumerate() {
            match join_match_identity(value, keys)? {
                Some(identity) => index.buckets.entry(identity).or_default().push(i),
                None => index.has_null_key = true,
            }
        }
        Ok(index)
    }

    /// Find all build doc indices whose key identity matches the probe key.
    /// A probe row with a NULL or missing key component matches none.
    ///
    /// A join with no equi-key at all (`ON a.x > b.y`, or a cross join) has
    /// nothing to hash on: every build row is a candidate, and the residual ON
    /// predicate decides which pairs survive. Treating "no keys requested" as
    /// "no key matched" would drop every pair and answer such a join with
    /// zero rows.
    #[cfg(test)]
    pub(super) fn probe(
        &self,
        probe_doc: &[u8],
        probe_keys: &[&str],
        build_docs: &[(String, Vec<u8>)],
    ) -> Result<Vec<usize>, EvalError> {
        self.probe_row(probe_doc, probe_keys, build_docs)
            .map(|(_, candidates)| candidates)
    }

    /// [`Self::probe`] that also reports whether the probe key has a NULL or
    /// missing component.
    pub(super) fn probe_row(
        &self,
        probe_doc: &[u8],
        probe_keys: &[&str],
        build_docs: &[(String, Vec<u8>)],
    ) -> Result<(bool, Vec<usize>), EvalError> {
        if probe_keys.is_empty() {
            return Ok((false, (0..build_docs.len()).collect()));
        }
        Ok(match join_match_identity(probe_doc, probe_keys)? {
            Some(identity) => (
                false,
                self.buckets.get(&identity).cloned().unwrap_or_default(),
            ),
            None => (true, Vec::new()),
        })
    }

    /// Whether a null-aware anti join (`NOT IN`) over this build side
    /// admits no probe row at all: the build side is non-empty and holds a
    /// NULL key, so `x NOT IN (...)` is never true.
    ///
    /// Single-column keys only: a composite key compares row-wise, see
    /// [`Self::composite_not_in_rejects`].
    pub(super) fn rejects_every_null_aware_probe(&self) -> bool {
        self.keys.len() == 1 && self.rows > 0 && self.has_null_key
    }

    /// Whether a composite `NOT IN` rejects the probe row `probe_doc`: some
    /// build tuple agrees with it on every component non-NULL in both (SQL
    /// row comparison is TRUE or UNKNOWN). An empty build side rejects none.
    ///
    /// The first call indexes every build tuple by NULL pattern, one pass
    /// over `build_docs`. Each probe then costs one hash lookup per NULL
    /// pattern on the build side, see
    /// [`super::null_aware_tuple::PossibleMatchIndex`].
    pub(super) fn composite_not_in_rejects(
        &self,
        probe_doc: &[u8],
        probe_keys: &[&str],
        build_docs: &[(String, Vec<u8>)],
    ) -> Result<bool, EvalError> {
        if self.rows == 0 {
            return Ok(false);
        }
        let probe = KeyTuple::of_row(probe_doc, probe_keys)?;
        let mut possible = self.possible.borrow_mut();
        if possible.is_none() {
            let mut built = PossibleMatchIndex::default();
            for (_, value) in build_docs {
                built.insert(KeyTuple::of_row(value, &self.keys)?);
            }
            *possible = Some(built);
        }
        Ok(possible
            .as_ref()
            .is_some_and(|index| index.possibly_matches(&probe)))
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Value;

    use super::*;

    fn row(fields: &[(&str, Value)]) -> Value {
        Value::Object(
            fields
                .iter()
                .map(|(key, value)| ((*key).to_owned(), value.clone()))
                .collect(),
        )
    }

    fn native(fields: &[(&str, Value)]) -> Vec<u8> {
        zerompk::to_msgpack_vec(&row(fields)).expect("native row")
    }

    fn plain(fields: &[(&str, Value)]) -> Vec<u8> {
        nodedb_types::value_to_msgpack(&row(fields)).expect("plain row")
    }

    /// A plain build row and a native probe row with equivalent keys match,
    /// in both orientations, and a distinct value does not.
    #[test]
    fn plain_and_native_rows_with_equal_keys_match() {
        for value in [
            Value::Integer(9_007_199_254_740_993),
            Value::String("k1".into()),
            Value::Array(vec![Value::Integer(4), Value::String("text".into())]),
        ] {
            let fields = [("k", value.clone()), ("side", Value::Integer(1))];
            for (build, probe) in [
                (plain(&fields), native(&fields)),
                (native(&fields), plain(&fields)),
            ] {
                let build_docs = vec![(String::new(), build)];
                let index = HashIndex::build(&build_docs, &["k"]).expect("build");
                assert_eq!(
                    index.probe(&probe, &["k"], &build_docs).expect("probe"),
                    vec![0],
                    "{value:?}"
                );
            }
        }
        let build_docs = vec![(String::new(), plain(&[("k", Value::Integer(1))]))];
        let index = HashIndex::build(&build_docs, &["k"]).expect("build");
        let other = native(&[("k", Value::String("1".into()))]);
        assert!(
            index
                .probe(&other, &["k"], &build_docs)
                .expect("probe")
                .is_empty()
        );
    }

    /// Native rows key by their typed values, never by the tagged array that
    /// encodes them, and composite keys match field by field.
    #[test]
    fn native_composite_keys_read_typed_values() {
        let decimal = Value::Decimal("123456789.123456789".parse().expect("decimal"));
        let build_docs = vec![
            (
                String::new(),
                native(&[("a", decimal.clone()), ("b", Value::Integer(2))]),
            ),
            (
                String::new(),
                native(&[("a", decimal.clone()), ("b", Value::Integer(3))]),
            ),
        ];
        let index = HashIndex::build(&build_docs, &["a", "b"]).expect("build");
        let probe = native(&[("x.a", decimal), ("x.b", Value::Integer(3))]);
        assert_eq!(
            index
                .probe(&probe, &["x.a", "x.b"], &build_docs)
                .expect("probe"),
            vec![1]
        );
    }

    /// Numbers equal by exact value match across types, `-0.0` matches
    /// `0.0`, and `Float(0.1)` does not match `Decimal(0.1)`.
    #[test]
    fn cross_type_numbers_match_by_exact_value() {
        let build = vec![
            (
                String::new(),
                native(&[("k", Value::Decimal("5.00".parse().expect("d")))]),
            ),
            (String::new(), native(&[("k", Value::Float(0.0))])),
            (
                String::new(),
                native(&[("k", Value::Decimal("0.1".parse().expect("d")))]),
            ),
        ];
        let index = HashIndex::build(&build, &["k"]).expect("build");
        let probe = |value: Value| {
            index
                .probe(&native(&[("k", value)]), &["k"], &build)
                .expect("probe")
        };
        assert_eq!(probe(Value::Integer(5)), vec![0]);
        assert_eq!(probe(Value::Float(5.0)), vec![0]);
        assert_eq!(probe(Value::Float(-0.0)), vec![1]);
        assert!(probe(Value::Float(0.1)).is_empty());
        assert_eq!(probe(Value::Decimal("0.10".parse().expect("d"))), vec![2]);
    }

    #[test]
    fn malformed_rows_return_errors() {
        let bad = vec![(String::new(), vec![0x92, 7, 0x81, 0xa1, b'k'])];
        assert!(HashIndex::build(&bad, &["k"]).is_err());
        let good = vec![(String::new(), plain(&[("k", Value::Integer(1))]))];
        let index = HashIndex::build(&good, &["k"]).expect("build");
        assert!(index.probe(&bad[0].1, &["k"], &good).is_err());
    }
}
