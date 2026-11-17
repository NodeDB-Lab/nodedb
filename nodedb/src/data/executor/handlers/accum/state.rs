// SPDX-License-Identifier: BUSL-1.1

//! Accumulator and per-group state type definitions.
//!
//! Each `AggAccum` variant holds only the derived state needed to compute the
//! final aggregate result — no raw document bytes are retained.  Memory per
//! group is O(num_aggregates × accumulator_size) regardless of how many
//! documents match that group.

use std::collections::{HashMap, HashSet};

use nodedb_physical::physical_plan::AggregateSpec;
use nodedb_types::Value;

/// Per-(group, aggregate-spec) running accumulator.
///
/// Encodes as MessagePack so that partial states can be spilled to disk by
/// `GroupBySpiller` and shipped between shards, then merged back during
/// finalize. MessagePack keeps NaN and ±Infinity exact.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(crate) enum AggAccum {
    /// count(*) or count(field).
    Count { n: u64 },
    /// sum / avg: exact integer total, compensated float total, count.
    SumAvg { sum: nodedb_query::ExactSum },
    /// sum(DISTINCT col) / avg(DISTINCT col): map each distinct input
    /// value (keyed by its raw msgpack bytes) to the number it contributes.
    /// The sum and count are derived at finalize time, so the state is
    /// order-independent and therefore mergeable across spilled runs.
    /// Memory: O(num_distinct).
    SumAvgDistinct { seen: HashMap<Vec<u8>, Value> },
    /// min: the original value, compared exactly.
    Min { best: Option<Value> },
    /// max: the original value, compared exactly.
    Max { best: Option<Value> },
    /// count_distinct: set of raw msgpack bytes.
    CountDistinct { seen: HashSet<Vec<u8>> },
    /// stddev / variance variants: Welford moments.
    Welford {
        moments: nodedb_types::array_agg::Moments,
    },
    /// approx_count_distinct: HyperLogLog.
    Hll {
        hll: nodedb_types::approx::HyperLogLog,
    },
    /// approx_percentile: t-digest and the requested percentile.
    TDigest {
        digest: nodedb_types::approx::TDigest,
        pct: f64,
    },
    /// approx_topk: space-saving.
    TopK {
        ss: nodedb_types::approx::SpaceSaving,
        k: usize,
    },
    /// array_agg: every non-NULL input value. Memory: O(group rows), held
    /// to the group-by memory budget by the spiller, like `count_distinct`.
    ArrayAgg { values: Vec<Value> },
    /// array_agg_distinct: each distinct value once, in first-seen order,
    /// paired with its raw msgpack bytes so a merge dedups by the same key.
    ArrayAggDistinct {
        seen: HashSet<Vec<u8>>,
        values: Vec<(Vec<u8>, Value)>,
    },
    /// percentile_cont: every input value.
    PercentileCont { values: Vec<f64>, pct: f64 },
    /// string_agg / group_concat: every input string.
    StringAgg { parts: Vec<String> },
}

/// The typed GROUP BY values of the first row a group admitted.
///
/// The group key identity is canonical, so `Integer(5)` and `Decimal(5.00)`
/// share one group. A group's output columns show this representative, never
/// the canonical identity.
///
/// Merges keep the representative with the least `(seq, encoded)`. `seq` is
/// the admission order within one accumulation, so one spiller keeps its
/// first row. `encoded` is the tagged msgpack of `values`, with object keys
/// sorted, so representatives from different shuffle producers that share a
/// `seq` still order one way. The order is total and decided by the states
/// alone, so the result does not depend on which partial state arrives first.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(crate) struct GroupKeyRow {
    pub(crate) seq: u64,
    pub(crate) values: Vec<Value>,
    pub(crate) encoded: Vec<u8>,
}

impl GroupKeyRow {
    /// Whether `self` orders before `other` and so replaces it.
    fn precedes(&self, other: &GroupKeyRow) -> bool {
        (self.seq, &self.encoded) < (other.seq, &other.encoded)
    }
}

/// Per-group running state: one `AggAccum` per aggregate spec, plus the
/// group's representative key values.
///
/// Encodes as MessagePack so that `GroupBySpiller` can spill partial states
/// to disk and the shuffle path can ship them between shards. The
/// representative travels with the state, so it survives both.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(crate) struct GroupState {
    pub(super) accums: Vec<AggAccum>,
    pub(super) key: Option<GroupKeyRow>,
}

impl GroupState {
    /// The zero state of one group. An aggregate without an accumulator is
    /// an error naming it.
    pub(crate) fn new(aggregates: &[AggregateSpec]) -> crate::Result<Self> {
        Ok(Self {
            accums: aggregates
                .iter()
                .map(AggAccum::new)
                .collect::<crate::Result<Vec<_>>>()?,
            key: None,
        })
    }

    /// The zero state of a group first admitted at `seq` with the typed key
    /// `values`.
    pub(crate) fn with_key(
        aggregates: &[AggregateSpec],
        seq: u64,
        values: Vec<Value>,
    ) -> crate::Result<Self> {
        let mut state = Self::new(aggregates)?;
        let encoded = zerompk::to_msgpack_vec(&Value::Array(values.clone())).map_err(|e| {
            crate::Error::Codec {
                detail: format!("group key representative encode: {e}"),
            }
        })?;
        state.key = Some(GroupKeyRow {
            seq,
            values,
            encoded,
        });
        Ok(state)
    }

    /// The representative key values, if the state carries them.
    pub(crate) fn key_values(&self) -> Option<&[Value]> {
        self.key.as_ref().map(|key| key.values.as_slice())
    }

    /// The representative key values, or a codec error naming `context` when
    /// the state carries none.
    pub(crate) fn require_key_values(&self, context: &str) -> crate::Result<Vec<Value>> {
        self.key_values()
            .map(<[Value]>::to_vec)
            .ok_or_else(|| crate::Error::Codec {
                detail: format!("{context}: group state carries no key values"),
            })
    }

    pub(crate) fn feed(
        &mut self,
        aggregates: &[AggregateSpec],
        doc: &[u8],
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> Result<(), nodedb_query::EvalError> {
        for (accum, agg) in self.accums.iter_mut().zip(aggregates) {
            accum.feed(agg, doc, crs)?;
        }
        Ok(())
    }

    /// Merge a partial `GroupState` from a spilled run into `self`.
    ///
    /// Delegates to `merge::merge_group_state`.
    /// The representative with the least `(seq, encoded)` survives, so the
    /// result is the same in any merge order.
    pub(crate) fn merge_from(&mut self, mut other: GroupState) {
        if let Some(theirs) = other.key.take() {
            let replace = match &self.key {
                Some(ours) => theirs.precedes(ours),
                None => true,
            };
            if replace {
                self.key = Some(theirs);
            }
        }
        super::merge::merge_group_state(self, other);
    }

    /// Produce `(alias, value)` per aggregate. Fails with
    /// `EvalError::NumericOverflow` when an exact SUM / AVG total lies
    /// outside the `Decimal` range.
    pub(crate) fn finalize(
        self,
        aggregates: &[AggregateSpec],
    ) -> Result<Vec<(String, Value)>, nodedb_query::EvalError> {
        self.accums
            .into_iter()
            .zip(aggregates)
            .map(|(accum, agg)| Ok((agg.alias.clone(), accum.finalize(agg)?)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count_spec() -> AggregateSpec {
        AggregateSpec {
            function: "count".into(),
            field: "*".into(),
            alias: "count(*)".into(),
            user_alias: None,
            expr: None,
            param: None,
        }
    }

    /// A merge keeps the earliest-admitted representative, whichever side
    /// it sits on, and keeps it through a MessagePack round trip.
    #[test]
    fn merge_keeps_the_earliest_representative() {
        let specs = [count_spec()];
        let decimal = Value::Decimal("5.00".parse().expect("decimal"));
        let mut later = GroupState::with_key(&specs, 7, vec![Value::Integer(5)]).expect("state");
        let earlier = GroupState::with_key(&specs, 2, vec![decimal.clone()]).expect("state");
        later.merge_from(earlier);
        assert_eq!(later.key_values(), Some(&[decimal.clone()][..]));
        let bytes = zerompk::to_msgpack_vec(&later).expect("encode");
        let back: GroupState = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back.key_values(), Some(&[decimal][..]));

        // Equal `seq` from two producers: the encoding decides, in either
        // merge order.
        let float = || GroupState::with_key(&specs, 1, vec![Value::Float(5.0)]).expect("state");
        let integer = || GroupState::with_key(&specs, 1, vec![Value::Integer(5)]).expect("state");
        let mut a = float();
        a.merge_from(integer());
        let mut b = integer();
        b.merge_from(float());
        assert_eq!(a.key_values(), b.key_values());
        assert!(
            GroupState::new(&specs)
                .expect("state")
                .require_key_values("test")
                .is_err()
        );
    }

    fn spec(function: &str) -> AggregateSpec {
        AggregateSpec {
            function: function.into(),
            field: "v".into(),
            alias: format!("{function}(v)"),
            user_alias: None,
            expr: None,
            param: None,
        }
    }

    fn row(value: Value) -> Vec<u8> {
        zerompk::to_msgpack_vec(&Value::Object(
            [("v".to_owned(), value)].into_iter().collect(),
        ))
        .expect("row")
    }

    /// DISTINCT aggregates count numbers equal by exact value once, as GROUP
    /// BY groups them, and SUM(DISTINCT) adds the first-seen typed value.
    #[test]
    fn distinct_aggregates_follow_canonical_identity() {
        let specs = [
            spec("count_distinct"),
            spec("sum_distinct"),
            spec("array_agg_distinct"),
        ];
        let crs = nodedb_crs::CrsContext::builtin_only();
        let mut state = GroupState::new(&specs).expect("state");
        for value in [
            Value::Integer(5),
            Value::Float(5.0),
            Value::Decimal("5.0".parse().expect("decimal")),
            Value::Decimal("5.00".parse().expect("decimal")),
            Value::Float(-0.0),
            Value::Float(0.0),
            Value::Float(f64::NAN),
            Value::Float(f64::from_bits(f64::NAN.to_bits() | 3)),
        ] {
            state.feed(&specs, &row(value), &crs).expect("feed");
        }
        let out = state.finalize(&specs).expect("finalize");
        assert_eq!(out[0].1, Value::Integer(3), "5, 0 and NaN");
        let Value::Array(values) = &out[2].1 else {
            panic!("array_agg_distinct gives an array, got {:?}", out[2].1);
        };
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], Value::Integer(5), "first-seen typed value");

        let mut sum = GroupState::new(&specs[1..2]).expect("state");
        for value in [
            Value::Integer(5),
            Value::Float(5.0),
            Value::Decimal("5.00".parse().expect("decimal")),
        ] {
            sum.feed(&specs[1..2], &row(value), &crs).expect("feed");
        }
        assert_eq!(
            sum.finalize(&specs[1..2]).expect("finalize")[0].1,
            Value::Integer(5)
        );
    }
}
