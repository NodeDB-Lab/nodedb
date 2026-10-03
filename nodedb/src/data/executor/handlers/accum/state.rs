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

/// Maximum items collected by materializing aggregates (`array_agg`,
/// `array_agg_distinct`, `percentile_cont`, `string_agg`).
pub(super) const ARRAY_AGG_CAP: usize = 10_000;

/// Per-(group, aggregate-spec) running accumulator.
///
/// Derives `Serialize` / `Deserialize` so that partial states can be spilled
/// to disk by `GroupBySpiller` and merged back during finalize.
#[derive(serde::Serialize, serde::Deserialize)]
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
    /// stddev / variance variants: Welford M2 accumulator.
    Welford { n: u64, mean: f64, m2: f64 },
    /// approx_count_distinct: HyperLogLog.
    Hll {
        hll: nodedb_types::approx::HyperLogLog,
    },
    /// approx_percentile: t-digest.
    TDigest {
        digest: nodedb_types::approx::TDigest,
    },
    /// approx_topk: space-saving.
    TopK {
        ss: nodedb_types::approx::SpaceSaving,
        k: usize,
    },
    /// array_agg (capped).
    ArrayAgg { values: Vec<Value> },
    /// array_agg_distinct (capped).
    ArrayAggDistinct {
        seen: HashSet<Vec<u8>>,
        values: Vec<Value>,
    },
    /// percentile_cont (capped).
    PercentileCont { values: Vec<f64>, pct: f64 },
    /// string_agg / group_concat (capped).
    StringAgg { parts: Vec<String> },
}

/// Per-group running state: one `AggAccum` per aggregate spec.
///
/// Serializable so that `GroupBySpiller` can spill partial states to disk.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct GroupState {
    pub(super) accums: Vec<AggAccum>,
}

impl GroupState {
    pub(crate) fn new(aggregates: &[AggregateSpec]) -> Self {
        Self {
            accums: aggregates.iter().map(AggAccum::new).collect(),
        }
    }

    pub(crate) fn feed(
        &mut self,
        aggregates: &[AggregateSpec],
        doc: &[u8],
    ) -> Result<(), nodedb_query::EvalError> {
        for (accum, agg) in self.accums.iter_mut().zip(aggregates) {
            accum.feed(agg, doc)?;
        }
        Ok(())
    }

    /// Merge a partial `GroupState` from a spilled run into `self`.
    ///
    /// Delegates to `merge::merge_group_state`.
    pub(crate) fn merge_from(&mut self, other: GroupState) {
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
