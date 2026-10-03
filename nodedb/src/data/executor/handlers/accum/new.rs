// SPDX-License-Identifier: BUSL-1.1

//! `AggAccum::new` — construct the zero-state accumulator for a spec.

use std::collections::{HashMap, HashSet};

use super::state::AggAccum;
use nodedb_physical::physical_plan::AggregateSpec;

impl AggAccum {
    pub(crate) fn new(agg: &AggregateSpec) -> Self {
        match agg.function.as_str() {
            "count" => AggAccum::Count { n: 0 },
            "sum" | "avg" => AggAccum::SumAvg {
                sum: nodedb_query::ExactSum::new(),
            },
            "sum_distinct" | "avg_distinct" => AggAccum::SumAvgDistinct {
                seen: HashMap::new(),
            },
            "min" => AggAccum::Min { best: None },
            "max" => AggAccum::Max { best: None },
            "count_distinct" => AggAccum::CountDistinct {
                seen: HashSet::new(),
            },
            "stddev" | "stddev_pop" | "stddev_samp" | "variance" | "var_pop" | "var_samp" => {
                AggAccum::Welford {
                    n: 0,
                    mean: 0.0,
                    m2: 0.0,
                }
            }
            "approx_count_distinct" => AggAccum::Hll {
                hll: nodedb_types::approx::HyperLogLog::new(),
            },
            "approx_percentile" => AggAccum::TDigest {
                digest: nodedb_types::approx::TDigest::new(),
            },
            "approx_topk" => {
                let k: usize = agg
                    .field
                    .find(':')
                    .and_then(|i| agg.field[..i].parse().ok())
                    .unwrap_or(10);
                AggAccum::TopK {
                    ss: nodedb_types::approx::SpaceSaving::new(k),
                    k,
                }
            }
            "array_agg" => AggAccum::ArrayAgg { values: Vec::new() },
            "array_agg_distinct" => AggAccum::ArrayAggDistinct {
                seen: HashSet::new(),
                values: Vec::new(),
            },
            "percentile_cont" => {
                let pct = agg
                    .field
                    .find(':')
                    .and_then(|i| agg.field[..i].parse().ok())
                    .unwrap_or(0.5);
                AggAccum::PercentileCont {
                    values: Vec::new(),
                    pct,
                }
            }
            "string_agg" | "group_concat" => AggAccum::StringAgg { parts: Vec::new() },
            _ => AggAccum::Count { n: 0 },
        }
    }
}
