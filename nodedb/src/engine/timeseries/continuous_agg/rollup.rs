// SPDX-License-Identifier: BUSL-1.1

//! Roll an upstream aggregate's refresh delta up into a downstream tier.
//!
//! A downstream aggregate sourced from another aggregate (a retention-policy
//! tier, or `CREATE CONTINUOUS AGGREGATE ... ON <aggregate>`) never re-reads
//! raw rows. It merges the upstream's partial buckets into its own coarser
//! buckets. Every partial part merges without loss, so the downstream bucket
//! equals one pass over the raw rows it covers.
//!
//! An upstream bucket lands whole in the downstream bucket that holds its
//! start. A downstream interval that is a multiple of the upstream interval
//! covers whole upstream buckets, so the rollup is exact.

use super::definition::ContinuousAggregateDef;
use super::partial::{ColumnLayout, PartialAggregate};
use super::refresh::Buckets;
use crate::engine::timeseries::time_bucket;

/// The downstream buckets `delta`, a refresh delta of `upstream`, adds to
/// `downstream`.
///
/// A downstream column or GROUP BY column the upstream does not carry takes
/// no input: its column state stays empty and its key part is `0`.
pub fn rollup_delta(
    upstream: &ContinuousAggregateDef,
    downstream: &ContinuousAggregateDef,
    delta: &Buckets,
) -> Buckets {
    let mut out = Buckets::new();
    if downstream.bucket_interval_ms <= 0 {
        return out;
    }
    let up_layout = ColumnLayout::of(upstream);
    let down_layout = ColumnLayout::of(downstream);
    let column_map = down_layout.map_from(&up_layout);
    let key_map: Vec<Option<usize>> = downstream
        .group_by
        .iter()
        .map(|name| upstream.group_by.iter().position(|g| g == name))
        .collect();

    for partial in delta.values() {
        let bucket = time_bucket::time_bucket(downstream.bucket_interval_ms, partial.bucket_ts);
        let key: Vec<u32> = key_map
            .iter()
            .map(|pos| {
                pos.and_then(|p| partial.group_key.get(p).copied())
                    .unwrap_or(0)
            })
            .collect();
        out.entry((bucket, key))
            .or_insert_with_key(|(bucket, key)| {
                PartialAggregate::new(*bucket, key.clone(), &down_layout)
            })
            .merge_mapped(partial, &column_map);
    }
    out
}
