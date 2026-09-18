// SPDX-License-Identifier: Apache-2.0

//! Whether a fanned-out physical plan can stream to the client unordered,
//! and the global row cap to apply while it streams.
//!
//! Kept beside the plan enum rather than inside it so `plan.rs` stays the
//! single declaration of the wire shape and nothing else.

use super::{PhysicalPlan, columnar, document, kv, query, timeseries};

impl PhysicalPlan {
    /// Whether a fanned-out plan can stream to the client as an unordered
    /// union of per-source batches, rather than merged on the coordinator
    /// first. `true` only for a scan with no ordering, distinctness, offset,
    /// or aggregation across the union — any interleaving is safe. Match is
    /// exhaustive: a new variant forces an explicit decision. `limit` is not
    /// a disqualifier — the coordinator applies a global take-N while streaming.
    pub fn is_streamable_unordered_scan(&self) -> bool {
        match self {
            PhysicalPlan::Document(document::DocumentOp::Scan {
                sort_keys,
                distinct,
                offset,
                window_functions,
                ..
            }) => sort_keys.is_empty() && !*distinct && *offset == 0 && window_functions.is_empty(),

            PhysicalPlan::Kv(kv::KvOp::Scan { sort_keys, .. }) => sort_keys.is_empty(),

            PhysicalPlan::Columnar(columnar::ColumnarOp::Scan { sort_keys, .. }) => {
                sort_keys.is_empty()
            }

            PhysicalPlan::Timeseries(timeseries::TimeseriesOp::Scan {
                group_by,
                aggregates,
                bucket_interval_ms,
                ..
            }) => group_by.is_empty() && aggregates.is_empty() && *bucket_interval_ms == 0,

            PhysicalPlan::Query(query::QueryOp::ProviderScan {
                sort_keys,
                offset,
                distinct,
                window_functions,
                ..
            }) => sort_keys.is_empty() && *offset == 0 && !*distinct && window_functions.is_empty(),

            // Every other Document / Kv / Columnar / Timeseries op, plus all
            // other engines and query ops, are not unordered-streamable.
            PhysicalPlan::Document(_)
            | PhysicalPlan::Kv(_)
            | PhysicalPlan::Columnar(_)
            | PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Vector(_)
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Text(_)
            | PhysicalPlan::Spatial(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_)
            | PhysicalPlan::ClusterEvent(_)
            | PhysicalPlan::Query(_) => false,
        }
    }

    /// Global take-N to apply when streaming an unordered scan (row cap, or
    /// `usize::MAX` if unlimited). Callers gate on
    /// [`PhysicalPlan::is_streamable_unordered_scan`] first, so the
    /// non-streamable fallthrough is never the deciding factor.
    pub fn streamable_scan_limit(&self) -> usize {
        match self {
            PhysicalPlan::Document(document::DocumentOp::Scan { limit, .. }) => *limit,
            PhysicalPlan::Kv(kv::KvOp::Scan { count, .. }) => *count,
            PhysicalPlan::Columnar(columnar::ColumnarOp::Scan { limit, .. }) => *limit,
            PhysicalPlan::Timeseries(timeseries::TimeseriesOp::Scan { limit, .. }) => *limit,
            PhysicalPlan::Query(query::QueryOp::ProviderScan { limit, .. }) => {
                limit.unwrap_or(usize::MAX)
            }
            _ => usize::MAX,
        }
    }
}
