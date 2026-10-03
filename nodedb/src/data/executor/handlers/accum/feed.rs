// SPDX-License-Identifier: BUSL-1.1

//! `AggAccum::feed` — fold one document into a running accumulator.

use std::collections::hash_map::Entry;

use super::state::{ARRAY_AGG_CAP, AggAccum};
use nodedb_physical::physical_plan::AggregateSpec;

impl AggAccum {
    /// Feed one document into this accumulator.
    ///
    /// Returns `Err(EvalError::DivisionByZero)` when the aggregate argument
    /// expression divides or takes a modulus by zero: a row whose `SUM(a/b)`
    /// argument sees `b = 0` fails the whole statement with SQLSTATE
    /// `22012`, exactly like a WHERE/projection expression, rather than
    /// being silently excluded from the accumulation.
    pub(crate) fn feed(
        &mut self,
        agg: &AggregateSpec,
        doc: &[u8],
    ) -> Result<(), nodedb_query::EvalError> {
        use nodedb_query::msgpack_scan::aggregate_helpers as ah;
        match self {
            AggAccum::Count { n } => {
                if (agg.field == "*" && agg.expr.is_none())
                    || ah::extract_non_null(doc, &agg.field, agg.expr.as_ref())?.is_some()
                {
                    *n += 1;
                }
            }
            AggAccum::SumAvg { sum } => {
                if let Some(v) = ah::extract_sum_value(doc, &agg.field, agg.expr.as_ref())? {
                    sum.add_value(&v);
                }
            }
            AggAccum::SumAvgDistinct { seen } => {
                // Dedupe by raw msgpack bytes of the extracted value so
                // SUM(DISTINCT col) / AVG(DISTINCT col) only credit each
                // distinct value once. NULL bytes (msgpack `0xc0`) are
                // ignored — they cannot meaningfully participate in a
                // numeric aggregate. The contributed number is stored
                // alongside the key so finalize can derive an
                // order-independent sum (which also makes the state
                // mergeable across spilled runs).
                if let Some(bytes) = ah::extract_bytes(doc, &agg.field, agg.expr.as_ref())?
                    && bytes != [0xc0u8]
                    && let Entry::Vacant(slot) = seen.entry(bytes)
                    && let Some(v) = ah::extract_sum_value(doc, &agg.field, agg.expr.as_ref())?
                {
                    slot.insert(v);
                }
            }
            AggAccum::Min { best } => feed_extremum(best, agg, doc, false)?,
            AggAccum::Max { best } => feed_extremum(best, agg, doc, true)?,
            AggAccum::CountDistinct { seen } => {
                if let Some(bytes) = ah::extract_bytes(doc, &agg.field, agg.expr.as_ref())?
                    && bytes != [0xc0u8]
                {
                    seen.insert(bytes);
                }
            }
            AggAccum::Welford { n, mean, m2 } => {
                if let Some(v) = ah::extract_f64(doc, &agg.field, agg.expr.as_ref())? {
                    *n += 1;
                    let delta = v - *mean;
                    *mean += delta / *n as f64;
                    let delta2 = v - *mean;
                    *m2 += delta * delta2;
                }
            }
            AggAccum::Hll { hll } => {
                if let Some(bytes) = ah::extract_bytes(doc, &agg.field, agg.expr.as_ref())?
                    && bytes != [0xc0u8]
                {
                    hll.add(fnv1a(&bytes));
                }
            }
            AggAccum::TDigest { digest } => {
                let actual = field_after_colon(&agg.field);
                if let Some(v) = ah::extract_f64(doc, actual, agg.expr.as_ref())? {
                    digest.add(v);
                }
            }
            AggAccum::TopK { ss, .. } => {
                let actual = field_after_colon(&agg.field);
                if let Some(bytes) = ah::extract_bytes(doc, actual, agg.expr.as_ref())?
                    && bytes != [0xc0u8]
                {
                    ss.add(fnv1a(&bytes));
                }
            }
            AggAccum::ArrayAgg { values } => {
                if values.len() < ARRAY_AGG_CAP
                    && let Some(v) = ah::extract_value(doc, &agg.field, agg.expr.as_ref())?
                    && !v.is_null()
                {
                    values.push(v);
                }
            }
            AggAccum::ArrayAggDistinct { seen, values } => {
                if values.len() < ARRAY_AGG_CAP
                    && let Some(bytes) = ah::extract_bytes(doc, &agg.field, agg.expr.as_ref())?
                    && bytes != [0xc0u8]
                    && seen.insert(bytes)
                    && let Some(v) = ah::extract_value(doc, &agg.field, agg.expr.as_ref())?
                {
                    values.push(v);
                }
            }
            AggAccum::PercentileCont { values, .. } => {
                let actual = field_after_colon(&agg.field);
                if values.len() < ARRAY_AGG_CAP
                    && let Some(v) = ah::extract_f64(doc, actual, agg.expr.as_ref())?
                {
                    values.push(v);
                }
            }
            AggAccum::StringAgg { parts } => {
                if parts.len() < ARRAY_AGG_CAP
                    && let Some(s) = ah::extract_str(doc, &agg.field, agg.expr.as_ref())?
                {
                    parts.push(s);
                }
            }
        }
        Ok(())
    }
}

/// Fold one document into a MIN (`want_max` false) or MAX (`want_max` true)
/// state. The original value is kept and compared exactly. A NaN extreme
/// yields to any candidate. A NaN candidate never replaces a number.
fn feed_extremum(
    best: &mut Option<nodedb_types::Value>,
    agg: &AggregateSpec,
    doc: &[u8],
    want_max: bool,
) -> Result<(), nodedb_query::EvalError> {
    use nodedb_query::msgpack_scan::aggregate_helpers as ah;
    if let Some(v) = ah::extract_value(doc, &agg.field, agg.expr.as_ref())?
        && !v.is_null()
        && nodedb_query::window::extremum::value_replaces(&v, best.as_ref(), want_max)
    {
        *best = Some(v);
    }
    Ok(())
}

/// FNV-1a hash (matches the implementation in nodedb-query aggregate.rs).
#[inline]
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Extract the actual field name from "prefix:field" format (e.g. "0.95:latency").
#[inline]
fn field_after_colon(field: &str) -> &str {
    field.find(':').map(|i| &field[i + 1..]).unwrap_or(field)
}
