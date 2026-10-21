// SPDX-License-Identifier: BUSL-1.1

//! `AggAccum::feed` — fold one document into a running accumulator.

use std::collections::hash_map::Entry;

use super::state::AggAccum;
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
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> Result<(), nodedb_query::EvalError> {
        use nodedb_query::msgpack_scan::aggregate_helpers as ah;
        match self {
            AggAccum::Count { n } => {
                if (agg.field == "*" && agg.expr.is_none())
                    || ah::extract_non_null(doc, &agg.field, agg.expr.as_ref(), crs)?.is_some()
                {
                    *n += 1;
                }
            }
            AggAccum::SumAvg { sum } => {
                if let Some(v) = ah::extract_sum_value(doc, &agg.field, agg.expr.as_ref(), crs)? {
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
                if let Some(bytes) = ah::extract_bytes(doc, &agg.field, agg.expr.as_ref(), crs)?
                    && bytes != [0xc0u8]
                    && let Entry::Vacant(slot) = seen.entry(bytes)
                    && let Some(v) = ah::extract_sum_value(doc, &agg.field, agg.expr.as_ref(), crs)?
                {
                    slot.insert(v);
                }
            }
            AggAccum::Min { best } => feed_extremum(best, agg, doc, false, crs)?,
            AggAccum::Max { best } => feed_extremum(best, agg, doc, true, crs)?,
            AggAccum::CountDistinct { seen } => {
                if let Some(bytes) = ah::extract_bytes(doc, &agg.field, agg.expr.as_ref(), crs)?
                    && bytes != [0xc0u8]
                {
                    seen.insert(bytes);
                }
            }
            AggAccum::Welford { moments } => {
                if let Some(v) = ah::extract_f64(doc, &agg.field, agg.expr.as_ref(), crs)? {
                    moments.push(v);
                }
            }
            AggAccum::Hll { hll } => {
                if let Some(bytes) = ah::extract_bytes(doc, &agg.field, agg.expr.as_ref(), crs)?
                    && bytes != [0xc0u8]
                {
                    hll.add(fnv1a(&bytes));
                }
            }
            AggAccum::TDigest { digest, .. } => {
                if let Some(v) = ah::extract_f64(doc, &agg.field, agg.expr.as_ref(), crs)? {
                    digest.add(v);
                }
            }
            AggAccum::TopK { ss, .. } => {
                if let Some(bytes) = ah::extract_bytes(doc, &agg.field, agg.expr.as_ref(), crs)?
                    && bytes != [0xc0u8]
                {
                    ss.add(fnv1a(&bytes));
                }
            }
            AggAccum::ArrayAgg { values } => {
                if let Some(v) = ah::extract_value(doc, &agg.field, agg.expr.as_ref(), crs)?
                    && !v.is_null()
                {
                    values.push(v);
                }
            }
            AggAccum::ArrayAggDistinct { seen, values } => {
                if let Some(bytes) = ah::extract_bytes(doc, &agg.field, agg.expr.as_ref(), crs)?
                    && bytes != [0xc0u8]
                    && !seen.contains(&bytes)
                    && let Some(v) = ah::extract_value(doc, &agg.field, agg.expr.as_ref(), crs)?
                {
                    seen.insert(bytes.clone());
                    values.push((bytes, v));
                }
            }
            AggAccum::PercentileCont { values, .. } => {
                if let Some(v) = ah::extract_f64(doc, &agg.field, agg.expr.as_ref(), crs)? {
                    values.push(v);
                }
            }
            AggAccum::StringAgg { parts } => {
                if let Some(s) = ah::extract_str(doc, &agg.field, agg.expr.as_ref(), crs)? {
                    parts.push(s);
                }
            }
        }
        Ok(())
    }
}

/// Fold one document into a MIN (`want_max` false) or MAX (`want_max` true)
/// state. The original value is kept and compared exactly. NaN sorts above
/// every number, as in PostgreSQL: it wins MAX and loses MIN.
fn feed_extremum(
    best: &mut Option<nodedb_types::Value>,
    agg: &AggregateSpec,
    doc: &[u8],
    want_max: bool,
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<(), nodedb_query::EvalError> {
    use nodedb_query::msgpack_scan::aggregate_helpers as ah;
    if let Some(v) = ah::extract_value(doc, &agg.field, agg.expr.as_ref(), crs)?
        && !v.is_null()
        && nodedb_query::window::extremum::value_replaces(&v, best.as_ref(), want_max)
    {
        *best = Some(v);
    }
    Ok(())
}

/// FNV-1a hash of raw value bytes.
#[inline]
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}
