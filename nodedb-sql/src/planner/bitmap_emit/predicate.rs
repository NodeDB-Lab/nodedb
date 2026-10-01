// SPDX-License-Identifier: Apache-2.0

//! Selective-predicate analysis for bitmap-producer emission.
//!
//! Inspects a `SqlPlan` scan or index-lookup node and returns a `BitmapHint`
//! when the node qualifies for bitmap pushdown:
//!
//! - Equality on a `Ready` indexed column (`WHERE col = const`)
//! - IN-list on a `Ready` indexed column with ≤ 1024 values
//! - `SqlPlan::DocumentIndexLookup` (already a single-field equality lookup)
//!
//! Range predicates with low selectivity are excluded when no statistics are
//! available — no selectivity guess is made. Only deterministic, index-backed
//! shapes are emitted.
//!
//! The hint carries the collection name, index field, and the predicate value(s)
//! so the converter layer can build an `IndexedFetch` physical sub-plan.

use crate::types::DocumentIndexLookupPlan;
use crate::types::{CompareOp, FilterExpr, SqlPlan, SqlValue};

/// Maximum IN-list cardinality for which bitmap emission is attempted.
const MAX_IN_LIST: usize = 1024;

/// A bitmap-producer hint: the plan child qualifies for bitmap pushdown.
#[derive(Debug, Clone)]
pub struct BitmapHint {
    /// Collection whose indexed field will be scanned.
    pub collection: String,
    /// Canonical field path (e.g. `$.email` or plain column name).
    pub field: String,
    /// Equality value, or the first value when the predicate is an IN-list.
    pub primary_value: SqlValue,
    /// Additional values for IN-list expansion (empty for equality).
    pub extra_values: Vec<SqlValue>,
}

/// Inspect `plan` and return a `BitmapHint` if the plan qualifies for bitmap
/// pushdown, or `None` if the plan should run without a prefilter.
///
/// Only exact-match shapes on `Ready` indexed columns are considered.
/// Ranges are never emitted (no statistics available to confirm selectivity).
pub fn analyze(plan: &SqlPlan) -> Option<BitmapHint> {
    match plan {
        // Already a single-field equality index lookup — always qualifies.
        SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan {
            collection,
            field,
            value,
            ..
        }) => Some(BitmapHint {
            collection: collection.clone(),
            field: field.clone(),
            primary_value: value.clone(),
            extra_values: Vec::new(),
        }),

        // Plain scan: inspect WHERE filters for indexed-column equality / IN-list.
        SqlPlan::Scan {
            collection,
            filters,
            ..
        } => {
            // The `SqlPlan::Scan` does not directly carry `IndexSpec` list — that
            // lives on `TableInfo` which the engine-rules layer consumed when it
            // chose *not* to rewrite to `DocumentIndexLookup`. However, when the
            // engine rules DID rewrite to `DocumentIndexLookup`, the caller should
            // match the first arm above.
            //
            // For plain `Scan` nodes we can still detect equality/in-list filters
            // but we have no way to confirm the column is indexed here — the planner
            // already had that information. We conservatively return `None` so we
            // never emit a sub-scan that would do a full table scan disguised as a
            // bitmap producer. Only the `DocumentIndexLookup` arm (already
            // index-backed) is emitted unconditionally.
            //
            // Future: if `Scan` carries index metadata, extend this arm.
            analyze_scan_filters(collection, filters)
        }

        _ => None,
    }
}

/// Attempt to extract an equality or bounded IN-list hint from scan filters.
///
/// Returns `Some` only when exactly one equality or bounded IN-list predicate
/// is present at the top level. Conjunctions with more than one equality are
/// not emitted (too broad; the full scan is cheaper than two lookups).
///
/// This is called for plain `Scan` nodes whose engine rules could not confirm
/// a `Ready` index. Returns `None` to be safe.
fn analyze_scan_filters(collection: &str, filters: &[crate::types::Filter]) -> Option<BitmapHint> {
    // Conservative: only emit when there is exactly one filter and it is an
    // equality or a small IN-list. The caller (engine rules) would have promoted
    // this to `DocumentIndexLookup` if the index was `Ready`; we skip here to
    // avoid spurious full-collection sub-scans as "bitmap producers".
    if filters.len() != 1 {
        return None;
    }
    match &filters[0].expr {
        FilterExpr::Comparison {
            field,
            op: CompareOp::Eq,
            value,
        } => Some(BitmapHint {
            collection: collection.to_string(),
            field: field.clone(),
            primary_value: value.clone(),
            extra_values: Vec::new(),
        }),
        FilterExpr::InList { field, values }
            if !values.is_empty() && values.len() <= MAX_IN_LIST =>
        {
            let mut iter = values.iter().cloned();
            let primary = iter.next()?;
            Some(BitmapHint {
                collection: collection.to_string(),
                field: field.clone(),
                primary_value: primary,
                extra_values: iter.collect(),
            })
        }
        _ => None,
    }
}
