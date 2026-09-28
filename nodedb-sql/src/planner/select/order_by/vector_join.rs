// SPDX-License-Identifier: Apache-2.0

//! Vector-search-on-join target detection.
//!
//! A `JOIN` shape `vector_collection ⋈ ARRAY_SLICE(...)` is fused into a
//! single `VectorSearch` plan whose surrogate-bitmap prefilter is sourced
//! from the array slice. This module isolates the AST inspection used by
//! the trigger detector to recognise the fusion-eligible join shape.

use crate::types::ArraySlicePlan;
use crate::types::SqlPlan;

/// Result of inspecting a `SqlPlan::Join` for the
/// `ORDER BY vector_distance(...) + JOIN ARRAY_SLICE(...)` fusion shape.
pub(super) struct VectorJoinTarget {
    /// Vector collection backing the search (left or right of the join).
    pub vector_collection: String,
    /// Array slice that materializes into a surrogate prefilter.
    pub array_prefilter: Option<crate::types::ArrayPrefilter>,
}

/// If the join has exactly one `SqlPlan::ArraySlice` side and the other
/// side is a vector-collection scan, return the fused target. Returns
/// `None` for any other shape — the caller falls through to non-fused
/// join planning.
pub(super) fn extract_vector_join_target(
    left: &SqlPlan,
    right: &SqlPlan,
) -> Option<VectorJoinTarget> {
    match (left, right) {
        (
            SqlPlan::Scan { collection, .. },
            SqlPlan::ArraySlice(ArraySlicePlan { name, slice, .. }),
        ) => Some(VectorJoinTarget {
            vector_collection: collection.clone(),
            array_prefilter: Some(crate::types::ArrayPrefilter {
                array_name: name.clone(),
                slice: slice.clone(),
            }),
        }),
        (
            SqlPlan::ArraySlice(ArraySlicePlan { name, slice, .. }),
            SqlPlan::Scan { collection, .. },
        ) => Some(VectorJoinTarget {
            vector_collection: collection.clone(),
            array_prefilter: Some(crate::types::ArrayPrefilter {
                array_name: name.clone(),
                slice: slice.clone(),
            }),
        }),
        _ => None,
    }
}
