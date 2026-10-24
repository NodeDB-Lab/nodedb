// SPDX-License-Identifier: Apache-2.0

//! Which spatial predicates route a query to the R-tree index.
//!
//! Search routing is a planner concern, not a property of the function's
//! semantics, so it lives here rather than in the shared catalog. Only the
//! five predicates the spatial engine can answer from its index are routed;
//! every other geo function evaluates per-row.

use crate::functions::registry::SearchTrigger;

pub(super) fn search_trigger(canonical: &str) -> SearchTrigger {
    match canonical {
        "st_dwithin" => SearchTrigger::SpatialDWithin,
        "st_3ddwithin" => SearchTrigger::SpatialDWithin3D,
        "st_contains" => SearchTrigger::SpatialContains,
        "st_intersects" => SearchTrigger::SpatialIntersects,
        "st_within" => SearchTrigger::SpatialWithin,
        _ => SearchTrigger::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_query::geo_functions::catalog;

    /// Every routed predicate must exist in the catalog — a trigger for a
    /// name the catalog does not declare would never fire.
    #[test]
    fn routed_predicates_are_declared_in_the_catalog() {
        for name in [
            "st_dwithin",
            "st_3ddwithin",
            "st_contains",
            "st_intersects",
            "st_within",
        ] {
            assert!(
                catalog::lookup(name).is_some(),
                "'{name}' is routed to the spatial index but is not in the catalog"
            );
            assert_ne!(search_trigger(name), SearchTrigger::None);
        }
    }

    #[test]
    fn unrouted_functions_evaluate_per_row() {
        assert_eq!(search_trigger("st_distance"), SearchTrigger::None);
        assert_eq!(search_trigger("st_astext"), SearchTrigger::None);
    }
}
