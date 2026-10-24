// SPDX-License-Identifier: Apache-2.0

//! Spatial join strategy: R-tree probe join.
//!
//! When two collections are joined on a spatial predicate
//! (`ST_Intersects(a.geom, b.geom)`), the naive approach is nested-loop
//! O(N*M). With an R-tree index on one side, we get O(N * log M):
//!
//! 1. Build R-tree on the smaller collection (or use existing index)
//! 2. For each geometry in the larger collection, R-tree range search
//! 3. For each candidate pair, apply exact predicate
//!
//! This module provides the join logic; the planner decides which side
//! to index based on collection cardinality.

use nodedb_mem::ScopedMemory;
use nodedb_types::geometry::Geometry;
use nodedb_types::{BoundingBox, geometry_bbox_in};

use crate::metric::Metric;
use crate::predicates;
use crate::rtree::{RTree, RTreeEntry, TreeSrid};

/// Result of a spatial join between two collections.
pub struct SpatialJoinResult {
    /// Matched pairs: (left_entry_id, right_entry_id).
    pub pairs: Vec<(u64, u64)>,
    /// Number of R-tree probes performed.
    pub probes: usize,
    /// Number of exact predicate evaluations (after R-tree filter).
    pub exact_evals: usize,
}

/// Execute a spatial join using R-tree probe.
///
/// `indexed_side`: R-tree built on one collection.
/// `probe_side`: entries from the other collection to probe against.
/// `get_geometry`: callback to retrieve the full geometry for an entry ID
///   (needed for exact predicate evaluation after R-tree bbox filter).
/// `predicate`: which spatial predicate to apply (intersects, contains, etc.).
/// `metric`: the shared SRID's metric. `DWithin` widens each probe box by
///   its distance under it and measures under it.
pub fn spatial_join(
    indexed_side: &RTree,
    probe_entries: &[(u64, BoundingBox)],
    get_indexed_geom: &dyn Fn(u64) -> Option<Geometry>,
    get_probe_geom: &dyn Fn(u64) -> Option<Geometry>,
    predicate: SpatialJoinPredicate,
    metric: &Metric,
) -> SpatialJoinResult {
    let mut pairs = Vec::new();
    let mut probes = 0;
    let mut exact_evals = 0;

    for &(probe_id, ref probe_bbox) in probe_entries {
        // R-tree range search: find indexed entries whose bbox intersects probe bbox.
        let search_bbox = match predicate {
            SpatialJoinPredicate::DWithin(metres) => metric.widen(probe_bbox, metres),
            SpatialJoinPredicate::Intersects
            | SpatialJoinPredicate::Contains
            | SpatialJoinPredicate::Within => *probe_bbox,
        };
        let candidates = indexed_side.search(&search_bbox);
        probes += 1;

        for candidate in &candidates {
            // Exact predicate evaluation.
            let Some(indexed_geom) = get_indexed_geom(candidate.id) else {
                continue;
            };
            let Some(probe_geom) = get_probe_geom(probe_id) else {
                continue;
            };
            exact_evals += 1;

            let matches = match predicate {
                SpatialJoinPredicate::Intersects => {
                    predicates::st_intersects(&probe_geom, &indexed_geom)
                }
                SpatialJoinPredicate::Contains => {
                    predicates::st_contains(&probe_geom, &indexed_geom)
                }
                SpatialJoinPredicate::Within => predicates::st_within(&probe_geom, &indexed_geom),
                SpatialJoinPredicate::DWithin(metres) => {
                    metric.dwithin(&probe_geom, &indexed_geom, metres)
                }
            };

            if matches {
                pairs.push((probe_id, candidate.id));
            }
        }
    }

    SpatialJoinResult {
        pairs,
        probes,
        exact_evals,
    }
}

/// Build an R-tree from a list of (entry_id, geometry) pairs, all in
/// `srid`, for join.
pub fn build_join_index(
    entries: &[(u64, Geometry)],
    memory: ScopedMemory,
    srid: TreeSrid,
) -> RTree {
    let space = srid.space;
    let rtree_entries: Vec<RTreeEntry> = entries
        .iter()
        .map(|(id, geom)| RTreeEntry {
            id: *id,
            bbox: geometry_bbox_in(geom, space),
        })
        .collect();
    RTree::bulk_load(rtree_entries, memory, srid)
}

/// Which spatial predicate to use for the join.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum SpatialJoinPredicate {
    Intersects,
    Contains,
    Within,
    /// Within a distance in metres under the join's metric.
    DWithin(f64),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_memory;
    use nodedb_types::geometry_bbox;

    #[test]
    fn join_overlapping_squares() {
        // Left side: 5 squares at positions (0,0), (5,5), (10,10), (15,15), (20,20).
        let left: Vec<(u64, Geometry)> = (0..5)
            .map(|i| {
                let base = (i * 5) as f64;
                (
                    i as u64,
                    Geometry::polygon(vec![vec![
                        [base, base],
                        [base + 4.0, base],
                        [base + 4.0, base + 4.0],
                        [base, base + 4.0],
                        [base, base],
                    ]]),
                )
            })
            .collect();

        // Right side: 5 squares offset by 2.
        let right: Vec<(u64, Geometry)> = (0..5)
            .map(|i| {
                let base = (i * 5 + 2) as f64;
                (
                    100 + i as u64,
                    Geometry::polygon(vec![vec![
                        [base, base],
                        [base + 4.0, base],
                        [base + 4.0, base + 4.0],
                        [base, base + 4.0],
                        [base, base],
                    ]]),
                )
            })
            .collect();

        // Build index on left side.
        let index = build_join_index(&left, test_memory(), TreeSrid::WGS84);

        // Probe with right side.
        let probe_entries: Vec<(u64, BoundingBox)> = right
            .iter()
            .map(|(id, geom)| (*id, geometry_bbox(geom)))
            .collect();

        let left_map: std::collections::HashMap<u64, Geometry> = left.into_iter().collect();
        let right_map: std::collections::HashMap<u64, Geometry> = right.into_iter().collect();

        let result = spatial_join(
            &index,
            &probe_entries,
            &|id| left_map.get(&id).cloned(),
            &|id| right_map.get(&id).cloned(),
            SpatialJoinPredicate::Intersects,
            &Metric::wgs84(),
        );

        // Adjacent overlapping squares should produce matches.
        assert!(!result.pairs.is_empty(), "expected some join matches");
        assert!(result.probes == 5); // One probe per right entry.
    }

    #[test]
    fn join_no_overlap() {
        let left = vec![(
            1u64,
            Geometry::polygon(vec![vec![
                [0.0, 0.0],
                [1.0, 0.0],
                [1.0, 1.0],
                [0.0, 1.0],
                [0.0, 0.0],
            ]]),
        )];
        let right = vec![(
            100u64,
            Geometry::polygon(vec![vec![
                [50.0, 50.0],
                [51.0, 50.0],
                [51.0, 51.0],
                [50.0, 51.0],
                [50.0, 50.0],
            ]]),
        )];

        let index = build_join_index(&left, test_memory(), TreeSrid::WGS84);
        let probes: Vec<(u64, BoundingBox)> = right
            .iter()
            .map(|(id, g)| (*id, geometry_bbox(g)))
            .collect();

        let left_map: std::collections::HashMap<u64, Geometry> = left.into_iter().collect();
        let right_map: std::collections::HashMap<u64, Geometry> = right.into_iter().collect();

        let result = spatial_join(
            &index,
            &probes,
            &|id| left_map.get(&id).cloned(),
            &|id| right_map.get(&id).cloned(),
            SpatialJoinPredicate::Intersects,
            &Metric::wgs84(),
        );

        assert!(result.pairs.is_empty());
    }

    #[test]
    fn join_with_dwithin() {
        let left = vec![(1u64, Geometry::point(0.0, 0.0))];
        let right = vec![(100u64, Geometry::point(0.001, 0.0))]; // ~111m away

        let index = build_join_index(&left, test_memory(), TreeSrid::WGS84);
        let probes: Vec<(u64, BoundingBox)> = right
            .iter()
            .map(|(id, g)| (*id, geometry_bbox(g)))
            .collect();

        let left_map: std::collections::HashMap<u64, Geometry> = left.into_iter().collect();
        let right_map: std::collections::HashMap<u64, Geometry> = right.into_iter().collect();

        let result = spatial_join(
            &index,
            &probes,
            &|id| left_map.get(&id).cloned(),
            &|id| right_map.get(&id).cloned(),
            SpatialJoinPredicate::DWithin(500.0),
            &Metric::wgs84(),
        );

        assert_eq!(result.pairs.len(), 1);
    }
}
