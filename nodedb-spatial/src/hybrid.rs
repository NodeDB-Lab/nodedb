//! Hybrid spatial-vector search: spatial R-tree as a pre-filter for
//! HNSW vector search.
//!
//! Query pattern:
//! ```sql
//! SELECT * FROM restaurants
//! WHERE ST_DWithin(location, geo_point(-73.98, 40.75), 500)
//! ORDER BY vector_distance(embedding, $query_vec) LIMIT 10
//! ```
//!
//! Pipeline:
//! 1. R-tree range search → candidate entry IDs
//! 2. Convert entry IDs to RoaringBitmap
//! 3. Pass bitmap into vector search as pre-filter
//! 4. HNSW traversal skips entries not in bitmap
//!
//! This is a unique NodeDB differentiator — PostGIS + pgvector can't do
//! this in a single index scan.

#[cfg(test)]
use nodedb_types::BoundingBox;
use nodedb_types::geometry::Geometry;
use nodedb_types::geometry_bbox;

use crate::predicates;
use crate::rtree::RTree;

/// Result of the spatial pre-filter phase.
///
/// Contains the candidate entry IDs that passed the spatial predicate.
/// These are then passed as a filter bitmap to the vector search engine.
pub struct SpatialPreFilterResult {
    /// Entry IDs that passed the spatial predicate.
    pub candidate_ids: Vec<u64>,
    /// Number of entries evaluated by R-tree range search.
    pub rtree_candidates: usize,
    /// Number of entries that passed exact predicate refinement.
    pub exact_matches: usize,
}

/// Execute the spatial pre-filter phase.
///
/// 1. Compute search bounding box from query geometry + optional distance expansion
/// 2. R-tree range search → rough candidates
/// 3. For each candidate, load geometry and apply exact predicate
/// 4. Return filtered entry IDs
///
/// The caller (vector engine) uses the candidate IDs to build a RoaringBitmap
/// that restricts HNSW graph traversal.
pub fn spatial_prefilter(
    rtree: &RTree,
    query_geometry: &Geometry,
    distance_meters: Option<f64>,
    exact_geometries: &dyn Fn(u64) -> Option<Geometry>,
) -> SpatialPreFilterResult {
    // Step 1: Compute search bbox.
    let search_bbox = if let Some(dist) = distance_meters {
        geometry_bbox(query_geometry).expand_meters(dist)
    } else {
        geometry_bbox(query_geometry)
    };

    // Step 2: R-tree range search.
    let rtree_results = rtree.search(&search_bbox);
    let rtree_candidates = rtree_results.len();

    // Step 3: Exact predicate refinement.
    let mut candidate_ids = Vec::with_capacity(rtree_candidates);
    for entry in &rtree_results {
        if let Some(doc_geom) = exact_geometries(entry.id) {
            let passes = if let Some(dist) = distance_meters {
                predicates::st_dwithin(&doc_geom, query_geometry, dist)
            } else {
                predicates::st_intersects(&doc_geom, query_geometry)
            };
            if passes {
                candidate_ids.push(entry.id);
            }
        }
    }

    let exact_matches = candidate_ids.len();
    SpatialPreFilterResult {
        candidate_ids,
        rtree_candidates,
        exact_matches,
    }
}

/// Convert candidate entry IDs to a packed bitmap suitable for vector
/// engine pre-filtering.
///
/// The bitmap format matches the existing `filter_bitmap` used by
/// `PhysicalPlan::VectorSearch`: byte array where bit N is set if
/// entry N is a candidate. This is a simple bitset, not RoaringBitmap,
/// for compatibility with the existing HNSW filter path.
pub fn ids_to_bitmap(candidate_ids: &[u64], max_id: u64) -> Vec<u8> {
    let num_bytes = ((max_id + 8) / 8) as usize;
    let mut bitmap = vec![0u8; num_bytes];
    for &id in candidate_ids {
        let byte_idx = (id / 8) as usize;
        let bit_idx = (id % 8) as u32;
        if byte_idx < bitmap.len() {
            bitmap[byte_idx] |= 1 << bit_idx;
        }
    }
    bitmap
}

/// Check if an entry ID is set in a bitmap.
pub fn bitmap_contains(bitmap: &[u8], id: u64) -> bool {
    let byte_idx = (id / 8) as usize;
    let bit_idx = (id % 8) as u32;
    if byte_idx < bitmap.len() {
        bitmap[byte_idx] & (1 << bit_idx) != 0
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rtree::RTreeEntry;

    fn make_tree() -> RTree {
        let mut tree = RTree::new();
        // 10 points in a grid from (0,0) to (9,9).
        for i in 0..10 {
            tree.insert(RTreeEntry {
                id: i,
                bbox: BoundingBox::from_point(i as f64, i as f64),
            });
        }
        tree
    }

    #[test]
    fn prefilter_with_distance() {
        let tree = make_tree();
        let query = Geometry::point(5.0, 5.0);

        // Geometries: each entry ID maps to Point(id, id).
        let get_geom = |id: u64| -> Option<Geometry> {
            if id < 10 {
                Some(Geometry::point(id as f64, id as f64))
            } else {
                None
            }
        };

        // 200km radius should capture several nearby points.
        let result = spatial_prefilter(&tree, &query, Some(200_000.0), &get_geom);
        assert!(!result.candidate_ids.is_empty());
        // Point (5,5) should definitely be in results (distance = 0).
        assert!(result.candidate_ids.contains(&5));
    }

    #[test]
    fn prefilter_intersects_no_distance() {
        let tree = make_tree();
        // A polygon covering (3,3) to (7,7).
        let query = Geometry::polygon(vec![vec![
            [3.0, 3.0],
            [7.0, 3.0],
            [7.0, 7.0],
            [3.0, 7.0],
            [3.0, 3.0],
        ]]);

        let get_geom = |id: u64| -> Option<Geometry> {
            if id < 10 {
                Some(Geometry::point(id as f64, id as f64))
            } else {
                None
            }
        };

        let result = spatial_prefilter(&tree, &query, None, &get_geom);
        // Points 4, 5, 6 should be inside (3 is on edge → not contained by intersects returns true).
        assert!(result.candidate_ids.contains(&4));
        assert!(result.candidate_ids.contains(&5));
        assert!(result.candidate_ids.contains(&6));
        // Points 0, 1, 2, 8, 9 should not be in results.
        assert!(!result.candidate_ids.contains(&0));
        assert!(!result.candidate_ids.contains(&9));
    }

    #[test]
    fn bitmap_roundtrip() {
        let ids = vec![0, 5, 7, 63, 100];
        let bitmap = ids_to_bitmap(&ids, 128);

        for &id in &ids {
            assert!(bitmap_contains(&bitmap, id), "id {id} should be set");
        }
        assert!(!bitmap_contains(&bitmap, 1));
        assert!(!bitmap_contains(&bitmap, 64));
    }

    #[test]
    fn empty_bitmap() {
        let bitmap = ids_to_bitmap(&[], 0);
        assert!(!bitmap_contains(&bitmap, 0));
    }
}
