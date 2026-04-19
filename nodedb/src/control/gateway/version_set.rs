//! `GatewayVersionSet` — deterministic ordered set of (collection, version)
//! pairs used as a plan cache key and as the payload for
//! `DescriptorVersionEntry` in `ExecuteRequest`.
//!
//! Collected from a `PhysicalPlan` by walking every variant and extracting
//! the collection name.

use std::hash::{DefaultHasher, Hash, Hasher};

use crate::bridge::physical_plan::PhysicalPlan;

/// Deterministic ordered set of `(collection_name, descriptor_version)` pairs.
///
/// - Sorted by `collection_name` for stable equality comparisons.
/// - Duplicate names are de-duped (last write wins — within a single plan
///   the version is stable, so duplicates carry the same version).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GatewayVersionSet(Vec<(String, u64)>);

impl GatewayVersionSet {
    /// Construct from explicit (name, version) pairs.
    pub fn from_pairs(mut pairs: Vec<(String, u64)>) -> Self {
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs.dedup_by(|a, b| a.0 == b.0);
        Self(pairs)
    }

    /// Collect all collection names touched by a plan with the provided
    /// version lookup function.
    ///
    /// `version_fn` receives a collection name and returns the current
    /// descriptor version (or 0 if unknown).
    pub fn from_plan(plan: &PhysicalPlan, version_fn: impl Fn(&str) -> u64) -> Self {
        let names = touched_collections(plan);
        let mut pairs: Vec<(String, u64)> = names
            .into_iter()
            .map(|name| {
                let v = version_fn(&name);
                (name, v)
            })
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs.dedup_by(|a, b| a.0 == b.0);
        Self(pairs)
    }

    /// Iterate over `(collection, version)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = &(String, u64)> {
        self.0.iter()
    }

    /// Returns `true` if the set mentions `name` at any version.
    pub fn contains_collection(&self, name: &str) -> bool {
        self.0.iter().any(|(n, _)| n == name)
    }

    /// Returns `true` if the set mentions `name` at exactly `version`.
    pub fn matches(&self, name: &str, version: u64) -> bool {
        self.0
            .iter()
            .any(|(n, v)| n.as_str() == name && *v == version)
    }

    /// Stable u64 hash of this set, used as part of `PlanCacheKey`.
    pub fn stable_hash(&self) -> u64 {
        let mut h = DefaultHasher::new();
        self.hash(&mut h);
        h.finish()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// Extract every collection name touched by a `PhysicalPlan`.
///
/// Returns a `Vec<String>` that may contain duplicates; callers are
/// responsible for de-duplication (e.g., `GatewayVersionSet::from_plan`).
pub fn touched_collections(plan: &PhysicalPlan) -> Vec<String> {
    use crate::bridge::physical_plan::*;

    let mut out: Vec<String> = Vec::new();

    match plan {
        // ── KV ──────────────────────────────────────────────────────────
        PhysicalPlan::Kv(op) => {
            use KvOp::*;
            match op {
                Get { collection, .. }
                | Put { collection, .. }
                | Delete { collection, .. }
                | Scan { collection, .. }
                | Expire { collection, .. }
                | Persist { collection, .. }
                | GetTtl { collection, .. }
                | BatchGet { collection, .. }
                | BatchPut { collection, .. }
                | RegisterIndex { collection, .. }
                | DropIndex { collection, .. }
                | FieldGet { collection, .. }
                | FieldSet { collection, .. }
                | Truncate { collection }
                | Incr { collection, .. }
                | IncrFloat { collection, .. }
                | Cas { collection, .. }
                | GetSet { collection, .. }
                | Transfer { collection, .. }
                | RegisterSortedIndex { collection, .. } => out.push(collection.clone()),

                // TransferItem touches two collections.
                TransferItem {
                    source_collection,
                    dest_collection,
                    ..
                } => {
                    out.push(source_collection.clone());
                    out.push(dest_collection.clone());
                }

                // Sorted index ops — not per-collection.
                DropSortedIndex { .. }
                | SortedIndexRank { .. }
                | SortedIndexTopK { .. }
                | SortedIndexRange { .. }
                | SortedIndexCount { .. }
                | SortedIndexScore { .. } => {}
            }
        }

        // ── Document ────────────────────────────────────────────────────
        PhysicalPlan::Document(op) => {
            use DocumentOp::*;
            match op {
                PointGet { collection, .. }
                | PointPut { collection, .. }
                | PointInsert { collection, .. }
                | PointDelete { collection, .. }
                | PointUpdate { collection, .. }
                | Scan { collection, .. }
                | BatchInsert { collection, .. }
                | RangeScan { collection, .. }
                | Register { collection, .. }
                | IndexLookup { collection, .. }
                | IndexedFetch { collection, .. }
                | DropIndex { collection, .. }
                | BackfillIndex { collection, .. }
                | Truncate { collection, .. }
                | EstimateCount { collection, .. }
                | Upsert { collection, .. }
                | BulkUpdate { collection, .. }
                | BulkDelete { collection, .. } => out.push(collection.clone()),

                InsertSelect {
                    target_collection,
                    source_collection,
                    ..
                } => {
                    out.push(target_collection.clone());
                    out.push(source_collection.clone());
                }
            }
        }

        // ── Vector ──────────────────────────────────────────────────────
        PhysicalPlan::Vector(op) => {
            use VectorOp::*;
            match op {
                Search { collection, .. }
                | Insert { collection, .. }
                | BatchInsert { collection, .. }
                | MultiSearch { collection, .. }
                | Delete { collection, .. }
                | SetParams { collection, .. }
                | QueryStats { collection, .. }
                | Seal { collection, .. }
                | CompactIndex { collection, .. }
                | Rebuild { collection, .. }
                | SparseInsert { collection, .. }
                | SparseSearch { collection, .. }
                | SparseDelete { collection, .. }
                | MultiVectorInsert { collection, .. }
                | MultiVectorDelete { collection, .. }
                | MultiVectorScoreSearch { collection, .. } => out.push(collection.clone()),
            }
        }

        // ── Text ────────────────────────────────────────────────────────
        PhysicalPlan::Text(op) => {
            use TextOp::*;
            match op {
                Search { collection, .. } | HybridSearch { collection, .. } => {
                    out.push(collection.clone())
                }
            }
        }

        // ── Graph ────────────────────────────────────────────────────────
        PhysicalPlan::Graph(op) => {
            use GraphOp::*;
            match op {
                // These ops target a named graph collection.
                RagFusion { collection, .. } => out.push(collection.clone()),

                // Structural ops use node IDs, not a collection name.
                EdgePut { .. }
                | EdgePutBatch { .. }
                | EdgeDelete { .. }
                | EdgeDeleteBatch { .. }
                | Hop { .. }
                | Neighbors { .. }
                | NeighborsMulti { .. }
                | Path { .. }
                | Subgraph { .. }
                | Algo { .. }
                | Match { .. }
                | SetNodeLabels { .. }
                | RemoveNodeLabels { .. } => {}
            }
        }

        // ── Columnar ─────────────────────────────────────────────────────
        PhysicalPlan::Columnar(op) => {
            use ColumnarOp::*;
            match op {
                Scan { collection, .. }
                | Insert { collection, .. }
                | Update { collection, .. }
                | Delete { collection, .. } => out.push(collection.clone()),
            }
        }

        // ── Timeseries ───────────────────────────────────────────────────
        PhysicalPlan::Timeseries(op) => {
            use TimeseriesOp::*;
            match op {
                Scan { collection, .. } | Ingest { collection, .. } => out.push(collection.clone()),
            }
        }

        // ── Spatial ──────────────────────────────────────────────────────
        PhysicalPlan::Spatial(op) => {
            use SpatialOp::*;
            match op {
                Scan { collection, .. } => out.push(collection.clone()),
            }
        }

        // ── CRDT ─────────────────────────────────────────────────────────
        PhysicalPlan::Crdt(op) => {
            use CrdtOp::*;
            match op {
                Read { collection, .. }
                | Apply { collection, .. }
                | SetPolicy { collection, .. }
                | ReadAtVersion { collection, .. }
                | RestoreToVersion { collection, .. }
                | ListInsert { collection, .. }
                | ListDelete { collection, .. }
                | ListMove { collection, .. } => out.push(collection.clone()),

                // No collection field.
                GetVersionVector | ExportDelta { .. } | CompactAtVersion { .. } => {}
            }
        }

        // ── Query ─────────────────────────────────────────────────────────
        PhysicalPlan::Query(op) => {
            use QueryOp::*;
            match op {
                Aggregate { collection, .. }
                | PartialAggregate { collection, .. }
                | FacetCounts { collection, .. }
                | RecursiveScan { collection, .. } => out.push(collection.clone()),

                HashJoin {
                    left_collection,
                    right_collection,
                    ..
                }
                | ShuffleJoin {
                    left_collection,
                    right_collection,
                    ..
                }
                | NestedLoopJoin {
                    left_collection,
                    right_collection,
                    ..
                }
                | SortMergeJoin {
                    left_collection,
                    right_collection,
                    ..
                } => {
                    out.push(left_collection.clone());
                    out.push(right_collection.clone());
                }

                BroadcastJoin {
                    large_collection,
                    small_collection,
                    ..
                } => {
                    out.push(large_collection.clone());
                    out.push(small_collection.clone());
                }

                // No user-collection field.
                InlineHashJoin { .. } => {}
            }
        }

        // ── Meta ─────────────────────────────────────────────────────────
        PhysicalPlan::Meta(_) => {
            // Meta ops target infrastructure, not user collections.
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::physical_plan::{KvOp, PhysicalPlan};

    #[test]
    fn from_plan_kv_get() {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: "users".into(),
            key: b"key".to_vec(),
            rls_filters: vec![],
        });
        let vs = GatewayVersionSet::from_plan(&plan, |_| 5);
        assert_eq!(vs.len(), 1);
        assert!(vs.matches("users", 5));
    }

    #[test]
    fn from_plan_deterministic_order() {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: "alpha".into(),
            key: vec![],
            rls_filters: vec![],
        });
        let vs1 = GatewayVersionSet::from_plan(&plan, |_| 1);
        let vs2 = GatewayVersionSet::from_plan(&plan, |_| 1);
        assert_eq!(vs1, vs2);
        assert_eq!(vs1.stable_hash(), vs2.stable_hash());
    }

    #[test]
    fn contains_collection() {
        let vs = GatewayVersionSet::from_pairs(vec![("orders".into(), 3), ("users".into(), 7)]);
        assert!(vs.contains_collection("orders"));
        assert!(vs.contains_collection("users"));
        assert!(!vs.contains_collection("products"));
    }

    #[test]
    fn dedup_on_construction() {
        let vs = GatewayVersionSet::from_pairs(vec![
            ("a".into(), 1),
            ("a".into(), 1), // duplicate
        ]);
        assert_eq!(vs.len(), 1);
    }

    #[test]
    fn kv_transfer_item_extracts_both_collections() {
        let plan = PhysicalPlan::Kv(KvOp::TransferItem {
            source_collection: "from_col".into(),
            dest_collection: "to_col".into(),
            item_key: vec![],
            dest_key: vec![],
        });
        let names = touched_collections(&plan);
        assert!(names.contains(&"from_col".to_string()));
        assert!(names.contains(&"to_col".to_string()));
    }
}
