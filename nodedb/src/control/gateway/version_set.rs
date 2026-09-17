// SPDX-License-Identifier: BUSL-1.1

//! `GatewayVersionSet` — deterministic ordered set of (collection, version)
//! pairs used as a plan cache key and as the payload for
//! `DescriptorVersionEntry` in `ExecuteRequest`.
//!
//! Collected from a `PhysicalPlan` by walking every variant and extracting
//! the collection name.

use std::hash::{DefaultHasher, Hash, Hasher};

use nodedb_physical::physical_plan::PhysicalPlan;

/// Deterministic ordered set of `(collection_name, descriptor_version)` pairs.
///
/// - Sorted by `collection_name` for stable equality comparisons.
/// - Duplicate names are de-duped (last write wins — within a single plan
///   the version is stable, so duplicates carry the same version).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GatewayVersionSet(Vec<(String, u64)>);

/// Prefix for the synthetic entry carrying a tenant's permission-tree
/// version. `\0` makes it unrepresentable as a real collection name, so it
/// can never collide with one.
const PERMISSION_TREE_VERSION_KEY_PREFIX: &str = "\0__permission_tree_version::";

/// Prefix for the synthetic entry carrying a tenant's RLS policy version.
const RLS_VERSION_KEY_PREFIX: &str = "\0__rls_version::";

/// The synthetic pseudo-collection key a tenant's permission-tree version is
/// folded into the set under, so a revoked grant makes a cached gateway plan
/// unlookupable exactly like a bumped descriptor does.
pub fn permission_tree_version_key(tenant_id: u64) -> String {
    format!("{PERMISSION_TREE_VERSION_KEY_PREFIX}{tenant_id}")
}

/// The synthetic pseudo-collection key a tenant's RLS policy version is
/// folded into the set under.
pub fn rls_version_key(tenant_id: u64) -> String {
    format!("{RLS_VERSION_KEY_PREFIX}{tenant_id}")
}

impl GatewayVersionSet {
    /// Construct from explicit (name, version) pairs.
    pub fn from_pairs(mut pairs: Vec<(String, u64)>) -> Self {
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs.dedup_by(|a, b| a.0 == b.0);
        Self(pairs)
    }

    /// Fold one more `(name, version)` pair into the set, re-sorting and
    /// re-deduping. Used to add the permission-tree / RLS pseudo-entries
    /// alongside the real collection entries `from_plan` already collected.
    pub fn with_extra(mut self, name: String, version: u64) -> Self {
        self.0.push((name, version));
        self.0.sort_by(|a, b| a.0.cmp(&b.0));
        self.0.dedup_by(|a, b| a.0 == b.0);
        self
    }

    /// Re-look-up the current descriptor version for every collection already
    /// in this set, returning a new set with refreshed versions.
    ///
    /// Used to check a cached version set is still current before trusting a
    /// plan-cache hit: if the result equals the original, the cached plan is
    /// still valid. `version_fn` receives a collection name and returns the
    /// current descriptor version (or 0 if unknown).
    pub fn reverify(&self, version_fn: impl Fn(&str) -> u64) -> Self {
        let pairs: Vec<(String, u64)> = self
            .0
            .iter()
            .map(|(name, _)| {
                let v = version_fn(name);
                (name.clone(), v)
            })
            .collect();
        Self::from_pairs(pairs)
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

/// Append every collection a KV op names into `out`.
///
/// Split out of [`touched_collections`] so `ResolveWrite` can recurse into the
/// write it wraps rather than restating that write's own shape.
fn kv_touched_collections(op: &nodedb_physical::physical_plan::KvOp, out: &mut Vec<String>) {
    use nodedb_physical::physical_plan::KvOp::*;
    match op {
        Get { collection, .. }
        | Put { collection, .. }
        | Insert { collection, .. }
        | InsertIfAbsent { collection, .. }
        | InsertOnConflictUpdate { collection, .. }
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
        | RegisterSortedIndex { collection, .. }
        | PredicateUpdate { collection, .. }
        | PredicateDelete { collection, .. }
        | MaterializeScan { collection, .. } => out.push(collection.as_str().to_owned()),

        // TransferItem touches two collections.
        TransferItem {
            source_collection,
            dest_collection,
            ..
        } => {
            out.push(source_collection.as_str().to_owned());
            out.push(dest_collection.as_str().to_owned());
        }

        // The wrapped op is the intercepted write verbatim, so it reads
        // exactly the collections that write touches.
        ResolveWrite(inner) => kv_touched_collections(inner, out),

        // Per-mutation: a resolved `TransferItem` writes into a different
        // collection than it deletes from.
        ResolvedWrite { mutations, .. } => {
            out.extend(mutations.iter().map(|m| m.collection().as_str().to_owned()));
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

/// Append every collection a document op names into `out`.
///
/// Split out for the same reason [`kv_touched_collections`] is: `ResolveWrite`
/// recurses into the write it wraps.
fn document_touched_collections(
    op: &nodedb_physical::physical_plan::DocumentOp,
    out: &mut Vec<String>,
) {
    use nodedb_physical::physical_plan::DocumentOp::*;
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
        | BulkDelete { collection, .. }
        | MaterializeScan { collection, .. }
        // The only collection ApplyBalanceDelta touches — the causing
        // source row rides a separate task on its own vShard.
        | ApplyBalanceDelta { collection, .. } => out.push(collection.as_str().to_owned()),

        InsertSelect {
            target_collection,
            source_collection,
            ..
        }
        | UpdateFromJoin {
            target_collection,
            source_collection,
            ..
        }
        | Merge {
            target_collection,
            source_collection,
            ..
        } => {
            out.push(target_collection.as_str().to_owned());
            out.push(source_collection.as_str().to_owned());
        }

        // The wrapped op is the intercepted write verbatim, so it reads
        // exactly the collections that write touches.
        ResolveWrite(inner) => document_touched_collections(inner, out),

        // Per-mutation: a resolved bulk write spans every row it matched.
        ResolvedWrite { mutations, .. } => {
            out.extend(mutations.iter().map(|m| m.collection().as_str().to_owned()));
        }
    }
}

/// Extract every collection name touched by a `PhysicalPlan`.
///
/// Returns a `Vec<String>` that may contain duplicates; callers are
/// responsible for de-duplication (e.g., `GatewayVersionSet::from_plan`).
pub fn touched_collections(plan: &PhysicalPlan) -> Vec<String> {
    use nodedb_physical::physical_plan::*;

    let mut out: Vec<String> = Vec::new();

    match plan {
        // ── KV ──────────────────────────────────────────────────────────
        PhysicalPlan::Kv(op) => kv_touched_collections(op, &mut out),

        // ── Document ────────────────────────────────────────────────────
        PhysicalPlan::Document(op) => document_touched_collections(op, &mut out),

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
                | DropIndex { collection, .. }
                | QueryStats { collection, .. }
                | Seal { collection, .. }
                | CompactIndex { collection, .. }
                | Rebuild { collection, .. }
                | SparseInsert { collection, .. }
                | SparseSearch { collection, .. }
                | SparseDelete { collection, .. }
                | MultiVectorInsert { collection, .. }
                | MultiVectorDelete { collection, .. }
                | MultiVectorScoreSearch { collection, .. }
                | DirectUpsert { collection, .. }
                | DeleteBySurrogate { collection, .. } => out.push(collection.as_str().to_owned()),
            }
        }

        // ── Text ────────────────────────────────────────────────────────
        PhysicalPlan::Text(op) => {
            use TextOp::*;
            match op {
                Search { collection, .. }
                | BM25ScoreScan { collection, .. }
                | HybridSearch { collection, .. }
                | HybridSearchTriple { collection, .. }
                | PhraseSearch { collection, .. }
                | FtsIndexDoc { collection, .. }
                | FtsDeleteDoc { collection, .. }
                | SetTextConfig { collection, .. } => out.push(collection.as_str().to_owned()),
            }
        }

        // ── Graph ────────────────────────────────────────────────────────
        PhysicalPlan::Graph(op) => {
            use GraphOp::*;
            match op {
                // These ops target a named graph collection.
                RagFusion { collection, .. } => out.push(collection.as_str().to_owned()),
                TemporalNeighbors { collection, .. } => out.push(collection.as_str().to_owned()),
                Stats {
                    collection: Some(c),
                    ..
                } => out.push(c.as_str().to_owned()),

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
                | BspSuperstep(_)
                | WccSuperstep(_)
                | Match { .. }
                | MatchContinuation { .. }
                | MatchVarLenResume { .. }
                | TemporalAlgorithm { .. }
                | Stats {
                    collection: None, ..
                }
                | SetNodeLabels { .. }
                | RemoveNodeLabels { .. }
                // The wrapped delete is structural too — node IDs, no collection.
                | ResolveEdgeDelete(_) => {}
            }
        }

        // ── Columnar ─────────────────────────────────────────────────────
        PhysicalPlan::Columnar(op) => {
            use ColumnarOp::*;
            match op {
                Scan { collection, .. }
                | Insert { collection, .. }
                | Update { collection, .. }
                | Delete { collection, .. }
                | ResolvedUpdate { collection, .. }
                | ResolvedDelete { collection, .. }
                | ResolveDml { collection, .. }
                | MaterializeScan { collection, .. } => out.push(collection.as_str().to_owned()),
            }
        }

        // ── Timeseries ───────────────────────────────────────────────────
        PhysicalPlan::Timeseries(op) => {
            use TimeseriesOp::*;
            match op {
                Scan { collection, .. } | Ingest { collection, .. } => {
                    out.push(collection.as_str().to_owned())
                }

                // The wrapped ingest is the intercepted write verbatim.
                ResolveIngest(inner) => {
                    if let Ingest { collection, .. } = inner.as_ref() {
                        out.push(collection.as_str().to_owned());
                    }
                }
            }
        }

        // ── Spatial ──────────────────────────────────────────────────────
        PhysicalPlan::Spatial(op) => {
            use SpatialOp::*;
            match op {
                Scan { collection, .. } => out.push(collection.as_str().to_owned()),
                // Sync ingest ops target a collection but do not produce
                // versioned read output — no version-set entry needed.
                Insert { .. } | Delete { .. } => {}
            }
        }

        // ── CRDT ─────────────────────────────────────────────────────────
        PhysicalPlan::Crdt(op) => {
            use CrdtOp::*;
            match op {
                Read { collection, .. }
                | PreviewApply { collection, .. }
                | Apply { collection, .. }
                | ApplyAuthenticated { collection, .. }
                | SetPolicy { collection, .. }
                | GetPolicy { collection, .. }
                | ReadAtVersion { collection, .. }
                | RestoreToVersion { collection, .. }
                | ListInsert { collection, .. }
                | ListDelete { collection, .. }
                | ListMove { collection, .. }
                | DocUpsert { collection, .. }
                | DocDelete { collection, .. } => out.push(collection.as_str().to_owned()),

                // `ImportSnapshot` is a whole-tenant Loro import and
                // `SetConstraints` / `DropConstraints` install validator rules —
                // none produce per-collection versioned read output.
                GetVersionVector { .. }
                | ReadConstraints { .. }
                | ExportDelta { .. }
                | CompactAtVersion { .. }
                | ImportSnapshot { .. }
                | SetConstraints { .. }
                | DropConstraints { .. } => {}
            }
        }

        // ── Query ─────────────────────────────────────────────────────────
        PhysicalPlan::Query(op) => {
            use QueryOp::*;
            match op {
                Aggregate { collection, .. }
                | PartialAggregate { collection, .. }
                | PartialAggregateState { collection, .. }
                | FacetCounts { collection, .. }
                | RecursiveScan { collection, .. } => out.push(collection.as_str().to_owned()),

                HashJoin {
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
                    out.push(left_collection.as_str().to_owned());
                    out.push(right_collection.as_str().to_owned());
                }

                LateralTopK {
                    inner_collection, ..
                } => {
                    out.push(inner_collection.as_str().to_owned());
                }

                LateralLoop {
                    inner_collection, ..
                } => {
                    out.push(inner_collection.as_str().to_owned());
                }

                // Exchange: recurse into the child plan for collection extraction.
                Exchange(op) => {
                    let child_collections = touched_collections(&op.child);
                    out.extend(child_collections);
                }

                // PostProcess: recurse into the materialized child for the
                // collections it reads.
                PostProcess { input, .. } => {
                    out.extend(touched_collections(input));
                }

                // SetOp: every branch is a body that reads its own collections.
                SetOp { inputs, .. } => {
                    for input in inputs {
                        out.extend(touched_collections(input));
                    }
                }

                // ProviderScan is a catalog/constant source — no user collection.
                ProviderScan { .. } => {}

                // ShuffleJoinConsume / ShuffleAggregateConsume read node-local
                // staged files keyed by path, not by a catalog collection — no
                // version contribution.
                ShuffleJoinConsume { .. } | ShuffleAggregateConsume { .. } => {}

                // No user-collection field.
                RecursiveValue { .. } => {}
            }
        }

        // ── Meta ─────────────────────────────────────────────────────────
        PhysicalPlan::Meta(nodedb_physical::physical_plan::MetaOp::TransactionBatch {
            plans,
            ..
        }) => {
            for plan in plans {
                out.extend(touched_collections(plan));
            }
        }
        PhysicalPlan::Meta(_) => {
            // Other Meta ops target infrastructure, not user collections.
        }

        // ── Array ────────────────────────────────────────────────────────
        // Arrays use a separate catalog from collection-based engines and
        // do not contribute to the version set.
        PhysicalPlan::Array(_) => {}

        // ClusterArray variants are handled on the Control Plane before reaching
        // the gateway; they carry no collection version set contribution.
        PhysicalPlan::ClusterArray(_) | PhysicalPlan::ClusterEvent(_) => {}
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
    use nodedb_types::{DatabaseId, QualifiedCollection};

    #[test]
    fn with_extra_folds_in_a_pseudo_entry_and_stays_deterministic() {
        let vs = GatewayVersionSet::from_pairs(vec![("orders".into(), 4)])
            .with_extra(permission_tree_version_key(7), 2)
            .with_extra(rls_version_key(7), 9);
        assert_eq!(vs.len(), 3);
        assert!(vs.matches(&permission_tree_version_key(7), 2));
        assert!(vs.matches(&rls_version_key(7), 9));
        assert!(vs.matches("orders", 4));
    }

    /// The plan-cache key is stale the instant the tenant's stamped
    /// permission-tree version diverges from what's live, exactly like a
    /// bumped collection descriptor.
    #[test]
    fn with_extra_pseudo_entry_participates_in_equality() {
        let a = GatewayVersionSet::from_pairs(vec![("orders".into(), 4)])
            .with_extra(permission_tree_version_key(7), 1);
        let b = GatewayVersionSet::from_pairs(vec![("orders".into(), 4)])
            .with_extra(permission_tree_version_key(7), 2);
        assert_ne!(a, b);
    }

    #[test]
    fn pseudo_keys_are_scoped_per_tenant() {
        assert_ne!(
            permission_tree_version_key(1),
            permission_tree_version_key(2)
        );
        assert_ne!(rls_version_key(1), rls_version_key(2));
        assert_ne!(permission_tree_version_key(1), rls_version_key(1));
    }

    #[test]
    fn from_plan_kv_get() {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
            key: b"key".to_vec(),
            rls_filters: vec![],
            surrogate_ceiling: None,
        });
        let vs = GatewayVersionSet::from_plan(&plan, |_| 5);
        assert_eq!(vs.len(), 1);
        assert!(vs.matches("users", 5));
    }

    #[test]
    fn from_plan_deterministic_order() {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "alpha"),
            key: vec![],
            rls_filters: vec![],
            surrogate_ceiling: None,
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
            source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "from_col"),
            dest_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "to_col"),
            item_key: vec![],
            dest_key: vec![],
            surrogate: nodedb_types::Surrogate::ZERO,
            source_rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            dest_rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        let names = touched_collections(&plan);
        assert!(names.contains(&"from_col".to_string()));
        assert!(names.contains(&"to_col".to_string()));
    }
}
