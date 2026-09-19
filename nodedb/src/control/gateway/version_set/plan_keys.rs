// SPDX-License-Identifier: BUSL-1.1

//! Extraction of every collection name touched by a `PhysicalPlan`, per
//! engine, feeding [`super::GatewayVersionSet::from_plan`].

use nodedb_physical::physical_plan::PhysicalPlan;

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
        | Truncate { collection, .. }
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
                | DirectInsert { collection, .. }
                | DirectInsertIfAbsent { collection, .. }
                | DirectDelete { collection, .. }
                | DirectUpdate { collection, .. }
                | DirectTruncate { collection, .. }
                | ResolvedDirectWrite { collection, .. }
                | DeleteBySurrogate { collection, .. } => out.push(collection.as_str().to_owned()),
                // The wrapped op is the intercepted write verbatim.
                ResolveDirectWrite(inner) => {
                    out.extend(inner.direct_write_collection().map(str::to_owned));
                }
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
                | MaterializeScan { collection, .. }
                | Truncate { collection, .. } => out.push(collection.as_str().to_owned()),
            }
        }

        // ── Timeseries ───────────────────────────────────────────────────
        PhysicalPlan::Timeseries(op) => {
            use TimeseriesOp::*;
            match op {
                Scan { collection, .. }
                | Ingest { collection, .. }
                | Truncate { collection, .. } => out.push(collection.as_str().to_owned()),

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

    #[test]
    fn kv_transfer_item_extracts_both_collections() {
        let plan = PhysicalPlan::Kv(nodedb_physical::physical_plan::KvOp::TransferItem {
            source_collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "from_col",
            ),
            dest_collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "to_col",
            ),
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
