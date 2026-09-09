// SPDX-License-Identifier: BUSL-1.1

//! Calvin dispatch classification and routing for cross-shard writes.
//!
//! This module is the single chokepoint for deciding whether a set of
//! [`PhysicalTask`]s should be dispatched via:
//!
//! - The single-shard fast path (existing path, no Calvin involvement).
//! - Calvin static dispatch (all write keys known upfront).
//! - Calvin dependent-read dispatch (OLLP) (write keys depend on a pre-read).
//! - Best-effort non-atomic dispatch (each vshard independently, no atomicity).
//!
//! `TxClass` construction lives in the sibling [`tx_class`](super::tx_class)
//! module; this module classifies and routes.
//!
//! # Note on predicate_class
//!
//! The ideal implementation of `predicate_class` would serialize the `Filter`
//! AST via zerompk and normalize bound parameter values to their type tags.
//! However, `nodedb_sql::types::Filter` does not derive `zerompk::ToMessagePack`
//! or `zerompk::FromMessagePack`. As a declared fallback, `predicate_class`
//! accepts the canonical SQL text string (post-parse-canonicalization) and
//! normalizes numeric and string literals to their type tags before hashing.
//! This is a degraded path relative to AST-level hashing.

use std::collections::BTreeSet;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use nodedb_cluster::calvin::sequencer::inbox::Inbox;
#[cfg(test)]
use nodedb_types::TenantId;

#[cfg(test)]
use crate::Error;
#[cfg(test)]
use crate::control::cluster::calvin::executor::ollp::orchestrator::OllpOrchestrator;
#[cfg(test)]
use crate::control::planner::calvin::cross_shard_mode::CrossShardTxnMode;
#[cfg(test)]
use crate::control::planner::calvin::tx_class::build_static_tx_class;
use crate::control::planner::calvin::types::DispatchClass;
#[cfg(test)]
use crate::control::planner::calvin::types::DispatchOutcome;
#[cfg(test)]
use crate::control::server::shared::session::TransactionState;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::types::VShardId;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

pub use crate::control::planner::calvin::predicate::predicate_class;
pub use crate::control::planner::calvin::write_class::is_write_plan;

// ── is_dependent_predicate ────────────────────────────────────────────────────

/// Returns `true` if the plan contains a value-dependent predicate that
/// requires OLLP dependent-read dispatch instead of static Calvin dispatch.
///
/// The detection criterion: the plan is a `BulkUpdate` or `BulkDelete`
/// (predicate is not a point-equality on the collection's primary key).
/// Point-equality writes (`PointPut`, `PointInsert`, `PointDelete`,
/// `PointUpdate`) have their write keys statically known and are routed
/// via the static Calvin path.
pub fn is_dependent_predicate(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Document(DocumentOp::BulkUpdate { .. })
            | PhysicalPlan::Document(DocumentOp::BulkDelete { .. })
    )
}

// ── classify_dispatch ─────────────────────────────────────────────────────────

/// Derive the set of vShards a transaction's session read-set touches.
///
/// Each [`ReadSetEntry`] homes to its collection's vShard using the SAME
/// collection→vShard map `ReadWriteSet::participating_vshards` uses to derive the
/// `TxClass` read_set's participants. Each read retains its session database so
/// classification and the database-scoped transaction class agree. A read with
/// no extractable collection contributes nothing.
pub fn read_vshards_of(reads: &[ReadSetEntry]) -> BTreeSet<u32> {
    reads
        .iter()
        .filter(|e| !e.collection.is_empty())
        .map(|e| VShardId::from_collection_in_database(e.database_id, &e.collection).as_u32())
        .collect()
}

/// Classify the dispatch class of a task slice from the union of its write
/// vShards and the session read-set's vShards (`read_vshards`).
///
/// 0 or 1 unique vShards → `SingleShard`.
/// 2+ unique vShards → `MultiShard` with the full `BTreeSet<u32>`.
///
/// A txn that writes shard X but READS shard Y participates in `{X, Y}` and must
/// route through Calvin with Y as a participant, so the read vShards widen the
/// class exactly as the write vShards do. Autocommit callers pass an empty
/// `read_vshards` (no session read-set is captured outside an explicit
/// transaction block), preserving write-only classification for them.
pub fn classify_dispatch(tasks: &[PhysicalTask], read_vshards: &BTreeSet<u32>) -> DispatchClass {
    let mut vshards: BTreeSet<u32> = BTreeSet::new();
    let mut last_vshard = None;

    for task in tasks {
        if is_write_plan(&task.plan) {
            let id = task.vshard_id.as_u32();
            vshards.insert(id);
            last_vshard = Some(task.vshard_id);
        }
    }

    // Union the session read-set's vShards into the participant candidate set.
    vshards.extend(read_vshards.iter().copied());

    match vshards.len() {
        0 => DispatchClass::SingleShard {
            vshard: tasks
                .first()
                .map(|t| t.vshard_id)
                .unwrap_or(VShardId::new(0)),
        },
        1 => DispatchClass::SingleShard {
            // The single vShard is a write shard whenever any write ran (the
            // common case: `last_vshard` is set). It could instead be a lone
            // read shard with no writes — unreachable via the COMMIT path, which
            // only classifies a non-empty write buffer — so the `unwrap_or_else`
            // is a defensive fallback upholding the no-panic contract.
            vshard: last_vshard.unwrap_or_else(|| VShardId::new(0)),
        },
        _ => DispatchClass::MultiShard { vshards },
    }
}

// ── dispatch_calvin_or_fast ───────────────────────────────────────────────────

/// Route a set of tasks to the appropriate dispatch path.
///
/// Decision tree:
/// 1. `InBlock` + `MultiShard` → `Err(CrossShardInExplicitTransaction)`.
/// 2. `MultiShard` + `Strict` + no inbox → `Err(SequencerUnavailable)`.
/// 3. `MultiShard` + `Strict` → Calvin static path via inbox.
/// 4. `MultiShard` + `BestEffortNonAtomic` → independent per-vshard dispatch.
/// 5. `SingleShard` → existing single-shard fast path.
///
/// The single-shard and best-effort paths are modeled here as outcomes only —
/// the caller is responsible for the actual Data Plane dispatch, since this
/// module lives in the Control Plane and has no direct Data Plane handle.
#[cfg(test)]
pub(crate) async fn dispatch_calvin_or_fast(
    tasks: &[PhysicalTask],
    mode: CrossShardTxnMode,
    tx_state: TransactionState,
    inbox: Option<&Inbox>,
    _orchestrator: Option<&Arc<OllpOrchestrator>>,
    tenant_id: TenantId,
    reads: &[ReadSetEntry],
) -> crate::Result<DispatchOutcome> {
    // Interactive COMMIT threads its session read-set here; autocommit passes an
    // empty slice. The read vShards widen both the classification (below) and the
    // TxClass read_set participants (in `build_static_tx_class`) in lockstep.
    let read_vshards = read_vshards_of(reads);
    let class = classify_dispatch(tasks, &read_vshards);

    match &class {
        DispatchClass::MultiShard { .. } => {
            // Reject cross-shard writes inside explicit transaction blocks.
            if tx_state == TransactionState::InBlock {
                return Err(Error::CrossShardInExplicitTransaction);
            }

            match mode {
                CrossShardTxnMode::Strict => {
                    let inbox = inbox.ok_or(Error::SequencerUnavailable)?;
                    // Populate the TxClass read_set from the session reads so the
                    // read shards are enumerated as Calvin participants.
                    let tx_class = build_static_tx_class(tasks, tenant_id, reads)?;
                    let inbox_seq = inbox.submit(tx_class).map_err(|e| Error::BadRequest {
                        detail: format!("Calvin sequencer rejected transaction: {e}"),
                    })?;
                    Ok(DispatchOutcome::CalvinStatic { inbox_seq })
                }
                CrossShardTxnMode::BestEffortNonAtomic => Ok(DispatchOutcome::BestEffortNonAtomic),
            }
        }
        DispatchClass::SingleShard { .. } => Ok(DispatchOutcome::SingleShard),
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for Calvin dispatch classification and routing.

    use super::*;
    use std::collections::BTreeSet;

    use crate::Error;
    use crate::control::planner::calvin::cross_shard_mode::CrossShardTxnMode;
    use crate::control::planner::calvin::types::{DispatchClass, DispatchOutcome};
    use crate::control::server::shared::session::TransactionState;
    use crate::control::server::shared::session::read_set::{
        EngineTag, ReadKey, ReadOrigin, ReadSetEntry,
    };
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
    use nodedb_physical::physical_plan::{ColumnarOp, CrdtOp, DocumentOp, PhysicalPlan};
    use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
    use nodedb_types::QualifiedCollection;

    fn crdt_apply_task(vshard: u32) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(vshard),
            database_id: crate::types::DatabaseId::DEFAULT,
            plan: PhysicalPlan::Crdt(CrdtOp::Apply {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, &format!("col_{vshard}")),
                document_id: "id1".to_owned(),
                delta: vec![],
                peer_id: 0,
                mutation_id: 0,
                surrogate: nodedb_types::Surrogate::new(1),
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: None,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    fn doc_insert_task(vshard: u32) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(vshard),
            database_id: crate::types::DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, &format!("col_{vshard}")),
                document_id: "id1".to_owned(),
                surrogate: nodedb_types::Surrogate::new(1),
                value: vec![],
                if_absent: false,
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    fn scan_task(vshard: u32) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(vshard),
            database_id: crate::types::DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::Scan {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, &format!("col_{vshard}")),
                filters: vec![],
                limit: 0,
                offset: 0,
                sort_keys: vec![],
                distinct: false,
                projection: vec![],
                computed_columns: vec![],
                window_functions: vec![],
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
                prefilter: None,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    fn bulk_update_task(vshard: u32) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(vshard),
            database_id: crate::types::DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, &format!("col_{vshard}")),
                filters: vec![],
                updates: vec![],
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
                rls_filters: vec![],
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    #[test]
    fn is_write_plan_classifies_correctly() {
        let write = doc_insert_task(0).plan;
        let read = scan_task(0).plan;
        assert!(is_write_plan(&write));
        assert!(!is_write_plan(&read));
    }

    #[test]
    fn is_write_plan_classifies_crdt_list_ops() {
        let list_ops = [
            (
                "ListInsert",
                PhysicalPlan::Crdt(CrdtOp::ListInsert {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
                    document_id: "id1".to_owned(),
                    list_path: "blocks".to_owned(),
                    index: 0,
                    fields_json: "{}".to_owned(),
                    surrogate: nodedb_types::Surrogate::new(1),
                }),
            ),
            (
                "ListDelete",
                PhysicalPlan::Crdt(CrdtOp::ListDelete {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
                    document_id: "id1".to_owned(),
                    list_path: "blocks".to_owned(),
                    index: 0,
                    surrogate: nodedb_types::Surrogate::new(1),
                }),
            ),
            (
                "ListMove",
                PhysicalPlan::Crdt(CrdtOp::ListMove {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
                    document_id: "id1".to_owned(),
                    list_path: "blocks".to_owned(),
                    from_index: 0,
                    to_index: 1,
                    surrogate: nodedb_types::Surrogate::new(1),
                }),
            ),
        ];
        for (name, plan) in &list_ops {
            assert!(is_write_plan(plan), "{name} should classify as a write");
        }
    }

    #[test]
    fn is_write_plan_classifies_columnar_update_and_delete() {
        let update = PhysicalPlan::Columnar(ColumnarOp::Update {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            filters: vec![],
            updates: vec![],
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        let delete = PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            filters: vec![],
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(
            is_write_plan(&update),
            "ColumnarOp::Update should be a write"
        );
        assert!(
            is_write_plan(&delete),
            "ColumnarOp::Delete should be a write"
        );
    }

    #[test]
    fn classify_dispatch_multi_shard_counts_newly_widened_crdt_apply_write() {
        // Before the `is_write_plan` widening, `CrdtOp::Apply` was misclassified
        // as a read: `classify_dispatch` would have counted zero write vshards
        // for this pair and returned `SingleShard`, silently dropping Calvin's
        // cross-shard atomicity for a real two-vshard CRDT write.
        let tasks = vec![crdt_apply_task(3), crdt_apply_task(7)];
        let class = classify_dispatch(&tasks, &BTreeSet::new());
        match class {
            DispatchClass::MultiShard { vshards } => {
                let v: Vec<u32> = vshards.into_iter().collect();
                assert_eq!(
                    v,
                    vec![3, 7],
                    "CrdtOp::Apply must be counted as a write vshard"
                );
            }
            other => panic!("expected MultiShard for two CrdtOp::Apply writes, got {other:?}"),
        }
    }

    #[test]
    fn is_dependent_predicate_bulk_update() {
        let task = bulk_update_task(0);
        assert!(is_dependent_predicate(&task.plan));
    }

    #[test]
    fn is_dependent_predicate_point_insert_is_false() {
        let task = doc_insert_task(0);
        assert!(!is_dependent_predicate(&task.plan));
    }

    #[test]
    fn classify_dispatch_single_shard() {
        let tasks = vec![doc_insert_task(5), doc_insert_task(5)];
        let class = classify_dispatch(&tasks, &BTreeSet::new());
        assert!(matches!(
            class,
            DispatchClass::SingleShard { vshard } if vshard.as_u32() == 5
        ));
    }

    #[test]
    fn classify_dispatch_multi_shard_returns_btreeset() {
        let tasks = vec![doc_insert_task(3), doc_insert_task(7)];
        let class = classify_dispatch(&tasks, &BTreeSet::new());
        match class {
            DispatchClass::MultiShard { vshards } => {
                let v: Vec<u32> = vshards.into_iter().collect();
                assert_eq!(v, vec![3, 7]);
            }
            _ => panic!("expected MultiShard"),
        }
    }

    #[test]
    fn classify_dispatch_zero_writes_is_single_shard() {
        let tasks = vec![scan_task(3), scan_task(7)];
        let class = classify_dispatch(&tasks, &BTreeSet::new());
        assert!(matches!(class, DispatchClass::SingleShard { .. }));
    }

    #[test]
    fn classify_dispatch_read_widened_multi_shard() {
        // A single-WRITE-shard batch (shard 5) that READS shard 8 classifies as
        // MultiShard{5,8}: the read vShard widens the participant set exactly as a
        // write vShard would.
        let tasks = vec![doc_insert_task(5)];
        let read_vshards: BTreeSet<u32> = [8u32].into_iter().collect();
        let class = classify_dispatch(&tasks, &read_vshards);
        match class {
            DispatchClass::MultiShard { vshards } => {
                let v: Vec<u32> = vshards.into_iter().collect();
                assert_eq!(v, vec![5, 8], "read shard 8 must union with write shard 5");
            }
            other => panic!("expected MultiShard{{5,8}} for write-5 + read-8, got {other:?}"),
        }
    }

    /// Find two collection names whose `DatabaseId::DEFAULT`-scoped vShard ids
    /// differ, so a write homed to one and a read homed to the other genuinely span
    /// two vShards. Mirrors the same-named helper the cross-node cluster tests use.
    fn two_distinct_vshard_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..512 {
            let name = format!("dispatch_home_{i}");
            let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
            match first {
                Some((ref fname, fv)) if fv != vshard => return (fname.clone(), name),
                None => first = Some((name, vshard)),
                _ => {}
            }
        }
        panic!("could not find two distinct-vshard collections in 512 tries");
    }

    #[test]
    fn read_entry_on_foreign_collection_widens_class_to_multishard() {
        // Regression pin for the cross-node "silent-pass" serializability hole.
        //
        // A transaction that BUFFERS a write on collection A's vShard and READS a
        // DIFFERENT collection B must classify `MultiShard`, because the read-set
        // entry for B homes (via `read_vshards_of`) to B's own vShard and widens the
        // participant set. This exercises the real routing seam: `read_vshards_of`
        // homing + `classify_dispatch` union, exactly as interactive COMMIT invokes
        // them (`session::commit::run_commit`).
        //
        // WHY this must stay `MultiShard`: only the `MultiShard` branch of COMMIT
        // flushes through the Calvin barrier (`run_commit_calvin`), which validates
        // B's read slice on B's OWNING node using the real per-shard `read_lsn`. If a
        // foreign read failed to widen the class, COMMIT would take the `SingleShard`
        // branch and run only the local-WAL `si_conflict_abort`, which never sees a
        // stale read on the remote owner — silently committing a non-serializable
        // cross-node transaction. This test guarantees a future refactor of
        // `read_vshards_of` / `classify_dispatch` cannot reopen that hole.
        let (write_coll, read_coll) = two_distinct_vshard_collections();
        let write_vshard =
            VShardId::from_collection_in_database(DatabaseId::DEFAULT, &write_coll).as_u32();
        let read_vshard =
            VShardId::from_collection_in_database(DatabaseId::DEFAULT, &read_coll).as_u32();

        let tasks = vec![doc_insert_task(write_vshard)];

        let read_entry = ReadSetEntry {
            engine: EngineTag::Document,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            collection: read_coll.clone(),
            key: ReadKey::Predicate,
            read_lsn: Lsn::new(1),
            read_version_lsn: Lsn::ZERO,
            origin: ReadOrigin::Session,
        };

        // The homing step under test: a foreign-collection read must home to a
        // vShard distinct from the write's, contributing a new participant.
        let read_vshards = read_vshards_of(std::slice::from_ref(&read_entry));
        assert!(
            read_vshards.contains(&read_vshard) && !read_vshards.contains(&write_vshard),
            "read entry for `{read_coll}` must home to vShard {read_vshard}, not the write's {write_vshard}"
        );

        match classify_dispatch(&tasks, &read_vshards) {
            DispatchClass::MultiShard { vshards } => {
                assert!(
                    vshards.contains(&write_vshard) && vshards.contains(&read_vshard),
                    "cross-collection read must widen the class to include both the write \
                     vShard {write_vshard} and the read vShard {read_vshard}, got {vshards:?}"
                );
            }
            other => panic!(
                "expected MultiShard for write-on-{write_vshard} + foreign-read-on-{read_vshard}, \
                 got {other:?} (a SingleShard here would route COMMIT to local-WAL \
                 si_conflict_abort and reopen the cross-node serializability hole)"
            ),
        }
    }

    #[test]
    fn classify_dispatch_read_on_write_shard_stays_single() {
        // Reading the same shard the writes target does not widen the class.
        let tasks = vec![doc_insert_task(5)];
        let read_vshards: BTreeSet<u32> = [5u32].into_iter().collect();
        let class = classify_dispatch(&tasks, &read_vshards);
        assert!(matches!(
            class,
            DispatchClass::SingleShard { vshard } if vshard.as_u32() == 5
        ));
    }

    #[test]
    fn predicate_class_byte_stable_across_runs() {
        let h1 = predicate_class("WHERE balance > 1000", "accounts");
        let h2 = predicate_class("WHERE balance > 1000", "accounts");
        assert_eq!(h1, h2);
    }

    #[test]
    fn predicate_class_normalizes_bound_parameters() {
        let h1 = predicate_class("WHERE balance > 1000", "accounts");
        let h2 = predicate_class("WHERE balance > 9999", "accounts");
        assert_eq!(
            h1, h2,
            "different numeric literals should normalize to the same predicate class"
        );
    }

    #[test]
    fn dispatch_inblock_multi_shard_rejects() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let tasks = vec![doc_insert_task(3), doc_insert_task(7)];
            let result = dispatch_calvin_or_fast(
                &tasks,
                CrossShardTxnMode::Strict,
                TransactionState::InBlock,
                None,
                None,
                TenantId::new(1),
                &[],
            )
            .await;
            assert!(
                matches!(result, Err(Error::CrossShardInExplicitTransaction)),
                "expected CrossShardInExplicitTransaction, got {result:?}"
            );
        });
    }

    #[test]
    fn dispatch_no_inbox_returns_sequencer_unavailable() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let tasks = vec![doc_insert_task(3), doc_insert_task(7)];
            let result = dispatch_calvin_or_fast(
                &tasks,
                CrossShardTxnMode::Strict,
                TransactionState::Idle,
                None,
                None,
                TenantId::new(1),
                &[],
            )
            .await;
            assert!(
                matches!(result, Err(Error::SequencerUnavailable)),
                "expected SequencerUnavailable, got {result:?}"
            );
        });
    }

    #[test]
    fn dispatch_best_effort_skips_inbox() {
        use nodedb_cluster::calvin::sequencer::config::SequencerConfig;
        use nodedb_cluster::calvin::sequencer::inbox::new_inbox;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (inbox, mut rx) = new_inbox(16, &SequencerConfig::default());
            let tasks = vec![doc_insert_task(3), doc_insert_task(7)];
            let result = dispatch_calvin_or_fast(
                &tasks,
                CrossShardTxnMode::BestEffortNonAtomic,
                TransactionState::Idle,
                Some(&inbox),
                None,
                TenantId::new(1),
                &[],
            )
            .await;
            assert!(
                matches!(result, Ok(DispatchOutcome::BestEffortNonAtomic)),
                "expected BestEffortNonAtomic, got {result:?}"
            );
            let mut out = Vec::new();
            let drained = rx.drain_into_capped(&mut out, 10, usize::MAX);
            assert_eq!(drained, 0, "inbox should not have been called");
        });
    }
}
