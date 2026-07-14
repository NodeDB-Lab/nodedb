// SPDX-License-Identifier: BUSL-1.1

//! Unit tests for Calvin dispatch classification and routing.

use super::*;
use std::collections::BTreeSet;

use crate::Error;
use crate::control::planner::calvin::cross_shard_mode::CrossShardTxnMode;
use crate::control::planner::calvin::types::{DispatchClass, DispatchOutcome};
use crate::control::server::shared::session::TransactionState;
use crate::control::server::shared::session::read_set::{EngineTag, ReadKey, ReadSetEntry};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use nodedb_physical::physical_plan::{ColumnarOp, CrdtOp, DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

fn crdt_apply_task(vshard: u32) -> PhysicalTask {
    PhysicalTask {
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(vshard),
        database_id: crate::types::DatabaseId::DEFAULT,
        plan: PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: format!("col_{vshard}"),
            document_id: "id1".to_owned(),
            delta: vec![],
            peer_id: 0,
            mutation_id: 0,
            surrogate: nodedb_types::Surrogate::new(1),
            provenance: None,
            constraint_version_required: 0,
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
            collection: format!("col_{vshard}"),
            document_id: "id1".to_owned(),
            surrogate: nodedb_types::Surrogate::new(1),
            value: vec![],
            if_absent: false,
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
            collection: format!("col_{vshard}"),
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
            collection: format!("col_{vshard}"),
            filters: vec![],
            updates: vec![],
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
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
                collection: "docs".to_owned(),
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
                collection: "docs".to_owned(),
                document_id: "id1".to_owned(),
                list_path: "blocks".to_owned(),
                index: 0,
                surrogate: nodedb_types::Surrogate::new(1),
            }),
        ),
        (
            "ListMove",
            PhysicalPlan::Crdt(CrdtOp::ListMove {
                collection: "docs".to_owned(),
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
        collection: "metrics".to_owned(),
        filters: vec![],
        updates: vec![],
    });
    let delete = PhysicalPlan::Columnar(ColumnarOp::Delete {
        collection: "metrics".to_owned(),
        filters: vec![],
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
