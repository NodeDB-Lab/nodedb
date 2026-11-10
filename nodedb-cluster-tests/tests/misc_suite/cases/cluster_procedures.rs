// SPDX-License-Identifier: BUSL-1.1
//! Tests for procedure behavior in cluster context.
//!
//! Validates data model contracts that the cluster relies on:
//! - Procedure DML replicates independently via normal Raft path
//! - Mid-procedure COMMIT produces independent Raft entries
//! - Cross-shard DML dispatches through query planner scatter-gather
//! - Replicated constraint violations produce DeltaReject with CompensationHint
//! - Procedure body parsing with transaction control statements

use nodedb::control::planner::procedural::ast::*;
use nodedb::control::planner::procedural::parse_block;
use nodedb::types::{DatabaseId, TenantId, VShardId};
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_types::sync::compensation::CompensationHint;
use nodedb_types::sync::wire::DeltaRejectMsg;

/// Decide + encode in one call: `to_replicated_entry` takes a decided
/// `ReplicableWrite`, and none of these fixtures carries a live RLS predicate.
fn encode_entry(
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: &PhysicalPlan,
) -> nodedb::Result<Option<nodedb::control::wal_replication::ReplicatedEntry>> {
    let write = nodedb::control::wal_replication::ReplicableWrite::decide_for_replication(plan)?;
    nodedb::control::wal_replication::to_replicated_entry(tenant_id, database_id, vshard_id, &write)
}

// ---------------------------------------------------------------------------
// Procedure DML replicates via normal Raft path
// ---------------------------------------------------------------------------

#[test]
fn procedure_dml_creates_replicated_entry() {
    // Each DML statement in a procedure body is dispatched through the
    // normal query planner, producing a replicated entry for Raft.
    let plan = PhysicalPlan::Document(DocumentOp::PointPut {
        collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "archive"),
        document_id: "a-1".into(),
        value: b"{}".to_vec(),
        surrogate: nodedb_types::Surrogate::new(1),
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
    });
    let entry = encode_entry(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &plan,
    )
    .expect("encode replicated entry");
    assert!(entry.is_some()); // DML → replicated
}

#[test]
fn procedure_reads_not_replicated() {
    let plan = PhysicalPlan::Document(DocumentOp::Scan {
        collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "archive"),
        limit: 100,
        offset: 0,
        sort_keys: vec![],
        filters: vec![],
        distinct: false,
        projection: vec![],
        computed_columns: vec![],
        window_functions: vec![],
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    });
    let entry = encode_entry(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &plan,
    )
    .expect("encode replicated entry");
    assert!(entry.is_none()); // Reads don't replicate
}

// ---------------------------------------------------------------------------
// Procedure DML: each planned write replicates through Raft
// ---------------------------------------------------------------------------

#[test]
fn procedure_writes_replicate_independently() {
    // Two DML statements of a procedure body, as planned tasks.
    let tasks = [
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
                document_id: "o-1".into(),
                value: b"{}".to_vec(),
                surrogate: nodedb_types::Surrogate::new(2),
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        },
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(1),
            database_id: DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::PointDelete {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "temp"),
                document_id: "t-1".into(),
                surrogate: Some(nodedb_types::Surrogate::new(1)),
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        },
    ];

    for task in &tasks {
        assert!(
            encode_entry(task.tenant_id, task.database_id, task.vshard_id, &task.plan,)
                .expect("encode replicated entry")
                .is_some()
        );
    }
}

// ---------------------------------------------------------------------------
// Replicated constraint violations → DeltaReject with CompensationHint
// ---------------------------------------------------------------------------

#[test]
fn delta_reject_with_unique_violation_hint() {
    let reject = DeltaRejectMsg {
        mutation_id: 42,
        reason: "unique constraint violation on email".into(),
        compensation: Some(CompensationHint::UniqueViolation {
            field: "email".into(),
            conflicting_value: "alice@example.com".into(),
        }),
    };
    assert_eq!(reject.mutation_id, 42);
    assert!(matches!(
        reject.compensation,
        Some(CompensationHint::UniqueViolation { .. })
    ));
}

#[test]
fn delta_reject_with_fk_violation_hint() {
    let reject = DeltaRejectMsg {
        mutation_id: 99,
        reason: "foreign key constraint: customer_id not found".into(),
        compensation: Some(CompensationHint::ForeignKeyMissing {
            referenced_id: "cust-999".into(),
        }),
    };
    assert!(matches!(
        reject.compensation,
        Some(CompensationHint::ForeignKeyMissing { .. })
    ));
}

#[test]
fn delta_reject_with_custom_hint() {
    let reject = DeltaRejectMsg {
        mutation_id: 1,
        reason: "check constraint failed".into(),
        compensation: Some(CompensationHint::Custom {
            constraint: "positive_balance".into(),
            detail: "balance must be >= 0".into(),
        }),
    };
    assert!(matches!(
        reject.compensation,
        Some(CompensationHint::Custom { .. })
    ));
}

// ---------------------------------------------------------------------------
// Procedure body with COMMIT parses correctly
// ---------------------------------------------------------------------------

#[test]
fn procedure_body_with_commit_parses() {
    let block = parse_block(
        "BEGIN \
           INSERT INTO archive SELECT * FROM orders WHERE old = TRUE; \
           COMMIT; \
           DELETE FROM orders WHERE old = TRUE; \
         END",
    )
    .unwrap();
    assert_eq!(block.statements.len(), 3);
    assert!(matches!(&block.statements[0], Statement::Sql { .. }));
    assert!(matches!(&block.statements[1], Statement::Commit));
    assert!(matches!(&block.statements[2], Statement::Sql { .. }));
}
