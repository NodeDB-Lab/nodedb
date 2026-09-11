// SPDX-License-Identifier: BUSL-1.1

//! The Control-Plane pass that resolves each planned write's materialized-sum
//! target rows and writes them onto the plan.

use std::sync::Arc;

use nodedb_physical::physical_plan::{
    DocumentOp, MaterializedSumBinding, PhysicalPlan, ResolvedSumTarget,
};
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::Surrogate;

use super::extract::join_value_from_body;
use super::resolve_target::resolve_one_target;
use super::settle::{
    SettleInput, co_resident_target_keys, omit_shipped, settle_cross_shard_images,
};
use super::stored::stored_row_scope;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::server::surrogate_exchange::lookup_surrogate_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

/// Resolve the materialized-sum target rows for every document write in
/// `tasks`, storing the result in that op's `resolved_sum_targets`.
///
/// Three join-value sources feed resolution, and one op may draw on more
/// than one: row BODIES read straight off each op; the PREDICATE
/// `BulkUpdate`/`BulkDelete`/`TRUNCATE` name rows by (via a recon scan);
/// and the STORED row a point write rewrites/removes (`PointDelete`/
/// `PointUpdate` have no other source; `PointPut`/`Upsert` need it for a
/// join-key rewrite's abandoned target). `UpdateFromJoin`/`InsertSelect`/
/// `Merge` are resolved by their own orchestrators instead. A collection
/// driving no binding costs one cached index probe and nothing else.
///
/// Also appends [`ApplyBalanceDelta`] tasks: a binding whose TARGET is
/// cross-shard can't apply inside the source write's transaction, so this
/// pass folds the read images and appends one task per settled balance,
/// homed on the target (deferral recorded by removing the join value from
/// the source op's resolution — see [`super::settle`]).
///
/// The returned [`ReadSetEntry`]s cover the folded images; the caller must
/// union them into the dispatch read-set so Calvin OCC aborts the
/// statement on concurrent source writes, before any row moves.
pub async fn resolve_materialized_sum_targets(
    state: &SharedState,
    tasks: &mut Vec<PhysicalTask>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<Vec<ReadSetEntry>> {
    let schema_version = state.schema_version.current();
    let catalog = state.credentials.catalog();
    let mut appended: Vec<PhysicalTask> = Vec::new();
    let mut reads: Vec<ReadSetEntry> = Vec::new();

    for task in tasks.iter_mut() {
        let txn_id = task.txn_id;
        let PhysicalPlan::Document(op) = &mut task.plan else {
            continue;
        };
        // Predicate-driven plans resolve from their own recon scan and settle
        // their own cross-shard balances; the body-driven path below never sees
        // them.
        if let Some(settlement) = super::predicate::resolve_predicate_sum_targets(
            state,
            op,
            txn_id,
            tenant_id,
            database_id,
            trace_id,
        )
        .await?
        {
            appended.extend(settlement.tasks);
            reads.extend(settlement.reads);
            continue;
        }
        // The read of `op` is scoped so the borrows the two classifications hold
        // end before the resolution is written back through `&mut op`.
        let outcome = {
            let carried = value_carrying(op);
            let stored = stored_row_scope(op);
            // The two classifications name the same collection when both apply;
            // an op that is neither carries no join value at all and is skipped.
            let Some(collection) = stored
                .as_ref()
                .map(|scope| scope.collection)
                .or_else(|| carried.as_ref().map(|(collection, _)| *collection))
            else {
                continue;
            };

            // The gate, before any read: a collection driving nothing pays one
            // cached index probe and nothing else.
            let source = strip_db_prefix(database_id, collection);
            let Some(bindings) = state.materialized_sum_index.bindings_for_source(
                catalog,
                schema_version,
                database_id,
                tenant_id,
                source,
            )?
            else {
                continue;
            };

            let mut resolved = match &carried {
                Some((_, bodies)) => {
                    resolve_bodies(state, &bindings, bodies, tenant_id, database_id, trace_id)
                        .await?
                }
                None => Vec::new(),
            };
            // A point write that rewrites a stored row reads it here — for the
            // join values it addresses AND for the pre-image a cross-shard
            // delta is folded from. One read, one snapshot: settling from a
            // second read would total a different one.
            match &stored {
                None => (resolved, None),
                Some(scope) => {
                    let images = super::stored::extend_with_stored_row(
                        state,
                        &bindings,
                        scope,
                        &mut resolved,
                        tenant_id,
                        database_id,
                        trace_id,
                    )
                    .await?;
                    let input = SettleInput {
                        source_collection: collection,
                        images: &images.images,
                        source_row: Some(scope.surrogate),
                        read_version_lsn: images.read_version_lsn,
                    };
                    let settlement = settle_cross_shard_images(
                        &bindings,
                        &input,
                        &resolved,
                        txn_id,
                        tenant_id,
                        database_id,
                    )?;
                    // The resolution the source op keeps is the one the source
                    // core may still apply. Removing the shipped values IS the
                    // deferral: what is left resolved is exactly what no
                    // sibling task carries.
                    omit_shipped(
                        &mut resolved,
                        &settlement.shipped,
                        &co_resident_target_keys(&bindings, &input, database_id)?,
                    );
                    (resolved, Some(settlement))
                }
            }
        };
        let (resolved, settlement) = outcome;
        set_resolved(op, resolved);
        if let Some(settlement) = settlement {
            appended.extend(settlement.tasks);
            reads.extend(settlement.reads);
        }
    }

    tasks.extend(appended);
    Ok(reads)
}

/// The `(collection, bodies)` of a write op that carries row bodies, or `None`
/// for every op that does not. The match is exhaustive so a new `DocumentOp`
/// variant must state which side it is on.
fn value_carrying(op: &DocumentOp) -> Option<(&str, Vec<&[u8]>)> {
    match op {
        DocumentOp::PointInsert {
            collection, value, ..
        }
        | DocumentOp::PointPut {
            collection, value, ..
        }
        | DocumentOp::Upsert {
            collection, value, ..
        } => Some((collection.as_str(), vec![value.as_slice()])),
        DocumentOp::BatchInsert {
            collection,
            documents,
            ..
        } => Some((
            collection.as_str(),
            documents.iter().map(|(_, v)| v.as_slice()).collect(),
        )),
        // No row body at plan time: a delete names a row it does not carry, and
        // an update carries field assignments rather than a whole row. Their
        // join values come off the STORED row instead — see
        // [`stored_row_scope`].
        DocumentOp::PointDelete { .. } | DocumentOp::PointUpdate { .. } => None,
        // Predicate-driven and read-only ops.
        DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::MaterializeScan { .. }
        // Already resolved: the resolve pass copied the intercepted plan's own
        // resolution onto every mutation it produced.
        | DocumentOp::ResolvedWrite { .. }
        // Already resolved: this op IS the resolution, carrying the target
        // row's surrogate the Control Plane looked up when it appended it.
        | DocumentOp::ApplyBalanceDelta { .. } => None,
    }
}

/// Write the resolution into the op's slot. Exhaustive for the same reason
/// [`value_carrying`] is.
fn set_resolved(op: &mut DocumentOp, resolved: Vec<ResolvedSumTarget>) {
    match op {
        DocumentOp::PointInsert {
            resolved_sum_targets,
            ..
        }
        | DocumentOp::PointPut {
            resolved_sum_targets,
            ..
        }
        | DocumentOp::Upsert {
            resolved_sum_targets,
            ..
        }
        | DocumentOp::BatchInsert {
            resolved_sum_targets,
            ..
        }
        // Resolved from the STORED row rather than from a carried body, but the
        // slot they travel in is the same one.
        | DocumentOp::PointDelete {
            resolved_sum_targets,
            ..
        }
        | DocumentOp::PointUpdate {
            resolved_sum_targets,
            ..
        } => *resolved_sum_targets = resolved,
        DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ResolvedWrite { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => {}
    }
}

/// Resolve the materialized-sum targets for a set of row BODIES of one source
/// collection, for a caller that holds the bodies itself rather than a plan.
///
/// This is the seam the Control-Plane orchestrators use. `INSERT ... SELECT`,
/// `MERGE` and `UPDATE ... FROM` all resolve their rows on the Control Plane and
/// re-issue concrete work (a `BatchInsert` page, an APPLY pass, a write pass)
/// through `dispatch_local`, which never passes through
/// [`resolve_materialized_sum_targets`]. Without this they would ship an empty
/// resolution and the Data-Plane fold would have no target to address.
///
/// `source_collection` is the db-qualified name as it appears on the plan.
/// Returns an empty vec — and issues no lookup at all — when the collection
/// drives no binding.
pub async fn resolve_sum_targets_for_bodies(
    state: &SharedState,
    bodies: &[&[u8]],
    source_collection: &str,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<Vec<ResolvedSumTarget>> {
    let schema_version = state.schema_version.current();
    let catalog = state.credentials.catalog();
    let source = strip_db_prefix(database_id, source_collection);
    let Some(bindings) = state.materialized_sum_index.bindings_for_source(
        catalog,
        schema_version,
        database_id,
        tenant_id,
        source,
    )?
    else {
        return Ok(Vec::new());
    };
    resolve_bodies(state, &bindings, bodies, tenant_id, database_id, trace_id).await
}

/// Whether `source_collection` drives any materialized-sum binding.
///
/// The gate every predicate-driven resolver checks FIRST: a collection that
/// drives nothing must not pay for a recon scan, which is the whole cost of the
/// predicate path. `source_collection` is the db-qualified plan name.
pub fn source_drives_bindings(
    state: &SharedState,
    source_collection: &str,
    tenant_id: TenantId,
    database_id: DatabaseId,
) -> crate::Result<Option<Arc<Vec<MaterializedSumBinding>>>> {
    let schema_version = state.schema_version.current();
    let catalog = state.credentials.catalog();
    let source = strip_db_prefix(database_id, source_collection);
    state.materialized_sum_index.bindings_for_source(
        catalog,
        schema_version,
        database_id,
        tenant_id,
        source,
    )
}

/// Resolve one already-extracted join VALUE to its target row's surrogate.
///
/// `lookup_surrogate_routed`, never `assign_surrogate_routed`: a join value that
/// names no existing target row must fail the statement, not mint identity for a
/// row that does not exist.
pub(super) async fn lookup_join_value(
    state: &SharedState,
    binding: &MaterializedSumBinding,
    join_value: &str,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<Surrogate> {
    let target = db_qualified(database_id, &binding.target_collection);
    let vshard = VShardId::from_key(join_value.as_bytes());
    lookup_surrogate_routed(
        state,
        vshard,
        database_id,
        tenant_id,
        &target,
        join_value.as_bytes(),
        trace_id,
    )
    .await?
    .ok_or_else(|| crate::Error::MaterializedSumTargetNotFound {
        target_collection: binding.target_collection.clone(),
        join_column: binding.join_column.clone(),
        join_value: join_value.to_string(),
    })
}

/// Resolve every `(binding, body)` pair to its target row's surrogate.
///
/// One entry per DISTINCT `(target collection, join value)` PAIR: a batch that
/// touches the same target row many times resolves it once, while two bindings
/// that share a join column and name different targets each get their own entry
/// — deduping on the value alone would resolve the first and silently hand its
/// target row to the second.
async fn resolve_bodies(
    state: &SharedState,
    bindings: &Arc<Vec<MaterializedSumBinding>>,
    bodies: &[&[u8]],
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<Vec<ResolvedSumTarget>> {
    let mut resolved: Vec<ResolvedSumTarget> = Vec::new();
    for binding in bindings.iter() {
        for body in bodies {
            let Some(join_value) = join_value_from_body(body, &binding.join_column) else {
                // The row does not carry this binding's join column, so it does
                // not participate in the binding at all — nothing to resolve
                // and nothing to add a delta to.
                continue;
            };
            resolve_one_target(
                &mut resolved,
                &binding.target_collection,
                join_value,
                async |v| {
                    lookup_join_value(state, binding, v, tenant_id, database_id, trace_id)
                        .await
                        .map(Some)
                },
            )
            .await?;
        }
    }
    Ok(resolved)
}

/// Qualify a catalog collection name for the plan / surrogate namespace.
fn db_qualified(database_id: DatabaseId, collection: &str) -> String {
    nodedb_types::QualifiedCollection::new(database_id, collection)
        .as_str()
        .to_owned()
}

/// Strip the `"<db_id>/"` prefix a planned collection name carries, yielding the
/// catalog name the binding index is keyed on.
fn strip_db_prefix(database_id: DatabaseId, qualified: &str) -> &str {
    if database_id == DatabaseId::DEFAULT {
        return qualified;
    }
    let prefix = format!("{}/", database_id.as_u64());
    qualified.strip_prefix(prefix.as_str()).unwrap_or(qualified)
}

#[cfg(test)]
mod tests {
    use super::*;

    use nodedb_physical::physical_task::PostSetOp;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::catalog::{MaterializedSumDef, StoredCollection};
    use crate::wal::WalManager;

    const TENANT: TenantId = TenantId::new(7);
    const DB: DatabaseId = DatabaseId::DEFAULT;

    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("create materialized-sum test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("msum.wal"))
                .expect("open materialized-sum test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct materialized-sum state");
        (state, directory)
    }

    /// Declare `entries` (the source collection) as the source of a
    /// `balance` materialized sum on `accounts`, joined on `account_id`.
    fn declare_binding(state: &SharedState) {
        let catalog = state.credentials.catalog();
        let mut target = StoredCollection::new(TENANT.as_u64(), "accounts", "tester");
        target.materialized_sums.push(MaterializedSumDef {
            target_collection: "accounts".to_string(),
            target_column: "balance".to_string(),
            source_collection: "entries".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
        });
        catalog
            .put_collection(DB, &target)
            .expect("persist target collection");
        let source = StoredCollection::new(TENANT.as_u64(), "entries", "tester");
        catalog
            .put_collection(DB, &source)
            .expect("persist source collection");
        state.materialized_sum_index.invalidate();
    }

    /// Declare a SECOND materialized sum on the same source, reading the SAME
    /// join column into a DIFFERENT target collection.
    fn declare_second_binding(state: &SharedState) {
        let catalog = state.credentials.catalog();
        let mut target = StoredCollection::new(TENANT.as_u64(), "audit_totals", "tester");
        target.materialized_sums.push(MaterializedSumDef {
            target_collection: "audit_totals".to_string(),
            target_column: "balance".to_string(),
            source_collection: "entries".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
        });
        catalog
            .put_collection(DB, &target)
            .expect("persist second target collection");
        state.materialized_sum_index.invalidate();
    }

    fn body(account_id: &str) -> Vec<u8> {
        let map = rmpv::Value::Map(vec![
            (
                rmpv::Value::String("account_id".into()),
                rmpv::Value::String(account_id.into()),
            ),
            (
                rmpv::Value::String("amount".into()),
                rmpv::Value::Integer(25.into()),
            ),
        ]);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &map).expect("encode test body");
        buf
    }

    fn insert_task(collection: &str, value: Vec<u8>) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TENANT,
            vshard_id: VShardId::new(0),
            database_id: DB,
            plan: PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: nodedb_types::QualifiedCollection::new(DB, collection),
                document_id: "e1".to_string(),
                value,
                if_absent: false,
                surrogate: Surrogate::new(900),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    fn resolved_of(task: &PhysicalTask) -> &[ResolvedSumTarget] {
        match &task.plan {
            PhysicalPlan::Document(DocumentOp::PointInsert {
                resolved_sum_targets,
                ..
            }) => resolved_sum_targets,
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A join key that names an existing target row resolves to THAT row's
    /// surrogate — the value the Data Plane needs to address the balance.
    #[tokio::test]
    async fn resolving_join_key_populates_the_target_surrogate() {
        let (state, _directory) = test_state();
        declare_binding(&state);
        let target_surrogate = state
            .surrogate_assigner
            .assign(DB, TENANT, "accounts", b"acc-1")
            .expect("bind target row");

        let mut tasks = vec![insert_task("entries", body("acc-1"))];
        resolve_materialized_sum_targets(&state, &mut tasks, TENANT, DB, TraceId::ZERO)
            .await
            .expect("resolution succeeds");

        assert_eq!(
            resolved_of(&tasks[0]),
            &[ResolvedSumTarget::new(
                "accounts",
                "acc-1",
                target_surrogate
            )]
        );
    }

    /// Two bindings of one source that share a join column resolve to TWO
    /// entries, one per target collection — not one entry the second binding
    /// silently inherits.
    ///
    /// Deduped on the join value alone, the second lookup never happens and the
    /// plan carries only `accounts`' row. The Data-Plane fold then writes
    /// `audit_totals`' balance into a row of that surrogate, and both stored
    /// totals are wrong with nothing reported.
    #[tokio::test]
    async fn two_bindings_sharing_a_join_column_resolve_separately() {
        let (state, _directory) = test_state();
        declare_binding(&state);
        declare_second_binding(&state);
        let accounts_row = state
            .surrogate_assigner
            .assign(DB, TENANT, "accounts", b"acc-1")
            .expect("bind accounts row");
        let audit_row = state
            .surrogate_assigner
            .assign(DB, TENANT, "audit_totals", b"acc-1")
            .expect("bind audit_totals row");
        assert_ne!(
            accounts_row, audit_row,
            "the two targets must be different rows, or the test cannot fail"
        );

        let mut tasks = vec![insert_task("entries", body("acc-1"))];
        resolve_materialized_sum_targets(&state, &mut tasks, TENANT, DB, TraceId::ZERO)
            .await
            .expect("resolution succeeds");

        let resolved = resolved_of(&tasks[0]);
        assert_eq!(
            resolved.len(),
            2,
            "one entry per binding, not one per join value: {resolved:?}"
        );
        assert_eq!(
            nodedb_physical::physical_plan::resolved_sum_surrogate(resolved, "accounts", "acc-1"),
            Some(accounts_row)
        );
        assert_eq!(
            nodedb_physical::physical_plan::resolved_sum_surrogate(
                resolved,
                "audit_totals",
                "acc-1"
            ),
            Some(audit_row)
        );
    }

    /// A join key naming no target row fails the statement with a typed error
    /// that says which collection, column, and value could not be resolved.
    /// Skipping the row would leave the stored balance short of the sum
    /// `VERIFY_BALANCE` recomputes over every source row.
    #[tokio::test]
    async fn unresolvable_join_key_fails_with_a_typed_error() {
        let (state, _directory) = test_state();
        declare_binding(&state);

        let mut tasks = vec![insert_task("entries", body("acc-missing"))];
        let error = resolve_materialized_sum_targets(&state, &mut tasks, TENANT, DB, TraceId::ZERO)
            .await
            .expect_err("a missing target must fail the statement");

        match error {
            crate::Error::MaterializedSumTargetNotFound {
                target_collection,
                join_column,
                join_value,
            } => {
                assert_eq!(target_collection, "accounts");
                assert_eq!(join_column, "account_id");
                assert_eq!(join_value, "acc-missing");
            }
            other => panic!("expected MaterializedSumTargetNotFound, got {other:?}"),
        }
    }

    /// A collection that drives no binding — nearly every collection — leaves
    /// the slot empty and never reaches the resolution path at all.
    #[tokio::test]
    async fn collection_without_bindings_resolves_nothing() {
        let (state, _directory) = test_state();
        declare_binding(&state);

        let mut tasks = vec![insert_task("unrelated", body("acc-1"))];
        resolve_materialized_sum_targets(&state, &mut tasks, TENANT, DB, TraceId::ZERO)
            .await
            .expect("resolution succeeds");

        assert!(resolved_of(&tasks[0]).is_empty());
        // The gate that produced that empty slot: the index reports no bindings
        // for this source, so no join value is extracted and no surrogate
        // lookup is issued.
        assert!(
            state
                .materialized_sum_index
                .bindings_for_source(
                    state.credentials.catalog(),
                    state.schema_version.current(),
                    DB,
                    TENANT,
                    "unrelated",
                )
                .expect("index probe")
                .is_none()
        );
    }

    /// A batch resolves each DISTINCT join value once, so a page of rows
    /// against one account yields one entry rather than one per row.
    #[tokio::test]
    async fn repeated_join_values_resolve_once() {
        let (state, _directory) = test_state();
        declare_binding(&state);
        let target_surrogate = state
            .surrogate_assigner
            .assign(DB, TENANT, "accounts", b"acc-1")
            .expect("bind target row");

        let mut tasks = vec![PhysicalTask {
            tenant_id: TENANT,
            vshard_id: VShardId::new(0),
            database_id: DB,
            plan: PhysicalPlan::Document(DocumentOp::BatchInsert {
                collection: nodedb_types::QualifiedCollection::new(DB, "entries"),
                documents: vec![
                    ("e1".to_string(), body("acc-1")),
                    ("e2".to_string(), body("acc-1")),
                ],
                surrogates: vec![Surrogate::new(901), Surrogate::new(902)],
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }];
        resolve_materialized_sum_targets(&state, &mut tasks, TENANT, DB, TraceId::ZERO)
            .await
            .expect("resolution succeeds");

        match &tasks[0].plan {
            PhysicalPlan::Document(DocumentOp::BatchInsert {
                resolved_sum_targets,
                ..
            }) => assert_eq!(
                resolved_sum_targets,
                &[ResolvedSumTarget::new(
                    "accounts",
                    "acc-1",
                    target_surrogate
                )]
            ),
            other => panic!("plan shape changed: {other:?}"),
        }
    }
}
