// SPDX-License-Identifier: BUSL-1.1

//! Append a task of its own for every materialized-sum target that does NOT
//! share its source collection's vShard.
//!
//! Co-resident targets ride the source write's transaction for free;
//! cross-shard ones ship as a separate [`DocumentOp::ApplyBalanceDelta`]
//! task homed on the target's vShard — mirroring the implicit graph edge
//! mechanism, so the pair goes multi-shard via the two tasks' own
//! `vshard_id`s and Calvin commits them together or not at all.
//!
//! Only runs where the plan determines the delta NUMBER by itself:
//! `PointInsert`/`BatchInsert` (new rows, whole value credited, no
//! pre-image to subtract). Every other shape's delta needs an image the
//! plan doesn't carry, so it keeps folding on the Data Plane instead.
//! `if_absent` inserts are excluded: a silently-skipped row owes nothing,
//! and the plan can't know which rows will be skipped.

use rust_decimal::Decimal;

use nodedb_physical::physical_plan::{
    DocumentOp, PhysicalPlan, ResolvedSumTarget, resolved_sum_surrogate,
};
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::state::SharedState;
use crate::query::sum_target_is_co_resident;
use crate::types::{DatabaseId, TenantId};

/// One balance write this pass decided to ship on its own task.
struct AppendedDelta {
    binding_target: String,
    task: PhysicalTask,
}

/// Append an `ApplyBalanceDelta` task per cross-shard target, and record the
/// deferral on the source op so the Data Plane does not also apply it.
///
/// Runs AFTER [`resolve_materialized_sum_targets`](super::resolve) — it consumes
/// that pass's `resolved_sum_targets`, and issues no lookup of its own. A
/// collection that drives no binding costs one cached index probe and nothing
/// else.
pub fn append_cross_shard_balance_tasks(
    state: &SharedState,
    tasks: &mut Vec<PhysicalTask>,
    tenant_id: TenantId,
    database_id: DatabaseId,
) -> crate::Result<()> {
    let schema_version = state.schema_version.current();
    let catalog = state.credentials.catalog();
    let crs = crate::control::crs_context::crs_context_for(state);

    // Collected first so the immutable walk of `tasks` does not borrow-conflict
    // with the `&mut Vec` pushed into below — the same two-phase shape
    // `append_implicit_edge_tasks` uses.
    let mut appended: Vec<(usize, Vec<AppendedDelta>)> = Vec::new();
    for (index, task) in tasks.iter().enumerate() {
        let PhysicalPlan::Document(op) = &task.plan else {
            continue;
        };
        let Some(SettleableInsert {
            collection,
            docs,
            resolved,
        }) = settleable_insert(op)?
        else {
            continue;
        };
        let Some(bindings) = state.materialized_sum_index.bindings_for_source(
            catalog,
            schema_version,
            database_id,
            tenant_id,
            strip_db_prefix(database_id, collection),
        )?
        else {
            continue;
        };

        let mut for_task = Vec::new();
        let source = nodedb_types::CollectionKey::from_qualified_str(database_id, collection)?;
        for binding in bindings.iter() {
            if sum_target_is_co_resident(source, &binding.target_collection) {
                continue;
            }
            for (join_value, delta) in crate::query::binding_insert_deltas(binding, &docs, &crs)? {
                // A zero net delta leaves the stored total unchanged, so the
                // read-modify-write on the target would rewrite the row
                // byte-for-byte. Shipping a task for it would also make an
                // otherwise single-shard statement multi-shard for nothing.
                if delta == Decimal::ZERO {
                    continue;
                }
                let surrogate =
                    resolved_sum_surrogate(resolved, &binding.target_collection, &join_value)
                        .ok_or_else(|| crate::Error::MaterializedSumTargetNotFound {
                            target_collection: binding.target_collection.clone(),
                            join_column: binding.join_column.clone(),
                            join_value: join_value.clone(),
                        })?;
                for_task.push(AppendedDelta {
                    binding_target: binding.target_collection.clone(),
                    task: super::settle::balance_task(super::settle::BalanceTaskSpec {
                        txn_id: task.txn_id,
                        database_id,
                        tenant_id,
                        binding,
                        surrogate,
                        join_value,
                        delta,
                    }),
                });
            }
        }
        if !for_task.is_empty() {
            appended.push((index, for_task));
        }
    }

    for (index, deltas) in appended {
        for delta in deltas {
            // The deferral is recorded on the SOURCE op before its sibling is
            // pushed, so a plan can never carry the appended task without the
            // instruction that stops the source core applying it too.
            defer_binding(&mut tasks[index].plan, delta.binding_target);
            tasks.push(delta.task);
        }
    }

    Ok(())
}

/// A write whose materialized-sum delta the PLAN already determines: its source
/// collection, the row images it will store, and the resolve pass's join-value →
/// surrogate table.
struct SettleableInsert<'a> {
    collection: &'a str,
    docs: Vec<serde_json::Value>,
    resolved: &'a [ResolvedSumTarget],
}

/// The settleable shape of `op`, or `None` for every other op.
///
/// The match is exhaustive so a new `DocumentOp` variant must state which side
/// it is on: a variant that silently fell through would either lose its
/// cross-shard balance or, worse, have one guessed for it.
fn settleable_insert(op: &DocumentOp) -> crate::Result<Option<SettleableInsert<'_>>> {
    match op {
        DocumentOp::PointInsert {
            collection,
            value,
            if_absent,
            resolved_sum_targets,
            ..
        } => {
            // A skipped conflict inserts nothing and owes its target nothing,
            // and the plan cannot tell which rows will be skipped.
            if *if_absent {
                return Ok(None);
            }
            Ok(Some(SettleableInsert {
                collection: collection.as_str(),
                docs: decode_bodies(std::slice::from_ref(value)),
                resolved: resolved_sum_targets.as_slice(),
            }))
        }
        DocumentOp::BatchInsert {
            collection,
            documents,
            resolved_sum_targets,
            ..
        } => {
            let bodies: Vec<&[u8]> = documents.iter().map(|(_, v)| v.as_slice()).collect();
            Ok(Some(SettleableInsert {
                collection: collection.as_str(),
                docs: decode_bodies(&bodies),
                resolved: resolved_sum_targets.as_slice(),
            }))
        }
        // Every other write's delta is a difference between two images, at
        // least one of which the plan does not carry — an UPDATE's pre-image,
        // a DELETE's removed row, a `PointPut`/`Upsert`'s stored row when one
        // is already there. They fold on the Data Plane from the real images
        // and their cross-shard targets are not deferred here.
        DocumentOp::PointPut { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchUpsert { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Merge { .. }
        // Reads, index DDL, and the balance task this pass itself appends.
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        // Already resolved: the resolve pass copied the plan's own
        // `resolved_sum_targets` onto every mutation it produced.
        | DocumentOp::ResolvedWrite { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => Ok(None),
    }
}

/// Record that one binding's delta travels on its own task. Exhaustive for the
/// same reason [`settleable_insert`] is.
fn defer_binding(plan: &mut PhysicalPlan, target_collection: String) {
    let PhysicalPlan::Document(op) = plan else {
        return;
    };
    let deferred = match op {
        DocumentOp::PointInsert {
            deferred_sum_targets,
            ..
        }
        | DocumentOp::BatchInsert {
            deferred_sum_targets,
            ..
        } => deferred_sum_targets,
        DocumentOp::PointPut { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchUpsert { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        // See `settleable_insert`.
        | DocumentOp::ResolvedWrite { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => return,
    };
    if !deferred.contains(&target_collection) {
        deferred.push(target_collection);
    }
}

/// Decode each MessagePack row body into a document.
///
/// A body that will not decode carries no column any binding can read, so it
/// contributes no delta — the same conclusion the Data-Plane hook reaches for a
/// submitted body it cannot decode.
fn decode_bodies<B: AsRef<[u8]>>(bodies: &[B]) -> Vec<serde_json::Value> {
    bodies
        .iter()
        .filter_map(|body| nodedb_types::json_from_msgpack(body.as_ref()).ok())
        .collect()
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
