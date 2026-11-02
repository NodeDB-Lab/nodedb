// SPDX-License-Identifier: BUSL-1.1

//! The prediction a predicate write inside a session transaction carries.
//!
//! A predicate `UPDATE` or `DELETE` in a transaction reads its rows at its
//! statement, as the transaction sees them: at the plan's match scope, the
//! block's snapshot, with the staged writes on top. Its plan carries the matched
//! surrogates, and the implicit edges of the matched edge documents when the
//! collection is edge-bearing. The statement's stage refuses a prediction its
//! rows do not match. The buffered plan keeps the prediction, so a Calvin
//! COMMIT stages exactly those rows and checks them again at its turn.
//!
//! An update on an edge-bearing collection derives its implicit-edge
//! reconciliation from the same read, as the autocommit recon path does.

use nodedb_physical::physical_plan::{DocumentOp, OllpPredictedEdge, PhysicalPlan, UpdateValue};
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::SystemTimeScope;

use super::dependent_recon_plan::{inject_ollp_predicted_edges, inject_ollp_surrogates};
use super::predicate::predicate_class_for_filters;
use super::preexec::{PreexecRequest, PreexecScan, RowIdentityRead, run_preexec_scan};
use crate::control::planner::implicit_edges::{
    EdgeUpdateCtx, append_implicit_edge_update_tasks, parse_edge_field_overrides,
};
use crate::control::state::SharedState;
use crate::types::{TraceId, TxnId};

/// The read a predicate write in a transaction took at its statement.
pub struct TxnPredictedRead {
    /// The matched rows, as the transaction saw them.
    pub scan: PreexecScan,
    /// The implicit-edge tasks an update on an edge-bearing collection
    /// derives from the read. Empty for a delete and for any other update.
    pub edge_tasks: Vec<PhysicalTask>,
    /// The predicate class the write's retries are budgeted on.
    pub predicate_class: u64,
}

/// The predicate-write shape of a plan.
enum PredicateWrite<'a> {
    Delete {
        declared_primary_key: Option<String>,
    },
    Update {
        updates: &'a [(String, UpdateValue)],
    },
}

/// Read the rows the predicate write `task` matches in transaction `txn_id`,
/// and inject them into its plan. `None` for any other task.
///
/// A delete on an edge-bearing collection also reads each row's identity,
/// which its node-delete edge tasks key on.
pub async fn predict_txn_bulk_write(
    state: &SharedState,
    task: &mut PhysicalTask,
    txn_id: Option<TxnId>,
) -> crate::Result<Option<TxnPredictedRead>> {
    let Some((collection, filters, shape, system_time)) = predicate_write(&task.plan) else {
        return Ok(None);
    };
    let (tenant_id, database_id) = (task.tenant_id, task.database_id);
    let predicate_class = predicate_class_for_filters(&filters, &collection);
    let edge_bearing = super::edge_truncate::collection_is_edge_bearing(
        state,
        tenant_id,
        database_id,
        &collection,
    )?;
    let identity = match &shape {
        PredicateWrite::Delete {
            declared_primary_key,
        } if edge_bearing => RowIdentityRead::Read {
            declared_primary_key: declared_primary_key.as_deref(),
        },
        PredicateWrite::Delete { .. } | PredicateWrite::Update { .. } => RowIdentityRead::Skip,
    };
    let scan = run_preexec_scan(
        state,
        tenant_id,
        database_id,
        PreexecRequest {
            collection: &collection,
            filters,
            prefilter: None,
            identity,
            txn_id,
            // The scope the stage matches at, so the stage finds the rows
            // this read predicted: the block's snapshot inside a block.
            system_time,
        },
    )
    .await?;

    let mut edge_tasks = Vec::new();
    if edge_bearing && let PredicateWrite::Update { updates } = &shape {
        let overrides = parse_edge_field_overrides(updates)?;
        append_implicit_edge_update_tasks(
            EdgeUpdateCtx {
                state,
                tenant_id,
                database_id,
                trace_id: TraceId::ZERO,
                collection: &collection,
            },
            &mut edge_tasks,
            &scan.edges,
            &scan.surrogates,
            &overrides,
        )
        .await?;
    }

    inject_ollp_surrogates(&mut task.plan, scan.surrogates.clone());
    if edge_bearing {
        let edges = scan.edges.iter().map(OllpPredictedEdge::from).collect();
        inject_ollp_predicted_edges(&mut task.plan, edges);
    }
    Ok(Some(TxnPredictedRead {
        scan,
        edge_tasks,
        predicate_class,
    }))
}

/// The collection, serialized filters, shape and match scope of a predicate
/// write. `None` for any other plan.
fn predicate_write(
    plan: &PhysicalPlan,
) -> Option<(String, Vec<u8>, PredicateWrite<'_>, SystemTimeScope)> {
    let op = match plan {
        PhysicalPlan::Document(op) => op,
        PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => return None,
    };
    match op {
        DocumentOp::BulkDelete {
            collection,
            filters,
            declared_primary_key,
            system_time,
            ..
        } => Some((
            collection.to_string(),
            filters.clone(),
            PredicateWrite::Delete {
                declared_primary_key: declared_primary_key.clone(),
            },
            *system_time,
        )),
        DocumentOp::BulkUpdate {
            collection,
            filters,
            updates,
            system_time,
            ..
        } => Some((
            collection.to_string(),
            filters.clone(),
            PredicateWrite::Update { updates },
            *system_time,
        )),
        DocumentOp::PointGet { .. }
        | DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchUpsert { .. }
        | DocumentOp::BatchInsert { .. }
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
        | DocumentOp::Merge { .. }
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ResolvedWrite { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => None,
    }
}
