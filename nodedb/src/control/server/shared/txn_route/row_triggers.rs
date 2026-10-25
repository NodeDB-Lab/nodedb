// SPDX-License-Identifier: BUSL-1.1

//! The row triggers of one point write in its statement's transaction.
//!
//! Before the write stages: read the OLD row through the transaction, derive
//! the NEW row, fire the INSTEAD OF and BEFORE bodies, and patch the write
//! with the NEW row a BEFORE body left. After it staged and changed a row:
//! fire the SYNC AFTER ROW bodies. Every body joins the transaction.
//!
//! A row write that finds no row to update or delete changes nothing, so no
//! row trigger fires for it.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{DocumentOp, UpdateValue};
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::Value;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::trigger::dml_hook::{
    DmlWriteInfo, classify_dml_write, fetch_old_row, patch_task_with_mutated_fields, update_new_row,
};
use crate::control::trigger::dml_hook_fire::{
    DispatchTriggerParams, PreDispatchResult, fire_after_row_triggers, fire_pre_dispatch_triggers,
};
use crate::control::trigger::statement_txn::fires_joined_body;
use crate::control::trigger::{DmlEvent, TriggerScope};

use super::route::TxnTaskContext;

type Row = HashMap<String, Value>;

/// One row write whose SYNC AFTER ROW bodies fire once it staged.
pub(super) struct RowWrite {
    /// The write's collection, event and NEW row after its BEFORE bodies.
    pub info: DmlWriteInfo,
    /// The row before the write, for an UPDATE or DELETE.
    pub old_row: Option<Row>,
}

/// What the BEFORE and INSTEAD OF pass left of a write.
pub(super) enum BeforeOutcome {
    /// An INSTEAD OF body replaced the write.
    InsteadOf,
    /// Stage the (possibly patched) write. `Some` names the row write whose
    /// SYNC AFTER ROW bodies fire once it staged.
    Proceed(Option<RowWrite>),
}

/// Fire the INSTEAD OF and BEFORE bodies of `task`'s row write, and patch
/// the write with the NEW row a BEFORE body left.
///
/// `expanded` marks a point op an in-transaction expansion emitted: its
/// `PointPut` is an updated row's post-image.
pub(super) async fn fire_before(
    ctx: &TxnTaskContext<'_>,
    task: &mut PhysicalTask,
    expanded: bool,
) -> crate::Result<BeforeOutcome> {
    let Some(mut info) = classify_dml_write(&task.plan) else {
        return Ok(BeforeOutcome::Proceed(None));
    };
    let scope = TriggerScope {
        database_id: task.database_id,
        tenant_id: task.tenant_id,
    };
    let Some(document_id) = info.document_id.clone() else {
        // A predicate write whose collection fires a row body reaches here
        // only as the point writes the route expanded it to (`predicate`).
        if fires_row_body(ctx, scope, &info) {
            return Err(crate::Error::Internal {
                detail: format!(
                    "a predicate write on '{}' reached its row triggers without the route \
                     expanding it to point writes",
                    info.collection
                ),
            });
        }
        return Ok(BeforeOutcome::Proceed(None));
    };
    if expanded
        && matches!(
            task.plan,
            PhysicalPlan::Document(DocumentOp::PointPut { .. })
        )
    {
        info.event = DmlEvent::Update;
    }
    if !fires_row_body(ctx, scope, &info) {
        return Ok(BeforeOutcome::Proceed(None));
    }

    // The OLD row as the transaction left it, for UPDATE, DELETE and the
    // upsert probe.
    let needs_old =
        matches!(info.event, DmlEvent::Update | DmlEvent::Delete) || info.needs_existence_probe;
    let old_row = if needs_old {
        let row = fetch_old_row(
            ctx.state,
            ctx.identity,
            task.database_id,
            ctx.auth,
            &nodedb_types::QualifiedCollection::from_stored(info.collection.clone()),
            &document_id,
            ctx.txn.sessions.tx_id(ctx.txn.session_id),
        )
        .await?;
        (!row.is_empty()).then_some(row)
    } else {
        None
    };
    if info.needs_existence_probe {
        info.event = if old_row.is_some() {
            DmlEvent::Update
        } else {
            DmlEvent::Insert
        };
    }
    let new_fields = new_row(
        &task.plan,
        &info,
        old_row.as_ref(),
        &crate::control::crs_context::crs_context_for(ctx.state),
    )?;
    let Some(new_fields) = new_fields else {
        // An UPDATE or DELETE that finds no row changes nothing.
        return Ok(BeforeOutcome::Proceed(None));
    };
    info.new_fields = new_fields;

    let fired = fire_pre_dispatch_triggers(DispatchTriggerParams {
        state: ctx.state,
        identity: ctx.identity,
        database_id: task.database_id,
        tenant_id: task.tenant_id,
        info: &info,
        old_row: &old_row,
        cascade_depth: 0,
        txn: ctx.txn,
    })
    .await?;
    match fired {
        PreDispatchResult::Handled => return Ok(BeforeOutcome::InsteadOf),
        PreDispatchResult::Proceed {
            mutated_fields: Some(mutated),
        } => {
            let before = info.new_fields.take().unwrap_or_default();
            patch_task_with_mutated_fields(task, &before, &mutated)?;
            info.new_fields = Some(mutated);
        }
        PreDispatchResult::Proceed {
            mutated_fields: None,
        } => {}
    }
    enforce_checks(ctx, task.database_id, task.tenant_id, &info).await?;
    Ok(BeforeOutcome::Proceed(Some(RowWrite { info, old_row })))
}

/// Check the NEW row an INSERT or UPDATE writes, as its BEFORE bodies left
/// it, against the collection's CHECK constraints.
async fn enforce_checks(
    ctx: &TxnTaskContext<'_>,
    database_id: crate::types::DatabaseId,
    tenant_id: crate::types::TenantId,
    info: &DmlWriteInfo,
) -> crate::Result<()> {
    if !matches!(info.event, DmlEvent::Insert | DmlEvent::Update) {
        return Ok(());
    }
    let Some(new_row) = info.new_fields.as_ref() else {
        return Ok(());
    };
    let collection = crate::control::target_identity::naming::bare_collection_name(
        database_id,
        &info.collection,
    );
    let Some(entry) = ctx.state.credentials.catalog().get_collection(
        database_id,
        tenant_id.as_u64(),
        &collection,
    )?
    else {
        return Ok(());
    };
    if entry.check_constraints.is_empty() {
        return Ok(());
    }
    crate::control::server::shared::check_constraint::enforce_check_constraints(
        ctx.state,
        ctx.identity,
        database_id,
        &entry.check_constraints,
        new_row,
    )
    .await
    .map_err(crate::Error::from)
}

/// Fire the SYNC AFTER ROW bodies of a row write that staged.
pub(super) async fn fire_after(
    ctx: &TxnTaskContext<'_>,
    task_scope: TriggerScope,
    row: &RowWrite,
) -> crate::Result<()> {
    fire_after_row_triggers(DispatchTriggerParams {
        state: ctx.state,
        identity: ctx.identity,
        database_id: task_scope.database_id,
        tenant_id: task_scope.tenant_id,
        info: &row.info,
        old_row: &row.old_row,
        cascade_depth: 0,
        txn: ctx.txn,
    })
    .await
}

/// Whether the write's collection has a BEFORE, INSTEAD OF or SYNC AFTER
/// trigger for any event the write can be.
fn fires_row_body(ctx: &TxnTaskContext<'_>, scope: TriggerScope, info: &DmlWriteInfo) -> bool {
    let events: &[DmlEvent] = if info.needs_existence_probe {
        &[DmlEvent::Insert, DmlEvent::Update]
    } else {
        std::slice::from_ref(&info.event)
    };
    events
        .iter()
        .any(|event| fires_joined_body(ctx.state, scope, &info.collection, *event))
}

/// The NEW row the write makes, `Some(None)` for a DELETE. `None` when an
/// UPDATE or DELETE finds no row.
fn new_row(
    plan: &PhysicalPlan,
    info: &DmlWriteInfo,
    old_row: Option<&Row>,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Option<Option<Row>>> {
    let incoming = || info.new_fields.clone().unwrap_or_default();
    match info.event {
        DmlEvent::Insert => Ok(Some(Some(incoming()))),
        DmlEvent::Delete => Ok(old_row.map(|_| None)),
        DmlEvent::Update => {
            let Some(old) = old_row else {
                return Ok(None);
            };
            let new = match plan {
                PhysicalPlan::Document(DocumentOp::PointUpdate { updates, .. }) => {
                    update_new_row(old, updates, crs)?
                }
                PhysicalPlan::Document(DocumentOp::Upsert {
                    on_conflict_updates,
                    ..
                }) => upsert_new_row(old, &incoming(), on_conflict_updates, crs)?,
                // An expanded update's `PointPut` carries the whole row.
                _ => incoming(),
            };
            Ok(Some(Some(new)))
        }
    }
}

/// The NEW row an UPSERT that found `old` writes: the incoming fields merged
/// over it, or, with `ON CONFLICT DO UPDATE`, its assignments applied against
/// it with `EXCLUDED` naming the incoming row.
fn upsert_new_row(
    old: &Row,
    incoming: &Row,
    on_conflict_updates: &[(String, UpdateValue)],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Row> {
    if on_conflict_updates.is_empty() {
        let mut new = old.clone();
        new.extend(incoming.iter().map(|(k, v)| (k.clone(), v.clone())));
        return Ok(new);
    }
    let old_row = Value::Object(old.clone());
    let excluded = Value::Object(incoming.clone());
    let mut new = old.clone();
    for (field, rhs) in on_conflict_updates {
        let value = match rhs {
            UpdateValue::Literal(bytes) => {
                nodedb_types::value_from_msgpack(bytes).map_err(|error| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("ON CONFLICT assignment to '{field}': {error}"),
                    }
                })?
            }
            UpdateValue::Expr(expr) => expr
                .eval_with_excluded(&old_row, &excluded, crs)
                .map_err(crate::Error::from)?,
        };
        new.insert(field.clone(), value);
    }
    Ok(new)
}
