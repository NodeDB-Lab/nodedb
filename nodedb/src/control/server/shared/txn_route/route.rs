// SPDX-License-Identifier: BUSL-1.1

//! One task of a statement in its transaction: the client's transaction
//! block, or the implicit transaction a statement runs in when its writes
//! fire a BEFORE, INSTEAD OF or SYNC AFTER body.
//!
//! Every protocol's statement loop routes each task through
//! [`route_txn_task`]:
//!
//! 1. An in-transaction `MERGE`, `UPDATE ... FROM`, `INSERT ... SELECT` or
//!    `BatchInsert` page expands into the point ops it writes, and so does a
//!    predicate `UPDATE` or `DELETE` whose collection fires a row body
//!    (`predicate`). Each op takes the steps below on its own.
//! 2. A point write fires its INSTEAD OF and BEFORE bodies (`row_triggers`).
//! 3. A write to a shadowed clone takes its copy-on-write steps, whose
//!    tombstones and copy-up mappings commit with the transaction.
//! 4. The write stages into the transaction's overlay, or buffers for COMMIT.
//! 5. A point write that changed a row fires its SYNC AFTER ROW bodies.
//!
//! Every body joins the transaction. The statement's SYNC AFTER STATEMENT
//! bodies fire once, after its last task ([`StatementEvents::fire`]).

use std::future::Future;
use std::sync::Arc;

use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::PhysicalTask;

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::lease::QueryLeaseScope;
use crate::control::planner::calvin::write_class::{
    plan_counts_toward_statement_tag, plans_have_user_write,
};
use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{PlanKind, describe_plan};
use crate::control::server::shared::clone_write::{
    TxnCloneWriteOutcome, maybe_intercept_clone_write_in_txn,
};
use crate::control::server::shared::returning::in_transaction_returning_unsupported;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::server::shared::session::expander_stage::expand_in_tx;
use crate::control::server::shared::session::staging_gate::{
    InTxnRoute, StagedWriteOutcome, StagingGateError, route_in_tx_write,
};
use crate::control::server::shared::sql::staging_predicates::{
    require_affected_count, stages_returning,
};
use crate::control::server::shared::write_admission::plan_is_write;
use crate::control::state::SharedState;
use crate::control::trigger::dml_hook_fire::fire_after_statement_triggers;
use crate::control::trigger::{DmlEvent, SyncFire, TriggerScope};

use super::joined::task_events;
use super::predicate::expand_predicate_write;
use super::row_triggers::{BeforeOutcome, RowWrite, fire_after, fire_before};
use crate::control::server::shared::sql::staging_predicates::StagedTagKind;

/// What one statement task routed through its transaction needs.
#[derive(Clone, Copy)]
pub struct TxnTaskContext<'a> {
    pub state: &'a SharedState,
    pub identity: &'a AuthenticatedIdentity,
    /// The statement's auth context, which the OLD-row read runs under.
    pub auth: &'a AuthContext,
    /// The statement's transaction.
    pub txn: &'a DmlTxnCtx<'a>,
    /// The statement's descriptor leases, kept on every task it buffers.
    pub lease_scope: &'a Arc<QueryLeaseScope>,
    /// Whether the route fires the statement's triggers. `false` for a
    /// caller that fired them around the statement itself.
    pub fire_triggers: bool,
}

/// What one task of a statement came to in its transaction.
pub enum TxnTaskOutcome {
    /// Not a write the transaction holds: a read, carrying the transaction
    /// id for read-your-own-writes. The caller dispatches it.
    Dispatch(Box<PhysicalTask>),
    /// Buffered for COMMIT: no count and no rows yet.
    Buffered,
    /// Staged into the transaction's overlay, with its real count and, for a
    /// write that answers `RETURNING`, its rows.
    Staged(StagedWriteOutcome),
    /// A clone copy-on-write answered the write without staging it.
    CloneHandled(Response),
    /// An INSTEAD OF body replaced the write, which changed nothing.
    InsteadOf,
}

/// The collections and events a statement's writes named, for its SYNC
/// AFTER STATEMENT bodies. Each `(collection, event)` fires once.
#[derive(Default)]
pub struct StatementEvents {
    named: Vec<(TriggerScope, String, DmlEvent)>,
}

impl StatementEvents {
    fn note(&mut self, scope: TriggerScope, collection: &str, event: DmlEvent) {
        let seen = self.named.iter().any(|(s, c, e)| {
            s.database_id == scope.database_id
                && s.tenant_id == scope.tenant_id
                && c == collection
                && *e == event
        });
        if !seen {
            self.named.push((scope, collection.to_owned(), event));
        }
    }

    /// Fire the statement's SYNC AFTER STATEMENT bodies, once per collection
    /// and event its writes named. Each body joins the transaction.
    pub async fn fire(self, ctx: &TxnTaskContext<'_>) -> crate::Result<()> {
        for (scope, collection, event) in self.named {
            fire_after_statement_triggers(
                SyncFire {
                    state: ctx.state,
                    identity: ctx.identity,
                    scope,
                    cascade_depth: 0,
                    txn: ctx.txn,
                },
                &collection,
                event,
            )
            .await?;
        }
        Ok(())
    }
}

/// Route one statement task through its transaction. `dispatch` sends a
/// staging request to the Data Plane the protocol's own way.
pub async fn route_txn_task<F, Fut>(
    ctx: &TxnTaskContext<'_>,
    mut task: PhysicalTask,
    events: &mut StatementEvents,
    dispatch: F,
) -> Result<TxnTaskOutcome, StagingGateError>
where
    F: Fn(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    refuse_unanswerable_returning(&task.plan)?;
    if ctx.fire_triggers
        && let Some((collection, named)) = task_events(&task)
    {
        let scope = TriggerScope {
            database_id: task.database_id,
            tenant_id: task.tenant_id,
        };
        for event in named {
            events.note(scope, &collection, event);
        }
    }

    let sessions = ctx.txn.sessions;
    let session_id = ctx.txn.session_id;
    if let Some((ops, kind)) = expand_in_tx(ctx.state, sessions, session_id, &mut task).await? {
        return route_expanded(ctx, ops, kind, true, &dispatch).await;
    }
    // A predicate write whose collection fires a row body writes each row
    // it matches by its primary key, so every row's bodies fire.
    if ctx.fire_triggers
        && let Some((ops, kind)) = expand_predicate_write(ctx, &task)
            .await
            .map_err(StagingGateError::Dispatch)?
    {
        return route_expanded(ctx, ops, kind, false, &dispatch).await;
    }
    route_point(ctx, task, false, &dispatch).await
}

/// Route the point writes one statement task expanded to, and answer with
/// their summed count and rows under the statement's `kind`. `expanded`
/// marks the ops an in-transaction expander emitted (see `fire_before`).
async fn route_expanded<F, Fut>(
    ctx: &TxnTaskContext<'_>,
    ops: Vec<PhysicalTask>,
    kind: StagedTagKind,
    expanded: bool,
    dispatch: &F,
) -> Result<TxnTaskOutcome, StagingGateError>
where
    F: Fn(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    // A balance task the expansion appended stages like any op, but its count
    // describes a row the statement never named.
    let has_user_write = plans_have_user_write(ops.iter().map(|op| &op.plan));
    let mut affected = 0usize;
    let mut returning_rows = Vec::new();
    for op in ops {
        let counts = plan_counts_toward_statement_tag(&op.plan, has_user_write);
        match route_point(ctx, op, expanded, dispatch).await? {
            TxnTaskOutcome::Staged(outcome) => {
                if counts {
                    affected += outcome.affected;
                }
                returning_rows.extend(outcome.returning_rows);
            }
            TxnTaskOutcome::CloneHandled(resp) => {
                let changed = require_affected_count(resp.payload.as_ref())
                    .map_err(StagingGateError::Dispatch)? as usize;
                if counts {
                    affected += changed;
                }
            }
            TxnTaskOutcome::InsteadOf => {}
            TxnTaskOutcome::Buffered | TxnTaskOutcome::Dispatch(_) => {
                return Err(StagingGateError::Dispatch(crate::Error::Internal {
                    detail: "a point write an in-transaction statement expanded to did not stage"
                        .into(),
                }));
            }
        }
    }
    Ok(TxnTaskOutcome::Staged(StagedWriteOutcome {
        kind,
        affected,
        payload: Vec::new(),
        returning_rows,
    }))
}

/// Route one point task: row triggers, clone copy-on-write, staging.
async fn route_point<F, Fut>(
    ctx: &TxnTaskContext<'_>,
    mut task: PhysicalTask,
    expanded: bool,
    dispatch: &F,
) -> Result<TxnTaskOutcome, StagingGateError>
where
    F: Fn(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    let scope = TriggerScope {
        database_id: task.database_id,
        tenant_id: task.tenant_id,
    };
    let before = if ctx.fire_triggers {
        fire_before(ctx, &mut task, expanded)
            .await
            .map_err(StagingGateError::Dispatch)?
    } else {
        BeforeOutcome::Proceed(None)
    };
    let row = match before {
        BeforeOutcome::InsteadOf => return Ok(TxnTaskOutcome::InsteadOf),
        BeforeOutcome::Proceed(row) => row,
    };

    let sessions = ctx.txn.sessions;
    let session_id = ctx.txn.session_id;
    // A write to a shadowed clone takes its copy-on-write steps first. Rows
    // its tombstones hid add to the staged count.
    let mut hidden = 0u64;
    if plan_is_write(&task.plan)
        && let Some(txn_id) = sessions.tx_id(session_id)
    {
        let tenant_id = task.tenant_id;
        match maybe_intercept_clone_write_in_txn(
            ctx.state,
            &mut task,
            ctx.identity,
            tenant_id,
            txn_id,
        )
        .await
        .map_err(StagingGateError::Dispatch)?
        {
            TxnCloneWriteOutcome::Passthrough => {}
            TxnCloneWriteOutcome::Handled(resp) => {
                let changed = require_affected_count(resp.payload.as_ref())
                    .map_err(StagingGateError::Dispatch)?
                    > 0;
                fire_after_changed(ctx, scope, row.as_ref(), changed).await?;
                return Ok(TxnTaskOutcome::CloneHandled(resp));
            }
            TxnCloneWriteOutcome::Narrowed { hidden: rows } => hidden = rows,
        }
    }

    let buffer_start = sessions.buffered_task_count(session_id);
    let routed = route_in_tx_write(ctx.state, sessions, session_id, task, dispatch).await;
    if sessions.buffered_task_count(session_id) > buffer_start
        && !sessions.attach_tx_lease_scope_since(
            session_id,
            buffer_start,
            Arc::clone(ctx.lease_scope),
        )
    {
        return Err(StagingGateError::Dispatch(crate::Error::Internal {
            detail: "retaining descriptor leases for buffered transaction tasks failed".into(),
        }));
    }
    let outcome = match routed? {
        InTxnRoute::Read(task) | InTxnRoute::Autocommit(task) => {
            // A row write whose bodies joined the transaction must stage
            // there too, or it will apply outside the transaction they
            // joined.
            if row.is_some() {
                return Err(StagingGateError::Dispatch(crate::Error::Internal {
                    detail: "a write that fires a trigger did not stage into its transaction"
                        .into(),
                }));
            }
            return Ok(TxnTaskOutcome::Dispatch(task));
        }
        InTxnRoute::Buffered => TxnTaskOutcome::Buffered,
        InTxnRoute::Staged(mut outcome) => {
            outcome.affected += hidden as usize;
            TxnTaskOutcome::Staged(outcome)
        }
    };
    let changed = matches!(&outcome, TxnTaskOutcome::Staged(staged) if staged.affected > 0);
    fire_after_changed(ctx, scope, row.as_ref(), changed).await?;
    Ok(outcome)
}

/// Fire the SYNC AFTER ROW bodies of `row` when its write changed a row.
async fn fire_after_changed(
    ctx: &TxnTaskContext<'_>,
    scope: TriggerScope,
    row: Option<&RowWrite>,
    changed: bool,
) -> Result<(), StagingGateError> {
    match row {
        Some(row) if changed => fire_after(ctx, scope, row)
            .await
            .map_err(StagingGateError::Dispatch),
        _ => Ok(()),
    }
}

/// Refuse a write that asks for `RETURNING` rows its transaction cannot
/// answer at the statement, before anything stages. A Document point write,
/// `UPSERT`, predicate `UPDATE` / `DELETE` and `BatchInsert` page answer from
/// the rows they stage. Every other write stages or buffers without a row
/// image.
fn refuse_unanswerable_returning(plan: &PhysicalPlan) -> Result<(), StagingGateError> {
    let answers = stages_returning(plan)
        || matches!(
            plan,
            PhysicalPlan::Document(DocumentOp::BatchInsert {
                returning: Some(_),
                ..
            })
        );
    if matches!(describe_plan(plan), PlanKind::ReturningRows) && !answers {
        return Err(StagingGateError::Dispatch(
            in_transaction_returning_unsupported(),
        ));
    }
    Ok(())
}
