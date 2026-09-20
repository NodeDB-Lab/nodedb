// SPDX-License-Identifier: BUSL-1.1

//! Fold a completed Calvin batch into the statement's ONE result set and ONE
//! command tag. Protocol-neutral: pgwire and native both render this fold.
//!
//! Calvin deposits ONE applied response for the whole transaction (a second
//! RETURNING-bearing participant fails the statement upstream), and every
//! task reads that same payload: a RETURNING task yields the rows, taken once;
//! every other task folds its count into the tag.

use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan};

use super::compose::{ShapeOutcome, shape_response_materialized};
use super::redaction::QueryRedaction;
use super::request::MaterializedShapeRequest;
use super::schema::OutputSchema;
use super::types::{
    DmlFoldError, DmlOutcome, FoldedTag, PlanKind, ShapedRows, StatementTag, describe_plan,
    dml_outcome_by_op, dml_outcome_from_payload,
};
use crate::bridge::envelope::Response;
use crate::control::planner::calvin::write_class::{
    plan_counts_toward_statement_tag, plans_have_user_write,
};
use crate::control::security::auth_context::AuthContext;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// Shared inputs for shaping one completed Calvin batch.
pub struct CalvinFoldCtx<'a> {
    /// The statement's announced output columns, when it announced any. A
    /// RETURNING write is held to them exactly as the single-shard dispatch
    /// loop holds it, so the same statement renders the same row whichever
    /// route it took.
    pub projection: Option<&'a OutputSchema>,
    pub state: &'a SharedState,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The requester's resolved context; its roles drive column-level
    /// redaction of any RETURNING rows this batch surfaces.
    pub auth: &'a AuthContext,
}

/// One Calvin task's contribution to the statement's response.
pub enum CalvinTaskOutcome {
    /// RETURNING rows, folded into the statement's single result set.
    Rows(ShapedRows),
    /// A count-bearing outcome, folded into the statement's one tag.
    Dml(DmlOutcome),
    /// No count and no verb: the statement renders `OK` unless a
    /// count-bearing task folds too.
    Opaque,
}

/// Why a batch did not fold.
#[derive(Debug)]
pub enum CalvinFoldError {
    /// Two tasks reported verbs that cannot share one tag.
    Verb(DmlFoldError),
    /// A task's payload could not be read as its plan kind requires.
    Shape(crate::Error),
}

/// The statement's answer: its rows, when a task carried RETURNING, and its tag.
pub struct CalvinBatchFold {
    pub rows: Option<ShapedRows>,
    pub tag: Option<FoldedTag>,
}

/// Fold every task of a completed batch, in dispatch order.
///
/// A derived implicit-edge write beside the user's own never answers the
/// statement: it folds as opaque, as it never deposits. The rows are taken
/// once rather than accumulated, which would repeat the identical payload per
/// task.
pub fn fold_calvin_batch(
    plans: &[&PhysicalPlan],
    apply_resp: Option<&Response>,
    ctx: &CalvinFoldCtx<'_>,
) -> Result<CalvinBatchFold, CalvinFoldError> {
    let has_user_write = plans_have_user_write(plans.iter().copied());
    let mut rows: Option<ShapedRows> = None;
    let mut tag = StatementTag::default();
    for plan in plans {
        if !plan_counts_toward_statement_tag(plan, has_user_write) {
            tag.fold_opaque();
            continue;
        }
        match calvin_task_outcome(plan, apply_resp, ctx).map_err(CalvinFoldError::Shape)? {
            CalvinTaskOutcome::Rows(shaped) => {
                rows.get_or_insert(shaped);
            }
            CalvinTaskOutcome::Dml(outcome) => tag.fold(outcome).map_err(CalvinFoldError::Verb)?,
            CalvinTaskOutcome::Opaque => tag.fold_opaque(),
        }
    }
    Ok(CalvinBatchFold {
        rows,
        tag: tag.finish(),
    })
}

/// Shape one task of a completed batch.
///
/// A plan carrying RETURNING yields its rows as protocol-neutral
/// [`ShapedRows`] when the deposited payload shapes; a multi-row write plans
/// one task per row, and the caller folds every such task's rows into ONE
/// result set. Every other task (and a RETURNING task with no shaped payload)
/// contributes to the statement's one command tag.
pub fn calvin_task_outcome(
    plan: &PhysicalPlan,
    apply_resp: Option<&Response>,
    ctx: &CalvinFoldCtx<'_>,
) -> crate::Result<CalvinTaskOutcome> {
    let plan_kind = describe_plan(plan);
    let redaction = QueryRedaction::for_plan(ctx.tenant_id, ctx.auth, plan);
    if let (PlanKind::ReturningRows, Some(resp)) = (plan_kind, apply_resp)
        && let Ok(ShapeOutcome::Rows(shaped)) =
            shape_response_materialized(MaterializedShapeRequest {
                payload: resp.payload.as_bytes(),
                plan,
                plan_kind: PlanKind::ReturningRows,
                projection: ctx.projection,
                state: ctx.state,
                database_id: ctx.database_id,
                tenant_id: ctx.tenant_id,
                redaction: Some(redaction.ctx(&ctx.state.redaction)),
                // A RETURNING list names stored columns only, never a
                // Control-Plane computed column.
                sequences: None,
            })
    {
        return Ok(CalvinTaskOutcome::Rows(shaped));
    }

    // Plain write: surface its ACTUAL affected count from the payload. Every
    // primary-write participant deposits its applied `Response` before
    // proposing the completion ack, so a count-bearing plan ALWAYS has one
    // here. If it does not, the deposit path regressed: fail loudly rather
    // than synthesise a count, which is what made a delete of an absent row
    // report a removed row.
    let applied = |tag: &str| {
        apply_resp.ok_or_else(|| crate::Error::Internal {
            detail: format!(
                "Calvin {tag} completed with no applied response to read its affected-row \
                 count from"
            ),
        })
    };
    match plan_kind {
        PlanKind::DmlResult(verb) => {
            let resp = applied(verb)?;
            Ok(CalvinTaskOutcome::Dml(dml_outcome_from_payload(
                resp.payload.as_bytes(),
                verb,
            )?))
        }
        // The verb is in the payload; the error text only names the kind.
        PlanKind::DmlResultByOp => {
            let resp = applied("insert-or-update")?;
            Ok(CalvinTaskOutcome::Dml(dml_outcome_by_op(
                resp.payload.as_bytes(),
            )?))
        }
        PlanKind::Execution
        | PlanKind::ArraySlice
        | PlanKind::ReturningRows
        | PlanKind::SingleDocument
        | PlanKind::MultiRow => match calvin_tag_for_plan(plan) {
            Some(outcome) => Ok(CalvinTaskOutcome::Dml(outcome)),
            None => Ok(CalvinTaskOutcome::Opaque),
        },
    }
}

/// The count-bearing outcome a plan answers without a round-trip to the
/// Data Plane, or `None` when its count depends on state the plan never read.
///
/// Folding is only sound for a write that CANNOT be a no-op — one that either
/// applies exactly one row or fails the statement. Any write whose row count
/// depends on state (a point delete of an absent row, an `ON CONFLICT DO
/// NOTHING` insert onto an existing key, a batch, a predicate) gets its
/// count from the mutation's own response, because a synthesised count is a
/// claim about rows nobody looked at.
///
/// Foldable: `DocumentOp::PointPut` (`INSERT 0 1`) and `KvOp::Put`
/// (`UPSERT 1`), the two upserts that always write one row. Every other plan,
/// read or write, answers `None`.
pub fn calvin_tag_for_plan(plan: &PhysicalPlan) -> Option<DmlOutcome> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointPut { .. }) => Some(DmlOutcome {
            verb: "INSERT",
            affected: 1,
        }),
        // The SQL `UPSERT` statement: same tag as its `DocumentOp::Upsert`
        // sibling and as `describe_plan`'s `DmlResult("UPSERT")` arm.
        PhysicalPlan::Kv(KvOp::Put { .. }) => Some(DmlOutcome {
            verb: "UPSERT",
            affected: 1,
        }),
        // The foldable arms above take precedence; every remaining op of each
        // engine answers from its payload or folds opaque.
        PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    #[test]
    fn a_read_never_folds_to_a_tag() {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        });
        assert!(calvin_tag_for_plan(&plan).is_none());
    }

    #[test]
    fn an_upsert_folds_without_a_round_trip() {
        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            key: Vec::new(),
            value: Vec::new(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        });
        assert_eq!(
            calvin_tag_for_plan(&plan),
            Some(DmlOutcome {
                verb: "UPSERT",
                affected: 1
            })
        );
    }

    /// A write that can legitimately touch nothing never folds: its count
    /// is only knowable from the mutation's own response. Folding a delete
    /// let a re-delete of an already-deleted key report a removed row.
    #[test]
    fn no_op_capable_writes_never_fold() {
        let delete = PhysicalPlan::Kv(KvOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            keys: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(calvin_tag_for_plan(&delete).is_none());

        let point_delete = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            document_id: "a".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(calvin_tag_for_plan(&point_delete).is_none());
    }
}
