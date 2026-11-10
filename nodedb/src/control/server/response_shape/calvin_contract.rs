// SPDX-License-Identifier: BUSL-1.1

//! The answer of a Calvin write whose `RETURNING` plan carries a contract.
//!
//! The coordinator stamps a contract on every plan that carries a full
//! `RETURNING` list. The participant that holds the plan answers its
//! encoded `CalvinFrozenReply`: the rows its leader evaluated before the
//! vote, stored in the participant's log. They are the final typed rows.
//! The coordinator answers them as they are, makes no sequence call, gives
//! the writer's session the `nextval` results the evaluation drew, and
//! raises the rejected-lines notice of an ingest that rejected lines.

use nodedb_physical::physical_plan::PhysicalPlan;

use super::calvin_fold::CalvinFoldCtx;
use super::project::cell_keys;
use super::types::{DdlColType, ShapedRow, ShapedRows};
use crate::control::server::shared::session::statement_notice;
use crate::control::server::shared::sql::staging_predicates::rejected_lines_notice;
use crate::control::wal_replication::types::{
    CalvinFrozenReply, FrozenPlanReply, FrozenSequenceEffect,
};
use crate::data::executor::response_codec::ReturningRowsReply;

/// Whether `plan` carries a `RETURNING` contract: the coordinator stamps one
/// on every plan whose spec holds the full list.
pub(super) fn carries_contract(plan: &PhysicalPlan) -> bool {
    plan.returning_target()
        .is_some_and(|(_, spec)| spec.precommit.is_some())
}

/// The shaped rows of the answer `payload` of a contract plan, an encoded
/// `CalvinFrozenReply`. Records its `nextval` results into the session
/// `ctx` names.
pub(super) fn contract_rows(payload: &[u8], ctx: &CalvinFoldCtx<'_>) -> crate::Result<ShapedRows> {
    let reply = CalvinFrozenReply::from_bytes(payload)?;
    record_sequence_effects(&reply, ctx);
    let plan = reply
        .answered_plan()
        .ok_or_else(|| crate::Error::Internal {
            detail: format!(
                "calvin RETURNING: the frozen reply of txn ({}, {}) holds no plan",
                reply.epoch, reply.position
            ),
        })?;
    frozen_rows(plan)
}

/// Give the writer's session every `nextval` result of `reply`'s plans, in
/// plan order and invocation order, so its `currval` is the last of them.
fn record_sequence_effects(reply: &CalvinFrozenReply, ctx: &CalvinFoldCtx<'_>) {
    let Some(sequences) = ctx.sequences else {
        return;
    };
    for plan in &reply.plans {
        for effect in &plan.sequence_effects {
            match effect {
                FrozenSequenceEffect::Nextval { name, value } => {
                    sequences.record_obtained(name, *value);
                }
                // A `setval` sets no session value.
                FrozenSequenceEffect::Setval { .. } => {}
            }
        }
    }
}

/// The shaped rows of `plan`'s frozen payload, under its column types. A
/// row set has no place for a rejected line, so the lines the plan rejected
/// reach the client as the rejected-lines notice.
fn frozen_rows(plan: &FrozenPlanReply) -> crate::Result<ShapedRows> {
    let decoded: ReturningRowsReply =
        zerompk::from_msgpack(&plan.payload).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!(
                "calvin RETURNING: the frozen rows of plan {} do not decode: {e}",
                plan.plan_index
            ),
        })?;
    let column_types = plan
        .column_types
        .iter()
        .map(|name| {
            DdlColType::from_wire_name(name).ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "calvin RETURNING: plan {} names the unknown column type '{name}'",
                    plan.plan_index
                ),
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;
    if column_types.len() != decoded.columns.len() {
        return Err(crate::Error::Internal {
            detail: format!(
                "calvin RETURNING: plan {} has {} columns and {} column types",
                plan.plan_index,
                decoded.columns.len(),
                column_types.len()
            ),
        });
    }
    if let Some(rejection) = plan
        .rejected
        .as_ref()
        .filter(|rejection| rejection.lines > 0)
    {
        statement_notice::raise(rejected_lines_notice(
            &rejection.collection,
            rejection.lines,
        ));
    }
    let keys = cell_keys(&decoded.columns);
    let rows = decoded
        .rows
        .into_iter()
        .map(|cells| {
            keys.iter()
                .cloned()
                .zip(cells.into_iter().map(|cell| cell.0))
                .collect::<ShapedRow>()
        })
        .collect();
    Ok(ShapedRows::from_rows(decoded.columns, column_types, rows))
}

#[cfg(test)]
mod tests {
    use nodedb_types::{NativeCell, Value};

    use super::*;
    use crate::data::executor::response_codec::RowsPayload;

    fn plan(rows: Vec<Vec<Value>>) -> FrozenPlanReply {
        FrozenPlanReply {
            plan_index: 0,
            payload: zerompk::to_msgpack_vec(&RowsPayload {
                columns: vec!["id".into(), "n".into()],
                rows: rows
                    .into_iter()
                    .map(|row| row.into_iter().map(NativeCell).collect())
                    .collect(),
            })
            .expect("encode rows"),
            column_types: vec!["text".into(), "int8".into()],
            sequence_effects: Vec::new(),
            rejected: None,
        }
    }

    /// The frozen cells answer as they are, under their frozen types.
    #[test]
    fn frozen_rows_keep_their_cells_and_types() {
        let shaped = frozen_rows(&plan(vec![
            vec![Value::String("a".into()), Value::Integer(1)],
            vec![Value::String("b".into()), Value::Integer(2)],
        ]))
        .expect("shape");
        assert_eq!(shaped.columns, ["id", "n"]);
        assert_eq!(shaped.column_types, [DdlColType::Text, DdlColType::Int8]);
        let keys = shaped.cell_keys();
        assert_eq!(shaped.rows[1][&keys[1]], Value::Integer(2));
    }

    /// A frozen ingest reply that rejected lines raises the rejected-lines
    /// notice beside its rows. One that rejected none raises nothing.
    #[tokio::test]
    async fn frozen_rejected_lines_raise_the_notice() {
        use crate::control::server::shared::session::conn_scope;
        use crate::control::wal_replication::types::FrozenRejection;
        conn_scope::scoped(async {
            let mut rejecting = plan(vec![vec![Value::String("a".into()), Value::Integer(1)]]);
            rejecting.rejected = Some(FrozenRejection {
                collection: "metrics".into(),
                lines: 2,
            });
            let shaped = frozen_rows(&rejecting).expect("shape");
            assert_eq!(shaped.rows.len(), 1);
            let notices = statement_notice::take();
            assert_eq!(notices.len(), 1);
            assert!(
                notices[0].contains("2 line(s)") && notices[0].contains("metrics"),
                "{notices:?}"
            );

            frozen_rows(&plan(Vec::new())).expect("shape");
            assert!(statement_notice::take().is_empty());
        })
        .await;
    }

    #[test]
    fn a_type_list_that_does_not_fit_the_columns_fails() {
        let mut short = plan(Vec::new());
        short.column_types.pop();
        assert!(frozen_rows(&short).is_err());
    }
}
