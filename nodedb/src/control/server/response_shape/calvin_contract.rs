// SPDX-License-Identifier: BUSL-1.1

//! The answer of a Calvin write whose `RETURNING` plans carry a contract.
//!
//! The coordinator stamps a contract on every plan that carries a full
//! `RETURNING` list. Each participant that holds such a plan answers its
//! encoded `CalvinFrozenReply`: the rows its leader evaluated before the
//! vote, stored in the participant's log. They are the final typed rows.
//! The coordinator's answer is the union of every participant's reply, in
//! statement order. It answers the rows as they are and makes no sequence
//! call: each participant evaluated its plans after the sequence changes of
//! every earlier plan they read. It gives the writer's session the
//! `nextval` and `setval` results the evaluation drew, and raises the
//! rejected-lines notice of an ingest that rejected lines.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_types::{ReturningColumns, ReturningItem};

use super::calvin_fold::CalvinFoldCtx;
use super::project::cell_keys;
use super::types::{DdlColType, ShapedRow, ShapedRows};
use crate::control::sequence::SequenceRecorder;
use crate::control::server::shared::session::statement_notice;
use crate::control::server::shared::sql::staging_predicates::rejected_lines_notice;
use crate::control::wal_replication::types::{CalvinFrozenReply, FrozenPlanReply};
use crate::data::executor::response_codec::ReturningRowsReply;

/// The shaped rows of the answer `payload` of a contract statement, an
/// encoded `CalvinFrozenReply`: one result set per plan, in plan order, as
/// the per-task path answers one per task. A plan that spans vShards folds
/// each one's rows into its set. Each participant held its plans to their
/// share of the statement's limits before it voted, so the whole answer is
/// within them. Records the reply's sequence results into the session `ctx`
/// names, even when the answer fails: the participants drew them.
///
/// A reply with no plan answers no row: no participant evaluated a plan.
/// It answers one empty set under the names `plan`'s list gives, and under
/// none with no `plan`.
pub(super) fn contract_rows(
    plan: Option<&PhysicalPlan>,
    payload: &[u8],
    ctx: &CalvinFoldCtx<'_>,
) -> crate::Result<Vec<ShapedRows>> {
    let reply = CalvinFrozenReply::from_bytes(payload)?;
    let sets = reply_sets(&reply);
    record_sequence_effects(&reply, ctx.session);
    let sets = sets?;
    if sets.is_empty() {
        return Ok(vec![no_rows(plan)]);
    }
    Ok(sets)
}

/// One result set per plan index of `reply`, in statement order. A plan
/// that spans vShards answers each one's rows in its one set.
fn reply_sets(reply: &CalvinFrozenReply) -> crate::Result<Vec<ShapedRows>> {
    let mut sets: Vec<(u32, ShapedRows)> = Vec::with_capacity(reply.plans.len());
    for plan in &reply.plans {
        let rows = frozen_rows(plan)?;
        match sets.last_mut() {
            Some((index, held)) if *index == plan.plan_index => held.append(rows),
            _ => sets.push((plan.plan_index, rows)),
        }
    }
    Ok(sets.into_iter().map(|(_, rows)| rows).collect())
}

/// The empty result set of `plan`'s list: one column per named item. A star
/// names its columns only from rows, so it adds none.
fn no_rows(plan: Option<&PhysicalPlan>) -> ShapedRows {
    let columns: Vec<String> = plan
        .and_then(PhysicalPlan::returning_target)
        .and_then(|(_, spec)| spec.precommit.as_ref())
        .map(|columns| match columns {
            ReturningColumns::Named(items) => items
                .iter()
                .filter(|item| !matches!(item, ReturningItem::Star))
                .map(|item| item.output_name().to_owned())
                .collect(),
            ReturningColumns::Star => Vec::new(),
        })
        .unwrap_or_default();
    let types = ShapedRows::text_types(columns.len());
    ShapedRows::from_rows(columns, types, Vec::new())
}

/// Give `session` every sequence result of `reply`'s plans, in plan order
/// and invocation order, so its `currval` is the last of them. A `nextval`
/// sets `currval` to its result. A two-argument `setval` marks its value
/// called, so it sets `currval` to that value, as in PostgreSQL.
fn record_sequence_effects(reply: &CalvinFrozenReply, session: &dyn SequenceRecorder) {
    for plan in &reply.plans {
        for effect in &plan.sequence_effects {
            let (name, value) = effect.name_value();
            session.record_obtained(name, value);
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
    use crate::control::wal_replication::types::FrozenSequenceEffect;
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

    fn ctx(session: &(dyn SequenceRecorder + Sync)) -> CalvinFoldCtx<'_> {
        CalvinFoldCtx { session }
    }

    /// A `RETURNING id, n` insert with a contract.
    fn star_insert() -> PhysicalPlan {
        use nodedb_physical::physical_plan::{DocumentOp, ReturningSpec};
        use nodedb_types::{DatabaseId, QualifiedCollection};
        let named = ReturningColumns::Named(vec![
            ReturningItem::Column {
                name: "id".into(),
                alias: None,
            },
            ReturningItem::Column {
                name: "n".into(),
                alias: None,
            },
        ]);
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            document_id: "a".into(),
            value: Vec::new(),
            if_absent: false,
            surrogate: nodedb_types::Surrogate::new(1),
            returning: Some(ReturningSpec {
                columns: named.clone(),
                precommit: Some(named),
            }),
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        })
    }

    /// A reply with no plan answers one empty set under the list's names.
    #[test]
    fn no_plan_answers_no_row() {
        let none = CalvinFrozenReply {
            epoch: 1,
            position: 0,
            vshard: 0,
            plans: Vec::new(),
        };
        let sets = contract_rows(
            Some(&star_insert()),
            &none.to_bytes().expect("encode"),
            &ctx(&Recorder),
        )
        .expect("no row");
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].columns, ["id", "n"]);
        assert!(sets[0].rows.is_empty());
    }

    /// A recorder with no session.
    struct Recorder;

    impl SequenceRecorder for Recorder {
        fn record_obtained(&self, _name: &str, _value: i64) {}
        fn obtained(&self, _name: &str) -> Option<i64> {
            None
        }
    }

    /// A union answers one result set per plan index. A plan that spans
    /// vShards answers each one's rows in its one set.
    #[test]
    fn a_union_answers_one_set_per_plan() {
        #[derive(Default)]
        struct Recorded(std::sync::Mutex<Vec<(String, i64)>>);
        impl SequenceRecorder for Recorded {
            fn record_obtained(&self, name: &str, value: i64) {
                self.0
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push((name.to_owned(), value));
            }
            fn obtained(&self, _name: &str) -> Option<i64> {
                None
            }
        }
        let at = |index: u32, id: &str| FrozenPlanReply {
            plan_index: index,
            ..plan(vec![vec![Value::String(id.into()), Value::Integer(1)]])
        };
        let mut setval = at(1, "c");
        setval.sequence_effects = vec![FrozenSequenceEffect::Setval {
            name: "s".into(),
            value: 9,
        }];
        let reply = CalvinFrozenReply {
            epoch: 1,
            position: 0,
            vshard: 0,
            plans: vec![at(0, "a"), at(0, "b"), setval],
        };
        let recorded = Recorded::default();
        let sets = contract_rows(
            Some(&star_insert()),
            &reply.to_bytes().expect("encode"),
            &ctx(&recorded),
        )
        .expect("rows");
        let ids: Vec<Vec<Value>> = sets
            .iter()
            .map(|set| {
                let key = &set.cell_keys()[0];
                set.rows.iter().map(|row| row[key].clone()).collect()
            })
            .collect();
        assert_eq!(
            ids,
            vec![
                vec![Value::String("a".into()), Value::String("b".into())],
                vec![Value::String("c".into())],
            ]
        );
        assert_eq!(
            *recorded.0.lock().unwrap_or_else(|p| p.into_inner()),
            vec![("s".to_owned(), 9)]
        );
    }

    #[test]
    fn a_type_list_that_does_not_fit_the_columns_fails() {
        let mut short = plan(Vec::new());
        short.column_types.pop();
        assert!(frozen_rows(&short).is_err());
    }
}
