// SPDX-License-Identifier: BUSL-1.1

//! Fold a completed Calvin batch into the statement's ONE result set and ONE
//! command tag. Protocol-neutral: pgwire and native both render this fold.
//!
//! Every task reads the one applied answer the coordinator assembled for the
//! whole transaction. Every `RETURNING` plan carries a contract, so that
//! answer holds the rows each participant froze before it voted, the union
//! of every plan's rows in statement order. The first `RETURNING` task
//! answers them as they are, one result set per plan, with no sequence call.
//! Later `RETURNING` tasks answer nothing more. Every other task folds its
//! count into the tag.

use nodedb_physical::physical_plan::PhysicalPlan;

use super::calvin_contract::contract_rows;
pub use super::calvin_tag::calvin_tag_for_plan;
use super::types::{
    DmlFoldError, DmlOutcome, FoldedTag, PlanKind, ShapedRows, StatementTag, TaskTagRole,
    describe_plan, dml_outcome_by_op, dml_outcome_from_payload, plan_count_reads_reply,
};
use crate::bridge::envelope::Response;
use crate::control::sequence::SequenceRecorder;
use crate::control::server::shared::returning::plan_carries_returning_contract;

/// Shared inputs for shaping one completed Calvin batch.
pub struct CalvinFoldCtx<'a> {
    /// The writer's session. It takes the `nextval` and `setval` results the
    /// frozen answer carries, so its `currval` matches the direct path. The
    /// fold draws no sequence value.
    pub session: &'a (dyn SequenceRecorder + Sync),
}

/// One Calvin task's contribution to the statement's response.
pub enum CalvinTaskOutcome {
    /// The RETURNING rows of every plan of the batch, one result set per
    /// plan, in plan order.
    Rows(Vec<ShapedRows>),
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
    /// A task's payload cannot be read as its plan kind requires.
    Shape(crate::Error),
}

/// The statement's answer: its rows, when a task carried RETURNING, and its tag.
pub struct CalvinBatchFold {
    /// One result set per RETURNING plan, in plan order, as the per-task
    /// path answers one per task. Empty when no task carried RETURNING.
    pub rows: Vec<ShapedRows>,
    pub tag: Option<FoldedTag>,
}

impl CalvinBatchFold {
    /// Every result set folded into one, as a protocol that answers one
    /// result set per statement folds its tasks' rows. `None` with no rows.
    pub fn one_result_set(rows: Vec<ShapedRows>) -> Option<ShapedRows> {
        let mut sets = rows.into_iter();
        let mut first = sets.next()?;
        for set in sets {
            first.append(set);
        }
        Some(first)
    }
}

/// Fold every task of a completed batch, in dispatch order.
///
/// A derived implicit-edge write beside the user's own never answers the
/// statement: it folds as opaque, as it never deposits. Every RETURNING task
/// reads the one answer, which holds the rows of every RETURNING plan, so
/// the first one answers them all. A RETURNING task folds nothing into the
/// tag.
///
/// Every count-bearing task reads the one deposited answer, which holds the
/// total of the whole batch: each slice adds its plans' counts, and the
/// coordinator adds its slices'. So the first such task folds the total, and
/// each later one folds its verb alone. Rows answer in place of a count, so a
/// plain task beside a RETURNING one folds as opaque.
pub fn fold_calvin_batch(
    plans: &[&PhysicalPlan],
    apply_resp: Option<&Response>,
    ctx: &CalvinFoldCtx<'_>,
) -> Result<CalvinBatchFold, CalvinFoldError> {
    let mut rows: Option<Vec<ShapedRows>> = None;
    let mut tag = StatementTag::for_request(plans.iter().copied());
    let rows_answer = apply_resp.is_some()
        && plans
            .iter()
            .any(|plan| matches!(describe_plan(plan), PlanKind::ReturningRows));
    let mut total_read = false;
    for plan in plans {
        let role = tag.role_of(plan);
        if role == TaskTagRole::Opaque {
            tag.fold_opaque();
            continue;
        }
        let reads_total = plan_count_reads_reply(plan);
        if rows_answer && reads_total {
            tag.fold_opaque();
            continue;
        }
        // The first RETURNING task answered every plan's rows.
        let returns_rows =
            apply_resp.is_some() && matches!(describe_plan(plan), PlanKind::ReturningRows);
        if returns_rows && rows.is_some() {
            continue;
        }
        // A later task folds no count, so it raises no second notice for the
        // lines the answer rejected.
        if let (PlanKind::DmlResult(verb), true, true) =
            (describe_plan(plan), reads_total, total_read)
        {
            tag.fold(role, DmlOutcome { verb, affected: 0 })
                .map_err(CalvinFoldError::Verb)?;
            continue;
        }
        match calvin_task_outcome(plan, apply_resp, ctx).map_err(CalvinFoldError::Shape)? {
            CalvinTaskOutcome::Rows(shaped) => {
                rows.get_or_insert(shaped);
            }
            CalvinTaskOutcome::Dml(outcome) => {
                let outcome = if reads_total && total_read {
                    DmlOutcome {
                        affected: 0,
                        ..outcome
                    }
                } else {
                    outcome
                };
                total_read |= reads_total;
                tag.fold(role, outcome).map_err(CalvinFoldError::Verb)?
            }
            CalvinTaskOutcome::Opaque => tag.fold_opaque(),
        }
    }
    Ok(CalvinBatchFold {
        rows: rows.unwrap_or_default(),
        tag: tag.finish(),
    })
}

/// Shape one task of a completed batch.
///
/// A plan carrying RETURNING yields the statement's rows as
/// protocol-neutral [`ShapedRows`]: the frozen rows of every RETURNING plan
/// of the batch, one result set per plan. A RETURNING payload that fails to
/// read fails the statement with its own error class. Every other task
/// contributes to the statement's one command tag.
pub fn calvin_task_outcome(
    plan: &PhysicalPlan,
    apply_resp: Option<&Response>,
    ctx: &CalvinFoldCtx<'_>,
) -> crate::Result<CalvinTaskOutcome> {
    let plan_kind = describe_plan(plan);
    if let (PlanKind::ReturningRows, Some(resp)) = (plan_kind, apply_resp) {
        // Every participant froze its rows under the contract (see
        // `super::calvin_contract`). A plan with no contract has no frozen
        // rows, and evaluating its list here draws its sequences again.
        if !plan_carries_returning_contract(plan) {
            return Err(crate::Error::Internal {
                detail: "calvin RETURNING: a plan carries no RETURNING contract, so no \
                         participant froze its rows"
                    .into(),
            });
        }
        return contract_rows(Some(plan), resp.payload.as_bytes(), ctx)
            .map(CalvinTaskOutcome::Rows);
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
        | PlanKind::GraphRag
        | PlanKind::ReturningRows
        | PlanKind::SingleDocument
        | PlanKind::MultiRow => match calvin_tag_for_plan(plan) {
            Some(outcome) => Ok(CalvinTaskOutcome::Dml(outcome)),
            None => Ok(CalvinTaskOutcome::Opaque),
        },
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, KvOp};

    use super::*;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    #[test]
    fn a_read_never_folds_to_a_tag() {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
            system_time: nodedb_types::SystemTimeScope::Current,
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
            surrogate: nodedb_types::Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
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
            provenance: None,
        });
        assert!(calvin_tag_for_plan(&delete).is_none());

        let point_delete = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            document_id: "a".into(),
            surrogate: None,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(calvin_tag_for_plan(&point_delete).is_none());
    }

    /// Counts every `nextval` call, and keeps every value recorded into the
    /// session.
    #[derive(Default)]
    struct CountingAccess {
        nextvals: std::sync::atomic::AtomicI64,
        recorded: std::sync::Mutex<Vec<(String, i64)>>,
    }

    impl crate::control::sequence::SequenceAccess for CountingAccess {
        fn nextval(&self, _name: &str) -> crate::Result<i64> {
            Ok(self
                .nextvals
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1)
        }
        fn currval(&self, _name: &str) -> crate::Result<i64> {
            Ok(self.nextvals.load(std::sync::atomic::Ordering::SeqCst))
        }
        fn setval(&self, _name: &str, value: i64) -> crate::Result<i64> {
            Ok(value)
        }
    }

    impl SequenceRecorder for CountingAccess {
        fn record_obtained(&self, name: &str, value: i64) {
            self.recorded
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((name.to_owned(), value));
        }
        fn obtained(&self, name: &str) -> Option<i64> {
            self.recorded
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .iter()
                .rev()
                .find(|(recorded, _)| recorded == name)
                .map(|(_, value)| *value)
        }
    }

    /// A `RETURNING *` insert of `id`. `contract` stamps the full list the
    /// coordinator fixes a contract from.
    fn returning_insert(id: &str, contract: bool) -> PhysicalPlan {
        use nodedb_physical::physical_plan::{ReturningColumns, ReturningSpec};
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "items"),
            document_id: id.into(),
            value: Vec::new(),
            if_absent: false,
            surrogate: nodedb_types::Surrogate::new(1),
            returning: Some(ReturningSpec {
                columns: ReturningColumns::Star,
                precommit: contract.then_some(ReturningColumns::Star),
            }),
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        })
    }

    /// The frozen plan `plan_index` answering `RETURNING id, n` over one row.
    fn frozen_plan(
        plan_index: u32,
        id: &str,
        n: i64,
        sequence_effects: Vec<crate::control::wal_replication::types::FrozenSequenceEffect>,
    ) -> crate::control::wal_replication::types::FrozenPlanReply {
        use crate::data::executor::response_codec::RowsPayload;
        use nodedb_types::{NativeCell, Value};
        crate::control::wal_replication::types::FrozenPlanReply {
            plan_index,
            payload: zerompk::to_msgpack_vec(&RowsPayload {
                columns: vec!["id".into(), "n".into()],
                rows: vec![vec![
                    NativeCell(Value::String(id.into())),
                    NativeCell(Value::Integer(n)),
                ]],
            })
            .expect("encode rows"),
            column_types: vec!["text".into(), "int8".into()],
            sequence_effects,
            rejected: None,
        }
    }

    fn response(payload: Vec<u8>) -> Response {
        Response {
            request_id: crate::types::RequestId::new(0),
            status: crate::bridge::envelope::Status::Ok,
            attempt: 1,
            partial: false,
            payload: crate::bridge::envelope::Payload::from_vec(payload),
            watermark_lsn: crate::types::Lsn::ZERO,
            error_code: None,
            stage_vote: None,
            read_vshards: crate::types::ReadShards::new(),
            write_set: Vec::new(),
        }
    }

    /// Two RETURNING tasks over one frozen union answer every plan's rows
    /// once, one result set per plan, in plan order, as they are. The fold draws no sequence value:
    /// the session takes the `nextval` and `setval` results the participants
    /// froze, in plan order.
    #[test]
    fn frozen_rows_fold_without_sequence_access() {
        use crate::control::wal_replication::types::{CalvinFrozenReply, FrozenSequenceEffect};
        use nodedb_types::Value;

        let union = CalvinFrozenReply {
            epoch: 3,
            position: 1,
            vshard: 0,
            plans: vec![
                frozen_plan(
                    0,
                    "a",
                    7,
                    vec![FrozenSequenceEffect::Nextval {
                        name: "s".into(),
                        value: 7,
                    }],
                ),
                frozen_plan(
                    1,
                    "b",
                    40,
                    vec![FrozenSequenceEffect::Setval {
                        name: "s".into(),
                        value: 40,
                    }],
                ),
            ],
        };
        let resp = response(union.to_bytes().expect("encode union"));
        let access = CountingAccess::default();
        let ctx = CalvinFoldCtx { session: &access };
        let (first, second) = (returning_insert("a", true), returning_insert("b", true));

        let fold = fold_calvin_batch(&[&first, &second], Some(&resp), &ctx)
            .unwrap_or_else(|e| panic!("fold: {e:?}"));

        assert_eq!(
            access.nextvals.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the fold draws no sequence value"
        );
        assert_eq!(fold.rows.len(), 2, "one result set per plan");
        let rows = CalvinBatchFold::one_result_set(fold.rows).expect("RETURNING rows");
        assert_eq!(rows.columns, ["id", "n"]);
        let keys = rows.cell_keys();
        let cells: Vec<(Value, Value)> = rows
            .rows
            .iter()
            .map(|row| (row[&keys[0]].clone(), row[&keys[1]].clone()))
            .collect();
        assert_eq!(
            cells,
            vec![
                (Value::String("a".into()), Value::Integer(7)),
                (Value::String("b".into()), Value::Integer(40)),
            ]
        );
        assert_eq!(
            *access.recorded.lock().unwrap_or_else(|p| p.into_inner()),
            vec![("s".to_owned(), 7), ("s".to_owned(), 40)],
            "currval ends at the setval value"
        );
    }

    /// A RETURNING plan with no contract has no frozen rows, and the fold
    /// refuses to evaluate its list.
    #[test]
    fn a_returning_plan_without_a_contract_fails() {
        let access = CountingAccess::default();
        let ctx = CalvinFoldCtx { session: &access };
        let plan = returning_insert("a", false);
        assert!(matches!(
            fold_calvin_batch(&[&plan], Some(&response(vec![0x90])), &ctx),
            Err(CalvinFoldError::Shape(crate::Error::Internal { .. }))
        ));
        assert_eq!(access.nextvals.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}
