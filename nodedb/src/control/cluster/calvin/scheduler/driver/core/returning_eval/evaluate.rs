// SPDX-License-Identifier: BUSL-1.1

//! Evaluate the `RETURNING` lists of a staged slice before its leader votes.
//!
//! Every input comes from the txn's contract and the stage's images, so the
//! evaluation reads nothing the coordinator did not fix. Sequence calls go
//! through the txn's transcripts: a later evaluation of the same txn replays
//! the first one's results.

use std::collections::BTreeMap;
use std::sync::Arc;

use nodedb_cluster::calvin::types::{AnnouncedColumn, TxClass};

use crate::bridge::envelope::ErrorCode;
use crate::control::cluster::calvin::scheduler::driver::returning_transcripts::TxnTranscripts;
use crate::control::sequence::{SessionSequenceAccess, SessionSequenceValues};
use crate::control::server::response_shape::schema::{OutputColumn, OutputSchema};
use crate::control::server::response_shape::types::DdlColType;
use crate::control::server::shared::returning::precommit::{
    FrozenReturningReply, PrecommitReturning, PreparedReturningRow, ReturningLimits,
    finalize_returning,
};
use crate::control::state::SharedState;

/// The inputs of one evaluation. Every field is owned, so the evaluation
/// runs off the scheduler loop.
pub(super) struct ReturningJob {
    pub shared: Arc<SharedState>,
    /// The txn's class, which holds its contract.
    pub tx_class: Arc<TxClass>,
    /// The images of each local plan the contract names, keyed by the
    /// plan's index in the class's plan batch.
    pub plans: BTreeMap<u32, Vec<PreparedReturningRow>>,
    pub transcripts: Arc<TxnTranscripts>,
}

/// Why an evaluation refuses its slice: the code the abort vote carries,
/// and the text the leader keeps as the stage error. The code is boxed so
/// the refusal stays small as the error of a `Result`.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ReturningRefusal {
    pub code: Box<ErrorCode>,
    pub detail: String,
}

impl ReturningRefusal {
    /// A refusal for a fault of the slice itself, not of the evaluation.
    pub(super) fn internal(detail: String) -> Self {
        Self {
            code: Box::new(ErrorCode::Internal {
                detail: detail.clone(),
            }),
            detail,
        }
    }

    /// The refusal of `error`, raised by the list of the plan at `index`.
    /// The code keeps the error's SQLSTATE.
    fn of_plan(index: u32, error: crate::Error) -> Self {
        let detail = format!("RETURNING of plan {index}: {error}");
        Self {
            code: Box::new(ErrorCode::from(error)),
            detail,
        }
    }
}

/// The frozen reply of each evaluated plan, keyed by plan index, or the
/// refusal of the first plan that failed.
pub(super) type ReturningOutcome = Result<BTreeMap<u32, FrozenReturningReply>, ReturningRefusal>;

/// Evaluate every plan of `job` in contract order.
///
/// The plans share one statement-local session, seeded with the `currval`
/// snapshot the contract carries. The evaluation holds the txn's
/// transcripts for its whole run. A `nextval` can wait on a range
/// reservation, so this runs on a blocking thread.
pub(super) fn evaluate(job: ReturningJob) -> ReturningOutcome {
    let ReturningJob {
        shared,
        tx_class,
        mut plans,
        transcripts,
    } = job;
    let Some(contract) = tx_class.returning.as_ref() else {
        return Ok(BTreeMap::new());
    };
    if let Some(index) = plans
        .keys()
        .find(|index| !contract.plans.iter().any(|plan| plan.plan_index == **index))
    {
        return Err(ReturningRefusal::internal(format!(
            "calvin RETURNING: plan {index} has images but no contract entry"
        )));
    }
    let _evaluation = transcripts.evaluation();
    let database_id = tx_class.database_id;
    let tenant_id = tx_class.tenant_id;
    let session = Arc::new(SessionSequenceValues::new());
    for value in &contract.sequence_values {
        session.record(
            database_id.as_u64(),
            tenant_id.as_u64(),
            &value.name,
            value.value,
        );
    }
    let sequences = SessionSequenceAccess::new(
        &shared.sequence_registry,
        Some(session),
        database_id,
        tenant_id,
    );
    let crs_set = crate::control::crs_context::crs_set_for(&shared);
    let mut frozen = BTreeMap::new();
    for plan in &contract.plans {
        let Some(rows) = plans.remove(&plan.plan_index) else {
            continue;
        };
        let redaction =
            shared
                .redaction
                .snapshot_rules(tenant_id, &plan.collection, &contract.roles);
        let announced = plan
            .announced
            .as_deref()
            .map(|columns| announced_schema(plan.plan_index, columns))
            .transpose()?;
        let request = PrecommitReturning {
            spec: &plan.spec,
            source_columns: &plan.source_columns,
            redaction: &redaction,
            sequences: &sequences,
            projection: announced.as_ref(),
            crs_set: crs_set.as_ref(),
            limits: ReturningLimits::from_contract(&plan.limits),
        };
        let transcript = transcripts.plan(plan.plan_index);
        let reply = finalize_returning(&request, rows, &transcript)
            .map_err(|error| ReturningRefusal::of_plan(plan.plan_index, error))?;
        frozen.insert(plan.plan_index, reply);
    }
    Ok(frozen)
}

/// The output schema `columns`, the columns plan `plan_index`'s statement
/// announced. The evaluation answers each cell under its column's name and
/// type, as the coordinator's shaping of the same statement does.
fn announced_schema(
    plan_index: u32,
    columns: &[AnnouncedColumn],
) -> Result<OutputSchema, ReturningRefusal> {
    let columns = columns
        .iter()
        .map(|column| {
            let ty = DdlColType::from_wire_name(&column.ty).ok_or_else(|| {
                ReturningRefusal::internal(format!(
                    "calvin RETURNING: plan {plan_index} announces column '{}' of the unknown \
                     type '{}'",
                    column.name, column.ty
                ))
            })?;
            Ok(OutputColumn {
                display_name: column.name.clone(),
                lookup_key: column.name.clone(),
                ty,
            })
        })
        .collect::<Result<Vec<_>, ReturningRefusal>>()?;
    Ok(OutputSchema {
        columns,
        is_star: false,
        cp_computed: Vec::new(),
        declared_key: None,
    })
}

#[cfg(test)]
mod tests {
    use nodedb_types::Value;

    use super::super::fixtures::{
        contract, create_sequence, division_by_zero, image_rows, nextval, sequence_next, txn_class,
    };
    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::build_test_scheduler;

    fn job(
        shared: &Arc<SharedState>,
        tx_class: Arc<TxClass>,
        rows: usize,
        transcripts: &Arc<TxnTranscripts>,
    ) -> ReturningJob {
        ReturningJob {
            shared: Arc::clone(shared),
            tx_class,
            plans: BTreeMap::from([(0, image_rows(rows))]),
            transcripts: Arc::clone(transcripts),
        }
    }

    #[tokio::test]
    async fn a_failing_expression_refuses_with_its_sqlstate_code() {
        let (scheduler, _dir) = build_test_scheduler(7);
        let tx_class = txn_class(contract(vec![division_by_zero()], 10));
        let outcome = evaluate(job(&scheduler.shared, tx_class, 1, &Arc::default()));
        let refusal = outcome.expect_err("10/0 refuses");
        assert_eq!(*refusal.code, ErrorCode::DivisionByZero);
    }

    #[tokio::test]
    async fn rows_past_the_contract_limit_refuse_the_slice() {
        let (scheduler, _dir) = build_test_scheduler(7);
        let tx_class = txn_class(contract(vec![nextval("s")], 1));
        create_sequence(&scheduler.shared, "s");
        let outcome = evaluate(job(&scheduler.shared, tx_class, 2, &Arc::default()));
        let refusal = outcome.expect_err("two rows exceed a limit of one");
        assert!(
            matches!(
                refusal.code.as_ref(),
                ErrorCode::ExecutionLimitExceeded { detail } if detail == "RETURNING rows 2 exceeds limit 1"
            ),
            "{refusal:?}"
        );
        assert_eq!(
            sequence_next(&scheduler.shared, "s"),
            1,
            "the limit refuses before any sequence call"
        );
    }

    #[tokio::test]
    async fn a_second_evaluation_replays_without_a_new_nextval() {
        let (scheduler, _dir) = build_test_scheduler(7);
        create_sequence(&scheduler.shared, "s");
        let tx_class = txn_class(contract(vec![nextval("s")], 10));
        let transcripts = Arc::default();
        let first = evaluate(job(
            &scheduler.shared,
            Arc::clone(&tx_class),
            2,
            &transcripts,
        ))
        .expect("first evaluation");
        let replay = evaluate(job(&scheduler.shared, tx_class, 2, &transcripts)).expect("replay");
        assert_eq!(first[&0].payload, replay[&0].payload);
        assert_eq!(
            first[&0]
                .shaped
                .rows
                .iter()
                .map(|row| row["n"].clone())
                .collect::<Vec<_>>(),
            vec![Value::Integer(1), Value::Integer(2)]
        );
        assert_eq!(first[&0].sequence_effects.len(), 2);
        assert_eq!(replay[&0].sequence_effects, first[&0].sequence_effects);
        assert_eq!(sequence_next(&scheduler.shared, "s"), 3, "two calls in all");
    }

    /// The announced columns name and type the frozen cells.
    #[tokio::test]
    async fn announced_columns_name_and_type_the_frozen_cells() {
        let (scheduler, _dir) = build_test_scheduler(7);
        create_sequence(&scheduler.shared, "s");
        let mut announced = contract(vec![nextval("s")], 10);
        announced.plans[0].announced = Some(vec![AnnouncedColumn {
            name: "next_id".into(),
            ty: "int8".into(),
        }]);
        let frozen = evaluate(job(
            &scheduler.shared,
            txn_class(announced),
            1,
            &Arc::default(),
        ))
        .expect("evaluation");
        let shaped = &frozen[&0].shaped;
        assert_eq!(shaped.columns, ["next_id"]);
        assert_eq!(shaped.column_types, [DdlColType::Int8]);
        assert_eq!(shaped.rows[0]["next_id"], Value::Integer(1));

        let mut unknown = contract(vec![nextval("s")], 10);
        unknown.plans[0].announced = Some(vec![AnnouncedColumn {
            name: "n".into(),
            ty: "int128".into(),
        }]);
        let refusal = evaluate(job(
            &scheduler.shared,
            txn_class(unknown),
            1,
            &Arc::default(),
        ))
        .expect_err("an unknown type refuses");
        assert!(matches!(*refusal.code, ErrorCode::Internal { .. }));
    }

    #[tokio::test]
    async fn images_with_no_contract_entry_refuse_as_internal() {
        let (scheduler, _dir) = build_test_scheduler(7);
        let tx_class = txn_class(contract(vec![division_by_zero()], 10));
        let mut job = job(&scheduler.shared, tx_class, 0, &Arc::default());
        job.plans = BTreeMap::from([(4, image_rows(1))]);
        let refusal = evaluate(job).expect_err("plan 4 has no entry");
        assert!(matches!(*refusal.code, ErrorCode::Internal { .. }));
    }
}
