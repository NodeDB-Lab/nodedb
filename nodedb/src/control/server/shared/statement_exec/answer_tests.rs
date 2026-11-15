// SPDX-License-Identifier: BUSL-1.1

use nodedb_physical::physical_plan::DocumentOp;
use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

use super::*;

/// The verb a tag of several verbs answers.
const MIXED_VERB: &str = "MIXED";

fn insert_into(collection: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
        document_id: "x".to_owned(),
        value: Vec::new(),
        if_absent: false,
        surrogate: Surrogate::new(1),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
    })
}

fn delete_from(collection: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointDelete {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
        document_id: "y".into(),
        surrogate: None,
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        resolved_sum_targets: Vec::new(),
    })
}

fn outcome(verb: &'static str, affected: u64) -> DmlOutcome {
    DmlOutcome { verb, affected }
}

fn map_of(statements: &[u32]) -> StatementMap {
    let mut map = StatementMap::default();
    for &statement in statements {
        map.push(statement);
    }
    map
}

fn request_fold(plans: &[PhysicalPlan], map: &StatementMap, count: usize) -> RequestFold {
    match RequestFold::for_plans(plans, map, count) {
        Ok(fold) => fold,
        Err(error) => panic!(
            "every task has a statement: {}",
            error.into_error(crate::types::TenantId::new(1))
        ),
    }
}

/// `INSERT a; DELETE b`, one row each, folded per statement.
fn insert_then_delete() -> RequestAnswer {
    let plans = [insert_into("a"), delete_from("b")];
    let mut fold = request_fold(&plans, &map_of(&[0, 1]), 2);
    for (task, (plan, verb)) in plans.iter().zip(["INSERT", "DELETE"]).enumerate() {
        let folded = fold
            .fold_of(task)
            .and_then(|statement| statement.fold_dml(plan, outcome(verb, 1)));
        assert!(
            folded.is_ok(),
            "statement {task} must fold its {verb} alone"
        );
    }
    fold.finish()
}

#[test]
fn each_statement_folds_its_own_verb() {
    let tags: Vec<_> = insert_then_delete()
        .statements
        .into_iter()
        .map(|statement| statement.tag)
        .collect();
    assert_eq!(
        tags,
        [
            Some(FoldedTag::Dml(outcome("INSERT", 1))),
            Some(FoldedTag::Dml(outcome("DELETE", 1))),
        ]
    );
}

/// The sum is main's request tag: different verbs answer `MIXED` with the
/// summed count.
#[test]
fn the_summed_answer_mixes_two_verbs() {
    let summed = match insert_then_delete().summed() {
        Ok(summed) => summed,
        Err(_) => panic!("a sum of two counted tags never fails"),
    };
    assert_eq!(summed.tag, Some(FoldedTag::Dml(outcome(MIXED_VERB, 2))));
}

#[test]
fn the_summed_answer_of_one_verb_keeps_it() {
    let plans = [insert_into("a"), insert_into("b")];
    let mut fold = request_fold(&plans, &map_of(&[0, 1]), 2);
    for (task, plan) in plans.iter().enumerate() {
        let folded = fold
            .fold_of(task)
            .and_then(|statement| statement.fold_dml(plan, outcome("INSERT", 2)));
        assert!(folded.is_ok(), "statement {task} must fold");
    }
    let summed = match fold.finish().summed() {
        Ok(summed) => summed,
        Err(_) => panic!("a sum of two counted tags never fails"),
    };
    assert_eq!(summed.tag, Some(FoldedTag::Dml(outcome("INSERT", 4))));
}

/// A trailing statement that planned no task answers an empty part.
#[test]
fn a_statement_with_no_task_answers_an_empty_part() {
    let plans = [insert_into("a")];
    let answer = request_fold(&plans, &map_of(&[0]), 2).finish();
    assert_eq!(answer.statements.len(), 2);
    let empty = &answer.statements[1];
    assert!(empty.rows.is_empty(), "no rows");
    assert!(empty.warnings.is_empty(), "no warnings");
    assert_eq!(empty.tag, None);
}

#[test]
fn a_task_with_no_statement_is_an_internal_error() {
    let plans = [insert_into("a"), delete_from("b")];
    let refused = RequestFold::for_plans(&plans, &map_of(&[0]), 1);
    assert!(
        matches!(
            refused,
            Err(StatementError::Error(crate::Error::Internal { .. }))
        ),
        "task 1 has no entry in the map"
    );
}

#[test]
fn an_error_names_its_statement_in_a_request_of_several() {
    let plans = [insert_into("a"), delete_from("b")];
    let fold = request_fold(&plans, &map_of(&[0, 1]), 2);
    assert!(matches!(
        fold.in_statement(1, StatementError::TenantIsolation),
        StatementError::InStatement {
            statement: 1,
            statements: 2,
            ..
        }
    ));
}

/// `INSERT a; INSERT a` plus a derived task of statement 0: statement 1
/// ends at task 1, statement 0 at its derived task 2. Each statement fires
/// its statement triggers once, at its own end.
#[test]
fn each_statement_ends_at_its_own_last_task() {
    let plans = [insert_into("a"), insert_into("a"), insert_into("a")];
    let fold = request_fold(&plans, &map_of(&[0, 1, 0]), 2);
    assert_eq!(fold.statement_count(), 2);
    let ends: Vec<_> = (0..4).map(|task| fold.statement_ending_at(task)).collect();
    assert_eq!(ends, [None, Some(1), Some(0), None]);
    assert_eq!(fold.statement_of_task(2), Some(0));
    assert_eq!(fold.statement_of_task(3), None);
}

/// A request of one statement ends once, at its last task.
#[test]
fn one_statement_ends_once_at_its_last_task() {
    let plans = [insert_into("a"), insert_into("b")];
    let fold = request_fold(&plans, &StatementMap::single(2), 1);
    let ends: Vec<_> = (0..2).map(|task| fold.statement_ending_at(task)).collect();
    assert_eq!(ends, [None, Some(0)]);
}

/// A statement that planned no task never ends: it fires no trigger.
#[test]
fn a_statement_with_no_task_never_ends() {
    let plans = [insert_into("a")];
    let fold = request_fold(&plans, &map_of(&[0]), 2);
    assert_eq!(fold.statement_count(), 2);
    assert_eq!(fold.statement_ending_at(0), Some(0));
    assert_eq!(fold.statement_ending_at(1), None);
}

#[test]
fn an_error_of_a_request_of_one_statement_stays_bare() {
    let plans = [insert_into("a")];
    let fold = request_fold(&plans, &StatementMap::single(1), 1);
    assert!(matches!(
        fold.in_statement(0, StatementError::TenantIsolation),
        StatementError::TenantIsolation
    ));
}

/// A counted part keyed `key`, with no rows.
fn counted_part(key: &'static str, affected: u64) -> StatementPart {
    StatementPart {
        rows: Vec::new(),
        warnings: Vec::new(),
        tag: Some(FoldedTag::Dml(outcome("INSERT", affected))),
        count_key: key,
    }
}

#[test]
fn a_sum_of_inserted_parts_answers_under_inserted() {
    let request = RequestAnswer {
        statements: vec![counted_part(INSERTED_KEY, 2), counted_part(INSERTED_KEY, 3)],
        last_lsn: 0,
    };
    let answer = request
        .summed()
        .unwrap_or_else(|_| panic!("one verb folds"));
    assert_eq!(answer.count_key, INSERTED_KEY);
}

#[test]
fn a_sum_with_an_affected_part_answers_under_affected() {
    let request = RequestAnswer {
        statements: vec![counted_part(INSERTED_KEY, 2), counted_part(AFFECTED_KEY, 1)],
        last_lsn: 0,
    };
    assert_eq!(
        request
            .summed()
            .unwrap_or_else(|_| panic!("one verb folds"))
            .count_key,
        AFFECTED_KEY
    );
    let empty = RequestAnswer {
        statements: Vec::new(),
        last_lsn: 0,
    };
    assert_eq!(
        empty
            .summed()
            .unwrap_or_else(|_| panic!("nothing folds"))
            .count_key,
        AFFECTED_KEY
    );
}
