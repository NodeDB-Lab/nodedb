// SPDX-License-Identifier: BUSL-1.1

//! Unit tests for the traced materialized-sum passes: the parents they
//! record, and the task list they build against the untraced passes.

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::cross_shard::{
    append_cross_shard_balance_tasks, append_cross_shard_balance_tasks_traced,
};
use super::resolve::tests::{DB, TENANT, body, declare_binding, insert_task, test_state};
use super::resolve::{resolve_materialized_sum_targets, resolve_materialized_sum_targets_traced};
use crate::control::server::shared::statement_exec::StatementMap;
use crate::control::state::SharedState;
use crate::types::TraceId;

/// Task 0 writes a collection with no binding. Task 1 inserts a source row
/// whose `accounts` target sits on another vShard.
async fn two_writes(state: &SharedState) -> Vec<PhysicalTask> {
    declare_binding(state);
    state
        .surrogate_assigner
        .assign(
            nodedb_types::CollectionKey::from_bare(DB, "accounts"),
            TENANT,
            b"acc-1",
        )
        .await
        .expect("bind target row");
    vec![
        insert_task("unrelated", body("acc-1")),
        insert_task("entries", body("acc-1")),
    ]
}

/// Both passes, traced, in admission order. Returns every parent recorded.
async fn run_traced(state: &SharedState, tasks: &mut Vec<PhysicalTask>) -> Vec<usize> {
    let mut parents = Vec::new();
    resolve_materialized_sum_targets_traced(
        state,
        tasks,
        TENANT,
        DB,
        None,
        TraceId::ZERO,
        &mut parents,
    )
    .await
    .expect("resolution succeeds");
    append_cross_shard_balance_tasks_traced(state, tasks, TENANT, DB, &mut parents)
        .expect("the balance pass succeeds");
    parents
}

fn is_balance(task: &PhysicalTask) -> bool {
    matches!(
        task.plan,
        PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta { .. })
    )
}

/// The appended `ApplyBalanceDelta` names task 1 as its parent. In a map
/// where task 1 is statement 1, the balance task maps to statement 1.
#[tokio::test]
async fn an_appended_balance_takes_its_parent_statement() {
    let (state, _directory) = test_state();
    let mut tasks = two_writes(&state).await;
    let mut map = StatementMap::default();
    map.push(0);
    map.push(1);

    let parents = run_traced(&state, &mut tasks).await;

    assert_eq!(tasks.len(), 3, "one balance task is appended: {tasks:?}");
    assert!(is_balance(&tasks[2]), "plan shape changed: {:?}", tasks[2]);
    assert_eq!(parents, [1]);
    map.push_derived(&parents)
        .expect("the parent precedes its child");
    map.debug_check_covers(&tasks);
    assert_eq!(map.statement_of(2), Some(1));
}

/// A request of one statement maps the balance task to statement 0.
#[tokio::test]
async fn one_statement_maps_the_balance_to_statement_zero() {
    let (state, _directory) = test_state();
    let mut tasks = two_writes(&state).await;
    let mut map = StatementMap::single(tasks.len());

    let parents = run_traced(&state, &mut tasks).await;
    map.push_derived(&parents)
        .expect("the parent precedes its child");

    assert_eq!(map, StatementMap::single(3));
}

/// The traced passes build the task list the untraced passes build.
#[tokio::test]
async fn traced_passes_build_the_same_tasks() {
    let (state, _directory) = test_state();
    let input = two_writes(&state).await;

    let mut untraced = input.clone();
    resolve_materialized_sum_targets(&state, &mut untraced, TENANT, DB, None, TraceId::ZERO)
        .await
        .expect("resolution succeeds");
    append_cross_shard_balance_tasks(&state, &mut untraced, TENANT, DB)
        .expect("the balance pass succeeds");

    let mut traced = input;
    let parents = run_traced(&state, &mut traced).await;

    assert_eq!(format!("{traced:?}"), format!("{untraced:?}"));
    assert_eq!(
        parents.len(),
        traced.len() - 2,
        "one parent per appended task"
    );
}
