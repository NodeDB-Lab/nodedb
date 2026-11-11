// SPDX-License-Identifier: BUSL-1.1

//! Unit tests for [`super::insert::append_implicit_edge_tasks_traced`]: the
//! parents it records, and the task list it builds against the untraced pass.

use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::insert::{append_implicit_edge_tasks, append_implicit_edge_tasks_traced};
use crate::control::planner::materialized_sum::resolve::tests::{
    DB, TENANT, body, insert_task, test_state,
};
use crate::control::server::shared::statement_exec::StatementMap;
use crate::control::state::SharedState;
use crate::types::TraceId;

/// A document body that carries an implicit edge from `src` to `dst`.
fn edge_body(src: &str, dst: &str) -> Vec<u8> {
    let map = rmpv::Value::Map(vec![
        (
            rmpv::Value::String("_from".into()),
            rmpv::Value::String(src.into()),
        ),
        (
            rmpv::Value::String("_to".into()),
            rmpv::Value::String(dst.into()),
        ),
        (
            rmpv::Value::String("_type".into()),
            rmpv::Value::String("knows".into()),
        ),
    ]);
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &map).expect("encode edge body");
    buf
}

/// Bind every endpoint up front, so the pass resolves each one from the
/// local catalog and both runs see the same surrogates.
async fn bind_endpoints(state: &SharedState, ids: &[&str]) {
    for id in ids {
        state
            .surrogate_assigner
            .assign(
                nodedb_types::CollectionKey::from_bare(DB, "links"),
                TENANT,
                id.as_bytes(),
            )
            .await
            .expect("bind edge endpoint");
    }
}

async fn run_traced(state: &SharedState, tasks: &mut Vec<PhysicalTask>) -> Vec<usize> {
    let mut parents = Vec::new();
    append_implicit_edge_tasks_traced(state, tasks, TENANT, DB, TraceId::ZERO, &mut parents)
        .await
        .expect("the edge pass succeeds");
    parents
}

fn is_edge_put(task: &PhysicalTask) -> bool {
    matches!(task.plan, PhysicalPlan::Graph(GraphOp::EdgePut { .. }))
}

/// The appended `EdgePut` names task 1 as its parent. In a map where task 1
/// is statement 1, the edge task maps to statement 1.
#[tokio::test]
async fn an_appended_edge_takes_its_parent_statement() {
    let (state, _directory) = test_state();
    bind_endpoints(&state, &["a", "b"]).await;
    let mut tasks = vec![
        insert_task("people", body("acc-1")),
        insert_task("links", edge_body("a", "b")),
    ];
    let mut map = StatementMap::default();
    map.push(0);
    map.push(1);

    let parents = run_traced(&state, &mut tasks).await;

    assert_eq!(tasks.len(), 3, "one edge task is appended: {tasks:?}");
    assert!(is_edge_put(&tasks[2]), "plan shape changed: {:?}", tasks[2]);
    assert_eq!(parents, [1]);
    map.push_derived(&parents)
        .expect("the parent precedes its child");
    map.debug_check_covers(&tasks);
    assert_eq!(map.statement_of(2), Some(1));
}

/// The traced pass builds the task list the untraced pass builds, and
/// records one parent per edge in append order.
#[tokio::test]
async fn traced_pass_builds_the_same_tasks() {
    let (state, _directory) = test_state();
    bind_endpoints(&state, &["a", "b", "c"]).await;
    let input = vec![
        insert_task("links", edge_body("a", "b")),
        insert_task("people", body("acc-1")),
        insert_task("links", edge_body("b", "c")),
    ];

    let mut untraced = input.clone();
    append_implicit_edge_tasks(&state, &mut untraced, TENANT, DB, TraceId::ZERO)
        .await
        .expect("the edge pass succeeds");
    let mut traced = input;
    let parents = run_traced(&state, &mut traced).await;

    assert_eq!(format!("{traced:?}"), format!("{untraced:?}"));
    assert_eq!(parents, [0, 2]);
    assert!(traced[3..].iter().all(is_edge_put));
}
