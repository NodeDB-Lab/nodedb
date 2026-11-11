// SPDX-License-Identifier: BUSL-1.1

//! Unit tests for the request planner: the solo-statement refusal, the
//! descriptor-version merge, and per-statement planning on a one-node
//! cluster.

use nodedb_cluster::{DescriptorId, DescriptorKind};
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_sql::types::PlanCacheEligibility;
use nodedb_types::{DatabaseId, TenantId};

use super::planning_request::{
    SoloKind, join_cache_eligibility, refuse_solo_statements, solo_kind,
};
use super::{QueryContext, RequestPlan};
use crate::control::cluster::test_one_node::{OneNodeCluster, boot};
use crate::control::planner::context::SystemPlanSecurity;
use crate::control::planner::descriptor_set::DescriptorVersionSet;
use crate::control::security::identity::{AuthenticatedIdentity, DatabaseSet, Role};
use crate::control::server::pgwire::types::error_to_sqlstate;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::{PlanKind, describe_plan};
use crate::control::server::shared::ddl::neutral::collection::create::handler::create_collection;
use crate::control::server::shared::ddl::neutral::collection::create::request::CreateCollectionRequest;
use crate::control::server::shared::returning::strip_returning;
use crate::control::server::shared::statement_exec::StatementMap;
use crate::control::state::SharedState;

fn request(statements: &[&str]) -> Vec<String> {
    statements.iter().map(|s| s.to_string()).collect()
}

// ── Solo-statement refusal ─────────────────────────────────────────────

#[test]
fn a_solo_kind_is_read_from_the_first_word() {
    for (statement, kind) in [
        ("BEGIN", SoloKind::TransactionControl),
        ("begin transaction", SoloKind::TransactionControl),
        ("START TRANSACTION", SoloKind::TransactionControl),
        ("COMMIT AND CHAIN", SoloKind::TransactionControl),
        ("ROLLBACK", SoloKind::TransactionControl),
        ("SAVEPOINT s1", SoloKind::TransactionControl),
        ("RELEASE SAVEPOINT s1", SoloKind::TransactionControl),
        ("SET search_path = 'x'", SoloKind::SessionCommand),
        ("SHOW ALL", SoloKind::SessionCommand),
        ("RESET ALL", SoloKind::SessionCommand),
        ("CREATE COLLECTION extra", SoloKind::Ddl),
        ("-- a note\nDROP COLLECTION docs", SoloKind::Ddl),
        ("ALTER COLLECTION docs ADD COLUMN note TEXT", SoloKind::Ddl),
        ("GRANT READ ON docs TO someone", SoloKind::Ddl),
    ] {
        assert_eq!(solo_kind(statement), Some(kind), "{statement}");
    }
    for statement in [
        "INSERT INTO docs (id, val) VALUES ('a', 1)",
        "UPSERT INTO docs (id, val) VALUES ('a', 1)",
        "UPDATE docs SET val = 2 WHERE id = 'a'",
        "DELETE FROM docs WHERE val > 5",
        "SELECT id FROM docs",
        "WITH t AS (SELECT id FROM docs) SELECT id FROM t",
        "MERGE INTO docs t USING src s ON t.id = s.id WHEN MATCHED THEN DELETE",
        "TRUNCATE docs",
    ] {
        assert_eq!(solo_kind(statement), None, "{statement}");
    }
}

#[test]
fn a_solo_statement_beside_dml_is_refused_with_0a000() {
    for solo in ["CREATE COLLECTION extra", "BEGIN", "COMMIT", "SET x = 1"] {
        for statements in [
            request(&["INSERT INTO docs (id) VALUES ('a')", solo]),
            request(&[solo, "DELETE FROM docs WHERE id = 'a'"]),
        ] {
            let error = refuse_solo_statements(&statements)
                .expect_err("a solo statement beside DML must be refused");
            assert!(
                matches!(error, crate::Error::FeatureNotSupported { .. }),
                "{solo}: {error:?}"
            );
            let (_, sqlstate, message) = error_to_sqlstate(&error);
            assert_eq!(sqlstate, "0A000", "{solo}: {message}");
            assert!(
                message.contains("of 2") && message.contains("as its own request"),
                "the message names the statement and the next action: {message}"
            );
        }
    }
}

#[test]
fn a_request_of_one_statement_is_never_refused() {
    for solo in ["CREATE COLLECTION extra", "BEGIN", "SHOW ALL"] {
        refuse_solo_statements(&request(&[solo])).expect("one statement is never refused");
    }
    refuse_solo_statements(&[]).expect("an empty request is not refused");
}

// ── Version merge ──────────────────────────────────────────────────────

fn collection(name: &str) -> DescriptorId {
    DescriptorId::new(0, 1, DescriptorKind::Collection, name.to_string())
}

fn entries(set: &DescriptorVersionSet) -> Vec<(&str, u64)> {
    set.iter()
        .map(|(id, version)| (id.name.as_str(), version))
        .collect()
}

#[test]
fn agreeing_version_sets_merge_into_one() {
    let mut first = DescriptorVersionSet::new();
    first.record(collection("a"), 1);
    first.record(collection("b"), 2);
    let mut second = DescriptorVersionSet::new();
    second.record(collection("b"), 2);
    second.record(collection("c"), 3);

    first
        .merge_statement(&second)
        .expect("one version per descriptor merges");

    assert_eq!(entries(&first), [("a", 1), ("b", 2), ("c", 3)]);
}

#[test]
fn a_descriptor_at_two_versions_does_not_merge() {
    let mut first = DescriptorVersionSet::new();
    first.record(collection("a"), 1);
    let before = first.clone();
    let mut second = DescriptorVersionSet::new();
    second.record(collection("z"), 1);
    second.record(collection("a"), 2);

    let error = first
        .merge_statement(&second)
        .expect_err("two versions of one descriptor must not merge");

    assert!(
        matches!(
            &error,
            crate::Error::RetryableSchemaChanged { descriptor } if descriptor == "a"
        ),
        "{error:?}"
    );
    assert_eq!(first, before, "a refused merge leaves the set unchanged");
}

#[test]
fn an_unleased_descriptor_at_two_versions_does_not_merge() {
    let array = DescriptorId::new(0, 1, DescriptorKind::Array, "scene".to_string());
    let mut first = DescriptorVersionSet::new();
    first.record_unleased(array.clone(), 7);
    let mut second = DescriptorVersionSet::new();
    second.record_unleased(array.clone(), 7);
    first
        .merge_statement(&second)
        .expect("one unleased version merges");

    let mut third = DescriptorVersionSet::new();
    third.record_unleased(array, 8);
    let error = first
        .merge_statement(&third)
        .expect_err("two versions of one array must not merge");
    assert!(
        matches!(error, crate::Error::RetryableSchemaChanged { .. }),
        "{error:?}"
    );
}

#[test]
fn a_merged_policy_version_takes_the_lower_value() {
    let mut first = DescriptorVersionSet::new();
    first.set_permission_tree_version(4);
    first.set_rls_version(5);
    first.set_redaction_version(6);
    let mut second = DescriptorVersionSet::new();
    second.set_permission_tree_version(3);
    second.set_rls_version(9);
    second.set_redaction_version(6);

    first
        .merge_statement(&second)
        .expect("policy versions merge");

    assert_eq!(first.permission_tree_version(), 3);
    assert_eq!(first.rls_version(), 5);
    assert_eq!(first.redaction_version(), 6);
}

#[test]
fn cache_eligibility_joins_to_data_dependent() {
    use nodedb_sql::types::PlanCacheEligibility::{Cacheable, DataDependent};
    assert_eq!(join_cache_eligibility(Cacheable, Cacheable), Cacheable);
    assert_eq!(
        join_cache_eligibility(Cacheable, DataDependent),
        DataDependent
    );
    assert_eq!(
        join_cache_eligibility(DataDependent, Cacheable),
        DataDependent
    );
}

// ── Planning on a one-node cluster ─────────────────────────────────────

const TEST_ACTOR: &str = "_request_planner_test";

fn identity() -> AuthenticatedIdentity {
    AuthenticatedIdentity::new_internal_service(
        0,
        TEST_ACTOR,
        TenantId::new(1),
        vec![Role::Superuser],
        true,
        None,
        DatabaseSet::All,
    )
}

/// A one-node cluster with two strict document collections, `docs` and
/// `kv_docs`. Each has the key `id` and an `INT` column `val`.
async fn planner_cluster() -> OneNodeCluster {
    let cluster = boot().await;
    let columns = vec![
        ("id".to_string(), "TEXT PRIMARY KEY".to_string()),
        ("val".to_string(), "INT".to_string()),
    ];
    for name in ["docs", "kv_docs"] {
        create_collection(
            &cluster.state,
            &identity(),
            &CreateCollectionRequest {
                name,
                engine: Some("document_strict"),
                columns: &columns,
                options: &[],
                flags: &[],
                balanced_raw: None,
            },
            DatabaseId::DEFAULT,
        )
        .await
        .unwrap_or_else(|error| panic!("create {name}: {error:?}"));
    }
    cluster
}

async fn plan_request(
    ctx: &QueryContext,
    state: &SharedState,
    statements: &[&str],
) -> crate::Result<RequestPlan> {
    let security = SystemPlanSecurity::new(TenantId::new(1), TEST_ACTOR);
    ctx.plan_request_with_rls_and_versions(
        &request(statements),
        TenantId::new(1),
        DatabaseId::DEFAULT,
        &security.context(state),
    )
    .await
}

/// The single-text path: strip the clause off the whole text, then plan it.
async fn plan_single_text(
    ctx: &QueryContext,
    state: &SharedState,
    sql: &str,
) -> (
    Vec<PhysicalTask>,
    OutputSchema,
    DescriptorVersionSet,
    PlanCacheEligibility,
) {
    let security = SystemPlanSecurity::new(TenantId::new(1), TEST_ACTOR);
    let (stripped, returning_items) = strip_returning(sql).expect("strip RETURNING");
    ctx.plan_sql_with_rls_and_versions(
        &stripped,
        TenantId::new(1),
        DatabaseId::DEFAULT,
        &security.context(state),
        returning_items.as_deref(),
    )
    .await
    .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
}

/// The tasks the map assigns to `statement`.
fn tasks_of(plan: &RequestPlan, statement: u32) -> Vec<&PhysicalTask> {
    plan.tasks
        .iter()
        .enumerate()
        .filter(|(index, _)| plan.statement_map.statement_of(*index) == Some(statement))
        .map(|(_, task)| task)
        .collect()
}

/// `statement` plans at least one task, and `kind` holds for each task.
fn assert_statement_kind(
    plan: &RequestPlan,
    statement: u32,
    kind: fn(PlanKind) -> bool,
    expected: &str,
) {
    let tasks = tasks_of(plan, statement);
    assert!(!tasks.is_empty(), "statement {statement} plans a task");
    for task in tasks {
        assert!(
            kind(describe_plan(&task.plan)),
            "statement {statement} must plan {expected}: {:?}",
            task.plan
        );
    }
}

/// `INSERT .. RETURNING id; DELETE ..`: the clause stays with the INSERT,
/// and the DELETE plans as a DELETE with no RETURNING spec.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn returning_stays_with_its_own_statement() {
    let cluster = planner_cluster().await;
    let ctx = QueryContext::for_state(&cluster.state);

    let plan = plan_request(
        &ctx,
        &cluster.state,
        &[
            "INSERT INTO docs (id, val) VALUES ('a1', 1) RETURNING id",
            "DELETE FROM docs WHERE val > 5",
        ],
    )
    .await
    .expect("both statements plan");

    assert_eq!(plan.statement_map.statement_count(), 2);
    assert_eq!(plan.output_schemas.len(), 2, "one schema per statement");
    assert_statement_kind(
        &plan,
        0,
        |kind| matches!(kind, PlanKind::ReturningRows),
        "an INSERT carrying the RETURNING spec",
    );
    assert_statement_kind(
        &plan,
        1,
        |kind| matches!(kind, PlanKind::DmlResult("DELETE")),
        "a DELETE with no RETURNING spec",
    );
    let names: Vec<&str> = plan.output_schemas[0]
        .columns
        .iter()
        .map(|column| column.display_name.as_str())
        .collect();
    assert_eq!(names, ["id"], "statement 0 announces its RETURNING column");
    assert!(
        plan.output_schemas[1].columns.is_empty(),
        "statement 1 announces no column: {:?}",
        plan.output_schemas[1]
    );
    cluster.shutdown().await;
}

/// `UPSERT ..; INSERT ..` plans the INSERT as an insert. `INSERT ..;
/// UPSERT ..` plans the UPSERT as an upsert.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_and_insert_each_plan_as_their_own_verb() {
    let cluster = planner_cluster().await;
    let ctx = QueryContext::for_state(&cluster.state);
    let upsert = "UPSERT INTO kv_docs (id, val) VALUES ('u1', 1)";
    let insert = "INSERT INTO docs (id, val) VALUES ('i1', 2)";
    let is_upsert = |kind: PlanKind| matches!(kind, PlanKind::DmlResult("UPSERT"));
    let is_insert = |kind: PlanKind| matches!(kind, PlanKind::DmlResult("INSERT"));

    let plan = plan_request(&ctx, &cluster.state, &[upsert, insert])
        .await
        .expect("UPSERT then INSERT plans");
    assert_statement_kind(&plan, 0, is_upsert, "an upsert");
    assert_statement_kind(&plan, 1, is_insert, "an insert");

    let plan = plan_request(&ctx, &cluster.state, &[insert, upsert])
        .await
        .expect("INSERT then UPSERT plans");
    assert_statement_kind(&plan, 0, is_insert, "an insert");
    assert_statement_kind(&plan, 1, is_upsert, "an upsert");
    cluster.shutdown().await;
}

/// A request of one statement plans what the single-text path plans. It
/// gets the same tasks, schema and versions, and the map `single(n)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_single_statement_plans_as_the_single_text_path() {
    let cluster = planner_cluster().await;
    let ctx = QueryContext::for_state(&cluster.state);

    for sql in [
        "INSERT INTO docs (id, val) VALUES ('s1', 1) RETURNING id, val",
        "DELETE FROM docs WHERE val > 5",
        "SELECT id, val FROM docs WHERE val > 0",
    ] {
        let (tasks, schema, versions, eligibility) =
            plan_single_text(&ctx, &cluster.state, sql).await;
        let plan = plan_request(&ctx, &cluster.state, &[sql])
            .await
            .unwrap_or_else(|error| panic!("plan request {sql}: {error}"));

        assert_eq!(plan.tasks.len(), tasks.len(), "{sql}");
        for (planned, single) in plan.tasks.iter().zip(&tasks) {
            assert_eq!(planned.tenant_id, single.tenant_id, "{sql}");
            assert_eq!(planned.vshard_id, single.vshard_id, "{sql}");
            assert_eq!(planned.database_id, single.database_id, "{sql}");
            assert_eq!(planned.plan, single.plan, "{sql}");
            assert_eq!(planned.post_set_op, single.post_set_op, "{sql}");
            assert_eq!(planned.txn_id, single.txn_id, "{sql}");
        }
        assert_eq!(
            plan.statement_map,
            StatementMap::single(tasks.len()),
            "{sql}"
        );
        assert_eq!(plan.output_schemas.len(), 1, "{sql}");
        assert_eq!(
            format!("{:?}", plan.output_schemas[0]),
            format!("{schema:?}"),
            "{sql}"
        );
        assert_eq!(plan.versions, versions, "{sql}");
        assert_eq!(plan.cache_eligibility, eligibility, "{sql}");
    }
    cluster.shutdown().await;
}

/// `INSERT 2 rows; DELETE`: the map holds statement 0 once per INSERT
/// task, then statement 1 once per DELETE task.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_map_holds_each_statement_once_per_task() {
    let cluster = planner_cluster().await;
    let ctx = QueryContext::for_state(&cluster.state);
    let insert = "INSERT INTO docs (id, val) VALUES ('m1', 1), ('m2', 2)";
    let delete = "DELETE FROM docs WHERE val > 5";
    let (insert_tasks, ..) = plan_single_text(&ctx, &cluster.state, insert).await;
    let (delete_tasks, ..) = plan_single_text(&ctx, &cluster.state, delete).await;
    assert!(!insert_tasks.is_empty() && !delete_tasks.is_empty());

    let plan = plan_request(&ctx, &cluster.state, &[insert, delete])
        .await
        .expect("both statements plan");

    let mut expected = StatementMap::default();
    for _ in 0..insert_tasks.len() {
        expected.push(0);
    }
    for _ in 0..delete_tasks.len() {
        expected.push(1);
    }
    assert_eq!(plan.statement_map, expected);
    assert_eq!(plan.tasks.len(), insert_tasks.len() + delete_tasks.len());
    assert_eq!(plan.output_schemas.len(), 2);
    cluster.shutdown().await;
}

/// The entry point refuses DDL beside DML before it plans anything. A
/// request of no statement plans no task.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_planner_refuses_ddl_beside_dml_and_plans_an_empty_request() {
    let cluster = planner_cluster().await;
    let ctx = QueryContext::for_state(&cluster.state);

    let error = plan_request(
        &ctx,
        &cluster.state,
        &[
            "INSERT INTO docs (id, val) VALUES ('d1', 1)",
            "CREATE COLLECTION extra",
        ],
    )
    .await
    .expect_err("DDL beside DML must be refused");

    let (_, sqlstate, message) = error_to_sqlstate(&error);
    assert_eq!(sqlstate, "0A000", "{message}");
    assert!(message.contains("statement 2 of 2"), "{message}");

    let empty = plan_request(&ctx, &cluster.state, &[])
        .await
        .expect("a request of no statement plans");
    assert!(empty.tasks.is_empty() && empty.statement_map.is_empty());
    assert!(empty.output_schemas.is_empty());
    cluster.shutdown().await;
}
