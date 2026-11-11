// SPDX-License-Identifier: BUSL-1.1

//! Admission over a request of one or several statements, on a one-node
//! cluster.
//!
//! The cluster's Data Plane core acknowledges every request and stores
//! nothing. A pass that reads stored row images reads none here.

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_types::{CollectionKey, DatabaseId, TenantId};

use super::plan_admission::{PlanAdmission, PlanAdmissionRequest, plan_authorize_and_admit};
use super::request_route::{RequestPolicy, select_route};
use super::sql_split::split_statements;
use super::statement_exec::{RequestError, StatementAt, StatementMap};
use crate::control::cluster::test_one_node::{OneNodeCluster, boot};
use crate::control::planner::context::QueryContext;
use crate::control::security::catalog::MaterializedSumDef;
use crate::control::security::identity::{AuthenticatedIdentity, DatabaseSet, Role};
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::pgwire::types::error_to_sqlstate;
use crate::control::server::shared::ddl::neutral::collection::create::handler::create_collection;
use crate::control::server::shared::ddl::neutral::collection::create::request::CreateCollectionRequest;
use crate::types::TraceId;

const TEST_ACTOR: &str = "_admission_test";
const TENANT: TenantId = TenantId::new(1);
const DB: DatabaseId = DatabaseId::DEFAULT;

fn identity() -> AuthenticatedIdentity {
    AuthenticatedIdentity::new_internal_service(
        0,
        TEST_ACTOR,
        TENANT,
        vec![Role::Superuser],
        true,
        None,
        DatabaseSet::All,
    )
}

/// Create `name`. `engine` `None` creates a schemaless collection.
async fn create(cluster: &OneNodeCluster, name: &str, engine: Option<&str>) {
    let columns = match engine {
        Some(_) => vec![
            ("id".to_string(), "TEXT PRIMARY KEY".to_string()),
            ("val".to_string(), "INT".to_string()),
        ],
        None => Vec::new(),
    };
    create_collection(
        &cluster.state,
        &identity(),
        &CreateCollectionRequest {
            name,
            engine,
            columns: &columns,
            options: &[],
            flags: &[],
            balanced_raw: None,
        },
        DB,
    )
    .await
    .unwrap_or_else(|error| panic!("create {name}: {error:?}"));
}

/// A one-node cluster with two strict collections, `adm_a` and `adm_b`.
/// Each has the key `id` and an `INT` column `val`.
async fn strict_cluster() -> OneNodeCluster {
    let cluster = boot().await;
    for name in ["adm_a", "adm_b"] {
        create(&cluster, name, Some("document_strict")).await;
    }
    cluster
}

async fn admit(cluster: &OneNodeCluster, sql: &str) -> crate::Result<PlanAdmission> {
    admit_located(cluster, sql)
        .await
        .map_err(RequestError::into_inner)
}

/// [`admit`] with the statement an error names.
async fn admit_located(cluster: &OneNodeCluster, sql: &str) -> Result<PlanAdmission, RequestError> {
    let identity = identity();
    let scope = RequestAuthScope::for_database(&identity, cluster.state.auth_stores(), DB)?;
    let query_ctx = QueryContext::for_state(&cluster.state);
    let statements = split_statements(sql);
    let policy = RequestPolicy {
        txn_wrapper: true,
        txn_control_native: false,
    };
    plan_authorize_and_admit(PlanAdmissionRequest {
        state: &cluster.state,
        query_ctx: &query_ctx,
        scope: &scope,
        sql,
        statements: &statements,
        route: select_route(&statements, policy),
        trace_id: TraceId::ZERO,
    })
    .await
}

fn column_names(admission: &PlanAdmission) -> Vec<&str> {
    admission
        .output_schema
        .columns
        .iter()
        .map(|column| column.display_name.as_str())
        .collect()
}

/// The statements the map assigns, in task order.
fn statements_of(admission: &PlanAdmission) -> Vec<Option<u32>> {
    (0..admission.tasks.len())
        .map(|task| admission.statement_map.statement_of(task))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_statement_admits_as_one_whole_statement() {
    let cluster = strict_cluster().await;

    let admission = admit(
        &cluster,
        "INSERT INTO adm_a (id, val) VALUES ('a1', 1) RETURNING id",
    )
    .await
    .expect("one statement admits");

    assert!(!admission.holds_several_statements());
    assert_eq!(admission.output_schemas.len(), 1);
    assert_eq!(
        admission.statement_map,
        StatementMap::single(admission.tasks.len())
    );
    assert_eq!(column_names(&admission), ["id"]);
    drop(admission);
    cluster.shutdown().await;
}

/// `INSERT .. RETURNING id; DELETE ..`: each statement plans alone. The
/// DELETE plans its own tasks, and the fold reads the RETURNING schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_statements_admit_each_with_its_own_tasks() {
    let cluster = strict_cluster().await;

    let admission = admit(
        &cluster,
        "INSERT INTO adm_a (id, val) VALUES ('a1', 1) RETURNING id; \
         DELETE FROM adm_b WHERE id = 'b1'",
    )
    .await
    .expect("two statements admit");

    assert!(admission.holds_several_statements());
    assert_eq!(admission.output_schemas.len(), 2);
    let statements = statements_of(&admission);
    assert!(statements.contains(&Some(0)), "{statements:?}");
    assert!(
        statements.contains(&Some(1)),
        "the DELETE plans its own tasks: {statements:?}"
    );
    assert_eq!(column_names(&admission), ["id"]);
    drop(admission);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ddl_beside_dml_is_refused_with_0a000() {
    let cluster = strict_cluster().await;

    let error = admit(
        &cluster,
        "CREATE COLLECTION adm_new; INSERT INTO adm_a (id, val) VALUES ('a1', 1)",
    )
    .await
    .err()
    .expect("DDL beside DML must be refused");

    let (_, sqlstate, message) = error_to_sqlstate(&error);
    assert_eq!(sqlstate, "0A000", "{message}");
    cluster.shutdown().await;
}

/// A plan error of the second statement names it. The same error alone
/// names no statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plan_error_of_the_second_statement_names_it() {
    let cluster = strict_cluster().await;
    let bad = "INSERT INTO adm_missing (id, val) VALUES ('m1', 1)";

    let error = admit_located(
        &cluster,
        &format!("INSERT INTO adm_a (id, val) VALUES ('a1', 1); {bad}"),
    )
    .await
    .err()
    .expect("the second statement must not plan");
    assert_eq!(error.at, StatementAt::of(1, 2), "{error}");

    let alone = admit_located(&cluster, bad)
        .await
        .err()
        .expect("the statement alone must not plan");
    assert_eq!(alone.at, None, "{alone}");
    assert_eq!(alone.to_string(), error.to_string());
    cluster.shutdown().await;
}

/// `entries` feeds a `balance` sum on `accounts`, on another vShard. The
/// target row `acc-1` holds a surrogate.
async fn sum_cluster() -> OneNodeCluster {
    assert_ne!(
        CollectionKey::from_bare(DB, "entries").vshard(),
        CollectionKey::from_bare(DB, "accounts").vshard(),
        "the fixture needs a cross-shard target"
    );
    let cluster = boot().await;
    for name in ["adm_unrelated", "entries", "accounts"] {
        create(&cluster, name, None).await;
    }
    let catalog = cluster.state.credentials.catalog();
    let mut accounts = catalog
        .get_collection(DB, TENANT.as_u64(), "accounts")
        .expect("read accounts")
        .expect("accounts exists");
    accounts.materialized_sums.push(MaterializedSumDef {
        target_collection: "accounts".to_string(),
        target_column: "balance".to_string(),
        source_collection: "entries".to_string(),
        join_column: "account_id".to_string(),
        value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
    });
    catalog
        .put_collection(DB, &accounts)
        .expect("persist the binding");
    cluster.state.materialized_sum_index.invalidate();
    cluster
        .state
        .surrogate_assigner
        .assign(CollectionKey::from_bare(DB, "accounts"), TENANT, b"acc-1")
        .await
        .expect("bind the target row");
    cluster
}

fn is_balance(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta { .. })
    )
}

/// Statement 1 inserts a source row. The balance task admission appends for
/// it maps to statement 1, not to statement 0 or to the last task's slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_derived_balance_maps_to_the_statement_that_wrote_its_source() {
    let cluster = sum_cluster().await;

    let admission = admit(
        &cluster,
        "INSERT INTO adm_unrelated (id, n) VALUES ('u1', 1); \
         INSERT INTO entries (id, account_id, amount) VALUES ('e1', 'acc-1', 25)",
    )
    .await
    .expect("two inserts admit");

    admission.statement_map.debug_check_covers(&admission.tasks);
    let balances: Vec<Option<u32>> = admission
        .tasks
        .iter()
        .enumerate()
        .filter(|(_, task)| is_balance(&task.plan))
        .map(|(index, _)| admission.statement_map.statement_of(index))
        .collect();
    assert!(
        !balances.is_empty(),
        "a cross-shard balance task is appended: {:?}",
        admission.tasks
    );
    assert!(
        balances.iter().all(|statement| *statement == Some(1)),
        "every balance task maps to statement 1: {balances:?}"
    );
    assert!(
        statements_of(&admission).contains(&Some(0)),
        "statement 0 plans its own task"
    );
    drop(admission);
    cluster.shutdown().await;
}
