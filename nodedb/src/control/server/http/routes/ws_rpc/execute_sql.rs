// SPDX-License-Identifier: BUSL-1.1

//! SQL execution via the SPSC gateway for WebSocket RPC.

use std::sync::Arc;

use crate::control::gateway::core::QueryContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::sequence::SessionSequenceAccess;
use crate::control::server::response_shape::redaction::{QueryRedaction, redact_decoded_value};
use crate::control::server::shared::authorization::authorize_database;
use crate::control::server::shared::cluster_array_dispatch::{is_cluster_array, run_cluster_array};
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::plan_admission::{
    PlanAdmissionRequest, plan_authorize_and_admit,
};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::control::server::shared::statement_exec::{
    AtomicStatement, StatementExec, authorize_one_task,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId};

use super::super::atomic_statement::{RoutedStatement, route_http_statement};

/// Execute SQL and return result as JSON.
///
/// A statement whose tasks commit together takes the shared atomic routes.
/// A Control-Plane orchestrated task (array DDL, a cluster array op,
/// `INSERT ... SELECT`) runs here. Every other task routes through the
/// gateway.
pub async fn execute_sql(
    shared: &Arc<SharedState>,
    query_ctx: &crate::control::planner::context::QueryContext,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
    trace_id: TraceId,
    peer_addr: &str,
) -> crate::Result<serde_json::Value> {
    let tenant_id = identity.tenant_id;
    let emitter = ArcAuditEmitter(Arc::clone(&shared.audit));
    authorize_database(identity, database_id, &emitter)?;

    // Quota enforcement — reject before planning or dispatch.
    shared.check_tenant_quota(tenant_id)?;

    // RPC-selected database is authoritative for RLS; passed as the session
    // database so `scope.database_id()` resolves to `database_id`.
    let request = RequestAuthScope::builder(identity, shared.auth_stores())
        .with_session_database(Some(database_id))
        .build_for_client(peer_addr);

    // Admission gate runs before planning/dispatch. WebSocket has no headers for
    // `X-RateLimit-*`, so the outcome is discarded on success; a denial still fails closed.
    crate::control::server::session_auth::check_request_admission(shared, &request, "sql")?;

    let (clean_sql, scope) = crate::control::server::session_auth::apply_per_query_on_deny(
        sql,
        request.into_resolved_scope(),
    );
    // Planning and lease admission run as one retried unit so a descriptor drain
    // between them is absorbed.
    let admission = plan_authorize_and_admit(PlanAdmissionRequest {
        state: shared,
        query_ctx,
        scope: &scope,
        sql: &clean_sql,
        trace_id,
    })
    .await?;
    let output_schema = admission.output_schema;

    // A statement admitted under a lease this node then loses ends with a
    // retryable error: a read mid-flight, a write only before dispatch.
    admission.lease_scope.check_not_revoked()?;

    // The shared atomic routes run a statement whose tasks commit together:
    // the implicit transaction, the implicit-edge gate, and a Calvin commit
    // for a write that spans vShards. WebSocket RPC carries no session:
    // `nextval` advances the registry, `currval` reports "not yet called in
    // this session".
    let sequences = SessionSequenceAccess::for_session(shared, None, database_id, tenant_id);
    let exec = StatementExec {
        state: shared,
        identity,
        scope: &scope,
        output_schema: Some(&output_schema),
        database_id,
        sequences: &sequences,
    };
    let routed = {
        let _request = shared.tenant_request_guard(tenant_id);
        route_http_statement(
            &exec,
            AtomicStatement {
                tasks: admission.tasks,
                lease_scope: admission.lease_scope,
                sum_target_reads: admission.sum_target_reads,
            },
        )
        .await?
    };
    let AtomicStatement {
        tasks, lease_scope, ..
    } = match routed {
        RoutedStatement::Answered(rows) => return Ok(rows_result(rows)),
        RoutedStatement::PerTask(statement) => statement,
    };

    let read_only = tasks
        .iter()
        .all(|task| !crate::control::server::shared::write_admission::plan_is_write(&task.plan));
    let body = async {
        let _request = shared.tenant_request_guard(tenant_id);

        // Resolved once and reused at every decode site — orchestrated rows and plain
        // dispatch rows must be redacted by the same policy snapshot.
        let redaction =
            QueryRedaction::for_plans(tenant_id, scope.auth(), tasks.iter().map(|t| &t.plan));

        let mut results = Vec::new();
        // Checked once, not per task: keeps per-task extraction a no-op when metering
        // is disabled (the default).
        let metering_enabled = shared.metering_config.enabled;
        for task in tasks {
            // Extracted before `task.plan` is cloned/moved; `results.len()` gives this
            // task's row-count baseline for the delta metered below.
            let plan_metering_info =
                metering_enabled.then(|| PlanMeteringInfo::extract(&task.plan));
            // A spent hard quota refuses the task before it runs; charging below is
            // success-path only and never refuses.
            if let Some(info) = &plan_metering_info {
                admit_quota_for_dispatch(shared, &scope, info)?;
            }
            let rows_before = results.len();

            // Array DDL proposes a replicated catalog entry, never a core task.
            if crate::control::array_catalog::ddl::is_array_ddl(&task.plan) {
                let authorized_task = authorize_one_task(shared, identity, &task)?;
                let resp = crate::control::array_catalog::ddl::run_authorized_array_ddl(
                    shared,
                    authorized_task,
                )
                .await?;
                let json =
                    crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
                match sonic_rs::from_str::<serde_json::Value>(&json) {
                    Ok(value) => results.push(value),
                    Err(_) => results.push(serde_json::Value::String(json)),
                }
                continue;
            }

            // A cluster array op runs through this node's array coordinator,
            // which routes it to the shards that own its cells.
            if is_cluster_array(&task.plan) {
                let authorized_task = authorize_one_task(shared, identity, &task)?;
                let payload = run_cluster_array(shared, authorized_task).await?;
                if !payload.is_empty() {
                    let json =
                        crate::data::executor::response_codec::decode_payload_to_json(&payload);
                    match sonic_rs::from_str::<serde_json::Value>(&json) {
                        Ok(mut v) => {
                            redact_decoded_value(Some(&redaction), &shared.redaction, &mut v);
                            results.push(v);
                        }
                        Err(_) => results.push(serde_json::Value::String(json)),
                    }
                }
                meter_task(shared, &scope, &plan_metering_info, rows_before, &results);
                continue;
            }

            // `INSERT ... SELECT` orchestrates on the Control Plane, never dispatched
            // to the Data Plane as a single op, and is never a clone-write shape.
            if let crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. },
            ) = &task.plan
            {
                let authorized_task = authorize_one_task(shared, identity, &task)?;
                match crate::control::insert_select::run_authorized_insert_select(
                    shared,
                    authorized_task,
                )
                .await
                {
                    Ok(resp) => {
                        let payload = resp.payload.to_vec();
                        if !payload.is_empty() {
                            let json =
                                crate::data::executor::response_codec::decode_payload_to_json(
                                    &payload,
                                );
                            match sonic_rs::from_str::<serde_json::Value>(&json) {
                                Ok(mut v) => {
                                    redact_decoded_value(
                                        Some(&redaction),
                                        &shared.redaction,
                                        &mut v,
                                    );
                                    results.push(v);
                                }
                                Err(_) => results.push(serde_json::Value::String(json)),
                            }
                        }
                    }
                    Err(e) => return Err(e),
                }
                meter_task(shared, &scope, &plan_metering_info, rows_before, &results);
                continue;
            }

            // Autocommit `MERGE` orchestrates on the Control Plane, never dispatched
            // to the Data Plane as a single op.
            if let crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::Merge {
                    target_collection: _,
                    source_collection: _,
                    source_alias: _,
                    target_join_col: _,
                    source_join_col: _,
                    clauses: _,
                    returning: _,
                    resolved_inserts: None,
                    resolved_insert_identities: _,
                    source_rows: _,
                    rls_filters: _,
                    rls_write_check: _,
                    resolved_sum_targets: _,
                    declared_primary_key: _,
                },
            ) = &task.plan
            {
                let authorized_task = authorize_one_task(shared, identity, &task)?;
                match crate::control::merge_orchestrator::run_authorized_merge(
                    shared,
                    authorized_task,
                )
                .await
                {
                    Ok(resp) => {
                        let payload = resp.payload.to_vec();
                        if !payload.is_empty() {
                            let json =
                                crate::data::executor::response_codec::decode_payload_to_json(
                                    &payload,
                                );
                            match sonic_rs::from_str::<serde_json::Value>(&json) {
                                Ok(mut v) => {
                                    redact_decoded_value(
                                        Some(&redaction),
                                        &shared.redaction,
                                        &mut v,
                                    );
                                    results.push(v);
                                }
                                Err(_) => results.push(serde_json::Value::String(json)),
                            }
                        }
                    }
                    Err(e) => return Err(e),
                }
                meter_task(shared, &scope, &plan_metering_info, rows_before, &results);
                continue;
            }

            // Autocommit `UPDATE ... FROM <source>` scans the source on its own core and
            // ships it into the plan, never dispatched as a single op.
            if let crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
                    target_collection: _,
                    source_collection: _,
                    source_alias: _,
                    target_join_col: _,
                    source_join_col: _,
                    updates: _,
                    target_filters: _,
                    returning: _,
                    source_rows: None,
                    rls_filters: _,
                    rls_write_check: _,
                    resolved_sum_targets: _,
                    declared_primary_key: _,
                },
            ) = &task.plan
            {
                let authorized_task = authorize_one_task(shared, identity, &task)?;
                match crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
                    shared,
                    authorized_task,
                )
                .await
                {
                    Ok(resp) => {
                        let payload = resp.payload.to_vec();
                        if !payload.is_empty() {
                            let json =
                                crate::data::executor::response_codec::decode_payload_to_json(&payload);
                            match sonic_rs::from_str::<serde_json::Value>(&json) {
                                Ok(mut v) => {
                                    redact_decoded_value(Some(&redaction), &shared.redaction, &mut v);
                                    results.push(v);
                                }
                                Err(_) => results.push(serde_json::Value::String(json)),
                            }
                        }
                    }
                    Err(e) => return Err(e),
                }
                meter_task(shared, &scope, &plan_metering_info, rows_before, &results);
                continue;
            }

            // A governed columnar predicate UPDATE/DELETE resolves to a concrete row set
            // before proposing.
            if let Some(resolver) = crate::control::write_resolve::resolver_for_plan(&task.plan) {
                let authorized_task = authorize_one_task(shared, identity, &task)?;
                match crate::control::write_resolve::run_authorized_write_resolve(
                    shared,
                    authorized_task,
                    resolver,
                )
                .await
                {
                    Ok(resp) => {
                        let payload = resp.payload.to_vec();
                        if !payload.is_empty() {
                            let json =
                                crate::data::executor::response_codec::decode_payload_to_json(
                                    &payload,
                                );
                            match sonic_rs::from_str::<serde_json::Value>(&json) {
                                Ok(mut v) => {
                                    redact_decoded_value(
                                        Some(&redaction),
                                        &shared.redaction,
                                        &mut v,
                                    );
                                    results.push(v);
                                }
                                Err(_) => results.push(serde_json::Value::String(json)),
                            }
                        }
                    }
                    Err(e) => return Err(e),
                }
                meter_task(shared, &scope, &plan_metering_info, rows_before, &results);
                continue;
            }

            // Clone CoW write-path interception, then authorization, run once per
            // task before dispatch — same protocol-neutral gate every transport runs.
            let emitter =
                crate::control::security::audit::ArcAuditEmitter(Arc::clone(&shared.audit));
            let checked =
                match crate::control::server::shared::clone_write::intercept_and_authorize(
                    crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                        state: shared,
                        task,
                        identity,
                        tenant_id,
                        permissions: &shared.permissions,
                        roles: &shared.roles,
                        emitter: &emitter,
                    },
                )
                .await?
                {
                    crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(
                        resp,
                    ) => {
                        let payload = resp.payload.to_vec();
                        if !payload.is_empty() {
                            let json =
                                crate::data::executor::response_codec::decode_payload_to_json(
                                    &payload,
                                );
                            match sonic_rs::from_str::<serde_json::Value>(&json) {
                                Ok(mut v) => {
                                    redact_decoded_value(
                                        Some(&redaction),
                                        &shared.redaction,
                                        &mut v,
                                    );
                                    results.push(v);
                                }
                                Err(_) => results.push(serde_json::Value::String(json)),
                            }
                        }
                        meter_task(shared, &scope, &plan_metering_info, rows_before, &results);
                        continue;
                    }
                    crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(
                        checked,
                    ) => checked,
                };

            // WebSocket RPC has no session transaction (no BEGIN / COMMIT), so
            // every task is autocommit and forwards with no transaction id.
            let gw_ctx = QueryContext {
                tenant_id: checked.tenant_id(),
                trace_id,
                database_id: checked.database_id(),
                txn_id: None,
                linearizable: true,
            };
            let payloads: crate::Result<Vec<Vec<u8>>> = match shared.installed_gateway() {
                Ok(gateway) => gateway.execute(&gw_ctx, checked).await,
                Err(error) => Err(error),
            };

            match payloads {
                Ok(vecs) => {
                    for payload in vecs {
                        if !payload.is_empty() {
                            let json =
                                crate::data::executor::response_codec::decode_payload_to_json(
                                    &payload,
                                );
                            match sonic_rs::from_str::<serde_json::Value>(&json) {
                                Ok(mut v) => {
                                    redact_decoded_value(
                                        Some(&redaction),
                                        &shared.redaction,
                                        &mut v,
                                    );
                                    results.push(v);
                                }
                                Err(_) => results.push(serde_json::Value::String(json)),
                            }
                        }
                    }
                }
                Err(e) => return Err(e),
            }
            meter_task(shared, &scope, &plan_metering_info, rows_before, &results);
        }

        Ok(rows_result(results))
    };
    if read_only {
        lease_scope.guard(body).await?
    } else {
        body.await
    }
}

/// The RPC result for a statement's JSON rows: `null` for none, the row
/// itself for one, an array for more.
fn rows_result(mut rows: Vec<serde_json::Value>) -> serde_json::Value {
    match rows.len() {
        0 => serde_json::Value::Null,
        1 => rows.pop().unwrap_or(serde_json::Value::Null),
        _ => serde_json::Value::Array(rows),
    }
}

/// Meter one task's dispatch after its rows are pushed onto `results` —
/// the row count is the delta since `rows_before`.
fn meter_task(
    shared: &SharedState,
    scope: &RequestAuthScope<'_>,
    info: &Option<PlanMeteringInfo>,
    rows_before: usize,
    results: &[serde_json::Value],
) {
    if let Some(info) = info {
        let task_rows = (results.len() - rows_before) as u64;
        meter_dispatch(shared, scope, info, Some(task_rows));
    }
}

#[cfg(test)]
mod tests {
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::planner::context::QueryContext as PlannerQueryContext;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::types::TenantId;
    use crate::wal::WalManager;

    use super::*;

    /// Returns the state plus the backing `TempDir` guard — the caller must
    /// keep the guard alive for as long as `state` is in use, mirroring
    /// `session_auth::admission`'s test harness.
    async fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");
        (state, dir)
    }

    fn regular_identity(user_id: u64) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            user_id,
            "regular-user",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        )
    }

    /// Guards `peer_addr` against regressing to a hardcoded placeholder: a
    /// non-IP value can't be parsed by `normalize_peer_ip`, so `check_ip`
    /// will silently match nothing and let a blacklisted client through.
    #[tokio::test]
    async fn blacklisted_peer_ip_is_rejected() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9101);
        state
            .blacklist
            .blacklist_ip("203.0.113.0/24", "test ban", "admin", 0)
            .expect("blacklist CIDR range");

        let query_ctx = PlannerQueryContext::for_state(&state);
        let trace_id = TraceId::generate();

        let result = execute_sql(
            &state,
            &query_ctx,
            &identity,
            DatabaseId::DEFAULT,
            "SELECT 1",
            trace_id,
            "203.0.113.42:54321",
        )
        .await;

        let error = result.expect_err("blacklisted peer IP must be rejected");
        let message = error.to_string();
        assert!(
            message.contains("IP blacklisted"),
            "expected an IP-blacklist rejection, got: {message}"
        );
    }

    /// A peer address outside the blacklisted range must still be admitted
    /// through the blacklist gate (it can fail later in planning/dispatch
    /// for unrelated reasons, but not on the IP check).
    #[tokio::test]
    async fn non_blacklisted_peer_ip_passes_blacklist_gate() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9102);
        state
            .blacklist
            .blacklist_ip("203.0.113.0/24", "test ban", "admin", 0)
            .expect("blacklist CIDR range");

        let query_ctx = PlannerQueryContext::for_state(&state);
        let trace_id = TraceId::generate();

        let result = execute_sql(
            &state,
            &query_ctx,
            &identity,
            DatabaseId::DEFAULT,
            "SELECT 1",
            trace_id,
            "198.51.100.7:54321",
        )
        .await;

        if let Err(error) = result {
            let message = error.to_string();
            assert!(
                !message.contains("IP blacklisted"),
                "peer outside the blacklisted range must not be rejected by the IP check, got: {message}"
            );
        }
    }

    fn kv_get_plan() -> crate::bridge::envelope::PhysicalPlan {
        crate::bridge::envelope::PhysicalPlan::Kv(nodedb_physical::physical_plan::KvOp::Get {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "widgets"),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        })
    }

    fn scope_for<'a>(
        identity: &'a AuthenticatedIdentity,
        state: &'a SharedState,
    ) -> RequestAuthScope<'a> {
        RequestAuthScope::for_database(identity, state.auth_stores(), DatabaseId::DEFAULT)
    }

    /// `meter_task` runs unconditionally after every dispatch branch in
    /// `execute_sql`, right after `results` updates — exercises the same
    /// enabled/disabled and row-count behavior every call site relies on.
    #[tokio::test]
    async fn meter_task_disabled_by_default_records_nothing() {
        let (state, _dir) = test_state().await;
        assert!(!state.metering_config.enabled, "default is disabled");
        let identity = regular_identity(9201);
        let scope = scope_for(&identity, &state);
        // `Some(...)` regardless of config, to prove `meter_dispatch`'s own
        // enabled-check protects this call, not only the caller's gate.
        let info = Some(PlanMeteringInfo::extract(&kv_get_plan()));
        let results = vec![serde_json::Value::Null; 3];

        meter_task(&state, &scope, &info, 0, &results);

        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    #[tokio::test]
    async fn meter_task_enabled_records_one_event_with_row_delta() {
        let (mut state, _dir) = test_state().await;
        std::sync::Arc::get_mut(&mut state)
            .expect("sole owner in test")
            .metering_config
            .enabled = true;
        let identity = regular_identity(9202);
        let scope = scope_for(&identity, &state);
        let info = metering_enabled_info(&state, &kv_get_plan());
        let results = vec![serde_json::Value::Null; 5];

        // rows_before = 2: this task contributed 3 of the 5 entries in `results`.
        meter_task(&state, &scope, &info, 2, &results);

        let events = state.usage_counter.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].collection, "widgets");
        assert_eq!(events[0].engine, "kv");
        let expected_cost = state
            .metering_config
            .operation_costs
            .get("kv_scan")
            .copied()
            .unwrap_or(1);
        assert_eq!(events[0].tokens, expected_cost * 3);
    }

    fn metering_enabled_info(
        state: &SharedState,
        plan: &crate::bridge::envelope::PhysicalPlan,
    ) -> Option<PlanMeteringInfo> {
        state
            .metering_config
            .enabled
            .then(|| PlanMeteringInfo::extract(plan))
    }
}
