// SPDX-License-Identifier: BUSL-1.1

//! SQL execution via the SPSC gateway for WebSocket RPC.

use std::sync::Arc;

use crate::control::gateway::core::QueryContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::authorization::{authorize_database, authorize_task_set};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId};

/// Execute SQL and return result as JSON.
///
/// Routes through the gateway when available (cluster-aware dispatch);
/// falls back to direct local SPSC dispatch on single-node boot before
/// the gateway is initialised.
pub async fn execute_sql(
    shared: &SharedState,
    query_ctx: &crate::control::planner::context::QueryContext,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
    trace_id: TraceId,
) -> crate::Result<serde_json::Value> {
    let tenant_id = identity.tenant_id;
    let emitter = ArcAuditEmitter(Arc::clone(&shared.audit));
    authorize_database(identity, database_id, &emitter)?;

    // Quota enforcement — reject before planning or dispatch.
    shared.check_tenant_quota(tenant_id)?;

    let mut auth_ctx = crate::control::server::session_auth::build_auth_context(identity);
    let clean_sql =
        crate::control::server::session_auth::extract_and_apply_on_deny(sql, &mut auth_ctx);
    let permission_cache = shared.permission_cache.read().await;
    let security = crate::control::planner::context::PlanSecurityContext {
        identity,
        auth: &auth_ctx,
        rls_store: &shared.rls,
        permissions: &shared.permissions,
        roles: &shared.roles,
        permission_cache: Some(&*permission_cache),
    };
    let (mut tasks, _output_schema) = query_ctx
        .plan_sql_with_rls(crate::control::planner::context::PlanSqlWithRlsParams {
            sql: &clean_sql,
            tenant_id,
            database_id,
            sec: &security,
        })
        .await?;
    drop(permission_cache);

    crate::control::planner::implicit_edges::append_implicit_edge_tasks(
        shared,
        &mut tasks,
        tenant_id,
        database_id,
        trace_id,
    )
    .await?;
    authorize_task_set(
        identity,
        &tasks,
        &shared.permissions,
        &shared.roles,
        &emitter,
    )?;

    shared.tenant_request_start(tenant_id);

    let mut results = Vec::new();
    for task in tasks {
        // `INSERT ... SELECT` is orchestrated on the Control Plane (fresh,
        // registered surrogate per target row + atomic `BatchInsert`), never
        // dispatched to the Data Plane as a single op.
        if let crate::bridge::envelope::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::InsertSelect {
                target_collection,
                source_collection,
                source_filters,
                source_limit,
            },
        ) = &task.plan
        {
            match crate::control::insert_select::run_insert_select(
                shared,
                task.tenant_id,
                task.database_id,
                target_collection,
                source_collection,
                source_filters,
                *source_limit,
            )
            .await
            {
                Ok(resp) => {
                    let payload = resp.payload.to_vec();
                    if !payload.is_empty() {
                        let json =
                            crate::data::executor::response_codec::decode_payload_to_json(&payload);
                        match sonic_rs::from_str::<serde_json::Value>(&json) {
                            Ok(v) => results.push(v),
                            Err(_) => results.push(serde_json::Value::String(json)),
                        }
                    }
                }
                Err(e) => {
                    shared.tenant_request_end(tenant_id);
                    return Err(e);
                }
            }
            continue;
        }

        // Autocommit `MERGE` is orchestrated on the Control Plane (fresh,
        // registered surrogate per NOT-MATCHED insert row + atomic apply),
        // never dispatched to the Data Plane as a single op.
        if let crate::bridge::envelope::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::Merge {
                target_collection,
                source_collection,
                source_alias,
                target_join_col,
                source_join_col,
                clauses,
                returning: _,
                resolve_only: false,
                resolved_inserts: None,
                source_rows: _,
            },
        ) = &task.plan
        {
            match crate::control::merge_orchestrator::run_merge(
                shared,
                crate::control::merge_orchestrator::MergeArgs {
                    tenant_id: task.tenant_id,
                    database_id: task.database_id,
                    target_collection,
                    source_collection,
                    source_alias,
                    target_join_col,
                    source_join_col,
                    clauses,
                },
            )
            .await
            {
                Ok(resp) => {
                    let payload = resp.payload.to_vec();
                    if !payload.is_empty() {
                        let json =
                            crate::data::executor::response_codec::decode_payload_to_json(&payload);
                        match sonic_rs::from_str::<serde_json::Value>(&json) {
                            Ok(v) => results.push(v),
                            Err(_) => results.push(serde_json::Value::String(json)),
                        }
                    }
                }
                Err(e) => {
                    shared.tenant_request_end(tenant_id);
                    return Err(e);
                }
            }
            continue;
        }

        // Autocommit `UPDATE ... FROM <source>` is orchestrated on the Control
        // Plane (source scanned on its own core + shipped into the plan), never
        // dispatched to the Data Plane as a single op reading a possibly-
        // non-resident source.
        if let crate::bridge::envelope::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
                target_collection,
                source_collection,
                source_alias,
                target_join_col,
                source_join_col,
                updates,
                target_filters,
                returning,
                resolve_only: false,
                source_rows: None,
            },
        ) = &task.plan
        {
            match crate::control::update_from_join_orchestrator::run_update_from_join(
                shared,
                crate::control::update_from_join_orchestrator::UpdateFromJoinArgs {
                    tenant_id: task.tenant_id,
                    database_id: task.database_id,
                    target_collection,
                    source_collection,
                    source_alias,
                    target_join_col,
                    source_join_col,
                    updates,
                    target_filters,
                    returning: returning.as_ref(),
                },
            )
            .await
            {
                Ok(resp) => {
                    let payload = resp.payload.to_vec();
                    if !payload.is_empty() {
                        let json =
                            crate::data::executor::response_codec::decode_payload_to_json(&payload);
                        match sonic_rs::from_str::<serde_json::Value>(&json) {
                            Ok(v) => results.push(v),
                            Err(_) => results.push(serde_json::Value::String(json)),
                        }
                    }
                }
                Err(e) => {
                    shared.tenant_request_end(tenant_id);
                    return Err(e);
                }
            }
            continue;
        }

        let payloads: crate::Result<Vec<Vec<u8>>> = match shared.gateway.as_ref() {
            Some(gw) => {
                let gw_ctx = QueryContext {
                    tenant_id: task.tenant_id,
                    trace_id,
                    database_id: task.database_id,
                };
                gw.execute(&gw_ctx, task.plan).await
            }
            None => {
                // Single-node boot: gateway not yet initialised — dispatch locally.
                crate::control::server::dispatch_utils::dispatch_to_data_plane(
                    shared,
                    task.tenant_id,
                    task.database_id,
                    task.vshard_id,
                    task.plan,
                    trace_id,
                )
                .await
                .map(|r| vec![r.payload.to_vec()])
            }
        };

        match payloads {
            Ok(vecs) => {
                for payload in vecs {
                    if !payload.is_empty() {
                        let json =
                            crate::data::executor::response_codec::decode_payload_to_json(&payload);
                        match sonic_rs::from_str::<serde_json::Value>(&json) {
                            Ok(v) => results.push(v),
                            Err(_) => results.push(serde_json::Value::String(json)),
                        }
                    }
                }
            }
            Err(e) => {
                shared.tenant_request_end(tenant_id);
                return Err(e);
            }
        }
    }

    shared.tenant_request_end(tenant_id);

    match results.len() {
        0 => Ok(serde_json::Value::Null),
        1 => Ok(results
            .into_iter()
            .next()
            .unwrap_or(serde_json::Value::Null)),
        _ => Ok(serde_json::Value::Array(results)),
    }
}
