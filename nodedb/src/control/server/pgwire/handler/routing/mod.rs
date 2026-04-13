//! Query routing: consistency selection, leader detection, SQL forwarding,
//! and the execute_planned_sql entry point for DML/query dispatch.

mod check_enforcement;
mod set_ops;

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::planner::physical::{PhysicalTask, PostSetOp};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::types::{ReadConsistency, TenantId};

use super::super::types::{error_to_sqlstate, response_status_to_sqlstate};
use super::core::NodeDbPgHandler;
use super::plan::{PlanKind, describe_plan, extract_collection, payload_to_response};

impl NodeDbPgHandler {
    /// Plan and dispatch SQL after quota and DDL checks have passed.
    ///
    /// When in a transaction block (BEGIN..COMMIT), write operations are
    /// buffered instead of dispatched. Read operations execute immediately.
    /// The buffer is dispatched atomically on COMMIT.
    pub(super) async fn execute_planned_sql(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        self.execute_planned_sql_inner(identity, sql, tenant_id, addr, &[])
            .await
    }

    /// Execute planned SQL with bound parameters (prepared statement path).
    pub(super) async fn execute_planned_sql_with_params(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
        params: &[nodedb_sql::ParamValue],
    ) -> PgWireResult<Vec<Response>> {
        self.execute_planned_sql_inner(identity, sql, tenant_id, addr, params)
            .await
    }

    async fn execute_planned_sql_inner(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
        params: &[nodedb_sql::ParamValue],
    ) -> PgWireResult<Vec<Response>> {
        // Resolve opaque session handle if SET LOCAL nodedb.auth_session is set.
        let mut auth_ctx = if let Some(handle) =
            self.sessions.get_parameter(addr, "nodedb.auth_session")
            && let Some(cached) = self.state.session_handles.resolve(&handle)
        {
            cached
        } else {
            crate::control::server::session_auth::build_auth_context_with_session(
                identity,
                &self.sessions,
                addr,
            )
        };

        // Extract per-query ON DENY override.
        let clean_sql =
            crate::control::server::session_auth::extract_and_apply_on_deny(sql, &mut auth_ctx);

        // Strip RETURNING clause before DataFusion planning.
        let (clean_sql, has_returning) = super::returning::strip_returning(&clean_sql);

        // Enforce general CHECK constraints for INSERT/UPDATE before planning.
        self.enforce_check_constraints_if_needed(&clean_sql, tenant_id)
            .await?;

        // Check plan cache before full planning.
        let schema_ver = self.state.schema_version.current();
        let cached_tasks = self.sessions.get_cached_plan(addr, &clean_sql, schema_ver);

        let tasks = if !params.is_empty() {
            let perm_cache = self.state.permission_cache.read().await;
            let sec = crate::control::planner::context::PlanSecurityContext {
                identity,
                auth: &auth_ctx,
                rls_store: &self.state.rls,
                permissions: &self.state.permissions,
                roles: &self.state.roles,
                permission_cache: Some(&*perm_cache),
            };
            self.query_ctx
                .plan_sql_with_params_and_rls(&clean_sql, params, tenant_id, &sec)
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?
        } else if let Some(tasks) = cached_tasks {
            tasks
        } else {
            // Retry transparently on `RetryableSchemaChanged`
            // so pgwire clients see a stable view of DDL in
            // flight. The retry helper re-runs the entire plan,
            // which re-acquires all needed descriptor leases
            // (drain may have cleared by the next attempt). Each
            // iteration re-reads the permission cache because the
            // RwLock guard cannot be held across awaits inside
            // the closure.
            let planned = super::retry::retry_on_schema_change(|| async {
                let perm_cache = self.state.permission_cache.read().await;
                let sec = crate::control::planner::context::PlanSecurityContext {
                    identity,
                    auth: &auth_ctx,
                    rls_store: &self.state.rls,
                    permissions: &self.state.permissions,
                    roles: &self.state.roles,
                    permission_cache: Some(&*perm_cache),
                };
                self.query_ctx
                    .plan_sql_with_rls_returning(&clean_sql, tenant_id, &sec, has_returning)
                    .await
            })
            .await
            .map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

            self.sessions
                .put_cached_plan(addr, &clean_sql, planned.clone(), schema_ver);

            planned
        };

        if tasks.is_empty() {
            return Ok(vec![Response::Execution(Tag::new("OK"))]);
        }

        let consistency = self.consistency_for_tasks(&tasks);

        if let Some(leader) = self.remote_leader_for_tasks(&tasks, consistency) {
            return self.forward_sql(sql, tenant_id, leader).await;
        }

        let needs_set_op = tasks.iter().any(|t| t.post_set_op != PostSetOp::None);
        let mut dedup_payloads: Vec<Vec<u8>> = Vec::new();
        let mut dedup_set_op = PostSetOp::None;
        let mut responses = Vec::with_capacity(tasks.len());
        for mut task in tasks {
            if task.tenant_id != tenant_id {
                tracing::error!(
                    expected = %tenant_id,
                    actual = %task.tenant_id,
                    "SECURITY: task tenant_id mismatch — rejecting"
                );
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42501".to_owned(),
                    "tenant isolation violation: task targets wrong tenant".to_owned(),
                ))));
            }

            self.check_permission(identity, &task.plan)?;

            if self.sessions.transaction_state(addr)
                == crate::control::server::pgwire::session::TransactionState::InBlock
            {
                let is_write = crate::control::wal_replication::to_replicated_entry(
                    task.tenant_id,
                    task.vshard_id,
                    &task.plan,
                )
                .is_some();
                if is_write {
                    self.sessions.buffer_write(addr, task);
                    responses.push(Response::Execution(Tag::new("OK")));
                    continue;
                }
            }

            let plan_kind = describe_plan(&task.plan);
            let collection_for_si = extract_collection(&task.plan).map(String::from);
            let resp_post_set_op = task.post_set_op;

            // --- Trigger interception for DML writes ---
            let dml_info = crate::control::trigger::dml_hook::classify_dml_write(&task.plan);

            // Fetch OLD row and fire BEFORE/INSTEAD OF triggers if applicable.
            let old_row = if let Some(ref info) = dml_info
                && info.document_id.is_some()
                && matches!(
                    info.event,
                    crate::control::trigger::DmlEvent::Update
                        | crate::control::trigger::DmlEvent::Delete
                ) {
                let doc_id = info.document_id.as_deref().unwrap_or("");
                let row = crate::control::trigger::dml_hook::fetch_old_row(
                    &self.state,
                    tenant_id,
                    &info.collection,
                    doc_id,
                )
                .await;
                if !row.is_empty() { Some(row) } else { None }
            } else {
                None
            };

            if let Some(ref info) = dml_info {
                use crate::control::trigger::dml_hook::PreDispatchResult;
                match crate::control::trigger::dml_hook::fire_pre_dispatch_triggers(
                    &self.state,
                    identity,
                    tenant_id,
                    info,
                    &old_row,
                    0,
                )
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })? {
                    PreDispatchResult::Handled => {
                        responses.push(Response::Execution(Tag::new("OK")));
                        continue;
                    }
                    PreDispatchResult::Proceed {
                        mutated_fields: Some(fields),
                    } => {
                        crate::control::trigger::dml_hook::patch_task_with_mutated_fields(
                            &mut task, &fields,
                        );
                    }
                    PreDispatchResult::Proceed {
                        mutated_fields: None,
                    } => {}
                }
            }

            // Extract truncate restart_identity info before task is moved into dispatch.
            let truncate_restart_collection =
                if let crate::bridge::physical_plan::PhysicalPlan::Document(
                    crate::bridge::physical_plan::DocumentOp::Truncate {
                        collection,
                        restart_identity: true,
                    },
                ) = &task.plan
                {
                    Some(collection.clone())
                } else {
                    None
                };

            // --- Normal dispatch ---
            let resp = self.dispatch_task(task).await.map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

            if let Some((severity, code, message)) =
                response_status_to_sqlstate(resp.status, &resp.error_code)
            {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                ))));
            }

            // --- TRUNCATE RESTART IDENTITY ---
            if let Some(collection) = &truncate_restart_collection {
                self.state
                    .sequence_registry
                    .restart_sequences_for_collection(tenant_id.as_u32(), collection);
            }

            // --- SYNC AFTER triggers ---
            if let Some(ref info) = dml_info {
                crate::control::trigger::dml_hook::fire_post_dispatch_triggers(
                    &self.state,
                    identity,
                    tenant_id,
                    info,
                    &old_row,
                    0,
                )
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;

                self.state
                    .dml_counter
                    .record_dml(tenant_id.as_u32(), &info.collection);
            }

            // Track reads for snapshot isolation conflict detection.
            if self.sessions.transaction_state(addr)
                == crate::control::server::pgwire::session::TransactionState::InBlock
                && let Some(collection) = collection_for_si
            {
                self.sessions
                    .record_read(addr, collection, String::new(), resp.watermark_lsn);
            }

            if needs_set_op && resp_post_set_op != PostSetOp::None {
                dedup_payloads.push(resp.payload.to_vec());
                if dedup_set_op == PostSetOp::None {
                    dedup_set_op = resp_post_set_op;
                }
            } else {
                responses.push(payload_to_response(&resp.payload, plan_kind));
            }
        }

        // Set operations: merge sub-query payloads.
        if needs_set_op && !dedup_payloads.is_empty() {
            responses.push(set_ops::apply_set_ops(&dedup_payloads, dedup_set_op));
        }

        Ok(responses)
    }

    /// Determine read consistency for a set of tasks.
    fn consistency_for_tasks(&self, tasks: &[PhysicalTask]) -> ReadConsistency {
        let has_writes = tasks.iter().any(|t| {
            crate::control::wal_replication::to_replicated_entry(t.tenant_id, t.vshard_id, &t.plan)
                .is_some()
        });

        if has_writes {
            ReadConsistency::Strong
        } else {
            ReadConsistency::BoundedStaleness(std::time::Duration::from_secs(5))
        }
    }

    /// Check if all tasks target a single remote leader.
    fn remote_leader_for_tasks(
        &self,
        tasks: &[PhysicalTask],
        consistency: ReadConsistency,
    ) -> Option<u64> {
        let routing = self.state.cluster_routing.as_ref()?;
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        let my_node = self.state.node_id;

        let mut remote_leader: Option<u64> = None;

        for task in tasks {
            let vshard_id = task.vshard_id.as_u16();
            let group_id = routing.group_for_vshard(vshard_id).ok()?;
            let info = routing.group_info(group_id)?;
            let leader = info.leader;

            if leader == my_node {
                return None;
            }
            if !consistency.requires_leader() && info.members.contains(&my_node) {
                return None;
            }
            if leader == 0 {
                return None;
            }

            match remote_leader {
                None => remote_leader = Some(leader),
                Some(prev) if prev != leader => return None,
                _ => {}
            }
        }

        remote_leader
    }

    /// Forward a SQL query to a remote leader node via QUIC.
    async fn forward_sql(
        &self,
        sql: &str,
        tenant_id: TenantId,
        leader: u64,
    ) -> PgWireResult<Vec<Response>> {
        let transport = match &self.state.cluster_transport {
            Some(t) => t,
            None => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "55000".to_owned(),
                    "cluster transport not available".to_owned(),
                ))));
            }
        };

        let req = nodedb_cluster::rpc_codec::RaftRpc::ForwardRequest(
            nodedb_cluster::rpc_codec::ForwardRequest {
                sql: sql.to_owned(),
                tenant_id: tenant_id.as_u32(),
                deadline_remaining_ms: std::time::Duration::from_secs(
                    self.state.tuning.network.default_deadline_secs,
                )
                .as_millis() as u64,
                trace_id: 0,
            },
        );

        let leader_addr = self
            .state
            .cluster_topology
            .as_ref()
            .and_then(|t| {
                let topo = t.read().unwrap_or_else(|p| p.into_inner());
                topo.get_node(leader).map(|n| n.addr.clone())
            })
            .unwrap_or_else(|| format!("node-{leader}"));

        let resp = transport.send_rpc(leader, req).await.map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "01R01".to_owned(),
                format!("not leader; redirect to {leader_addr} (forward failed: {e})"),
            )))
        })?;

        match resp {
            nodedb_cluster::rpc_codec::RaftRpc::ForwardResponse(fwd) => {
                if !fwd.success {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!("remote execution failed: {}", fwd.error_message),
                    ))));
                }

                let mut responses = Vec::with_capacity(fwd.payloads.len());
                for payload in &fwd.payloads {
                    responses.push(payload_to_response(payload, PlanKind::MultiRow));
                }
                if responses.is_empty() {
                    responses.push(Response::Execution(Tag::new("OK")));
                }
                Ok(responses)
            }
            other => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("unexpected response from leader: {other:?}"),
            )))),
        }
    }
}
