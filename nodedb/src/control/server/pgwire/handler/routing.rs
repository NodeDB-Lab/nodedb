//! Query routing: consistency selection, leader detection, SQL forwarding,
//! and the execute_planned_sql entry point for DML/query dispatch.

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

        // Strip RETURNING clause before DataFusion planning (DataFusion doesn't
        // support RETURNING for DML). The flag is passed per-query through the
        // planning call chain — no shared mutable state.
        let (clean_sql, has_returning) = super::returning::strip_returning(&clean_sql);

        // Check plan cache before full planning. Cache key includes schema_version
        // for automatic invalidation on DDL. RLS policies and permissions are still
        // validated per-query after cache lookup — caching does not bypass security.
        let schema_ver = self.state.schema_version.current();
        let cached_tasks = self.sessions.get_cached_plan(addr, &clean_sql, schema_ver);

        let tasks = if let Some(tasks) = cached_tasks {
            tasks
        } else {
            let perm_cache = self.state.permission_cache.read().await;
            let sec = crate::control::planner::context::PlanSecurityContext {
                identity,
                auth: &auth_ctx,
                rls_store: &self.state.rls,
                permissions: &self.state.permissions,
                roles: &self.state.roles,
                permission_cache: Some(&*perm_cache),
            };
            let planned = self
                .query_ctx
                .plan_sql_with_rls_returning(&clean_sql, tenant_id, &sec, has_returning)
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;

            // Cache the result for future identical queries.
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
        let mut dedup_plan_kind = None;
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
                if row.is_empty() { None } else { Some(row) }
            } else {
                None
            };

            if let Some(ref info) = dml_info {
                use crate::control::trigger::dml_hook::PreDispatchResult;
                let result = crate::control::trigger::dml_hook::fire_pre_dispatch_triggers(
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

                match result {
                    PreDispatchResult::Handled => {
                        // INSTEAD OF trigger handled the write — skip dispatch.
                        responses.push(Response::Execution(Tag::new("OK")));
                        continue;
                    }
                    PreDispatchResult::Proceed {
                        mutated_fields: Some(ref fields),
                    } => {
                        // BEFORE trigger mutated the row — patch the task.
                        crate::control::trigger::dml_hook::patch_task_with_mutated_fields(
                            &mut task, fields,
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

            // --- TRUNCATE RESTART IDENTITY: reset sequences after successful truncate ---
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

                // Track DML for auto-ANALYZE threshold.
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
                dedup_plan_kind = Some(plan_kind);
                if dedup_set_op == PostSetOp::None {
                    dedup_set_op = resp_post_set_op;
                }
            } else {
                responses.push(payload_to_response(&resp.payload, plan_kind));
            }
        }

        // Set operations: merge sub-query payloads.
        if needs_set_op && !dedup_payloads.is_empty() {
            let kind = dedup_plan_kind.unwrap_or(PlanKind::MultiRow);
            let merged = match dedup_set_op {
                PostSetOp::Intersect | PostSetOp::IntersectAll => {
                    merge_set_op_payloads(&dedup_payloads, SetMergeMode::Intersect)
                }
                PostSetOp::Except | PostSetOp::ExceptAll => {
                    merge_set_op_payloads(&dedup_payloads, SetMergeMode::Except)
                }
                _ => dedup_union_payloads(&dedup_payloads),
            };
            responses.push(payload_to_response(&merged, kind));
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

/// Merge multiple Data Plane response payloads and deduplicate rows (UNION DISTINCT).
///
/// Each payload is a msgpack-encoded array of rows. Deduplication is performed
/// at the binary level: each row's raw msgpack bytes serve as the canonical key,
/// eliminating the decode → JSON string → re-encode round-trip.
///
/// Output: a single msgpack array containing all unique rows in encounter order.
fn dedup_union_payloads(payloads: &[Vec<u8>]) -> Vec<u8> {
    use nodedb_query::msgpack_scan;

    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    // Collect raw byte slices of unique rows before writing output.
    let mut unique_row_bytes: Vec<Vec<u8>> = Vec::new();

    for payload in payloads {
        if payload.is_empty() {
            continue;
        }

        let bytes = payload.as_slice();
        let first = bytes[0];

        // Decode msgpack array header to get element count and header length.
        let (count, hdr_len) = if (0x90..=0x9f).contains(&first) {
            ((first & 0x0f) as usize, 1)
        } else if first == 0xdc && bytes.len() >= 3 {
            (u16::from_be_bytes([bytes[1], bytes[2]]) as usize, 3)
        } else if first == 0xdd && bytes.len() >= 5 {
            (
                u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize,
                5,
            )
        } else {
            // Not a msgpack array — treat the whole payload as one row.
            tracing::warn!(
                payload_len = bytes.len(),
                "dedup_union_payloads: payload is not a msgpack array; treating as single row"
            );
            let key = bytes.to_vec();
            if seen.insert(key.clone()) {
                unique_row_bytes.push(key);
            }
            continue;
        };

        let mut pos = hdr_len;
        for _ in 0..count {
            if pos >= bytes.len() {
                break;
            }
            let elem_start = pos;
            match msgpack_scan::skip_value(bytes, pos) {
                Some(next_pos) => {
                    let row_bytes = bytes[elem_start..next_pos].to_vec();
                    if seen.insert(row_bytes.clone()) {
                        unique_row_bytes.push(row_bytes);
                    }
                    pos = next_pos;
                }
                None => {
                    tracing::warn!(
                        pos,
                        payload_len = bytes.len(),
                        "dedup_union_payloads: could not skip msgpack element; stopping early"
                    );
                    break;
                }
            }
        }
    }

    // Write output: msgpack array header + concatenated unique row bytes.
    let row_count = unique_row_bytes.len();
    let total_data: usize = unique_row_bytes.iter().map(|r| r.len()).sum();
    let mut out = Vec::with_capacity(total_data + 5);

    // Write array header.
    if row_count < 16 {
        out.push(0x90 | row_count as u8);
    } else if row_count <= u16::MAX as usize {
        out.push(0xdc);
        out.extend_from_slice(&(row_count as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&(row_count as u32).to_be_bytes());
    }

    for row in unique_row_bytes {
        out.extend_from_slice(&row);
    }

    out
}

enum SetMergeMode {
    Intersect,
    Except,
}

/// Merge payloads for INTERSECT or EXCEPT set operations.
///
/// For INTERSECT: keep rows that appear in ALL payloads.
/// For EXCEPT: keep rows from first payload that don't appear in any subsequent payload.
fn merge_set_op_payloads(payloads: &[Vec<u8>], mode: SetMergeMode) -> Vec<u8> {
    use nodedb_query::msgpack_scan;

    if payloads.is_empty() {
        return vec![0x90]; // empty msgpack array
    }

    // Extract rows as byte slices from each payload.
    fn extract_rows(payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.is_empty() {
            return Vec::new();
        }
        let first = payload[0];
        let (count, hdr_len) = if (0x90..=0x9f).contains(&first) {
            ((first & 0x0f) as usize, 1)
        } else if first == 0xdc && payload.len() >= 3 {
            (u16::from_be_bytes([payload[1], payload[2]]) as usize, 3)
        } else if first == 0xdd && payload.len() >= 5 {
            (
                u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]) as usize,
                5,
            )
        } else {
            return vec![payload.to_vec()];
        };

        let mut rows = Vec::with_capacity(count);
        let mut pos = hdr_len;
        for _ in 0..count {
            if pos >= payload.len() {
                break;
            }
            let start = pos;
            match msgpack_scan::skip_value(payload, pos) {
                Some(next) => {
                    rows.push(payload[start..next].to_vec());
                    pos = next;
                }
                None => break,
            }
        }
        rows
    }

    fn write_array_header(out: &mut Vec<u8>, count: usize) {
        if count < 16 {
            out.push(0x90 | count as u8);
        } else if count <= u16::MAX as usize {
            out.push(0xdc);
            out.extend_from_slice(&(count as u16).to_be_bytes());
        } else {
            out.push(0xdd);
            out.extend_from_slice(&(count as u32).to_be_bytes());
        }
    }

    // Scan payloads wrap logical SQL rows as {"id": "...", "data": {...}}.
    // Set operations should compare and return the projected row in `data`,
    // not the outer document wrapper.
    fn logical_row_bytes(row: &[u8]) -> &[u8] {
        msgpack_scan::extract_field(row, 0, "data")
            .map(|(start, end)| &row[start..end])
            .unwrap_or(row)
    }

    fn write_values_only_key(value: &[u8], out: &mut Vec<u8>) -> Option<()> {
        if let Some((count, mut pos)) = msgpack_scan::map_header(value, 0) {
            write_array_header(out, count);
            for _ in 0..count {
                pos = msgpack_scan::skip_value(value, pos)?;
                let val_start = pos;
                pos = msgpack_scan::skip_value(value, pos)?;
                write_values_only_key(&value[val_start..pos], out)?;
            }
            return Some(());
        }

        if let Some((count, mut pos)) = msgpack_scan::array_header(value, 0) {
            write_array_header(out, count);
            for _ in 0..count {
                let elem_start = pos;
                pos = msgpack_scan::skip_value(value, pos)?;
                write_values_only_key(&value[elem_start..pos], out)?;
            }
            return Some(());
        }

        out.extend_from_slice(value);
        Some(())
    }

    fn extract_value_parts(row: &[u8]) -> Vec<Vec<u8>> {
        let logical = logical_row_bytes(row);

        if let Some((count, mut pos)) = msgpack_scan::map_header(logical, 0) {
            let mut parts = Vec::with_capacity(count);
            for _ in 0..count {
                pos = match msgpack_scan::skip_value(logical, pos) {
                    Some(next) => next,
                    None => return vec![logical.to_vec()],
                };
                let val_start = pos;
                pos = match msgpack_scan::skip_value(logical, pos) {
                    Some(next) => next,
                    None => return vec![logical.to_vec()],
                };
                let mut normalized = Vec::with_capacity(pos - val_start);
                if write_values_only_key(&logical[val_start..pos], &mut normalized).is_none() {
                    return vec![logical.to_vec()];
                }
                parts.push(normalized);
            }
            return parts;
        }

        if let Some((count, mut pos)) = msgpack_scan::array_header(logical, 0) {
            let mut parts = Vec::with_capacity(count);
            for _ in 0..count {
                let elem_start = pos;
                pos = match msgpack_scan::skip_value(logical, pos) {
                    Some(next) => next,
                    None => return vec![logical.to_vec()],
                };
                let mut normalized = Vec::with_capacity(pos - elem_start);
                if write_values_only_key(&logical[elem_start..pos], &mut normalized).is_none() {
                    return vec![logical.to_vec()];
                }
                parts.push(normalized);
            }
            return parts;
        }

        vec![logical.to_vec()]
    }

    // Extract only values (no keys) from the logical row for comparison.
    // This makes INTERSECT/EXCEPT compare on projected column positions rather
    // than wrapper keys or collection-specific field names.
    fn extract_values_key(row: &[u8]) -> Vec<u8> {
        let parts = extract_value_parts(row);
        let mut vals = Vec::new();
        write_array_header(&mut vals, parts.len());
        for part in parts {
            vals.extend_from_slice(&part);
        }
        vals
    }

    fn rows_match(left: &[u8], right: &[u8]) -> bool {
        let left_parts = extract_value_parts(left);
        let right_parts = extract_value_parts(right);
        let shared_len = left_parts.len().min(right_parts.len());

        if shared_len == 0 {
            return left_parts.is_empty() && right_parts.is_empty();
        }

        left_parts[..shared_len] == right_parts[..shared_len]
            && (left_parts.len() == shared_len || right_parts.len() == shared_len)
    }

    let first_rows = extract_rows(&payloads[0]);
    let mut result_rows: Vec<Vec<u8>> = match mode {
        SetMergeMode::Intersect => {
            // Keep rows from first that appear in ALL other payloads (by logical values).
            let other_rows: Vec<Vec<Vec<u8>>> =
                payloads[1..].iter().map(|p| extract_rows(p)).collect();
            first_rows
                .into_iter()
                .filter(|row| {
                    other_rows
                        .iter()
                        .all(|rows| rows.iter().any(|other| rows_match(row, other)))
                })
                .map(|row| logical_row_bytes(&row).to_vec())
                .collect()
        }
        SetMergeMode::Except => {
            // Keep rows from first that don't appear in ANY other payload (by logical values).
            let other_rows: Vec<Vec<u8>> =
                payloads[1..].iter().flat_map(|p| extract_rows(p)).collect();
            first_rows
                .into_iter()
                .filter(|row| !other_rows.iter().any(|other| rows_match(row, other)))
                .map(|row| logical_row_bytes(&row).to_vec())
                .collect()
        }
    };

    // Deduplicate result by values (not full row bytes, since keys differ).
    let mut seen = std::collections::HashSet::new();
    result_rows.retain(|r| seen.insert(extract_values_key(r)));

    // Write msgpack array.
    let row_count = result_rows.len();
    let total: usize = result_rows.iter().map(|r| r.len()).sum();
    let mut out = Vec::with_capacity(total + 5);
    if row_count < 16 {
        out.push(0x90 | row_count as u8);
    } else if row_count <= u16::MAX as usize {
        out.push(0xdc);
        out.extend_from_slice(&(row_count as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&(row_count as u32).to_be_bytes());
    }
    for row in result_rows {
        out.extend_from_slice(&row);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{SetMergeMode, merge_set_op_payloads};

    fn encode_array(rows: &[serde_json::Value]) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::Value::Array(rows.to_vec())).unwrap()
    }

    #[test]
    fn intersect_compares_wrapped_rows_by_logical_data_values() {
        let left = encode_array(&[
            serde_json::json!({"id":"u1","data":{"id":"u1","name":"Alice"}}),
            serde_json::json!({"id":"u2","data":{"id":"u2","name":"Bob"}}),
        ]);
        let right = encode_array(&[
            serde_json::json!({"id":"doc-1","data":{"user_id":"u1"}}),
            serde_json::json!({"id":"doc-2","data":{"user_id":"u3"}}),
        ]);

        let merged = merge_set_op_payloads(&[left, right], SetMergeMode::Intersect);
        let json = crate::data::executor::response_codec::decode_payload_to_json(&merged);

        assert_eq!(json, r#"[{"id":"u1","name":"Alice"}]"#);
    }

    #[test]
    fn except_returns_unwrapped_logical_rows() {
        let left = encode_array(&[
            serde_json::json!({"id":"u1","data":{"id":"u1"}}),
            serde_json::json!({"id":"u2","data":{"id":"u2"}}),
        ]);
        let right = encode_array(&[serde_json::json!({"id":"doc-1","data":{"user_id":"u1"}})]);

        let merged = merge_set_op_payloads(&[left, right], SetMergeMode::Except);
        let json = crate::data::executor::response_codec::decode_payload_to_json(&merged);

        assert_eq!(json, r#"[{"id":"u2"}]"#);
    }
}
