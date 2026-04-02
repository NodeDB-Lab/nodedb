//! Main SQL execution entry point with transaction handling and DDL dispatch.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;

use super::super::session::TransactionState;
use super::super::types::text_field;
use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Execute a SQL query: session state → identity → DDL check → quota → plan → perms → dispatch.
    pub(super) async fn execute_sql(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        use super::super::types::error_to_sqlstate;

        let sql_trimmed = sql.trim();
        let upper = sql_trimmed.to_uppercase();

        self.sessions.ensure_session(*addr);

        if sql_trimmed.is_empty() || sql_trimmed == ";" {
            return Ok(vec![Response::EmptyQuery]);
        }

        if upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper == "START TRANSACTION" {
            // Capture current WAL LSN as the snapshot point for this transaction.
            let snapshot_lsn = {
                let next = self.state.wal.next_lsn();
                crate::types::Lsn::new(next.as_u64().saturating_sub(1))
            };
            self.sessions.begin(addr, snapshot_lsn).map_err(|msg| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "25P02".to_owned(),
                    msg.to_owned(),
                )))
            })?;
            return Ok(vec![Response::Execution(Tag::new("BEGIN"))]);
        }

        if upper == "COMMIT" || upper == "END" || upper == "END TRANSACTION" {
            // Snapshot isolation: check for write conflicts before committing.
            // If any collection read during this transaction has been modified
            // since the snapshot LSN, reject with serialization failure.
            let read_set = self.sessions.take_read_set(addr);
            if let Some(snapshot_lsn) = self.sessions.snapshot_lsn(addr) {
                let current_lsn = self.state.wal.next_lsn();
                let current = crate::types::Lsn::new(current_lsn.as_u64().saturating_sub(1));
                for (_collection, _doc_id, read_lsn) in &read_set {
                    if current > *read_lsn && current > snapshot_lsn {
                        // WAL advanced past what we read — concurrent write detected.
                        // Rollback GAP_FREE reservations held by this transaction.
                        if let Ok(reservations) = self.sessions.rollback(addr) {
                            for handle in &reservations {
                                let key = handle.sequence_key.clone();
                                let registry = &self.state.sequence_registry;
                                registry.gap_free_manager().rollback(handle, || {
                                    let map = registry.sequences_read();
                                    if let Some(h) = map.get(&key) {
                                        h.rollback_one();
                                    }
                                });
                            }
                        }
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "40001".to_owned(),
                            "could not serialize access due to concurrent update".to_owned(),
                        ))));
                    }
                }
            }

            let buffered = self.sessions.commit(addr).map_err(|msg| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "25000".to_owned(),
                    msg.to_owned(),
                )))
            })?;

            if !buffered.is_empty() {
                let tenant_id = identity.tenant_id;
                let vshard_id = buffered[0].vshard_id;

                let mut sub_records: Vec<(u16, Vec<u8>)> = Vec::with_capacity(buffered.len());
                for task in &buffered {
                    if let Some(entry) = crate::control::wal_replication::to_replicated_entry(
                        task.tenant_id,
                        task.vshard_id,
                        &task.plan,
                    ) {
                        let bytes = entry.to_bytes();
                        sub_records.push((nodedb_wal::record::RecordType::Put as u16, bytes));
                    }
                }

                if !sub_records.is_empty() {
                    let tx_payload = rmp_serde::to_vec_named(&sub_records).map_err(|e| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "XX000".to_owned(),
                            format!("transaction WAL serialization failed: {e}"),
                        )))
                    })?;
                    self.state
                        .wal
                        .append_transaction(tenant_id, vshard_id, &tx_payload)
                        .map_err(|e| {
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                "ERROR".to_owned(),
                                "XX000".to_owned(),
                                format!("transaction WAL append failed: {e}"),
                            )))
                        })?;
                }

                // Dispatch all writes as a single TransactionBatch for
                // atomic execution on the Data Plane.
                let plans: Vec<crate::bridge::envelope::PhysicalPlan> =
                    buffered.iter().map(|t| t.plan.clone()).collect();
                let batch_task = crate::control::planner::physical::PhysicalTask {
                    tenant_id,
                    vshard_id,
                    plan: crate::bridge::envelope::PhysicalPlan::Meta(
                        crate::bridge::physical_plan::MetaOp::TransactionBatch { plans },
                    ),
                };
                if let Err(e) = self.dispatch_task_no_wal(batch_task).await {
                    tracing::warn!(error = %e, "transaction batch dispatch failed");
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "40001".to_owned(),
                        format!("transaction commit failed: {e}"),
                    ))));
                }
            }

            // Flush pending offset commits (deferred from COMMIT OFFSET inside transaction).
            let pending_offsets = self.sessions.take_pending_offsets(addr);
            for (tid, stream, group, partition_id, lsn) in pending_offsets {
                if let Err(e) =
                    self.state
                        .offset_store
                        .commit_offset(tid, &stream, &group, partition_id, lsn)
                {
                    tracing::warn!(
                        stream = %stream,
                        group = %group,
                        partition = partition_id,
                        error = %e,
                        "failed to commit deferred offset"
                    );
                }
            }

            // Finalize GAP_FREE reservations (numbers become permanent).
            let reservations = self.sessions.take_pending_reservations(addr);
            for handle in &reservations {
                self.state
                    .sequence_registry
                    .gap_free_manager()
                    .commit(handle);
            }

            // Close non-WITH-HOLD cursors on transaction end.
            self.sessions.close_non_hold_cursors(addr);
            return Ok(vec![Response::Execution(Tag::new("COMMIT"))]);
        }

        if upper == "ROLLBACK" || upper == "ABORT" {
            // Rollback GAP_FREE reservations (numbers recycled).
            let reservations = self.sessions.rollback(addr).unwrap_or_default();
            for handle in &reservations {
                let key = &handle.sequence_key;
                let registry = &self.state.sequence_registry;
                registry.gap_free_manager().rollback(handle, || {
                    let map = registry.sequences_read();
                    if let Some(h) = map.get(key.as_str()) {
                        h.rollback_one();
                    }
                });
            }
            // Close non-WITH-HOLD cursors on transaction end.
            self.sessions.close_non_hold_cursors(addr);
            return Ok(vec![Response::Execution(Tag::new("ROLLBACK"))]);
        }

        // Transactional consumption: if COMMIT OFFSET is inside a transaction,
        // defer the offset commit until the transaction's COMMIT.
        if (upper.starts_with("COMMIT OFFSET ") || upper.starts_with("COMMIT OFFSETS "))
            && self.sessions.transaction_state(addr)
                == crate::control::server::pgwire::session::TransactionState::InBlock
        {
            let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
            let tenant_id = identity.tenant_id.as_u32();

            // Parse single-partition: COMMIT OFFSET PARTITION <p> AT <lsn> ON <stream> CONSUMER GROUP <name>
            if parts.len() >= 11
                && parts[2].eq_ignore_ascii_case("PARTITION")
                && parts[4].eq_ignore_ascii_case("AT")
                && parts[6].eq_ignore_ascii_case("ON")
            {
                let partition_id: u16 = parts[3].parse().unwrap_or(0);
                let lsn: u64 = parts[5].parse().unwrap_or(0);
                let stream_name = parts[7].to_lowercase();
                let group_name = parts[10].to_lowercase();
                self.sessions.defer_offset_commit(
                    addr,
                    tenant_id,
                    stream_name,
                    group_name,
                    partition_id,
                    lsn,
                );
                return Ok(vec![Response::Execution(Tag::new("COMMIT OFFSET"))]);
            }

            // Batch: COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>
            if parts.len() >= 7
                && parts[1].eq_ignore_ascii_case("OFFSETS")
                && parts[2].eq_ignore_ascii_case("ON")
            {
                let stream_name = parts[3].to_lowercase();
                let group_name = parts[6].to_lowercase();
                // Read current latest LSNs from the buffer and defer them.
                if let Some(buffer) = self.state.cdc_router.get_buffer(tenant_id, &stream_name) {
                    let events = buffer.read_from_lsn(0, usize::MAX);
                    let mut latest: std::collections::HashMap<u16, u64> =
                        std::collections::HashMap::new();
                    for e in &events {
                        let entry = latest.entry(e.partition).or_insert(0);
                        if e.lsn > *entry {
                            *entry = e.lsn;
                        }
                    }
                    for (pid, lsn) in latest {
                        self.sessions.defer_offset_commit(
                            addr,
                            tenant_id,
                            stream_name.clone(),
                            group_name.clone(),
                            pid,
                            lsn,
                        );
                    }
                }
                return Ok(vec![Response::Execution(Tag::new("COMMIT OFFSETS"))]);
            }
        }

        if upper.starts_with("SAVEPOINT ") {
            let sp_name = sql_trimmed
                .split_whitespace()
                .nth(1)
                .unwrap_or("sp")
                .to_string();
            self.sessions.create_savepoint(addr, sp_name);
            return Ok(vec![Response::Execution(Tag::new("SAVEPOINT"))]);
        }

        if upper.starts_with("RELEASE SAVEPOINT ") || upper.starts_with("RELEASE ") {
            let sp_name = sql_trimmed
                .split_whitespace()
                .last()
                .unwrap_or("sp")
                .to_string();
            self.sessions.release_savepoint(addr, &sp_name);
            return Ok(vec![Response::Execution(Tag::new("RELEASE"))]);
        }

        // DECLARE cursor_name CURSOR FOR SELECT ...
        // DECLARE [name] [SCROLL | NO SCROLL] CURSOR [WITH HOLD] FOR query
        if upper.starts_with("DECLARE ") && upper.contains(" CURSOR ") {
            let scrollable =
                upper.contains(" SCROLL CURSOR") && !upper.contains(" NO SCROLL CURSOR");
            let with_hold = upper.contains(" WITH HOLD ");
            let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
            let cursor_name = parts.get(1).unwrap_or(&"default").to_string();
            if let Some(for_pos) = upper.find(" FOR ") {
                let inner_sql = sql_trimmed[for_pos + 5..].trim();
                match self
                    .execute_query_for_cursor(addr, inner_sql, identity)
                    .await
                {
                    Ok(rows) => {
                        // Enforce cursor memory limit.
                        let spill_config =
                            super::super::session::cursor_spill::CursorSpillConfig::default();
                        let (rows, _truncated) =
                            super::super::session::cursor_spill::enforce_cursor_limit(
                                rows,
                                &spill_config,
                            );

                        self.sessions.declare_cursor(
                            addr,
                            cursor_name,
                            rows,
                            scrollable,
                            with_hold,
                        );
                        return Ok(vec![Response::Execution(Tag::new("DECLARE CURSOR"))]);
                    }
                    Err(e) => return Err(e),
                }
            }
            return Ok(vec![Response::Execution(Tag::new("DECLARE CURSOR"))]);
        }

        // FETCH [ALL | FORWARD n | BACKWARD n | n] FROM cursor_name
        if upper.starts_with("FETCH ") {
            return self.handle_fetch(addr, sql_trimmed, &upper);
        }

        // MOVE [FORWARD | BACKWARD] n IN cursor_name
        if upper.starts_with("MOVE ") {
            return self.handle_move(addr, &upper);
        }

        // CLOSE cursor_name
        if upper.starts_with("CLOSE ") {
            let cursor_name = sql_trimmed
                .split_whitespace()
                .nth(1)
                .unwrap_or("default")
                .to_string();
            self.sessions.close_cursor(addr, &cursor_name);
            return Ok(vec![Response::Execution(Tag::new("CLOSE CURSOR"))]);
        }

        if upper.starts_with("ROLLBACK TO ") {
            let sp_name = sql_trimmed
                .split_whitespace()
                .last()
                .unwrap_or("sp")
                .to_string();
            if let Err(msg) = self.sessions.rollback_to_savepoint(addr, &sp_name) {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "3B001".to_owned(),
                    msg.to_string(),
                ))));
            }
            return Ok(vec![Response::Execution(Tag::new("ROLLBACK"))]);
        }

        if self.sessions.transaction_state(addr) == TransactionState::Failed {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25P02".to_owned(),
                "current transaction is aborted, commands ignored until end of transaction block"
                    .to_owned(),
            ))));
        }

        if upper.starts_with("SET ") {
            return self.handle_set(addr, sql_trimmed);
        }

        if upper == "SHOW CONNECTIONS" {
            let schema = Arc::new(vec![
                text_field("peer_address"),
                text_field("transaction_state"),
            ]);
            let sessions = self.sessions.all_sessions();
            let mut rows = Vec::with_capacity(sessions.len());
            let mut encoder = DataRowEncoder::new(schema.clone());
            for (addr_str, tx_state) in &sessions {
                encoder.encode_field(addr_str)?;
                encoder.encode_field(tx_state)?;
                rows.push(Ok(encoder.take_row()));
            }
            return Ok(vec![Response::Query(QueryResponse::new(
                schema,
                futures::stream::iter(rows),
            ))]);
        }

        if upper.starts_with("KILL CONNECTION ") {
            if !identity.is_superuser {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42501".to_owned(),
                    "permission denied: only superuser can kill connections".to_owned(),
                ))));
            }
            let target = sql_trimmed[16..]
                .trim()
                .trim_matches('\'')
                .trim_matches('"');
            if let Ok(target_addr) = target.parse::<std::net::SocketAddr>() {
                self.sessions.remove(&target_addr);
                return Ok(vec![Response::Execution(Tag::new("KILL"))]);
            }
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                format!("invalid connection address: '{target}'. Use SHOW CONNECTIONS to list."),
            ))));
        }

        if upper.starts_with("SHOW ")
            && !upper.starts_with("SHOW USERS")
            && !upper.starts_with("SHOW TENANTS")
            && !upper.starts_with("SHOW SESSION")
            && !upper.starts_with("SHOW CLUSTER")
            && !upper.starts_with("SHOW RAFT")
            && !upper.starts_with("SHOW MIGRATIONS")
            && !upper.starts_with("SHOW PEER")
            && !upper.starts_with("SHOW NODES")
            && !upper.starts_with("SHOW NODE ")
            && !upper.starts_with("SHOW COLLECTIONS")
            && !upper.starts_with("SHOW AUDIT")
            && !upper.starts_with("SHOW PERMISSIONS")
            && !upper.starts_with("SHOW GRANTS")
            && upper != "SHOW CONNECTIONS"
            && !upper.starts_with("SHOW INDEXES")
        {
            return self.handle_show(addr, sql_trimmed);
        }

        if upper.starts_with("RESET ") {
            let param = sql_trimmed[6..].trim().to_lowercase();
            self.sessions.set_parameter(addr, param, String::new());
            return Ok(vec![Response::Execution(Tag::new("RESET"))]);
        }

        if upper == "DISCARD ALL" {
            self.sessions.remove(addr);
            self.sessions.ensure_session(*addr);
            return Ok(vec![Response::Execution(Tag::new("DISCARD ALL"))]);
        }

        // SQL-level prepared statements: PREPARE / EXECUTE / DEALLOCATE.
        if upper.starts_with("PREPARE ") {
            return self.handle_prepare(addr, sql_trimmed);
        }
        if upper.starts_with("EXECUTE ") {
            return self.handle_execute(identity, addr, sql_trimmed).await;
        }
        if upper.starts_with("DEALLOCATE ") {
            return self.handle_deallocate(addr, sql_trimmed);
        }

        if upper.starts_with("EXPLAIN ") {
            return self.handle_explain(identity, sql_trimmed).await;
        }

        // LIVE SELECT: create subscription, store in session, return channel info.
        if upper.starts_with("LIVE SELECT ") {
            return self.handle_live_select(identity, addr, sql_trimmed);
        }

        // Temporary tables: CREATE TEMPORARY TABLE / CREATE TEMP TABLE.
        if upper.starts_with("CREATE TEMPORARY TABLE ") || upper.starts_with("CREATE TEMP TABLE ") {
            return super::super::ddl::temp_table::create_temp_table(
                &self.sessions,
                identity,
                addr,
                sql_trimmed,
            );
        }

        if let Some(result) = super::super::ddl::dispatch(&self.state, identity, sql_trimmed).await
        {
            return result;
        }

        let tenant_id = identity.tenant_id;

        self.state.check_tenant_quota(tenant_id).map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        self.state.tenant_request_start(tenant_id);
        let result = self
            .execute_planned_sql(identity, sql_trimmed, tenant_id, addr)
            .await;
        self.state.tenant_request_end(tenant_id);

        if result.is_err() {
            self.sessions.fail_transaction(addr);
        }

        result
    }

    /// Handle LIVE SELECT: create a change stream subscription, store it in the
    /// session, and return the subscription_id + channel name to the client.
    ///
    /// After this query returns, pending change events are drained between
    /// subsequent queries and delivered as pgwire `NotificationResponse` messages.
    fn handle_live_select(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let coll_name = super::super::ddl::sql_parse::extract_collection_after(sql, " FROM ")
            .ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "syntax: LIVE SELECT [*|fields] FROM <collection> [WHERE ...]".to_owned(),
                )))
            })?;

        let tenant_id = identity.tenant_id;
        let sub = self
            .state
            .change_stream
            .subscribe(Some(coll_name.clone()), Some(tenant_id));
        let sub_id = sub.id;
        let channel = format!("live_{coll_name}");

        // Store the subscription in the session for notification delivery.
        self.sessions
            .add_live_subscription(addr, channel.clone(), sub);

        tracing::info!(
            sub_id,
            collection = coll_name,
            channel,
            "LIVE SELECT subscription created"
        );

        use futures::stream;
        let schema = Arc::new(vec![
            text_field("subscription_id"),
            text_field("channel"),
            text_field("collection"),
            text_field("status"),
        ]);
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&sub_id.to_string());
        let _ = encoder.encode_field(&channel);
        let _ = encoder.encode_field(&coll_name);
        let _ = encoder.encode_field(&"active");
        let row = encoder.take_row();
        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::iter(vec![Ok(row)]),
        ))])
    }

    /// Execute a SELECT query and return results as JSON strings for cursor storage.
    async fn execute_query_for_cursor(
        &self,
        _addr: &std::net::SocketAddr,
        sql: &str,
        identity: &AuthenticatedIdentity,
    ) -> PgWireResult<Vec<String>> {
        let tenant_id = identity.tenant_id;
        let query_ctx = crate::control::planner::context::QueryContext::for_state(
            &self.state,
            tenant_id.as_u32(),
        );

        // Apply session rounding mode to per-query context if set.
        if let Some(mode) = self.sessions.get_parameter(_addr, "rounding_mode") {
            query_ctx.set_rounding_mode(&mode);
        }

        let auth_ctx = crate::control::server::session_auth::build_auth_context(identity);
        let sec = crate::control::planner::context::PlanSecurityContext {
            identity,
            auth: &auth_ctx,
            rls_store: &self.state.rls,
            permissions: &self.state.permissions,
            roles: &self.state.roles,
        };
        let tasks = query_ctx
            .plan_sql_with_rls(sql, tenant_id, &sec)
            .await
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42000".to_owned(),
                    e.to_string(),
                )))
            })?;

        let mut rows = Vec::new();
        for task in tasks {
            let resp = crate::control::server::dispatch_utils::dispatch_to_data_plane(
                &self.state,
                task.tenant_id,
                task.vshard_id,
                task.plan,
                0,
            )
            .await
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    e.to_string(),
                )))
            })?;

            if !resp.payload.is_empty() {
                let json =
                    crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
                rows.push(json);
            }
        }
        Ok(rows)
    }
}
