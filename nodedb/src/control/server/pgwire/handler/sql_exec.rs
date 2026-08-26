// SPDX-License-Identifier: BUSL-1.1

//! Main SQL execution entry point with DDL dispatch.
//!
//! Transaction commands (BEGIN/COMMIT/ROLLBACK/SAVEPOINT) are in `transaction_cmds.rs`.
//! Session commands (SET/SHOW/RESET/DISCARD) are in `session_cmds.rs`.
//! Cursor commands (DECLARE/FETCH/MOVE/CLOSE) are in `cursor_cmds.rs`.
//! SQL statement splitting is in `sql_split.rs`.

use std::sync::Arc;

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use nodedb_types::strip_prefix_ascii_case_insensitive;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity};
use crate::control::server::session_auth::identity::stored_user_identity;

use super::super::types::text_field;
use super::connection_admin;
use super::core::NodeDbPgHandler;
use super::sql_split::split_sql_statements;
use crate::control::server::shared::session::{SessionId, TransactionState};

impl NodeDbPgHandler {
    /// Execute a SQL query: session state → identity → DDL check → quota → plan → perms → dispatch.
    ///
    /// Handles multi-statement queries (e.g. psql heredoc sends all statements in one message).
    /// Splits at top-level semicolons before dispatching so that `parts[2]` is never polluted
    /// with trailing `;` characters.
    pub(super) async fn execute_sql(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let statements = split_sql_statements(sql);
        match statements.len() {
            0 => Ok(vec![Response::EmptyQuery]),
            1 => {
                self.execute_single_sql(identity, session_id, &statements[0])
                    .await
            }
            _ => {
                let total = statements.len();
                let mut all = Vec::new();
                for stmt in statements {
                    let mut resp = self.execute_single_sql(identity, session_id, &stmt).await?;
                    all.append(&mut resp);
                }
                // B1 ack reconciliation (2026-08-26): every statement must
                // produce at least one response. A short count means the
                // executor silently dropped work (observed: batch 64 → 32
                // landed, batch 128 → ~25%, no error). Surface it as a hard
                // error instead of letting the client believe the batch
                // committed. Defense-in-depth until the propose-path drop
                // site is traced and fixed.
                if all.len() < total {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!(
                            "incomplete batch: {}/{} statements produced responses — \
                             some work may have been dropped; retry with a smaller batch",
                            all.len(),
                            total,
                        ),
                    ))));
                }
                Ok(all)
            }
        }
    }

    /// Execute a single (already-split) SQL statement.
    async fn execute_single_sql(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        use super::super::types::error_to_sqlstate;

        let sql_trimmed = sql.trim();
        let upper = sql_trimmed.to_ascii_uppercase();

        if sql_trimmed.is_empty() || sql_trimmed == ";" {
            return Ok(vec![Response::EmptyQuery]);
        }

        // ── Transaction commands ──────────────────────────────────────

        if upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper == "START TRANSACTION" {
            return self.handle_begin(session_id);
        }

        if upper == "COMMIT" || upper == "END" || upper == "END TRANSACTION" {
            return self.handle_commit(identity, session_id).await;
        }

        if upper == "ROLLBACK" || upper == "ABORT" {
            return self.handle_rollback(identity, session_id).await;
        }

        if let Some(result) =
            self.try_handle_deferred_offset(identity, session_id, sql_trimmed, &upper)
        {
            return result;
        }

        // ── Wire-streaming COPY shapes for backup/restore ─────────────
        if let Some(intent) = crate::control::backup::detect(sql_trimmed) {
            return self
                .intent_to_response(identity, session_id, intent)
                .await
                .map(|r| vec![r]);
        }

        if upper.starts_with("SAVEPOINT ") {
            return self
                .handle_savepoint(identity, session_id, sql_trimmed)
                .await;
        }

        if upper.starts_with("RELEASE SAVEPOINT ") || upper.starts_with("RELEASE ") {
            return self.handle_release_savepoint(session_id, sql_trimmed);
        }

        if upper.starts_with("ROLLBACK TO ") {
            return self
                .handle_rollback_to_savepoint(identity, session_id, sql_trimmed)
                .await;
        }

        // ── Cursor commands ───────────────────────────────────────────

        if upper.starts_with("DECLARE ") && upper.contains(" CURSOR ") {
            let scrollable =
                upper.contains(" SCROLL CURSOR") && !upper.contains(" NO SCROLL CURSOR");
            let with_hold = upper.contains(" WITH HOLD ");
            let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
            let cursor_name = parts.get(1).unwrap_or(&"default").to_string();
            if let Some(for_pos) = find_ascii_case_insensitive(sql_trimmed, " FOR ") {
                let inner_sql = sql_trimmed[for_pos + 5..].trim();
                match self
                    .execute_query_for_cursor(session_id, inner_sql, identity)
                    .await
                {
                    Ok(rows) => {
                        let spill_config =
                            crate::control::server::shared::session::cursor_spill::CursorSpillConfig::default();
                        let (rows, _truncated) =
                            crate::control::server::shared::session::cursor_spill::enforce_cursor_limit(
                                rows,
                                &spill_config,
                            );
                        self.sessions.declare_cursor(
                            session_id,
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

        if upper.starts_with("FETCH ") {
            return self.handle_fetch(session_id, sql_trimmed, &upper);
        }

        if upper.starts_with("MOVE ") && !upper.starts_with("MOVE TENANT ") {
            return self.handle_move(session_id, &upper);
        }

        if upper.starts_with("CLOSE ") {
            let cursor_name = sql_trimmed
                .split_whitespace()
                .nth(1)
                .unwrap_or("default")
                .to_string();
            self.sessions.close_cursor(session_id, &cursor_name);
            return Ok(vec![Response::Execution(Tag::new("CLOSE CURSOR"))]);
        }

        // ── Failed transaction guard ──────────────────────────────────

        if self.sessions.transaction_state(session_id) == TransactionState::Failed {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25P02".to_owned(),
                "current transaction is aborted, commands ignored until end of transaction block"
                    .to_owned(),
            ))));
        }

        // ── Session commands ──────────────────────────────────────────

        if upper.starts_with("SET ") {
            return self.handle_set(identity, session_id, sql_trimmed);
        }

        if upper == "SHOW CONNECTIONS" {
            if !identity.is_superuser {
                return Err(connection_admin::denied());
            }
            let schema = Arc::new(vec![
                text_field("connection_id"),
                text_field("peer_address"),
                text_field("local_address"),
                text_field("transaction_state"),
            ]);
            let sessions = self.sessions.connection_snapshot_with_state();
            let mut rows = Vec::with_capacity(sessions.len());
            let mut encoder = DataRowEncoder::new(schema.clone());
            for (connection_id, metadata, transaction_state) in sessions {
                encoder.encode_field(&connection_id.to_string())?;
                encoder.encode_field(&metadata.peer_addr.to_string())?;
                encoder.encode_field(&metadata.local_addr.to_string())?;
                let transaction_state = match transaction_state {
                    TransactionState::Idle => "idle",
                    TransactionState::InBlock => "in_transaction",
                    TransactionState::Failed => "failed",
                };
                encoder.encode_field(&transaction_state)?;
                rows.push(Ok(encoder.take_row()));
            }
            return Ok(vec![Response::Query(QueryResponse::new(
                schema,
                futures::stream::iter(rows),
            ))]);
        }

        if let Some(kill_id) = connection_admin::parse_kill(sql_trimmed) {
            if !identity.is_superuser {
                return Err(connection_admin::denied());
            }
            let id = kill_id.map_err(|_| connection_admin::invalid_id())?;
            if !self.registry.request_cancel(id) {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "08003".to_owned(),
                    "connection does not exist".to_owned(),
                ))));
            }
            return Ok(vec![Response::Execution(Tag::new("KILL"))]);
        }

        // `SHOW <name>` is routed last — after the DDL / AST router has
        // had a chance to claim administrative SHOW commands
        // (`SHOW DATABASES`, `SHOW ROLES`, `SHOW SCHEDULES`, etc.). Only
        // genuine PostgreSQL runtime-parameter requests (`SHOW
        // client_encoding`, `SHOW ALL`, ...) fall through to
        // `handle_show`, which validates against an explicit known-
        // parameter allowlist and rejects unknown names with `42704`.
        // See [the dispatch order below].

        if let Some(rest) = strip_prefix_ascii_case_insensitive(sql_trimmed, "RESET ") {
            let param = rest.trim().to_lowercase();
            // `RESET TENANT` is the inverse of `SET TENANT = ...` and must
            // clear the session's effective_tenant_id override (not just an
            // entry in the parameter bag). All policy checks (superuser,
            // no-active-txn) live in handle_reset_tenant.
            if param == "tenant" || param == "nodedb.tenant_id" {
                return self.handle_reset_tenant(identity, session_id);
            }
            if param == "all" {
                if self.sessions.get_effective_tenant_id(session_id).is_some() {
                    self.handle_reset_tenant(identity, session_id)?;
                }
                self.sessions.reset_all_parameters(session_id);
                return Ok(vec![Response::Execution(Tag::new("RESET"))]);
            }
            if !crate::control::server::shared::session::is_known_settable_runtime_parameter(&param)
            {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42704".to_owned(),
                    format!("unrecognized configuration parameter \"{param}\""),
                ))));
            }
            self.sessions.reset_parameter(session_id, &param);
            return Ok(vec![Response::Execution(Tag::new("RESET"))]);
        }

        if upper == "DISCARD ALL" {
            // Reset all mutable session state while retaining the identity
            // established at authentication. First release any transaction
            // staging overlays; dropping the session entry alone would lose
            // their vShard/transaction identifiers and leak the overlays.
            if self.sessions.transaction_state(session_id) != TransactionState::Idle {
                self.handle_rollback(identity, session_id).await?;
            }

            // Rebuild the durable Trust identity only after overlay cleanup;
            // while a transaction is active the session retains its effective
            // identity so teardown targets the correct tenant's staging overlays.
            let authenticated_identity =
                if matches!(&self.auth_mode, crate::config::auth::AuthMode::Trust) {
                    Some(
                        stored_user_identity(&self.state, &identity.username, AuthMethod::Trust)
                            .filter(|current_identity| current_identity.user_id == identity.user_id)
                            .ok_or_else(|| {
                                PgWireError::UserError(Box::new(ErrorInfo::new(
                                    "FATAL".to_owned(),
                                    "28000".to_owned(),
                                    format!(
                                        "trust auth: user '{}' does not exist",
                                        identity.username
                                    ),
                                )))
                            })?,
                    )
                } else {
                    None
                };
            self.sessions.reset_session(session_id);
            if let Some(authenticated_identity) = authenticated_identity {
                self.sessions
                    .set_identity(session_id, authenticated_identity);
            }
            return Ok(vec![Response::Execution(Tag::new("DISCARD ALL"))]);
        }

        // ── Prepared statements ───────────────────────────────────────

        if upper.starts_with("PREPARE ") {
            return self.handle_prepare(session_id, sql_trimmed);
        }
        if upper.starts_with("EXECUTE ") {
            return self.handle_execute(identity, session_id, sql_trimmed).await;
        }
        if upper.starts_with("DEALLOCATE ") {
            return self.handle_deallocate(session_id, sql_trimmed);
        }

        if upper.starts_with("EXPLAIN ") {
            return self.handle_explain(identity, session_id, sql_trimmed).await;
        }

        // ── Special query forms ───────────────────────────────────────

        if upper.starts_with("LIVE SELECT ") {
            return self.handle_live_select(identity, session_id, sql_trimmed);
        }

        // ── LISTEN / NOTIFY / UNLISTEN ────────────────────────────────

        if upper.starts_with("LISTEN ") {
            return self.handle_listen(identity, session_id, sql_trimmed);
        }

        if upper.starts_with("NOTIFY ") {
            return self.handle_notify(identity, session_id, sql_trimmed);
        }

        if upper.starts_with("UNLISTEN ") || upper == "UNLISTEN *" {
            return self.handle_unlisten(identity, session_id, sql_trimmed);
        }

        if upper.starts_with("SELECT FACET_COUNTS") {
            return super::facet::execute_facet_counts_sql(self, identity, session_id, sql_trimmed)
                .await;
        }

        if upper.starts_with("SELECT SEARCH_WITH_FACETS") {
            return super::facet::execute_search_with_facets_sql(
                self,
                identity,
                session_id,
                sql_trimmed,
            )
            .await;
        }

        if upper.starts_with("SELECT CURRENT_SETTING") {
            return self.handle_current_setting(session_id, sql_trimmed);
        }

        // ── USE DATABASE — session reset ──────────────────────────────
        // Intercepted before the DDL router because it requires access to both
        // `self.sessions` and `addr` for the per-connection state reset.

        if upper.starts_with("USE DATABASE ") {
            let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
            let name = parts.get(2).copied().unwrap_or("").trim_matches('"');
            let dp = super::transaction_cmds::PgwireTxnDp { handler: self };
            return super::super::ddl::database::use_database::handle_use_database(
                &self.state,
                identity,
                &self.sessions,
                session_id,
                name,
                &dp,
            )
            .await;
        }

        // ── DDL / Temp tables ─────────────────────────────────────────

        if upper.starts_with("CREATE TEMPORARY TABLE ") || upper.starts_with("CREATE TEMP TABLE ") {
            return super::super::ddl::temp_table::create_temp_table(
                &self.sessions,
                identity,
                session_id,
                sql_trimmed,
            );
        }

        let database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);

        // Increment per-database QPS counter and per-database metrics registry.
        let catalog = self.state.credentials.catalog();
        if let Ok(Some(desc)) = catalog.get_database(database_id) {
            if let Some(ref m) = self.state.system_metrics {
                m.record_database_query(&desc.name);
            }
            self.state.database_metrics.record_qps(&desc.name);
        }

        // Request-admission gate: internal-service exemption, blacklist,
        // account status, then rate limit — run exactly once per statement,
        // right here, before it can branch to `shared::ddl::dispatch` below
        // or fall through to the DataFusion planner (`plan_statement_to_tasks`,
        // which used to admit but no longer does — this call replaces it and
        // is positioned earlier specifically so DDL/DSL text is covered too,
        // not just the statements that reach the planner).
        self.admit_statement(identity, session_id, database_id)
            .await?;

        let txn_ctx = crate::control::server::shared::session::DmlTxnCtx {
            sessions: &self.sessions,
            session_id,
        };

        if let Some(rewritten) =
            super::super::system_functions::rewrite_purge_collection(sql_trimmed, &upper)
            && let Some(result) = crate::control::server::shared::ddl::dispatch(
                &self.state,
                identity,
                &rewritten,
                database_id,
                &txn_ctx,
            )
            .await
        {
            return crate::control::server::pgwire::ddl_encode::ddl_results_to_pgwire(result);
        }

        if let Some(result) = crate::control::server::shared::ddl::dispatch(
            &self.state,
            identity,
            sql_trimmed,
            database_id,
            &txn_ctx,
        )
        .await
        {
            return crate::control::server::pgwire::ddl_encode::ddl_results_to_pgwire(result);
        }

        // SHOW commands the DDL / AST router did not claim are PG
        // runtime-parameter requests. `handle_show` validates against
        // the known-parameter allowlist; unrecognised names return
        // `42704` instead of being silently swallowed as empty rows.
        if upper.starts_with("SHOW ") {
            return self.handle_show(identity, session_id, sql_trimmed);
        }

        // ── DataFusion-planned query execution ────────────────────────

        let tenant_id = identity.tenant_id;

        self.state.check_tenant_quota(tenant_id).map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        let _request = self.state.tenant_request_guard(tenant_id);
        let result = self
            .execute_planned_sql(identity, sql_trimmed, tenant_id, session_id)
            .await;

        if result.is_err() {
            self.sessions.fail_transaction(session_id);
        }

        result
    }
}
