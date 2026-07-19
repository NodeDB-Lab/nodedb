// SPDX-License-Identifier: BUSL-1.1

//! Main SQL execution entry point with DDL dispatch.
//!
//! Transaction commands (BEGIN/COMMIT/ROLLBACK/SAVEPOINT) are in `transaction_cmds.rs`.
//! Session commands (SET/SHOW/RESET/DISCARD) are in `session_cmds.rs`.
//! Cursor commands (DECLARE/FETCH/MOVE/CLOSE) are in `cursor_cmds.rs`.
//! SQL statement splitting is in `sql_split.rs`.

use std::sync::Arc;

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity};
use crate::control::server::session_auth::identity::stored_user_identity;

use super::super::types::text_field;
use super::core::NodeDbPgHandler;
use super::sql_split::split_sql_statements;
use crate::control::server::shared::session::TransactionState;

impl NodeDbPgHandler {
    /// Execute a SQL query: session state → identity → DDL check → quota → plan → perms → dispatch.
    ///
    /// Handles multi-statement queries (e.g. psql heredoc sends all statements in one message).
    /// Splits at top-level semicolons before dispatching so that `parts[2]` is never polluted
    /// with trailing `;` characters.
    pub(super) async fn execute_sql(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let statements = split_sql_statements(sql);
        match statements.len() {
            0 => Ok(vec![Response::EmptyQuery]),
            1 => {
                self.execute_single_sql(identity, addr, &statements[0])
                    .await
            }
            _ => {
                let mut all = Vec::new();
                for stmt in statements {
                    let mut resp = self.execute_single_sql(identity, addr, &stmt).await?;
                    all.append(&mut resp);
                }
                Ok(all)
            }
        }
    }

    /// Execute a single (already-split) SQL statement.
    async fn execute_single_sql(
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

        // ── Transaction commands ──────────────────────────────────────

        if upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper == "START TRANSACTION" {
            return self.handle_begin(addr);
        }

        if upper == "COMMIT" || upper == "END" || upper == "END TRANSACTION" {
            return self.handle_commit(identity, addr).await;
        }

        if upper == "ROLLBACK" || upper == "ABORT" {
            return self.handle_rollback(identity, addr).await;
        }

        if let Some(result) = self.try_handle_deferred_offset(identity, addr, sql_trimmed, &upper) {
            return result;
        }

        // ── Wire-streaming COPY shapes for backup/restore ─────────────
        if let Some(intent) = crate::control::backup::detect(sql_trimmed) {
            return self
                .intent_to_response(identity, *addr, intent)
                .await
                .map(|r| vec![r]);
        }

        if upper.starts_with("SAVEPOINT ") {
            return self.handle_savepoint(identity, addr, sql_trimmed).await;
        }

        if upper.starts_with("RELEASE SAVEPOINT ") || upper.starts_with("RELEASE ") {
            return self.handle_release_savepoint(addr, sql_trimmed);
        }

        if upper.starts_with("ROLLBACK TO ") {
            return self
                .handle_rollback_to_savepoint(identity, addr, sql_trimmed)
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
                    .execute_query_for_cursor(addr, inner_sql, identity)
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

        if upper.starts_with("FETCH ") {
            return self.handle_fetch(addr, sql_trimmed, &upper);
        }

        if upper.starts_with("MOVE ") && !upper.starts_with("MOVE TENANT ") {
            return self.handle_move(addr, &upper);
        }

        if upper.starts_with("CLOSE ") {
            let cursor_name = sql_trimmed
                .split_whitespace()
                .nth(1)
                .unwrap_or("default")
                .to_string();
            self.sessions.close_cursor(addr, &cursor_name);
            return Ok(vec![Response::Execution(Tag::new("CLOSE CURSOR"))]);
        }

        // ── Failed transaction guard ──────────────────────────────────

        if self.sessions.transaction_state(addr) == TransactionState::Failed {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25P02".to_owned(),
                "current transaction is aborted, commands ignored until end of transaction block"
                    .to_owned(),
            ))));
        }

        // ── Session commands ──────────────────────────────────────────

        if upper.starts_with("SET ") {
            return self.handle_set(identity, addr, sql_trimmed);
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

        // `SHOW <name>` is routed last — after the DDL / AST router has
        // had a chance to claim administrative SHOW commands
        // (`SHOW DATABASES`, `SHOW ROLES`, `SHOW SCHEDULES`, etc.). Only
        // genuine PostgreSQL runtime-parameter requests (`SHOW
        // client_encoding`, `SHOW ALL`, ...) fall through to
        // `handle_show`, which validates against an explicit known-
        // parameter allowlist and rejects unknown names with `42704`.
        // See [the dispatch order below].

        if upper.starts_with("RESET ") {
            let param = sql_trimmed[6..].trim().to_lowercase();
            // `RESET TENANT` is the inverse of `SET TENANT = ...` and must
            // clear the session's effective_tenant_id override (not just an
            // entry in the parameter bag). All policy checks (superuser,
            // no-active-txn) live in handle_reset_tenant.
            if param == "tenant" || param == "nodedb.tenant_id" {
                return self.handle_reset_tenant(identity, addr);
            }
            if param == "all" {
                if self.sessions.get_effective_tenant_id(addr).is_some() {
                    self.handle_reset_tenant(identity, addr)?;
                }
                self.sessions.reset_all_parameters(addr);
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
            self.sessions.reset_parameter(addr, &param);
            return Ok(vec![Response::Execution(Tag::new("RESET"))]);
        }

        if upper == "DISCARD ALL" {
            // Reset all mutable session state while retaining the identity
            // established at authentication. First release any transaction
            // staging overlays; dropping the session entry alone would lose
            // their vShard/transaction identifiers and leak the overlays.
            if self.sessions.transaction_state(addr) != TransactionState::Idle {
                self.handle_rollback(identity, addr).await?;
            }

            // Trust connections cannot safely re-resolve an empty-store
            // identity from the credential store on later statements, because
            // that identity is intentionally ephemeral. Rebuild a Trust base
            // identity only after overlay cleanup; while a transaction is
            // active the session retains its effective identity so teardown
            // targets the correct tenant's staging overlays.
            let authenticated_identity =
                if matches!(&self.auth_mode, crate::config::auth::AuthMode::Trust) {
                    if identity.user_id == 0 {
                        let mut base_identity = identity.clone();
                        base_identity.tenant_id = crate::types::TenantId::new(1);
                        Some(base_identity)
                    } else {
                        stored_user_identity(&self.state, &identity.username, AuthMethod::Trust)
                            .filter(|current_identity| current_identity.user_id == identity.user_id)
                    }
                } else {
                    None
                };
            self.sessions.remove(addr);
            self.sessions.ensure_session(*addr);
            if let Some(authenticated_identity) = authenticated_identity {
                self.sessions.set_identity(addr, authenticated_identity);
            }
            return Ok(vec![Response::Execution(Tag::new("DISCARD ALL"))]);
        }

        // ── Prepared statements ───────────────────────────────────────

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
            return self.handle_explain(identity, addr, sql_trimmed).await;
        }

        // ── Special query forms ───────────────────────────────────────

        if upper.starts_with("LIVE SELECT ") {
            return self.handle_live_select(identity, addr, sql_trimmed);
        }

        // ── LISTEN / NOTIFY / UNLISTEN ────────────────────────────────

        if upper.starts_with("LISTEN ") {
            return self.handle_listen(identity, addr, sql_trimmed);
        }

        if upper.starts_with("NOTIFY ") {
            return self.handle_notify(identity, addr, sql_trimmed);
        }

        if upper.starts_with("UNLISTEN ") || upper == "UNLISTEN *" {
            return self.handle_unlisten(identity, addr, sql_trimmed);
        }

        if upper.starts_with("SELECT FACET_COUNTS") {
            return super::facet::execute_facet_counts_sql(self, identity, addr, sql_trimmed).await;
        }

        if upper.starts_with("SELECT SEARCH_WITH_FACETS") {
            return super::facet::execute_search_with_facets_sql(self, identity, addr, sql_trimmed)
                .await;
        }

        if upper.starts_with("SELECT CURRENT_SETTING") {
            return self.handle_current_setting(addr, sql_trimmed);
        }

        // ── USE DATABASE — session reset ──────────────────────────────
        // Intercepted before the DDL router because it requires access to both
        // `self.sessions` and `addr` for the per-connection state reset.

        if upper.starts_with("USE DATABASE ") {
            let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
            let name = parts.get(2).copied().unwrap_or("").trim_matches('"');
            return super::super::ddl::database::use_database::handle_use_database(
                &self.state,
                identity,
                &self.sessions,
                addr,
                name,
            );
        }

        // ── DDL / Temp tables ─────────────────────────────────────────

        if upper.starts_with("CREATE TEMPORARY TABLE ") || upper.starts_with("CREATE TEMP TABLE ") {
            return super::super::ddl::temp_table::create_temp_table(
                &self.sessions,
                identity,
                addr,
                sql_trimmed,
            );
        }

        let database_id = self
            .sessions
            .get_current_database(addr)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);

        // Increment per-database QPS counter and per-database metrics registry.
        let catalog = self.state.credentials.catalog();
        if let Ok(Some(desc)) = catalog.get_database(database_id) {
            if let Some(ref m) = self.state.system_metrics {
                m.record_database_query(&desc.name);
            }
            self.state.database_metrics.record_qps(&desc.name);
        }

        let txn_ctx = crate::control::server::shared::session::DmlTxnCtx {
            sessions: &self.sessions,
            addr,
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
            return self.handle_show(identity, addr, sql_trimmed);
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
}
