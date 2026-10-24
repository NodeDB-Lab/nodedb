// SPDX-License-Identifier: BUSL-1.1

//! Execute a prepared statement from an extended query portal.
//!
//! Binds parameter values from the portal into the SQL, then executes
//! through the same `execute_sql` path as SimpleQuery — preserving
//! all DDL dispatch, transaction handling, and permission checks.

use std::fmt::Debug;

use futures::sink::Sink;
use pgwire::api::portal::Portal;
use pgwire::api::results::Response;
use pgwire::api::{ClientInfo, ClientPortalStore};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::control::server::response_shape::schema::{OutputColumn, OutputSchema};
use crate::control::server::shared::session::TransactionState;

use super::super::core::NodeDbPgHandler;
use super::super::routing::result_shaping::ResultShaping;
use super::param_bind::convert_portal_params;
use super::statement::ParsedStatement;
use crate::control::server::shared::txn_control::classify as classify_txn_control;

impl NodeDbPgHandler {
    /// Execute a prepared statement from a portal.
    ///
    /// Called by the `ExtendedQueryHandler::do_query` implementation.
    /// Binds parameters at the AST level (not SQL text substitution), then
    /// plans and dispatches through the standard pipeline.
    pub(crate) async fn execute_prepared<C>(
        &self,
        client: &mut C,
        portal: &Portal<ParsedStatement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session_id = self.session_id;
        let identity = self.resolve_identity(client, &session_id)?;
        let stmt = &portal.statement.statement;

        // An aborted transaction block refuses every statement until it ends,
        // the same gate Parse and the simple-query path apply. Transaction
        // control passes: its session handlers in `execute_sql` end the block.
        // The gate runs before backup COPY detection, admission and parameter
        // conversion, so none of them runs in an aborted block.
        if self.sessions.transaction_state(session_id) == TransactionState::Failed
            && classify_txn_control(&stmt.sql).is_none()
        {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25P02".to_owned(),
                "current transaction is aborted, commands ignored until end of transaction block"
                    .to_owned(),
            ))));
        }

        self.authorize_session_database(&identity, session_id)?;
        let tenant_id = identity.tenant_id;

        // The extended-query path also runs DDL (a prepared `CREATE
        // COLLECTION` binds parameters, then dispatches), so it installs the
        // statement's audit context too. A credential statement installs its
        // redacted text.
        let _audit_scope =
            crate::control::server::shared::session::audit_context::AuditScope::statement(
                &identity, &stmt.sql,
            );

        // Wire-streaming COPY shapes for backup/restore. Recognised before
        // sqlparser-based execution because the shapes aren't standard COPY
        // grammar. See `control::backup::detect`.
        if let Some(intent) = crate::control::backup::detect(&stmt.sql) {
            return self.intent_to_response(&identity, session_id, intent).await;
        }

        // Convert pgwire binary parameters to typed ParamValues for AST/DSL
        // binding. Done once, used by both the DSL path and the planned-SQL
        // path below.
        let params = convert_portal_params(
            &portal.parameters,
            &stmt.param_types,
            &portal.parameter_format,
        )?;

        // DSL passthroughs (SEARCH, GRAPH, MATCH, UPSERT INTO, etc.) cannot be
        // handled by the planned-SQL path because sqlparser doesn't parse the
        // DSL grammar. Before dispatching, substitute `$N` placeholders in the
        // SQL text via sqlparser's tokenizer (string/identifier/comment-aware).
        // `BoundDslSql` is a newtype — the compiler refuses to pass a raw
        // `&str` to a DSL execution path, so forgetting binding on a future
        // DSL is a compile error, not a runtime silent-drop.
        if stmt.is_dsl {
            let bound = nodedb_sql::dsl_bind::bind_dsl(&stmt.sql, &params).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".into(),
                    "42601".into(),
                    format!("DSL parameter bind: {e}"),
                )))
            })?;
            let mut results = self
                .execute_sql(
                    &identity,
                    session_id,
                    bound.as_str(),
                    &portal.result_column_format,
                )
                .await?;
            return Ok(results.pop().unwrap_or(Response::EmptyQuery));
        }

        // Request-admission gate: internal-service exemption, blacklist,
        // account status, then rate limit. The DSL branch above already ran
        // this (via `execute_sql` -> `execute_single_sql` -> `admit_statement`),
        // so it must not run again here; every other statement on this
        // extended-query (Bind/Execute) path reaches `execute_planned_sql_with_params`
        // directly without going through `execute_sql` at all, so this is its
        // only admission gate.
        let database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        self.admit_statement(&identity, session_id, database_id)
            .await?;

        // When the statement declared typed result columns via Describe, the
        // client expects DataRow messages with one field per declared column
        // (the RowDescription was already sent by Describe). Build the
        // projection from the declared result fields: each one reads the key
        // the executor emits its value under and renders under its field
        // name, as the planned output schema does on the simple-query path.
        // The SELECT-read producer shapes and projects the response in one
        // pass.
        // When no result columns were declared, no projection is applied.
        //
        // DML RETURNING rows are shaped from a `RowsPayload` whose own column
        // list comes from the STORED row, which for `RETURNING *` on a
        // schemaless collection need not match the columns Describe already
        // announced. The same projection therefore governs them: the shaper
        // holds those rows to exactly the announced columns, so the DataRow
        // field count equals the RowDescription column count by construction.
        // The client's requested result formats (from the Bind message) travel
        // to the encoder, which resolves each column's format from its type.
        let projection: Option<OutputSchema> = if stmt.result_columns.is_empty() {
            None
        } else {
            Some(OutputSchema {
                columns: stmt
                    .result_columns
                    .iter()
                    .map(|column| OutputColumn {
                        display_name: column.name.clone(),
                        lookup_key: column.lookup_key.clone(),
                        // The column's type from Parse, so the encoder
                        // renders the matching PostgreSQL form.
                        ty: column.ty,
                    })
                    .collect(),
                is_star: false,
                // The Describe-phase fields carry no expressions; the
                // execute path merges the statement's own computed list in.
                cp_computed: Vec::new(),
                // The execute path takes the key from the statement's own plan.
                declared_key: None,
            })
        };

        // Execute through the planned SQL path with AST-level parameter binding.
        // An error here aborts an open transaction block: the connection loop
        // applies that rule to every failed extended-query message.
        let mut results = self
            .execute_planned_sql_with_params(
                &identity,
                &stmt.sql,
                tenant_id,
                session_id,
                &params,
                ResultShaping {
                    projection: projection.as_ref(),
                    formats: &portal.result_column_format,
                },
            )
            .await?;
        Ok(results.pop().unwrap_or(Response::EmptyQuery))
    }
}
