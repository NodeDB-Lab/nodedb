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

use super::super::core::NodeDbPgHandler;
use super::super::routing::result_shaping::ResultShaping;
use super::param_bind::convert_portal_params;
use super::result_format::{pg_type_to_ddl_col_type, resolve_result_formats};
use super::statement::ParsedStatement;

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
        self.authorize_session_database(&identity, session_id)?;
        let stmt = &portal.statement.statement;
        let tenant_id = identity.tenant_id;

        // J.4: mirror `do_query`'s audit scope. The extended-query
        // path also triggers DDL (a prepared `CREATE COLLECTION`
        // binds parameters then dispatches), so audit context must
        // be installed here too or followers receive a plain
        // `CatalogDdl` with no SQL trail.
        let _audit_scope = crate::control::server::shared::session::audit_context::AuditScope::new(
            crate::control::server::shared::session::audit_context::AuditCtx {
                auth_user_id: identity.user_id.to_string(),
                auth_user_name: identity.username.clone(),
                sql_text: stmt.sql.clone(),
            },
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
                .execute_sql(&identity, session_id, bound.as_str())
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
        // (the RowDescription was already sent by Describe). Build a neutral
        // projection from the declared result fields — lookup_key == display_name
        // == field name, exactly matching the prior post-hoc reproject — so the
        // SELECT-read producer shapes and projects the response in one pass.
        // When no result columns were declared, no projection is applied.
        //
        // DML RETURNING rows are shaped from a `RowsPayload` whose own column
        // list comes from the STORED row, which for `RETURNING *` on a
        // schemaless collection need not match the columns Describe already
        // announced. The same projection therefore governs them: the shaper
        // holds those rows to exactly the announced columns, so the DataRow
        // field count equals the RowDescription column count by construction.
        // Resolve the client's requested per-column result formats (from the
        // Bind message), downgrading any column whose binary encoding is
        // feature-blocked back to text. Parallel to `stmt.result_fields`.
        let result_formats =
            resolve_result_formats(&stmt.result_fields, &portal.result_column_format);

        let projection: Option<OutputSchema> = if stmt.result_fields.is_empty() {
            None
        } else {
            Some(OutputSchema {
                columns: stmt
                    .result_fields
                    .iter()
                    .map(|f| OutputColumn {
                        display_name: f.name().into(),
                        lookup_key: f.name().into(),
                        // Carry each column's real catalog type (from the
                        // Describe-phase field) so the encoder can render the
                        // matching PostgreSQL text form and, for binary
                        // columns, extract the correctly-typed scalar.
                        ty: pg_type_to_ddl_col_type(f.datatype()),
 sequence: None,
})
                    .collect(),
                is_star: false,
            })
        };

        // Execute through the planned SQL path with AST-level parameter binding.
        let mut results = self
            .execute_planned_sql_with_params(
                &identity,
                &stmt.sql,
                tenant_id,
                session_id,
                &params,
                ResultShaping {
                    projection: projection.as_ref(),
                    formats: &result_formats,
                },
            )
            .await?;
        Ok(results.pop().unwrap_or(Response::EmptyQuery))
    }
}
