// SPDX-License-Identifier: BUSL-1.1

//! NodeDbQueryParser — pgwire `QueryParser` implementation.
//!
//! Converts incoming SQL (from a Parse message) into a `ParsedStatement`
//! with inferred parameter types and result schema. Uses nodedb-sql for
//! schema resolution instead of DataFusion.

use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::results::{FieldFormat, FieldInfo};
use pgwire::api::stmt::QueryParser;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::config::auth::AuthMode;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::response_shape::types::DdlColType;
use crate::control::server::shared::authorization::{authorize_database, authorize_task_set};
use crate::control::server::shared::session::SessionStore;
use crate::control::state::SharedState;

use super::super::auth::{pgwire_authorization_error, resolve_session_identity};
use super::statement::ParsedStatement;
use parser_schema::{
    count_placeholders, is_dsl_statement, result_fields_for_returning,
    substitute_placeholders_with_null,
};

#[path = "parser_schema.rs"]
mod parser_schema;

/// Maps the response shaper's protocol-neutral wire type to a pgwire
/// `Type` for RowDescription. Mirrors the (now-deleted) SQL-reparse path's
/// `sql_data_type_to_pg`; variants with no dedicated wire type still fall
/// back to `Type::TEXT` where that was the prior fallback, matching
/// today's behavior.
fn ddl_col_type_to_pg(ty: &DdlColType) -> Type {
    match ty {
        DdlColType::Int8 => Type::INT8,
        DdlColType::Int4 => Type::INT4,
        DdlColType::Int2 => Type::INT2,
        DdlColType::Float8 => Type::FLOAT8,
        DdlColType::Float4 => Type::FLOAT4,
        DdlColType::Text => Type::TEXT,
        DdlColType::Bool => Type::BOOL,
        DdlColType::Bytea => Type::BYTEA,
        DdlColType::Json => Type::JSON,
        DdlColType::Jsonb => Type::JSONB,
        DdlColType::Timestamp => Type::TIMESTAMP,
        DdlColType::Timestamptz => Type::TIMESTAMPTZ,
        DdlColType::Varchar => Type::VARCHAR,
        DdlColType::Float4Array => Type::FLOAT4_ARRAY,
        DdlColType::Float8Array => Type::FLOAT8_ARRAY,
    }
}

/// Implements pgwire's `QueryParser` trait for NodeDB.
///
/// On Parse message: parses SQL via sqlparser, extracts placeholder types
/// from the catalog schema, and computes the result schema.
pub struct NodeDbQueryParser {
    state: Arc<SharedState>,
    auth_mode: AuthMode,
    sessions: Arc<SessionStore>,
}

impl NodeDbQueryParser {
    pub fn new(state: Arc<SharedState>, auth_mode: AuthMode, sessions: Arc<SessionStore>) -> Self {
        Self {
            state,
            auth_mode,
            sessions,
        }
    }

    fn placeholder_types(sql: &str, client_types: &[Option<Type>]) -> Vec<Option<Type>> {
        let param_count = count_placeholders(sql);
        let mut param_types = vec![None; param_count.max(client_types.len())];
        for (index, client_type) in client_types.iter().enumerate() {
            if let Some(client_type) = client_type {
                param_types[index] = Some(client_type.clone());
            }
        }
        param_types
    }

    async fn authorize_plannable_sql(
        &self,
        sql: &str,
        identity: &crate::control::security::identity::AuthenticatedIdentity,
        database_id: crate::types::DatabaseId,
        emitter: &ArcAuditEmitter,
    ) -> PgWireResult<bool> {
        let (sql_without_returning, _) =
            match crate::control::server::pgwire::handler::returning::strip_returning(sql) {
                Ok(parts) => parts,
                Err(_) => return Ok(false),
            };
        let sql_for_planning = substitute_placeholders_with_null(&sql_without_returning);
        let query_ctx =
            crate::control::planner::context::QueryContext::for_state_with_lease(&self.state);
        let auth_ctx = crate::control::server::session_auth::build_auth_context(identity);
        let permission_cache = self.state.permission_cache.read().await;
        let security = crate::control::planner::context::PlanSecurityContext {
            identity,
            auth: &auth_ctx,
            rls_store: &self.state.rls,
            permissions: &self.state.permissions,
            roles: &self.state.roles,
            permission_cache: Some(&*permission_cache),
        };
        let Ok((mut tasks, _)) = query_ctx
            .plan_sql_with_rls(crate::control::planner::context::PlanSqlWithRlsParams {
                sql: &sql_for_planning,
                tenant_id: identity.tenant_id,
                database_id,
                sec: &security,
            })
            .await
        else {
            return Ok(false);
        };
        drop(permission_cache);

        crate::control::planner::implicit_edges::append_implicit_edge_tasks(
            &self.state,
            &mut tasks,
            identity.tenant_id,
            database_id,
            crate::types::TraceId::ZERO,
        )
        .await
        .map_err(|error| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                error.to_string(),
            )))
        })?;
        authorize_task_set(
            identity,
            &tasks,
            &self.state.permissions,
            &self.state.roles,
            emitter,
        )
        .map_err(pgwire_authorization_error)?;
        Ok(true)
    }

    /// Infer parameter and result types using nodedb-sql catalog, scoped to
    /// the connecting user's tenant so a tenant-N user's Parse message
    /// resolves against tenant-N's catalog (not tenant 1).
    fn try_infer_types(
        &self,
        sql: &str,
        client_types: &[Option<Type>],
        tenant_id: u64,
        database_id: crate::types::DatabaseId,
    ) -> (Vec<Option<Type>>, Vec<FieldInfo>) {
        let catalog = crate::control::planner::catalog_adapter::OriginCatalog::new(
            Arc::clone(&self.state.credentials),
            tenant_id,
            database_id,
            Some(Arc::clone(&self.state.retention_policy_registry)),
        );

        // Placeholder inference runs unconditionally so an unplannable
        // SQL string (e.g. `WHERE id = $1` where the planner needs bound
        // params to typecheck) still reports the right number of
        // parameter slots in Describe.
        let param_types = Self::placeholder_types(sql, client_types);

        // Strip RETURNING from DML before passing to DataFusion. Retain the
        // parsed spec so we can build result fields for Describe.
        let (sql_stripped, returning_spec) =
            match crate::control::server::pgwire::handler::returning::strip_returning(sql) {
                Ok(pair) => pair,
                Err(_) => return (param_types, Vec::new()),
            };

        // Parse and plan to get collection info for result schema.
        //
        // The planner type-checks WHERE/projection expressions, which
        // fails on raw `$N` placeholders (no bound value to typecheck).
        // For schema inference we only need the collection + projection
        // structure, so substitute placeholders with NULL literals just
        // for this planning pass. Execution re-plans with real bound
        // values.
        let sql_for_inference = substitute_placeholders_with_null(&sql_stripped);
        let plans = match nodedb_sql::plan_sql(&sql_for_inference, &catalog) {
            Ok(p) => p,
            Err(_) => return (param_types, Vec::new()),
        };

        // When the original SQL had a RETURNING clause on a DML statement,
        // build result fields from the collection schema and the RETURNING spec.
        if let Some(spec) = returning_spec
            && let Some(fields) = result_fields_for_returning(&spec, plans.first(), &catalog)
        {
            return (param_types, fields);
        }

        // Infer result fields from the planner's authoritative output
        // schema — the same derivation used to shape response rows, so
        // Describe's RowDescription always matches what Execute returns.
        // Empty `plans` (already handled above) or a plan variant with no
        // resolvable projection yields an empty `OutputSchema`, matching
        // today's `Vec::new()` fallback for DSL/non-SELECT statements.
        let output_schema =
            crate::control::planner::sql_plan_convert::output_schema::build_output_schema(
                &plans,
                &catalog,
                database_id,
            );
        let result_fields: Vec<FieldInfo> = output_schema
            .columns
            .iter()
            .map(|c| {
                FieldInfo::new(
                    c.display_name.clone(),
                    None,
                    None,
                    ddl_col_type_to_pg(&c.ty),
                    FieldFormat::Text,
                )
            })
            .collect();

        (param_types, result_fields)
    }
}

#[async_trait]
impl QueryParser for NodeDbQueryParser {
    type Statement = ParsedStatement;

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let addr = client.socket_addr();
        let identity = resolve_session_identity(
            &self.state,
            self.auth_mode.clone(),
            &self.sessions,
            client,
            &addr,
        )?;
        let database_id = self
            .sessions
            .get_current_database(&addr)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        let emitter = ArcAuditEmitter(Arc::clone(&self.state.audit));
        authorize_database(&identity, database_id, &emitter).map_err(pgwire_authorization_error)?;

        // Wire-streaming COPY shapes for backup/restore: bypass nodedb-sql
        // entirely. Authorization still precedes this early return so a denied
        // Parse cannot create a statement that later reaches Execute.
        if crate::control::backup::detect(sql).is_some() {
            return Ok(ParsedStatement {
                sql: sql.to_owned(),
                param_types: Vec::new(),
                result_fields: Vec::new(),
                is_dsl: false,
            });
        }

        let can_infer_schema = self
            .authorize_plannable_sql(sql, &identity, database_id, &emitter)
            .await?;
        let (param_types, result_fields) = if can_infer_schema {
            self.try_infer_types(sql, types, identity.tenant_id.as_u64(), database_id)
        } else {
            (Self::placeholder_types(sql, types), Vec::new())
        };

        // If type inference produced no result fields and the SQL matches a
        // known DSL prefix, mark the statement as a DSL passthrough. The
        // Execute handler will route it through the full DSL dispatcher
        // (same as the simple-query path) instead of `execute_planned_sql_with_params`.
        let is_dsl = result_fields.is_empty() && is_dsl_statement(sql);

        Ok(ParsedStatement {
            sql: sql.to_owned(),
            param_types,
            result_fields,
            is_dsl,
        })
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(stmt
            .param_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::UNKNOWN))
            .collect())
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        _column_format: Option<&pgwire::api::portal::Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(stmt.result_fields.clone())
    }
}
