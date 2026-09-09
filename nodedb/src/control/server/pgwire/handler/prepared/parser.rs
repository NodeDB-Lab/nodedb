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
use pgwire::error::PgWireResult;

use crate::config::auth::AuthMode;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::server::response_shape::types::DdlColType;
use crate::control::server::shared::authorization::{authorize_database, authorize_task_set};
use crate::control::server::shared::session::{SessionId, SessionStore};
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

/// Maps a server-inferred parameter type to the pgwire `Type` to advertise
/// in `ParameterDescription`, or `None` when no faithful wire type exists.
///
/// Reuses the two established hops — `sql_data_type_to_ddl_col_type_with_width`
/// then [`ddl_col_type_to_pg`] — rather than introducing a third mapping. The
/// declared numeric widths travel with the inferred type so a column declared
/// `INT` is advertised as int4 (oid 23), not int8, and one declared `REAL` as
/// float4 (oid 700), not float8: the client encodes its bind value at exactly
/// the width described, and a 4-byte column behind an 8-byte promise is a
/// decode failure.
///
/// # Why some variants are refused
///
/// Advertising a concrete OID makes the client commit to that type's binary
/// encoding, so a lossy mapping is worse than saying nothing: an unknown
/// parameter type is sent as text, which the bind layer already handles.
/// `Decimal`, `Uuid`, `Vector` and `Geometry` all currently fold into
/// `DdlColType::Text`, so advertising them would tell a client holding a
/// `Decimal`/`Uuid` that the server wants TEXT — a client-side `WrongType`
/// failure where `Unknown` would have worked. They stay unresolved until
/// each has a real wire type.
fn inferred_param_type(inferred: &nodedb_sql::InferredParamType) -> Option<Type> {
    use crate::control::server::response_shape::schema::sql_data_type_to_ddl_col_type_with_width;
    use nodedb_sql::types_expr::SqlDataType;

    match &inferred.data_type {
        SqlDataType::Int64
        | SqlDataType::Float64
        | SqlDataType::String
        | SqlDataType::Bool
        | SqlDataType::Bytes
        | SqlDataType::Timestamp
        | SqlDataType::Timestamptz => Some(ddl_col_type_to_pg(
            &sql_data_type_to_ddl_col_type_with_width(
                &inferred.data_type,
                inferred.int_width,
                inferred.float_width,
            ),
        )),
        SqlDataType::Decimal
        | SqlDataType::Uuid
        | SqlDataType::Vector(_)
        | SqlDataType::Geometry
        | SqlDataType::Unknown => None,
    }
}

/// Implements pgwire's `QueryParser` trait for NodeDB.
///
/// On Parse message: parses SQL via sqlparser, resolves each `$N` placeholder
/// to the type the client declared or (failing that) to the type the SQL
/// itself pins down, and computes the result schema from the catalog.
pub struct NodeDbQueryParser {
    state: Arc<SharedState>,
    auth_mode: AuthMode,
    sessions: Arc<SessionStore>,
    session_id: SessionId,
}

impl NodeDbQueryParser {
    pub fn new(
        state: Arc<SharedState>,
        auth_mode: AuthMode,
        sessions: Arc<SessionStore>,
        session_id: SessionId,
    ) -> Self {
        Self {
            state,
            auth_mode,
            sessions,
            session_id,
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

    /// Placeholder slots for `sql`, with client-declared types applied and
    /// every remaining slot filled from SQL-level inference.
    ///
    /// Used on both Parse paths — the schema-inferring one and the fallback
    /// for SQL the planner cannot plan — because inference reads only the SQL
    /// text: whether planning succeeded has no bearing on it, and letting the
    /// advertised parameter types depend on that would be arbitrary.
    fn param_types_with_inference(
        sql: &str,
        client_types: &[Option<Type>],
        catalog: &dyn nodedb_sql::SqlCatalog,
    ) -> Vec<Option<Type>> {
        let mut param_types = Self::placeholder_types(sql, client_types);
        Self::fill_inferred_param_types(sql, catalog, &mut param_types);
        param_types
    }

    /// Fill every parameter slot the client left undeclared with the type
    /// inferred from the SQL itself.
    ///
    /// A client-declared type always wins — that is PostgreSQL semantics: the
    /// Parse message's type oids are the client's contract, and the server may
    /// only resolve the positions the client left as unspecified (oid 0).
    ///
    /// Inference runs on the *unsubstituted* SQL. The schema-inference pass
    /// below rewrites `$N` to `NULL` before planning (the resolver cannot
    /// typecheck a bare placeholder), which erases the position → type link,
    /// so this must happen before that rewrite and independently of it.
    fn fill_inferred_param_types(
        sql: &str,
        catalog: &dyn nodedb_sql::SqlCatalog,
        param_types: &mut Vec<Option<Type>>,
    ) {
        let inferred = nodedb_sql::infer_placeholder_types(sql, catalog);
        if inferred.len() > param_types.len() {
            param_types.resize(inferred.len(), None);
        }
        for (slot, ty) in param_types.iter_mut().zip(inferred.iter()) {
            if slot.is_some() {
                continue;
            }
            *slot = ty.as_ref().and_then(inferred_param_type);
        }
    }

    async fn authorize_plannable_sql(
        &self,
        sql: &str,
        identity: &crate::control::security::identity::AuthenticatedIdentity,
        database_id: crate::types::DatabaseId,
        emitter: &ArcAuditEmitter,
    ) -> PgWireResult<bool> {
        let (sql_without_returning, _) =
            match crate::control::server::shared::returning::strip_returning(sql) {
                Ok(parts) => parts,
                Err(_) => return Ok(false),
            };
        let sql_for_planning = substitute_placeholders_with_null(&sql_without_returning);
        let query_ctx =
            crate::control::planner::context::QueryContext::for_state_with_lease(&self.state);
        // Parse plans against this selected database, including RLS
        // variables — `for_database` stamps `database_id` through the single
        // lockstep path and runs scope-grant enrichment.
        let scope = crate::control::security::request_scope::RequestAuthScope::for_database(
            identity,
            self.state.auth_stores(),
            database_id,
        );
        let permission_cache = self.state.permission_cache.read().await;
        let security = crate::control::planner::context::PlanSecurityContext {
            identity,
            auth: scope.auth(),
            rls_store: &self.state.rls,
            redaction_store: &self.state.redaction,
            permissions: &self.state.permissions,
            roles: &self.state.roles,
            permission_cache: Some(&*permission_cache),
        };
        let Ok((tasks, _)) = query_ctx
            .plan_sql_with_rls_metadata(crate::control::planner::context::PlanSqlWithRlsParams {
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

        // Parse/Describe is metadata-only: authorize the original task set,
        // but do not materialize implicit graph edges while describing it.
        let _authorized_tasks = authorize_task_set(
            identity,
            &tasks,
            &self.state.permissions,
            &self.state.roles,
            emitter,
        )
        .map_err(pgwire_authorization_error)?;
        Ok(true)
    }

    /// The tenant-scoped catalog a single Parse message resolves against, so a
    /// tenant-N user's statement sees tenant-N's collections (not tenant 1).
    ///
    /// Built once per Parse and shared by both consumers — parameter-type
    /// inference and schema planning — so the two can never disagree about
    /// what a name resolves to.
    fn build_catalog(
        &self,
        tenant_id: u64,
        database_id: crate::types::DatabaseId,
    ) -> crate::control::planner::catalog_adapter::OriginCatalog {
        crate::control::planner::catalog_adapter::OriginCatalog::new(
            Arc::clone(&self.state.credentials),
            tenant_id,
            database_id,
            Some(Arc::clone(&self.state.retention_policy_registry)),
        )
    }

    /// Infer parameter and result types using the nodedb-sql catalog.
    fn try_infer_types(
        &self,
        sql: &str,
        client_types: &[Option<Type>],
        catalog: &crate::control::planner::catalog_adapter::OriginCatalog,
        database_id: crate::types::DatabaseId,
    ) -> (Vec<Option<Type>>, Vec<FieldInfo>) {
        // Placeholder *counting* runs unconditionally so an unplannable SQL
        // string (e.g. `WHERE id = $1` where the planner needs bound params
        // to typecheck) still reports the right number of parameter slots in
        // Describe. Type inference then fills the slots the client left
        // undeclared, from the SQL alone — it is independent of the planning
        // pass below and survives that pass failing.
        let param_types = Self::param_types_with_inference(sql, client_types, catalog);

        // Strip RETURNING from DML before passing to DataFusion. Retain the
        // parsed spec so we can build result fields for Describe.
        let (sql_stripped, returning_spec) =
            match crate::control::server::shared::returning::strip_returning(sql) {
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
        let plans = match nodedb_sql::plan_sql(&sql_for_inference, catalog) {
            Ok(p) => p,
            Err(_) => return (param_types, Vec::new()),
        };

        // When the original SQL had a RETURNING clause on a DML statement,
        // build result fields from the collection schema and the RETURNING spec.
        if let Some(ref spec) = returning_spec
            && let Some(fields) = result_fields_for_returning(spec, plans.first(), catalog)
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
                catalog,
                database_id,
                returning_spec.as_ref(),
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
    ) -> PgWireResult<Option<Self::Statement>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let identity = resolve_session_identity(
            &self.state,
            self.auth_mode.clone(),
            &self.sessions,
            client,
            &self.session_id,
        )?;
        let database_id = self
            .sessions
            .get_current_database(self.session_id)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        let emitter = ArcAuditEmitter(Arc::clone(&self.state.audit));
        authorize_database(&identity, database_id, &emitter).map_err(pgwire_authorization_error)?;

        // Wire-streaming COPY shapes for backup/restore: bypass nodedb-sql
        // entirely. Authorization still precedes this early return so a denied
        // Parse cannot create a statement that later reaches Execute.
        if crate::control::backup::detect(sql).is_some() {
            return Ok(Some(ParsedStatement {
                sql: sql.to_owned(),
                param_types: Vec::new(),
                result_fields: Vec::new(),
                is_dsl: false,
            }));
        }

        let can_infer_schema = self
            .authorize_plannable_sql(sql, &identity, database_id, &emitter)
            .await?;
        // One catalog per Parse, shared by parameter-type inference and schema
        // planning. The unplannable branch still needs it: inference resolves
        // `WHERE col = $1` from the catalog whether or not the statement as a
        // whole could be planned.
        let catalog = self.build_catalog(identity.tenant_id.as_u64(), database_id);
        let (param_types, result_fields) = if can_infer_schema {
            self.try_infer_types(sql, types, &catalog, database_id)
        } else {
            (
                Self::param_types_with_inference(sql, types, &catalog),
                Vec::new(),
            )
        };

        // If type inference produced no result fields and the SQL matches a
        // known DSL prefix, mark the statement as a DSL passthrough. The
        // Execute handler will route it through the full DSL dispatcher
        // (same as the simple-query path) instead of `execute_planned_sql_with_params`.
        let is_dsl = result_fields.is_empty() && is_dsl_statement(sql);

        Ok(Some(ParsedStatement {
            sql: sql.to_owned(),
            param_types,
            result_fields,
            is_dsl,
        }))
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
