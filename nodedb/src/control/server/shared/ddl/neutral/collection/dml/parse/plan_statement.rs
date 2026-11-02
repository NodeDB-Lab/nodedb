// SPDX-License-Identifier: BUSL-1.1

//! Plan one collection-DML statement under the requester's own scope, from
//! SQL text or from plans built against the catalog.

use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::SqlPlan;

use crate::control::planner::context::PlanSecurityContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::pgwire::types::error_to_sqlstate;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;

use super::types::ddl_err;

/// Builds a statement's `SqlPlan`s against the planner's catalog adapter.
pub(in crate::control::server::shared::ddl::neutral::collection) type PlanBuilder<'b> =
    Box<dyn FnOnce(&dyn SqlCatalog) -> nodedb_sql::Result<Vec<SqlPlan>> + Send + 'b>;

/// What a statement plans from.
pub(super) enum StatementSource<'s> {
    /// SQL text, with the item text of a stripped `RETURNING` clause.
    Sql {
        sql: &'s str,
        returning_items: Option<&'s str>,
    },
    /// Plans built against the catalog.
    Built(PlanBuilder<'s>),
}

/// A planned statement, ready for authorization and dispatch.
pub(super) struct PlannedStatement {
    pub tasks: Vec<nodedb_physical::physical_task::PhysicalTask>,
    pub output_schema: crate::control::server::response_shape::schema::OutputSchema,
    pub versions: crate::control::planner::descriptor_set::DescriptorVersionSet,
    pub has_returning: bool,
}

/// Plan `source` under the requester's own scope.
///
/// This is a client statement — the object-literal `INSERT INTO c { … }` and
/// `UPSERT` forms land here after being rewritten to standard SQL, and a bulk
/// load lands here with built plans — so it plans under the requester's own
/// scope, the same one it is authorized and metered as. Planning it as the
/// system will apply no row policy to it: read filters will not be injected,
/// and the write gates will decide nothing, on a transport a client can reach
/// directly.
///
/// Injection happens inside planning, before the task set is consumed:
/// implicit-edge extraction, authorization, staging, and dispatch all read
/// the tasks after this point, and injecting later will hand them
/// un-injected copies.
pub(super) async fn plan_statement(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: nodedb_types::TenantId,
    database_id: crate::types::DatabaseId,
    source: StatementSource<'_>,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<PlannedStatement, DdlError> {
    let scope = RequestAuthScope::for_database(identity, state.auth_stores(), database_id)?;
    crate::control::security::auth_fence::admit_permission_view(state, tenant_id)
        .await
        .map_err(|error| DdlError::from_error(&error))?;
    let sec = PlanSecurityContext {
        identity,
        auth: scope.auth(),
        rls_store: &state.rls,
        redaction_store: &state.redaction,
        permissions: &state.permissions,
        roles: &state.roles,
        permission_tree: crate::control::planner::context::PermissionTreeSource::Live(
            &state.permission_cache,
        ),
    };
    let query_ctx = crate::control::planner::context::QueryContext::for_state(state);
    // A column DEFAULT that calls `nextval` draws per row in row order,
    // and records into this session's `currval` map, as the SQL form does.
    query_ctx.set_session_sequences(txn_ctx.sessions.sequence_values(txn_ctx.session_id));
    // Inside an open block every read and every predicate match of the
    // statement reads the block's snapshot, as the SQL path's statements do.
    // A system transaction (a trigger or event action on its own private
    // session) takes its own snapshot. A trigger body that joins a client
    // statement's transaction plans on the client's session and reads its
    // snapshot.
    let txn_snapshot = txn_ctx.txn_snapshot(state).await.map_err(|error| {
        let (_, sqlstate, message) = error_to_sqlstate(&error);
        ddl_err(sqlstate, message)
    })?;
    query_ctx.set_txn_snapshot(txn_snapshot);
    let planned = match source {
        StatementSource::Sql {
            sql,
            returning_items,
        } => query_ctx
            .plan_sql_with_rls_and_versions(sql, tenant_id, database_id, &sec, returning_items)
            .await
            .map(|(tasks, output_schema, versions, _)| PlannedStatement {
                tasks,
                output_schema,
                versions,
                has_returning: returning_items.is_some(),
            }),
        StatementSource::Built(build) => query_ctx
            .plan_built_with_rls_and_versions(build, tenant_id, database_id, &sec)
            .await
            .map(|(tasks, output_schema, versions)| PlannedStatement {
                tasks,
                output_schema,
                versions,
                has_returning: false,
            }),
    };
    planned.map_err(|error| {
        let (_, sqlstate, message) = error_to_sqlstate(&error);
        ddl_err(sqlstate, message)
    })
}
