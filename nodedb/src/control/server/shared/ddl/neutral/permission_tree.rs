// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL handlers for permission tree management.
//!
//! ```sql
//! ALTER COLLECTION documents SET PERMISSION_TREE = '{
//!   "resource_column": "id",
//!   "graph_index": "resource_tree",
//!   "permission_table": "permissions"
//! }';
//!
//! ALTER COLLECTION documents DROP PERMISSION_TREE;
//!
//! SELECT RESOLVE_PERMISSION('user-42', 'doc-123', 'documents');
//! ```
//!
//! Ported from the pgwire `ddl::permission_tree` handlers. The JSON parse /
//! validate, catalog get/put, in-memory permission-cache update, and audit
//! side effects are preserved verbatim; only the result construction changed
//! from pgwire `Response` / `Tag` to the protocol-neutral [`DdlResult`].

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use nodedb_types::DatabaseId;

use crate::control::catalog_entry::persist_collection_replicated;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::permission_tree::types::PermissionTreeDef;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// Construct a [`DdlError`], preserving the exact SQLSTATE codes and messages
/// the pgwire handlers produced.
fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Build a single-tag status result.
fn status(command: impl Into<String>) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.into(),
        rows_affected: None,
    }]
}

/// ALTER COLLECTION <name> SET PERMISSION_TREE = '<json>'
pub async fn set_permission_tree(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    // Extract collection name: between "ALTER COLLECTION " and " SET PERMISSION_TREE".
    let start = "ALTER COLLECTION ".len();
    let end = find_ascii_case_insensitive(sql, " SET PERMISSION_TREE")
        .ok_or_else(|| err("42601", "expected SET PERMISSION_TREE"))?;
    let collection = parse_ident_token(sql[start..end].trim())?;

    // Extract JSON: everything after '=' trimmed, between single quotes.
    let eq_pos = sql[end..]
        .find('=')
        .ok_or_else(|| err("42601", "expected '=' after SET PERMISSION_TREE"))?;
    let json_part = sql[end + eq_pos + 1..].trim();
    let json_str = if json_part.starts_with('\'') && json_part.ends_with('\'') {
        &json_part[1..json_part.len() - 1]
    } else {
        json_part
    };

    let def: PermissionTreeDef = sonic_rs::from_str(json_str)
        .map_err(|e| err("42601", format!("invalid PERMISSION_TREE JSON: {e}")))?;

    def.validate()
        .map_err(|e| err("42601", format!("invalid PERMISSION_TREE: {e}")))?;

    let tenant_id = identity.tenant_id;

    // Verify collection exists.
    let catalog = state.credentials.catalog();
    let mut coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), &collection)
        .map_err(|e| err("XX000", e.to_string()))?
        .ok_or_else(|| err("42P01", format!("collection '{collection}' does not exist")))?;

    if !coll.is_active {
        return Err(err(
            "42P01",
            format!("collection '{collection}' is not active"),
        ));
    }

    // Serialize and persist.
    let def_json = sonic_rs::to_string(&def)
        .map_err(|e| err("XX000", format!("serialize PERMISSION_TREE: {e}")))?;
    coll.permission_tree_def = Some(def_json);
    persist_collection_replicated(state, DatabaseId::DEFAULT, &coll)
        .map_err(|e| err("XX000", e.to_string()))?;

    let sources = [collection.clone(), def.permission_table.clone()];

    // Update in-memory cache.
    state
        .permission_cache
        .write()
        .await
        .register_tree_def(tenant_id.as_u64(), &collection, def);

    // Rows already in the sources are grants and edges too. Hold the
    // acknowledgement until every lease holder covers each source group
    // through its current commit.
    source_group_barrier(state, &sources)
        .await
        .map_err(|e| DdlError::from_error(&e))?;

    // Audit.
    state
        .audit
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(tenant_id),
            &identity.username,
            &format!("SET PERMISSION_TREE on '{collection}'"),
        );

    Ok(status("ALTER COLLECTION"))
}

/// Barrier on every Raft group homing one of `sources`, at a read index
/// taken now. A single node has no groups; its planning reloads the cache.
async fn source_group_barrier(state: &SharedState, sources: &[String]) -> crate::Result<()> {
    let Some(timing) = state.authorization_fence.timing() else {
        return Ok(());
    };
    let mut targets: Vec<nodedb_cluster::GroupCoverage> = Vec::new();
    for source in sources {
        let vshard =
            crate::types::VShardId::from_collection_in_database(DatabaseId::DEFAULT, source);
        let group_id =
            crate::control::security::auth_fence::cluster::group_of_vshard(state, vshard.as_u32())?;
        if targets.iter().any(|target| target.group_id == group_id) {
            continue;
        }
        let through = crate::control::security::auth_fence::cluster::confirmed_read_index(
            state,
            group_id,
            timing.lease,
        )
        .await?;
        targets.push(nodedb_cluster::GroupCoverage { group_id, through });
    }
    crate::control::security::auth_lease::authorization_barrier(state, targets).await
}

/// ALTER COLLECTION <name> DROP PERMISSION_TREE
pub async fn drop_permission_tree(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let start = "ALTER COLLECTION ".len();
    let end = find_ascii_case_insensitive(sql, " DROP PERMISSION_TREE")
        .ok_or_else(|| err("42601", "expected DROP PERMISSION_TREE"))?;
    let collection = parse_ident_token(sql[start..end].trim())?;

    let tenant_id = identity.tenant_id;

    let catalog = state.credentials.catalog();
    let mut coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), &collection)
        .map_err(|e| err("XX000", e.to_string()))?
        .ok_or_else(|| err("42P01", format!("collection '{collection}' does not exist")))?;

    coll.permission_tree_def = None;
    persist_collection_replicated(state, DatabaseId::DEFAULT, &coll)
        .map_err(|e| err("XX000", e.to_string()))?;

    // Update in-memory cache.
    state
        .permission_cache
        .write()
        .await
        .unregister_tree_def(tenant_id.as_u64(), &collection);

    state
        .audit
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(tenant_id),
            &identity.username,
            &format!("DROP PERMISSION_TREE on '{collection}'"),
        );

    Ok(status("ALTER COLLECTION"))
}
