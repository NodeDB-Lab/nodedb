// SPDX-License-Identifier: BUSL-1.1

//! `COMPACT collection [PARTITION 'name']` — trigger manual compaction.
//!
//! Dispatches a MetaOp::Compact to the Data Plane via the standard
//! dispatch path. The Data Plane merges segments for the receiving core.

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// Handle `COMPACT collection [PARTITION 'name']`.
pub fn handle_compact(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 2 {
        return Err(ddl_err("42601", "COMPACT requires a collection name"));
    }

    let collection = parse_ident_token(parts[1])?;
    crate::control::server::shared::ddl::neutral::object_permission::require_owner_or_admin(
        state,
        identity,
        database_id,
        &collection,
        "compact collections",
    )?;
    let tenant_id = identity.tenant_id;

    // Verify collection exists.
    if state
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id.as_u64(), &collection)
        .ok()
        .flatten()
        .is_none()
    {
        return Err(ddl_err(
            "42P01",
            format!("collection \"{collection}\" does not exist"),
        ));
    }

    // Dispatch MetaOp::Compact to all Data Plane cores via distributed helper.
    super::distributed::dispatch_maintenance_to_all_cores(
        state,
        tenant_id,
        database_id,
        nodedb_physical::physical_plan::MetaOp::Compact,
    );

    tracing::info!(%collection, "COMPACT dispatched");

    Ok(vec![DdlResult::Status {
        command: "COMPACT".to_string(),
        rows_affected: None,
    }])
}
