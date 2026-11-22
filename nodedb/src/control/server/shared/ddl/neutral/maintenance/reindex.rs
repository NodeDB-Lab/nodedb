// SPDX-License-Identifier: BUSL-1.1

//! `REINDEX [CONCURRENTLY] collection` — rebuild indexes.
//!
//! Grammar:
//!   REINDEX [INDEX <name>] [CONCURRENTLY] <collection>
//!
//! Both forms dispatch `MetaOp::RebuildIndex` to every core and await the
//! cross-core ACK barrier. Without `INDEX <name>` every index the
//! collection has is rebuilt: HNSW, full-text and CSR.
//!
//! Both forms rebuild the same way: off the core, with the core serving
//! reads and writes meanwhile and swapping each rebuilt index in on a later
//! tick. They differ only in when a core answers:
//!
//! - Non-concurrent: once its cutovers are done. Past the statement
//!   deadline it answers `DeadlineExceeded`; the rebuilds still complete.
//! - Concurrent: once its rebuilds have started.
//!
//! The grammar is parsed once by `nodedb_sql::ddl_ast::parse` into
//! `NodedbStatement::Reindex { .. }`; this handler receives the already-parsed
//! fields and never re-tokenises the SQL string.

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TraceId;
use nodedb_physical::physical_plan::MetaOp;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// Execute a parsed `REINDEX [INDEX name] [CONCURRENTLY] collection` statement.
pub async fn handle_reindex(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection: &str,
    index_name: Option<&str>,
    concurrent: bool,
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    // `nodedb_sql::ddl_ast::parse` already applied the canonical identifier
    // convention, so both names arrive in their stored form.
    let collection = collection.to_string();
    crate::control::server::shared::ddl::neutral::object_permission::require_owner_or_admin(
        state,
        identity,
        database_id,
        &collection,
        "reindex collections",
    )?;
    let index_name = index_name.map(str::to_string);
    let tenant_id = identity.tenant_id;

    // Verify the collection exists.
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

    // Every core rebuilds the indexes it holds for the collection. A core
    // answers the plain form after its cutovers, the concurrent form after
    // its rebuilds start.
    let plan = crate::bridge::envelope::PhysicalPlan::Meta(MetaOp::RebuildIndex {
        collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
        index_name,
        concurrent,
    });
    let trace_id = TraceId::generate();
    crate::control::server::broadcast::broadcast_register_to_all_cores(
        state,
        tenant_id,
        database_id,
        plan,
        trace_id,
    )
    .await
    .map_err(|e| DdlError::from_error_in_context("REINDEX failed", &e))?;

    tracing::info!(%collection, concurrent, "REINDEX acknowledged by all cores");

    Ok(vec![DdlResult::Status {
        command: "REINDEX".to_string(),
        rows_affected: None,
    }])
}
