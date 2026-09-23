// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `PUBLISH TO` handler — thin wrapper over the unified SQL
//! dispatcher.
//!
//! Ported from the pgwire `ddl::topic::publish` adapter. Parsing, escape
//! handling, and cluster-aware forwarding are delegated to the protocol-agnostic
//! `sql_dispatch::dispatch_sql` verbatim; the success tag, the per-variant
//! SQLSTATE mapping, and the unrecognized-syntax error are preserved, only the
//! result construction changed from pgwire `Response` / `PgWireError` to the
//! protocol-neutral [`DdlResult`] / [`DdlError`].
//!
//! Syntax: `PUBLISH TO <topic> '<payload>'`

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::sql_dispatch::dispatch_sql_in_database;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::status;

/// Handle `PUBLISH TO <topic> '<payload>'`.
///
/// Delegates parsing, escape handling, and cluster-aware forwarding to the
/// protocol-agnostic `sql_dispatch::dispatch_sql`.
pub async fn handle_publish(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    match dispatch_sql_in_database(state, identity, database_id, sql).await {
        Ok(Some(_)) => Ok(status("PUBLISH")),
        Err(e) => {
            let sqlstate = match &e {
                crate::Error::CollectionNotFound { .. } => "42704",
                crate::Error::BadRequest { .. } => "42601",
                crate::Error::Dispatch { .. } => "58000",
                crate::Error::DispatchCapacity { .. } => {
                    nodedb_types::error::sqlstate::SERVER_OVERLOAD
                }
                _ => "XX000",
            };
            Err(DdlError::new(sqlstate.to_string(), e.to_string()))
        }
        Ok(None) => Err(DdlError::new(
            "42601",
            "expected PUBLISH TO <topic> '<payload>'",
        )),
    }
}
