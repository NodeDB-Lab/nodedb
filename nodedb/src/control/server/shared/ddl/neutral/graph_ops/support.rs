// SPDX-License-Identifier: BUSL-1.1

//! Shared error constructor and catalog gating for the protocol-neutral
//! graph-ops handlers (and `match_ops`, a sibling of this module's parent).

use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::DdlError;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
///
/// The SQLSTATE and message reach the client unchanged.
pub(super) fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Surface a terminal Data-Plane verdict carried on a dispatched response.
///
/// A refused write comes back as `Ok(Response)` carrying `Status::Error`: the
/// dispatch funnel reports the dispatch itself as having succeeded and puts the
/// verdict inside the response. A handler that drops the response therefore
/// answers a refusal with a success tag — the row-level-security write gate, a
/// dangling-endpoint rejection, and a storage failure all reported as applied.
/// Every handler that dispatches a write through this family runs the response
/// through here so the verdict reaches the client.
///
/// `None` means the Data Plane reported a failure without a code, which is a bug
/// in whatever produced it; it is surfaced as an internal error rather than
/// silently downgraded to success.
pub(super) fn data_plane_verdict(
    response: &crate::bridge::envelope::Response,
) -> Result<(), DdlError> {
    if response.status != crate::bridge::envelope::Status::Error {
        return Ok(());
    }
    Err(match response.error_code.as_deref() {
        Some(code) => {
            let (_, sqlstate, message) =
                super::super::super::sqlstate::error_code_to_sqlstate(code);
            ddl_err(sqlstate, message)
        }
        None => DdlError::internal("unknown data plane error"),
    })
}

/// Gate a named collection on catalog `is_active`: a plain `DROP COLLECTION`
/// (no PURGE) only flips `is_active=false` in the catalog and does not
/// reclaim edges/CSR, so graph reads must independently hide it until UNDROP
/// or a hard purge. Mirrors base-engine `SELECT ... FROM c` behavior on a
/// soft-dropped collection (not-found/deactivated).
///
/// Shared by `SHOW GRAPH STATS '<collection>'`, `GRAPH RAG FUSION`, and
/// `MATCH ... IN c` so the not-found / deactivated semantics can't drift
/// between the three call sites.
pub(crate) fn ensure_collection_active(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
) -> Result<(), DdlError> {
    let catalog = state.credentials.catalog();
    match catalog.get_collection(database_id, tenant_id, name) {
        Ok(Some(c)) if c.is_active => Ok(()),
        Ok(Some(_)) => Err(ddl_err(
            "42P01",
            format!("collection '{name}' is deactivated"),
        )),
        _ => Err(ddl_err("42P01", format!("collection '{name}' not found"))),
    }
}

/// Gate an edge collection a `MATCH ... IN c` reads. Edge writes accept a
/// collection that has no catalog row, so a collection made only by
/// `GRAPH INSERT EDGE` reads like any other. A collection that exists and is
/// soft-dropped stays hidden until UNDROP or a hard purge.
pub(crate) fn ensure_edge_collection_readable(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
) -> Result<(), DdlError> {
    let catalog = state.credentials.catalog();
    match catalog.get_collection(database_id, tenant_id, name) {
        Ok(Some(c)) if !c.is_active => Err(ddl_err(
            "42P01",
            format!("collection '{name}' is deactivated"),
        )),
        Ok(_) => Ok(()),
        Err(e) => Err(DdlError::internal(format!("collection catalog read: {e}"))),
    }
}
