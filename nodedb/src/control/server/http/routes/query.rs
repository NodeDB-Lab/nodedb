// SPDX-License-Identifier: BUSL-1.1

//! Query endpoint helpers shared by materialized and NDJSON SQL execution.

use axum::http::HeaderMap;
use serde::Deserialize;

use super::super::auth::{ApiError, AppState};

mod materialized;
mod ndjson;

pub use materialized::query;
pub use ndjson::query_ndjson;

/// Query string parameters for `/v1/query` and `/v1/query/stream`.
///
/// `?database=<name>` is the fallback when `X-NodeDB-Database` is absent.
#[derive(Debug, Default, Deserialize)]
pub struct DatabaseQueryParam {
    pub database: Option<String>,
}

/// Resolve the active `DatabaseId` from an HTTP request.
///
/// Priority: `X-NodeDB-Database` header > `?database=` query param > DEFAULT.
///
/// When a name is supplied but does not resolve to an existing database,
/// returns `ApiError::BadRequest` with SQLSTATE-style detail
/// (`3D000 database '<name>' does not exist`). Silently falling back to
/// DEFAULT would mask client mistakes and run queries against the wrong
/// database; that is a correctness bug, not a usability convenience.
pub(super) fn resolve_database_id(
    headers: &HeaderMap,
    param: &DatabaseQueryParam,
    state: &AppState,
) -> Result<nodedb_types::DatabaseId, ApiError> {
    let name = headers
        .get("x-nodedb-database")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .or_else(|| param.database.clone().filter(|s| !s.is_empty()));

    let Some(db_name) = name else {
        return Ok(nodedb_types::DatabaseId::DEFAULT);
    };

    let catalog = state.shared.credentials.catalog();

    match catalog.get_database_id_by_name(&db_name) {
        Ok(Some(id)) => Ok(id),
        Ok(None) => Err(ApiError::BadRequest(format!(
            "3D000 database '{db_name}' does not exist"
        ))),
        Err(e) => Err(ApiError::Internal(format!("catalog lookup failed: {e}"))),
    }
}
