// SPDX-License-Identifier: BUSL-1.1

//! HTTP endpoint: `PUT /v1/functions/{name}/wasm`
//!
//! Accepts a raw WASM binary as the request body and updates the function
//! through the replicated `PutFunction` metadata path.
//!
//! Auth: the authenticated tenant administrator and selected database are
//! authorized before any catalog or module-blob access, ensuring the caller's
//! identity — not a hardcoded literal — governs which function is updated.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use super::super::admission::admit_without_rate_limit;
use super::super::auth::{ApiError, AppState, ResolvedIdentity, require_tenant_admin_for_database};
use super::super::peer::PeerAddr;
use crate::control::planner::wasm;

/// Largest accepted raw WASM upload body (10 MiB).
///
/// This matches the WASM runtime's module validation limit and is also applied
/// at the route's body-extraction boundary.
pub const MAX_WASM_MODULE_BYTES: usize = 10 * 1024 * 1024;

/// The request header that names the database.
const DATABASE_HEADER: &str = "x-nodedb-database";

/// `PUT /v1/functions/:name/wasm` — upload a WASM binary for a function.
///
/// The function must already exist with `language = WASM` in the catalog.
/// The binary replaces the previous one (if any) atomically.
pub async fn upload_wasm(
    ResolvedIdentity(identity): ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    // Authentication runs as a parts extractor before the body extractor. The
    // tenant-admin/database gate must complete before catalog or blob access.
    // The `X-NodeDB-Database` header selects the database the function lives
    // in. Without it, the identity's default database applies.
    let database_id = if headers.contains_key(DATABASE_HEADER) {
        super::query::resolve_database_id(
            &headers,
            &super::query::DatabaseQueryParam::default(),
            &state,
        )?
    } else {
        identity
            .default_database
            .unwrap_or(crate::types::DatabaseId::DEFAULT)
    };
    // Blacklist + account status, no rate limit: a module upload is a bulk
    // administrative transfer, not the per-query traffic the rate limiter's
    // cost table models. It runs before the tenant-admin check and before any
    // catalog, validation, or blob work, so a blacklisted IP or a
    // suspended/banned account is refused without the module being read.
    admit_without_rate_limit(&state, &identity, database_id, peer.as_str())?;
    require_tenant_admin_for_database(&identity, database_id)?;
    reject_oversized_body(&body)?;

    let name = name.to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();
    let catalog = state.shared.credentials.catalog();

    // Verify the function exists and is a WASM function.
    let mut func = match catalog.get_function_in_database(database_id, tenant_id, &name) {
        Ok(Some(f)) => f,
        Ok(None) => {
            return Ok((
                StatusCode::NOT_FOUND,
                format!("function '{name}' does not exist"),
            )
                .into_response());
        }
        Err(e) => {
            return Ok((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("catalog read error: {e}"),
            )
                .into_response());
        }
    };

    if func.language != crate::control::security::catalog::function_types::FunctionLanguage::Wasm {
        return Ok((
            StatusCode::BAD_REQUEST,
            format!("function '{name}' is not a WASM function"),
        )
            .into_response());
    }

    // Validate before proposing. The replicated applier validates again,
    // stores the blob locally, and persists metadata without the payload.
    let hash = match wasm::store::validate_wasm_binary(&body, MAX_WASM_MODULE_BYTES) {
        Ok(h) => h,
        Err(e) => {
            return Ok(
                (StatusCode::BAD_REQUEST, format!("invalid WASM binary: {e}")).into_response(),
            );
        }
    };

    func.wasm_hash = Some(hash.clone());
    func.wasm_module = Some(body.to_vec());
    // `func` was fetched using `database_id`; retaining that descriptor in
    // the proposal keeps the update within the authenticated database scope.
    let entry = crate::control::catalog_entry::CatalogEntry::PutFunction(Box::new(func));
    let proposed = crate::control::server::shared::session::audit_context::with_action_context(
        &identity,
        format!("UPLOAD WASM MODULE FOR FUNCTION {name} (hash: {hash})"),
        crate::control::metadata_proposer::propose_catalog_entry_async(&state.shared, &entry),
    )
    .await;
    if let Err(e) = proposed {
        return Ok((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("metadata propose error: {e}"),
        )
            .into_response());
    }

    state.shared.audit_record_with_db(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        Some(database_id),
        &identity.username,
        &format!("WASM binary uploaded for function '{name}' (hash: {hash})"),
    );

    Ok((
        StatusCode::OK,
        serde_json::json!({"hash": hash}).to_string(),
    )
        .into_response())
}

/// Enforce the module size again after extraction so direct handler calls and
/// future router changes cannot bypass the HTTP boundary limit.
fn reject_oversized_body(body: &Bytes) -> Result<(), ApiError> {
    if body.len() > MAX_WASM_MODULE_BYTES {
        return Err(ApiError::HttpStatus(
            StatusCode::PAYLOAD_TOO_LARGE.as_u16(),
            "WASM module exceeds maximum size".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::types::{DatabaseId, TenantId};

    fn identity(roles: Vec<Role>, databases: DatabaseSet) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            7,
            "writer",
            TenantId::new(9),
            AuthMethod::ApiKey,
            roles,
            Some(DatabaseId::DEFAULT),
            databases,
        )
    }

    #[test]
    fn unauthorized_identity_is_rejected_before_catalog_or_proposal() {
        let identity = identity(
            vec![Role::ReadWrite],
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        );

        assert!(matches!(
            require_tenant_admin_for_database(&identity, DatabaseId::DEFAULT),
            Err(ApiError::Forbidden(_))
        ));
    }

    #[test]
    fn tenant_admin_cannot_bypass_selected_database_scope() {
        let identity = identity(
            vec![Role::TenantAdmin],
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        );

        assert!(matches!(
            require_tenant_admin_for_database(&identity, DatabaseId::new(42)),
            Err(ApiError::Forbidden(_))
        ));
    }

    #[test]
    fn oversized_module_is_rejected_by_handler_defense_in_depth_limit() {
        let body = Bytes::from(vec![0; MAX_WASM_MODULE_BYTES + 1]);

        assert!(matches!(
            reject_oversized_body(&body),
            Err(ApiError::HttpStatus(413, _))
        ));
    }
}
