// SPDX-License-Identifier: BUSL-1.1

//! CRDT delta application endpoint.
//!
//! POST /v1/collections/{name}/crdt/apply
//! Request: `{ "doc_id": "...", "delta": "hex_encoded_bytes" }`
//! Response: `{ "status": "ok", "collection": "...", "doc_id": "..." }`

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::audit::{ArcAuditEmitter, AuditEmitter};
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::security::permission::PermissionStore;
use crate::control::security::role::RoleStore;
use crate::control::server::http::admission::admit;
use crate::control::server::http::auth::{ApiError, AppState, resolve_identity};
use crate::control::server::http::peer::PeerAddr;
use crate::control::server::http::transport::ClientTransport;
use crate::control::server::http::types::{HttpCrdtApplyRequest, HttpCrdtApplyResponse};
use crate::control::server::shared::authorization::{
    AuthorizationError, authorize_collection, authorize_task_set,
};
use crate::control::server::shared::ddl::sql_parse::hex_decode;
use nodedb_physical::physical_plan::CrdtOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::document::extract_request_id;

/// Maximum encoded JSON request body for one CRDT apply request.
pub const CRDT_HTTP_BODY_MAX_BYTES: usize = 2 * nodedb_crdt::DEFAULT_MAX_DELTA_BYTES + 4096;

/// POST /v1/collections/{name}/crdt/apply
///
/// Apply a CRDT delta to a document in the collection.
///
/// Request body:
/// ```json
/// {
///   "doc_id": "doc-1",
///   "delta": "hex_encoded_delta_bytes"
/// }
/// ```
///
/// The request body arrives as raw [`Bytes`] rather than `axum::Json` so that
/// nothing in it is parsed before the admission gate below has admitted the
/// request — a refused caller must not get its delta decoded, its surrogate
/// assigned, or its collection touched.
pub async fn crdt_apply(
    headers: HeaderMap,
    peer: PeerAddr,
    transport: ClientTransport,
    State(state): State<AppState>,
    Path(collection): Path<String>,
    raw_body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let identity = resolve_identity(&headers, &state, peer.as_str(), transport.security()).await?;
    // The `X-NodeDB-Database` header selects the database, as on every other
    // HTTP route. An unknown name is refused with `3D000`.
    let database_id = super::query::resolve_database_id(
        &headers,
        &super::query::DatabaseQueryParam::default(),
        &state,
    )?;

    // Request-admission gate: internal-service exemption, blacklist, account
    // status, risk, then rate limit — run first, before the body is parsed or
    // the collection is authorized, so a blacklisted IP or a suspended/banned
    // account is refused without spending any work. The rate-limited door is
    // the right one here (unlike the sync transport's delta apply, which uses
    // the blacklist-and-status door): a sync session is admitted once at
    // handshake and then streams deltas that the per-query cost table does not
    // model, whereas this is a discrete, session-less HTTP request that gets
    // no other admission check and does have a response to carry
    // `X-RateLimit-*` / `Retry-After` on.
    let rate_limit_headers = admit(&state, &identity, database_id, peer.as_str(), "crdt_apply")?;

    let body: HttpCrdtApplyRequest = sonic_rs::from_slice(&raw_body)
        .map_err(|e| ApiError::BadRequest(format!("invalid CRDT apply request body: {e}")))?;

    let audit = ArcAuditEmitter(std::sync::Arc::clone(&state.shared.audit));
    preflight_crdt_apply(
        &identity,
        database_id,
        &collection,
        &state.shared.permissions,
        &state.shared.roles,
        &audit,
    )
    .map_err(crate::Error::from)
    .map_err(ApiError::from)?;

    // Decode and bound the external delta before allocating identity state.
    let delta = decode_bounded_delta(&body.delta)?;

    let _trace_id = extract_request_id(&headers);

    let surrogate = crate::control::server::surrogate_exchange::assign_surrogate_routed(
        &state.shared,
        nodedb_types::CollectionKey::from_bare(database_id, &collection),
        identity.tenant_id,
        body.doc_id.as_bytes(),
        crate::types::TraceId::ZERO,
    )
    .await
    .map_err(ApiError::from)?;

    let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
        document_id: body.doc_id.clone(),
        delta,
        peer_id: identity.user_id,
        mutation_id: 0,
        surrogate,
        provenance: None,
        // Local HTTP write, not a replicated peer sync — no constraint fence.
        constraint_version_required: 0,
        expected_frontier_digest: None,
    });

    let task = PhysicalTask {
        tenant_id: identity.tenant_id,
        vshard_id: nodedb_types::CollectionKey::from_bare(database_id, &collection).vshard(),
        database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let authorized = authorize_task_set(
        &identity,
        std::slice::from_ref(&task),
        &state.shared.permissions,
        &state.shared.roles,
        &audit,
    )
    .map_err(crate::Error::from)
    .map_err(ApiError::from)?
    .into_tasks()
    .into_iter()
    .next()
    .ok_or_else(|| ApiError::Internal("authorization returned no capability".into()))?;

    // Route through the Raft proposer gate so the delta is quorum-durable under
    // replication. A local-only dispatch will land it on the receiving node only
    // — lost to followers and entirely on leader failover.
    let _request = state.shared.tenant_request_guard(identity.tenant_id);
    // RLS write policies are stored keyed by `db_qualified(database_id,
    // collection)`.
    let qualified_collection =
        crate::control::planner::sql_plan_convert::convert::db_qualified(database_id, &collection);
    let crs_set = crate::control::crs_context::crs_set_for(&state.shared);
    let policy =
        crate::control::crdt_post_image_policy::ExternalCrdtPostImagePolicy::from_identity(
            identity.tenant_id,
            database_id,
            &qualified_collection,
            &identity,
            "http".into(),
            crate::control::crdt_post_image_policy::PolicyServices {
                rls: &state.shared.rls,
                audit: &audit,
                crs_set: &crs_set,
            },
        )
        .with_type_guards_from(&state.shared, database_id);
    let result = crate::control::crdt_admission::dispatch_authorized_crdt_apply_admitted(
        &state.shared,
        crate::control::crdt_admission::AuthorizedCrdtApplyAdmissionRequest {
            authorized,
            collection: &collection,
            deadline: crate::control::server::shared::session::StatementDeadline::of_statement(
                &state.shared,
            ),
            event_source: crate::event::EventSource::User,
            policy: &policy,
        },
    )
    .await;

    result.map_err(ApiError::from)?;

    Ok((
        rate_limit_headers,
        axum::Json(HttpCrdtApplyResponse::ok(collection, body.doc_id)),
    ))
}

/// Authorize the write in the request's database before any CRDT apply work.
fn preflight_crdt_apply(
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    collection: &str,
    permissions: &PermissionStore,
    roles: &RoleStore,
    audit: &dyn AuditEmitter,
) -> Result<(), AuthorizationError> {
    authorize_collection(
        identity,
        database_id,
        collection,
        Permission::Write,
        permissions,
        roles,
        audit,
    )
}

fn decode_bounded_delta(encoded: &str) -> Result<Vec<u8>, ApiError> {
    let delta = hex_decode(encoded)
        .ok_or_else(|| ApiError::BadRequest("invalid hex in 'delta' field".into()))?;
    if delta.len() > nodedb_crdt::DEFAULT_MAX_DELTA_BYTES {
        return Err(ApiError::HttpStatus(
            413,
            "CRDT delta exceeds maximum size".into(),
        ));
    }
    Ok(delta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::audit::AuditEvent;
    use crate::control::security::audit::emitter::test_helpers::CapturingEmitter;
    use crate::control::security::identity::{AuthMethod, DatabaseSet};
    use crate::types::TenantId;

    fn regular_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            7,
            "writer",
            TenantId::new(9),
            AuthMethod::ApiKey,
            Vec::new(),
            None,
            DatabaseSet::Some(smallvec::smallvec![crate::types::DatabaseId::DEFAULT]),
        )
    }

    fn assert_one_audit_denial(audit: &CapturingEmitter) {
        let events = audit.recorded();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, AuditEvent::PermissionDenied);
    }

    #[test]
    fn ungranted_regular_identity_is_denied_before_authorized_work_runs() {
        // This endpoint has no read action, so that authorization-matrix cell
        // is N/A.
        let identity = regular_identity();
        let permissions = PermissionStore::new();
        let roles = RoleStore::new();
        let audit = CapturingEmitter::new();
        let mut authorized_work_runs = 0;
        let preflight = preflight_crdt_apply(
            &identity,
            crate::types::DatabaseId::DEFAULT,
            "orders",
            &permissions,
            &roles,
            &audit,
        );

        if preflight.is_ok() {
            authorized_work_runs += 1;
        }

        assert!(preflight.is_err());
        assert_eq!(authorized_work_runs, 0);
        assert_one_audit_denial(&audit);
    }

    #[test]
    fn regular_identity_cannot_write_system_audit_log_even_with_grant() {
        // This endpoint has no read action, so that authorization-matrix cell
        // is N/A.
        let identity = regular_identity();
        let permissions = PermissionStore::new();
        permissions
            .grant(
                "collection:0:9:_system.audit_log",
                "user:writer",
                Permission::Write,
                "admin",
                None,
            )
            .expect("in-memory grant succeeds");
        let roles = RoleStore::new();
        let audit = CapturingEmitter::new();

        assert!(
            preflight_crdt_apply(
                &identity,
                crate::types::DatabaseId::DEFAULT,
                "_system.audit_log",
                &permissions,
                &roles,
                &audit,
            )
            .is_err()
        );
        assert_one_audit_denial(&audit);
    }

    #[test]
    fn tenant_ten_grant_does_not_authorize_tenant_nine_identity() {
        // This endpoint has no read action, so that authorization-matrix cell
        // is N/A.
        let identity = regular_identity();
        let permissions = PermissionStore::new();
        permissions
            .grant(
                "collection:0:10:orders",
                "user:writer",
                Permission::Write,
                "admin",
                None,
            )
            .expect("in-memory grant succeeds");
        let roles = RoleStore::new();
        let audit = CapturingEmitter::new();

        assert!(
            preflight_crdt_apply(
                &identity,
                crate::types::DatabaseId::DEFAULT,
                "orders",
                &permissions,
                &roles,
                &audit,
            )
            .is_err()
        );
        assert_one_audit_denial(&audit);
    }

    #[test]
    fn decoded_delta_limit_is_exact_and_invalid_hex_is_bad_request() {
        let exact = "00".repeat(nodedb_crdt::DEFAULT_MAX_DELTA_BYTES);
        assert_eq!(
            decode_bounded_delta(&exact).expect("exact limit").len(),
            nodedb_crdt::DEFAULT_MAX_DELTA_BYTES
        );

        let oversized = "00".repeat(nodedb_crdt::DEFAULT_MAX_DELTA_BYTES + 1);
        assert!(matches!(
            decode_bounded_delta(&oversized),
            Err(ApiError::HttpStatus(413, _))
        ));
        assert!(matches!(
            decode_bounded_delta("xyz"),
            Err(ApiError::BadRequest(_))
        ));
    }
}
