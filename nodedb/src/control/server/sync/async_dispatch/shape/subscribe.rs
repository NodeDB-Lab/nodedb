// SPDX-License-Identifier: BUSL-1.1

//! Shape-subscription snapshot dispatch (subscribe + resync).

use tracing::{info, warn};

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::ClientRequestScope;
use crate::control::server::sync::session::SyncSession;
use crate::control::state::SharedState;

use super::super::super::shape::ShapeScope;
use super::super::super::wire::{SyncFrame, SyncMessageType};
use super::authorize::{ShapeAuthorizationFailure, authorize_shape_subscription};
use super::snapshot::{SnapshotRequest, take_shape_snapshot};

/// Rewrite the collection a document or vector shape names to its bare
/// name in `database_id`. A foreign or malformed name is an error.
fn normalize_shape_collection(
    database_id: DatabaseId,
    shape: &mut nodedb_types::sync::shape::ShapeDefinition,
) -> crate::Result<()> {
    use nodedb_types::sync::shape::ShapeType;

    match &mut shape.shape_type {
        ShapeType::Document { collection, .. } | ShapeType::Vector { collection, .. } => {
            *collection = crate::control::server::shared::request_collection::request_collection(
                database_id,
                collection,
            )?;
            Ok(())
        }
        ShapeType::Graph { .. } | ShapeType::Array { .. } => Ok(()),
        // A shape type this build does not know names no checked collection.
        _ => Err(crate::Error::BadRequest {
            detail: "sync: unsupported shape type".into(),
        }),
    }
}

/// Handle ShapeSubscribe with real WAL LSN and Data Plane snapshot.
pub(in crate::control::server::sync) async fn handle_shape_subscribe_async(
    shared: &SharedState,
    session: &SyncSession,
    frame: &SyncFrame,
) -> Option<SyncFrame> {
    let mut msg: super::super::super::shape::handler::ShapeSubscribeMsg = frame.decode_body()?;
    if let Err(error) = normalize_shape_collection(session.database_id(), &mut msg.shape) {
        warn!(
            session = %session.session_id,
            shape_id = %msg.shape.shape_id,
            error = %error,
            "sync: shape subscribe refused, the collection name is not valid in the session database"
        );
        return None;
    }

    // Authorize before the registry records anything and before any tenant
    // accounting: a subscription the session may not read must leave no trace,
    // and must not be answered with a snapshot frame of any kind.
    let identity = match authorize_shape_subscription(shared, session, &msg.shape) {
        Ok(identity) => identity,
        Err(failure) => {
            log_refusal(&session.session_id, &msg.shape.shape_id, failure);
            return None;
        }
    };
    let tenant_id = identity.tenant_id;
    let database_id = session.database_id();

    // Blacklist + account status, no rate limit: shape subscription is not
    // the per-query traffic the rate-limiter's cost table models, so
    // charging it against a query rate limit would throttle legitimate
    // offline-first sync traffic. A blacklisted or suspended/banned account
    // must not be able to keep subscribing, though —
    // `check_blacklist_and_status` runs that half of
    // `check_request_admission`'s gate (plus the internal-service exemption
    // every other transport gets) using the session's real remote address.
    let request = match subscription_admission_scope(shared, identity, database_id, session) {
        Ok(request) => request,
        Err(e) => {
            warn!(
                tenant_id = tenant_id.as_u64(),
                error = %e,
                "sync: shape subscribe refused, request scope could not be resolved"
            );
            return None;
        }
    };
    if let Err(e) =
        crate::control::server::session_auth::check_blacklist_and_status(shared, &request)
    {
        warn!(
            tenant_id = tenant_id.as_u64(),
            error = %e,
            "sync: shape subscribe rejected by blacklist or account status"
        );
        return None;
    }

    // Quota enforcement — reject before dispatch.
    if let Err(e) = shared.check_tenant_quota(tenant_id) {
        warn!(
            tenant_id = tenant_id.as_u64(),
            error = %e,
            "sync: shape subscribe rejected by quota"
        );
        return None;
    }

    // Get current WAL LSN — this is the watermark for the snapshot.
    let current_lsn = shared.wal.next_lsn().as_u64().saturating_sub(1);

    let snapshot_data = take_shape_snapshot(SnapshotRequest {
        shared,
        session_id: &session.session_id,
        shape: &msg.shape,
        identity,
        tenant_id,
        database_id,
        peer_addr: &session.device_metadata.remote_addr,
    })
    .await?;

    // Register the shape subscription in the persistent registry.
    let response = super::super::super::shape::handler::handle_subscribe(
        &session.session_id,
        tenant_id.as_u64(),
        database_id,
        &msg,
        &shared.shape_registry,
        current_lsn,
        |_shape, _lsn| snapshot_data,
    );

    info!(
        session = %session.session_id,
        shape_id = %msg.shape.shape_id,
        lsn = current_lsn,
        "shape subscribed with WAL LSN watermark"
    );

    response
}

/// Re-snapshot a previously subscribed shape in response to a ResyncRequest.
///
/// Decodes the request, re-authorizes the shape, looks it up in the persistent
/// registry, runs the same snapshot machinery as subscribe, and returns a
/// ShapeSnapshot frame re-based at the current WAL LSN.
///
/// Authorization is repeated here rather than trusted from subscribe time: a
/// grant revoked between subscribing and resyncing must take effect on the next
/// read, not at the next reconnect.
pub(in crate::control::server::sync) async fn handle_resync_request_async(
    shared: &SharedState,
    session: &SyncSession,
    frame: &SyncFrame,
) -> Option<SyncFrame> {
    use nodedb_types::sync::wire::ResyncRequestMsg;

    let msg: ResyncRequestMsg = frame.decode_body()?;

    if msg.shape_id.is_empty() {
        warn!(
            session = %session.session_id,
            "resync request missing shape_id; ignoring"
        );
        return None;
    }

    // Session IDs are reusable across reconnects, so the registry lookup is
    // scoped to the authenticated tenant and database. A session with no
    // established identity has no scope and therefore nothing to resync.
    let Some(identity) = session.identity.as_ref() else {
        log_refusal(
            &session.session_id,
            &msg.shape_id,
            ShapeAuthorizationFailure::IdentityNotEstablished,
        );
        return None;
    };
    let scope = ShapeScope {
        tenant_id: identity.tenant_id.as_u64(),
        database_id: session.database_id(),
    };

    let shape = match shared
        .shape_registry
        .get_shape(&session.session_id, scope, &msg.shape_id)
    {
        Some(s) => s,
        None => {
            warn!(
                session = %session.session_id,
                shape_id = %msg.shape_id,
                "resync for unknown or unsubscribed shape; ignoring"
            );
            return None;
        }
    };

    let identity = match authorize_shape_subscription(shared, session, &shape) {
        Ok(identity) => identity,
        Err(failure) => {
            log_refusal(&session.session_id, &msg.shape_id, failure);
            return None;
        }
    };
    let tenant_id = identity.tenant_id;
    let database_id = session.database_id();

    // See `handle_shape_subscribe_async` above: blacklist + account status,
    // no rate limit, with the same internal-service exemption, resolved
    // against the same real remote address.
    let request = match subscription_admission_scope(shared, identity, database_id, session) {
        Ok(request) => request,
        Err(e) => {
            warn!(
                tenant_id = tenant_id.as_u64(),
                error = %e,
                "sync: resync request refused, request scope could not be resolved"
            );
            return None;
        }
    };
    if let Err(e) =
        crate::control::server::session_auth::check_blacklist_and_status(shared, &request)
    {
        warn!(
            tenant_id = tenant_id.as_u64(),
            error = %e,
            "sync: resync request rejected by blacklist or account status"
        );
        return None;
    }

    if let Err(e) = shared.check_tenant_quota(tenant_id) {
        warn!(
            tenant_id = tenant_id.as_u64(),
            error = %e,
            "sync: resync request rejected by quota"
        );
        return None;
    }

    let current_lsn = shared.wal.next_lsn().as_u64().saturating_sub(1);

    let snapshot_data = take_shape_snapshot(SnapshotRequest {
        shared,
        session_id: &session.session_id,
        shape: &shape,
        identity,
        tenant_id,
        database_id,
        peer_addr: &session.device_metadata.remote_addr,
    })
    .await?;

    let snapshot = super::super::super::shape::handler::ShapeSnapshotMsg {
        shape_id: msg.shape_id.clone(),
        data: snapshot_data.data,
        snapshot_lsn: current_lsn,
        doc_count: snapshot_data.doc_count,
    };

    info!(
        session = %session.session_id,
        shape_id = %msg.shape_id,
        lsn = current_lsn,
        doc_count = snapshot.doc_count,
        "resync snapshot sent"
    );

    SyncFrame::try_encode(SyncMessageType::ShapeSnapshot, &snapshot)
}

/// Resolve the admission scope for a shape subscription or resync from the
/// session's own real remote address.
///
/// Shared by both handlers so neither can resolve a scope the other would not.
/// The address is the session's, not a placeholder: it is what stamps
/// `$auth.risk_score` — without it the risk gate refuses every subscription as
/// unassessed once `[auth.risk]` is enabled — and what lets a `REQUIRE IP`
/// scope grant apply to this subscriber instead of being silently withheld.
fn subscription_admission_scope<'a>(
    shared: &'a SharedState,
    identity: &'a AuthenticatedIdentity,
    database_id: DatabaseId,
    session: &'a SyncSession,
) -> crate::Result<ClientRequestScope<'a, 'a>> {
    ClientRequestScope::for_database(
        identity,
        shared.auth_stores(),
        database_id,
        &session.device_metadata.remote_addr,
    )
}

fn log_refusal(session_id: &str, shape_id: &str, failure: ShapeAuthorizationFailure) {
    match failure {
        ShapeAuthorizationFailure::IdentityNotEstablished => warn!(
            session = session_id,
            shape_id, "shape read refused: session has no established identity"
        ),
        ShapeAuthorizationFailure::PermissionDenied => warn!(
            session = session_id,
            shape_id, "shape read refused: no read grant on the shape's collection"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::identity::{AuthMethod, DatabaseSet, Role};
    use crate::control::security::risk::RiskConfig;
    use crate::control::security::scope::grant::ScopeGrantParams;
    use crate::types::TenantId;
    use crate::wal::WalManager;

    use super::*;

    /// A shared state whose risk scorer is enabled with an allow band that
    /// covers every score, so anything the gate refuses was refused because
    /// nothing could be scored — not because the score was bad.
    fn state_with_risk(risk: RiskConfig) -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create shape admission test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("shape-admission.wal"))
                .expect("open shape admission test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new_with_risk_config(dispatcher, wal, risk)
            .expect("construct shape admission state");
        (state, dir)
    }

    fn subscriber() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            77,
            "device",
            TenantId::new(1),
            AuthMethod::ApiKey,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        )
    }

    fn session_from(remote_addr: &str) -> SyncSession {
        let mut session = SyncSession::new("s_shape_admission".into());
        session.device_metadata.remote_addr = remote_addr.to_string();
        session
    }

    /// The subscription scope must carry the session's own remote address, so
    /// risk scoring produces a verdict instead of the gate refusing every
    /// subscription as unassessed. Before the address reached this scope, an
    /// operator enabling `[auth.risk]` took shape subscribe and resync offline
    /// entirely.
    #[test]
    fn subscription_scope_is_scored_from_the_sessions_remote_address() {
        let (state, _dir) = state_with_risk(RiskConfig {
            enabled: true,
            allow_threshold: 1.0,
            deny_threshold: 2.0,
            ..Default::default()
        });
        let identity = subscriber();
        let session = session_from("10.0.0.7:44321");

        let request =
            subscription_admission_scope(&state, &identity, DatabaseId::DEFAULT, &session)
                .expect("resolve admission scope");

        assert_eq!(request.peer_addr(), "10.0.0.7:44321");
        assert!(
            request.scope().auth().risk_score.is_some(),
            "the session's address must reach the risk scorer"
        );
        assert!(
            state
                .risk_scorer
                .refusal_for(request.scope().auth())
                .is_none(),
            "an assessed, in-band subscription must not be refused"
        );
    }

    /// The silent half of the same defect: a grant conditioned on the client's
    /// network must apply to a subscriber inside it. With no address in the
    /// scope the condition can never be satisfied, so the scope was withheld
    /// with no error anywhere.
    #[test]
    fn ip_conditional_grant_is_honoured_for_a_subscriber_inside_the_network() {
        let (state, _dir) = state_with_risk(RiskConfig::default());
        let identity = subscriber();
        state
            .scope_grants
            .grant(ScopeGrantParams {
                scope_name: "sync:shapes",
                grantee_type: "user",
                grantee_id: "77",
                granted_by: "admin",
                expires_at: 0,
                grace_period_secs: 0,
                on_expire_action: "",
                conditions: vec![
                    crate::control::security::conditional::GrantCondition::RequireIp {
                        allowed_cidrs: vec!["10.0.0.0/8".into()],
                    },
                ],
            })
            .expect("grant the IP-conditional scope");

        let inside = session_from("10.0.0.7:44321");
        let granted = subscription_admission_scope(&state, &identity, DatabaseId::DEFAULT, &inside)
            .expect("resolve admission scope");
        assert_eq!(
            granted
                .scope()
                .auth()
                .metadata
                .get("scope_status.sync:shapes"),
            Some(&nodedb_types::Value::String("active".into())),
            "a subscriber inside the permitted network must hold the conditional scope"
        );

        let outside = session_from("203.0.113.9:44321");
        let withheld =
            subscription_admission_scope(&state, &identity, DatabaseId::DEFAULT, &outside)
                .expect("resolve admission scope");
        assert!(
            !withheld
                .scope()
                .auth()
                .metadata
                .contains_key("scope_status.sync:shapes"),
            "a subscriber outside the permitted network must not hold it"
        );
    }
}
