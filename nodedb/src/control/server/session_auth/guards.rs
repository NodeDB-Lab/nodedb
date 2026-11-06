// SPDX-License-Identifier: BUSL-1.1

//! Post-identity authorization guards: blacklist, transport security, risk,
//! and rate-limit checks.

use crate::control::security::auth_context::AuthContext;
use crate::control::security::blacklist::store::user_entry_id;
use crate::control::security::escalation::{
    AuthViolation, ViolationSubject, record_auth_violation,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::tls_policy::TransportSecurity;
use crate::control::state::SharedState;

/// Check if a user is blacklisted. Returns `Err` if blocked.
///
/// Called after identity is resolved, before authorization.
pub fn check_blacklist(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    peer_addr: &str,
) -> crate::Result<()> {
    // Check user blacklist.
    let user_id = user_entry_id(identity.user_id, &identity.username);
    if let Some(entry) = state.blacklist.check_user(&user_id) {
        record_auth_violation(
            state,
            AuthViolation {
                subject: ViolationSubject::Identity(identity),
                tenant_id: Some(identity.tenant_id),
                source: peer_addr,
                detail: &format!(
                    "blacklisted user '{}' denied: {}",
                    identity.username, entry.reason
                ),
            },
        );
        return Err(crate::Error::RejectedAuthz {
            tenant_id: identity.tenant_id,
            resource: format!("user blacklisted: {}", entry.reason),
        });
    }

    // Check IP blacklist.
    if let Some(entry) = state.blacklist.check_ip(peer_addr) {
        record_auth_violation(
            state,
            AuthViolation {
                subject: ViolationSubject::Identity(identity),
                tenant_id: Some(identity.tenant_id),
                source: peer_addr,
                detail: &format!("blacklisted IP '{peer_addr}' denied: {}", entry.reason),
            },
        );
        return Err(crate::Error::RejectedAuthz {
            tenant_id: identity.tenant_id,
            resource: format!("IP blacklisted: {}", entry.reason),
        });
    }

    // Check auth user status (JIT-provisioned users).
    if let Some(status) = state.auth_users.get_status(&user_id) {
        let ctx_status = status;
        if matches!(
            ctx_status,
            crate::control::security::auth_context::AuthStatus::Suspended
                | crate::control::security::auth_context::AuthStatus::Banned
        ) {
            // Audit only: this rejection *is* the standing verdict being
            // enforced, so counting it would let a retry loop advance the
            // ladder from its own refusals.
            record_auth_violation(
                state,
                AuthViolation {
                    subject: ViolationSubject::AuditOnly,
                    tenant_id: Some(identity.tenant_id),
                    source: peer_addr,
                    detail: &format!(
                        "auth user '{}' denied: account {}",
                        identity.username, ctx_status
                    ),
                },
            );
            return Err(crate::Error::RejectedAuthz {
                tenant_id: identity.tenant_id,
                resource: format!("account {ctx_status}"),
            });
        }
    }

    // Check org status overrides member status.
    // If any of the user's orgs is suspended/banned, block the user.
    let user_org_ids = state.orgs.orgs_for_user(&user_id);
    for org_id in &user_org_ids {
        if !state.orgs.is_active(org_id) {
            // Audit only: the org's status, not the user's conduct, is what
            // refuses this request.
            record_auth_violation(
                state,
                AuthViolation {
                    subject: ViolationSubject::AuditOnly,
                    tenant_id: Some(identity.tenant_id),
                    source: peer_addr,
                    detail: &format!(
                        "org '{}' is not active — user '{}' blocked",
                        org_id, identity.username
                    ),
                },
            );
            return Err(crate::Error::RejectedAuthz {
                tenant_id: identity.tenant_id,
                resource: format!("organization '{org_id}' is suspended"),
            });
        }
    }

    Ok(())
}

/// Enforce the TLS policy for the connection an authenticated identity
/// arrived on.
///
/// `transport` is what the connection negotiated, captured at accept by the
/// listener that owns the socket and carried to here — the earliest point at
/// which `is_superuser` is known, which the policy's cleartext carve-out needs.
/// Every transport that can carry a client connection calls this once its
/// identity is resolved.
///
/// Refusals use [`crate::Error::RejectedAuthz`], the same authorization
/// rejection the blacklist, account-status, and risk guards raise, so clients
/// see one consistent non-retryable code; the reason string names the
/// transport fault. Returns `Ok(())` when enforcement is off — the default.
pub fn check_transport_security(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    transport: TransportSecurity,
    peer_addr: &str,
) -> crate::Result<()> {
    let Err(refusal) = state
        .tls_policy
        .check_connection(transport, identity.is_superuser)
    else {
        return Ok(());
    };

    // A transport refusal is a repeated-violation signal like any other
    // rejection on this path, so it goes through the shared recorder: one
    // call, audit entry plus violation count.
    record_auth_violation(
        state,
        AuthViolation {
            subject: ViolationSubject::Identity(identity),
            tenant_id: Some(identity.tenant_id),
            source: peer_addr,
            detail: &format!("TLS policy refused user '{}': {refusal}", identity.username),
        },
    );

    Err(crate::Error::RejectedAuthz {
        tenant_id: identity.tenant_id,
        resource: refusal.to_string(),
    })
}

/// Enforce the adaptive-auth risk decision for a request.
///
/// The score itself was computed once, in
/// [`RequestAuthScopeBuilder::build`](crate::control::security::request_scope::RequestAuthScopeBuilder::build),
/// where the transport's real client address was in hand; this guard only
/// turns the stamped `$auth.risk_score` into a refusal. Returns `Ok(())`
/// when scoring is disabled or the score is in the allow band.
///
/// Refusals use [`crate::Error::RejectedAuthz`] — the same authorization
/// rejection the blacklist and account-status guards raise on this path, so
/// clients see one consistent, non-retryable code for "this request is not
/// allowed" rather than a risk-specific status they would have to learn.
/// The reason string distinguishes the three cases (deny, step-up required,
/// unassessed).
pub fn check_risk(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    auth_ctx: &AuthContext,
    peer_addr: &str,
) -> crate::Result<()> {
    let Some(refusal) = state.risk_scorer.refusal_for(auth_ctx) else {
        return Ok(());
    };

    // A refusal here is exactly the repeated-violation signal auto-escalation
    // consumes, so it goes through the shared recorder like every other
    // rejection: one call, audit entry plus violation count.
    record_auth_violation(
        state,
        AuthViolation {
            subject: ViolationSubject::Identity(identity),
            tenant_id: Some(identity.tenant_id),
            source: peer_addr,
            detail: &format!(
                "risk gate refused user '{}': {}",
                identity.username, refusal.audit_detail
            ),
        },
    );

    Err(crate::Error::RejectedAuthz {
        tenant_id: identity.tenant_id,
        resource: refusal.resource,
    })
}

/// Check rate limit for a request.
///
/// Called after identity and blacklist checks, before query execution.
/// Returns `Err(RateLimited)` if the request exceeds the rate limit.
///
/// Tenant and database QPS caps are read from the quota catalog when available.
/// Check order: user → org → tenant → database.
pub fn check_rate_limit(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    auth_ctx: &AuthContext,
    operation: &str,
    database_id: nodedb_types::DatabaseId,
) -> crate::Result<crate::control::security::ratelimit::limiter::RateLimitResult> {
    use crate::control::security::ratelimit::limiter::QuotaCheckParams;

    let plan_tier = auth_ctx.metadata.get("plan").and_then(|v| v.as_str());

    // Resolve tenant and database QPS caps from the quota catalog if available.
    let quota_params = {
        let catalog = state.credentials.catalog();
        let tenant_max_qps = catalog
            .get_tenant_quota(database_id, identity.tenant_id)
            .ok()
            .flatten()
            .and_then(|r| {
                if r.max_qps > 0 {
                    Some(r.max_qps as u64)
                } else {
                    None
                }
            });

        let database_max_qps = catalog
            .get_database_quota(database_id)
            .ok()
            .flatten()
            .and_then(|r| {
                if r.max_qps > 0 {
                    Some(r.max_qps as u64)
                } else {
                    None
                }
            });

        if tenant_max_qps.is_some() || database_max_qps.is_some() {
            Some(QuotaCheckParams {
                tenant_max_qps,
                database_max_qps,
                tenant_id: identity.tenant_id,
                database_id,
            })
        } else {
            None
        }
    };

    let result = state.rate_limiter.check(
        &identity.user_id.to_string(),
        &auth_ctx.org_ids,
        plan_tier,
        operation,
        quota_params.as_ref(),
    );

    if !result.allowed {
        return Err(crate::Error::RateExceeded {
            gate: operation.to_string(),
            detail: format!("rate limited for user {}", identity.user_id),
            retry_after_ms: result.retry_after_secs.saturating_mul(1000),
        });
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::DatabaseId;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, CatalogPrincipal, DatabaseSet, Role,
    };
    use crate::control::security::tls_policy::{TlsPolicyConfig, TlsVersion};
    use crate::types::TenantId;
    use crate::wal::WalManager;

    use super::*;

    /// Returns the state plus the backing `TempDir` guard — the caller must
    /// keep the guard alive for as long as `state` is in use.
    fn test_state(config: TlsPolicyConfig) -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new_with_tls_policy_config(dispatcher, wal, config)
            .expect("construct shared state");
        (state, dir)
    }

    /// An entry names no auth method. A number refuses an OIDC identity with
    /// that number, and a subject refuses a token with no `user_id` claim.
    #[test]
    fn a_user_entry_refuses_by_number_across_auth_methods_and_by_subject() {
        let (state, _dir) = test_state(tls_policy_config(false, "1.2", false));
        let oidc = |user_id: u64, username: &str| {
            AuthenticatedIdentity::new_regular(
                user_id,
                username,
                TenantId::new(1),
                AuthMethod::OidcBearer,
                vec![Role::ReadWrite],
                None,
                DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
            )
        };
        let peer = "10.0.0.1:1";
        state
            .blacklist
            .blacklist_user("77", "abuse", "admin", 0)
            .expect("blacklist a number");
        state
            .blacklist
            .blacklist_user("sub-a", "abuse", "admin", 0)
            .expect("blacklist a subject");

        assert!(check_blacklist(&state, &oidc(77, "sub-x"), peer).is_err());
        assert!(check_blacklist(&state, &oidc(0, "sub-a"), peer).is_err());
        assert!(check_blacklist(&state, &oidc(0, "sub-b"), peer).is_ok());
        // A subject never stands in for a number the identity carries.
        assert!(check_blacklist(&state, &oidc(78, "sub-a"), peer).is_ok());
    }

    fn tls_policy_config(enabled: bool, min: &str, reject_cleartext: bool) -> TlsPolicyConfig {
        TlsPolicyConfig {
            enabled,
            min_tls_version: min.into(),
            reject_cleartext,
        }
    }

    fn regular_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            9301,
            "regular-user",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        )
    }

    /// A real superuser — `new_regular` strips the superuser role by design,
    /// so the carve-out can only be exercised through a catalog principal.
    fn superuser_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::from_catalog_principal(CatalogPrincipal {
            user_id: 9302,
            username: "root".into(),
            tenant_id: TenantId::new(1),
            auth_method: AuthMethod::ScramSha256,
            roles: vec![Role::Superuser],
            is_superuser: true,
            default_database: None,
            accessible_databases: DatabaseSet::All,
        })
    }

    fn rejection_reason(error: &crate::Error) -> String {
        match error {
            crate::Error::RejectedAuthz { resource, .. } => resource.clone(),
            other => panic!("expected an authz rejection, got {other:?}"),
        }
    }

    #[test]
    fn cleartext_is_refused_when_the_policy_rejects_it() {
        let (state, _dir) = test_state(tls_policy_config(true, "1.2", true));
        let identity = regular_identity();

        let error = check_transport_security(
            &state,
            &identity,
            TransportSecurity::Cleartext,
            "10.0.0.1:5432",
        )
        .expect_err("a plaintext connection must be refused");
        assert_eq!(
            rejection_reason(&error),
            "cleartext connections rejected by TLS policy"
        );
    }

    #[test]
    fn cleartext_is_allowed_when_the_policy_does_not_reject_it() {
        let (state, _dir) = test_state(tls_policy_config(true, "1.2", false));
        let identity = regular_identity();

        check_transport_security(
            &state,
            &identity,
            TransportSecurity::Cleartext,
            "10.0.0.1:5432",
        )
        .expect("a plaintext connection must be admitted when cleartext is permitted");
    }

    /// The carve-out, pinned end-to-end: `reject_cleartext` refuses the
    /// regular identity and admits the superuser on the same connection.
    #[test]
    fn the_superuser_carve_out_covers_cleartext_only() {
        let (state, _dir) = test_state(tls_policy_config(true, "1.3", true));

        assert!(
            check_transport_security(
                &state,
                &regular_identity(),
                TransportSecurity::Cleartext,
                "10.0.0.1:5432",
            )
            .is_err()
        );
        check_transport_security(
            &state,
            &superuser_identity(),
            TransportSecurity::Cleartext,
            "10.0.0.1:5432",
        )
        .expect("a superuser keeps a cleartext way in");

        // ...but not below the minimum version.
        let error = check_transport_security(
            &state,
            &superuser_identity(),
            TransportSecurity::Tls(TlsVersion::Tls1_2),
            "10.0.0.1:5432",
        )
        .expect_err("a superuser on an obsolete TLS version must still be refused");
        assert_eq!(
            rejection_reason(&error),
            "TLS 1.2 is below the required minimum TLS 1.3"
        );
    }

    #[test]
    fn tls_below_the_minimum_is_refused_and_at_or_above_is_allowed() {
        let (state, _dir) = test_state(tls_policy_config(true, "1.3", false));
        let identity = regular_identity();

        assert!(
            check_transport_security(
                &state,
                &identity,
                TransportSecurity::Tls(TlsVersion::Tls1_2),
                "10.0.0.1:5432",
            )
            .is_err()
        );
        check_transport_security(
            &state,
            &identity,
            TransportSecurity::Tls(TlsVersion::Tls1_3),
            "10.0.0.1:5432",
        )
        .expect("a connection at the minimum must be admitted");
    }

    #[test]
    fn an_unidentifiable_tls_connection_fails_closed() {
        let (state, _dir) = test_state(tls_policy_config(true, "1.2", false));

        let error = check_transport_security(
            &state,
            &regular_identity(),
            TransportSecurity::TlsUnidentified,
            "10.0.0.1:5432",
        )
        .expect_err("an unrankable TLS connection must not be admitted");
        assert_eq!(
            rejection_reason(&error),
            "negotiated TLS version could not be identified"
        );
    }

    /// The knob is not inert: the very same connection is refused or admitted
    /// depending only on what the server config said, and nothing is enforced
    /// at all while `enabled` is false.
    #[test]
    fn configured_policy_reaches_shared_state_and_changes_the_outcome() {
        let identity = regular_identity();
        let connection = TransportSecurity::Tls(TlsVersion::Tls1_2);

        let (strict, _dir_a) = test_state(tls_policy_config(true, "1.3", true));
        assert!(
            check_transport_security(&strict, &identity, connection, "10.0.0.1:5432").is_err(),
            "min_tls_version = 1.3 must refuse a TLS 1.2 connection"
        );

        let (relaxed, _dir_b) = test_state(tls_policy_config(true, "1.2", true));
        assert!(
            check_transport_security(&relaxed, &identity, connection, "10.0.0.1:5432").is_ok(),
            "min_tls_version = 1.2 must admit the same connection"
        );

        let (disabled, _dir_c) = test_state(tls_policy_config(false, "1.3", true));
        assert!(
            check_transport_security(&disabled, &identity, connection, "10.0.0.1:5432").is_ok()
        );
        assert!(
            check_transport_security(
                &disabled,
                &identity,
                TransportSecurity::Cleartext,
                "10.0.0.1:5432"
            )
            .is_ok(),
            "a disabled policy must refuse nothing, including cleartext"
        );
    }

    /// The out-of-the-box server enforces nothing, so no existing plaintext
    /// deployment changes behaviour by upgrading.
    #[test]
    fn the_default_configuration_admits_plaintext() {
        let (state, _dir) = test_state(TlsPolicyConfig::default());
        assert!(
            check_transport_security(
                &state,
                &regular_identity(),
                TransportSecurity::Cleartext,
                "10.0.0.1:5432",
            )
            .is_ok()
        );
    }

    /// An unparseable minimum is a load-time failure, not a silent default:
    /// state construction — which is what production startup runs — errors.
    #[test]
    fn an_unparseable_min_version_fails_state_construction() {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);

        let result = SharedState::new_with_tls_policy_config(
            dispatcher,
            wal,
            tls_policy_config(true, "1.2 or better", true),
        );
        assert!(
            matches!(result, Err(crate::Error::Config { .. })),
            "an unparseable min_tls_version must fail startup"
        );
    }
}
