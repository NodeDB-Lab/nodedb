// SPDX-License-Identifier: BUSL-1.1

//! Audit entries of bearer-token logins on the catalog OIDC route and the
//! static `[auth.jwt]` route.
//!
//! Every refusal records one `AuthFailure` with the reason, the provider, the
//! peer address, and the claimed `sub`. Every login records one `AuthSuccess`
//! with the user, the session, the provider, and the verified `sub`.
//! Neither entry holds the token.

use nodedb::config::auth::{JwtAuthConfig, JwtProviderConfig};
use nodedb::control::security::audit::{AuditEntry, AuditEvent};
use nodedb::control::security::oidc::verify_bearer_token;
use nodedb::control::security::ratelimit::limiter::LoginRateLimitOutcome;
use nodedb::control::server::session_auth::authenticate_bearer_jwt;
use nodedb::control::state::SharedState;
use nodedb_test_support::pgwire_auth_helpers::{ddl_ok, superuser};

use super::oidc_authentication::{
    PEER, SESSION, forged_signature, loopback_jwks_config, signed_jwt_fixture, spawn_static_jwks,
    state_with_registry,
};

/// The bearer entries of `event`, oldest first, from the buffer and the catalog.
fn bearer_entries(state: &SharedState, event: AuditEvent) -> Vec<AuditEntry> {
    let prefix = match event {
        AuditEvent::AuthSuccess => "bearer login accepted:",
        _ => "bearer login refused:",
    };
    state
        .recent_audit_entries(10_000, |entry| {
            entry.event == event && entry.detail.starts_with(prefix)
        })
        .expect("read audit entries")
}

/// Create tenant 42 and the catalog provider `name` for `issuer`, `audience`, and `jwks_uri`.
async fn create_catalog_provider(
    state: &SharedState,
    name: &str,
    issuer: &str,
    audience: &str,
    jwks_uri: &str,
    mapped_sub: &str,
) {
    let su = superuser();
    ddl_ok(
        state,
        &su,
        &format!(
            "CREATE OIDC PROVIDER {name} \
             ISSUER '{issuer}' \
             JWKS_URI '{jwks_uri}' \
             AUDIENCE '{audience}' \
             TENANT 42 \
             CLAIM MAPPING WHEN sub = '{mapped_sub}' SET DEFAULT_DATABASE = 0"
        ),
    )
    .await;
}

#[tokio::test]
async fn oidc_login_records_user_session_provider_and_sub() {
    let issuer = "https://audit-success-idp.example/";
    let (jwks, token) = signed_jwt_fixture(issuer, "audit-api", 999);
    let jwks_uri = spawn_static_jwks(jwks).await;
    let state = state_with_registry(loopback_jwks_config()).await;
    ddl_ok(&state, &superuser(), "CREATE TENANT audit_ok ID 42").await;
    create_catalog_provider(&state, "audit_idp", issuer, "audit-api", &jwks_uri, "*").await;

    let (identity, _claims) = verify_bearer_token(&state, &token, PEER, SESSION)
        .await
        .expect("catalog-backed OIDC token must validate");

    let successes = bearer_entries(&state, AuditEvent::AuthSuccess);
    assert_eq!(successes.len(), 1, "one login, one entry: {successes:?}");
    let entry = &successes[0];
    assert_eq!(
        entry.detail,
        "bearer login accepted: method=oidc_bearer provider=\"audit_idp\" sub=\"alice\""
    );
    assert_eq!(entry.source, PEER);
    assert_eq!(entry.tenant_id, Some(identity.tenant_id));
    assert_eq!(entry.auth_user_id, identity.user_id.to_string());
    assert_eq!(entry.auth_user_name, "alice");
    assert_eq!(entry.session_id, SESSION);
    assert!(!entry.detail.contains(&token));
    assert!(bearer_entries(&state, AuditEvent::AuthFailure).is_empty());
}

#[tokio::test]
async fn oidc_refusals_record_reason_provider_peer_and_claimed_sub() {
    let issuer = "https://audit-refusal-idp.example/";
    let (jwks, valid_token) = signed_jwt_fixture(issuer, "audit-api", 999);
    let jwks_uri = spawn_static_jwks(jwks).await;
    let (_, unknown_issuer_token) =
        signed_jwt_fixture("https://unknown-audit-idp.example/", "audit-api", 999);
    let (_, wrong_audience_token) = signed_jwt_fixture(issuer, "other-api", 999);
    let forged_token = forged_signature(&valid_token);
    let state = state_with_registry(loopback_jwks_config()).await;
    ddl_ok(&state, &superuser(), "CREATE TENANT audit_refusal ID 42").await;
    create_catalog_provider(&state, "refusal_idp", issuer, "audit-api", &jwks_uri, "*").await;

    for token in [
        "not-a-jwt",
        unknown_issuer_token.as_str(),
        wrong_audience_token.as_str(),
        forged_token.as_str(),
    ] {
        verify_bearer_token(&state, token, PEER, SESSION)
            .await
            .expect_err("the token must be refused");
    }

    let details: Vec<String> = bearer_entries(&state, AuditEvent::AuthFailure)
        .into_iter()
        .map(|entry| {
            assert_eq!(entry.source, PEER);
            entry.detail
        })
        .collect();
    assert_eq!(
        details,
        [
            "bearer login refused: method=oidc_bearer provider=<unknown> sub=<unknown> \
             reason=\"malformed JWT token\"",
            "bearer login refused: method=oidc_bearer provider=<unknown> sub=\"alice\" \
             reason=\"JWT issuer mismatch\"",
            "bearer login refused: method=oidc_bearer provider=<unknown> sub=\"alice\" \
             reason=\"JWT audience mismatch\"",
            "bearer login refused: method=oidc_bearer provider=\"refusal_idp\" sub=\"alice\" \
             reason=\"JWT signature verification failed\"",
        ]
    );
    for token in [&unknown_issuer_token, &wrong_audience_token, &forged_token] {
        assert!(
            details
                .iter()
                .all(|detail| !detail.contains(token.as_str())),
            "the token must never reach the audit log: {details:?}"
        );
    }
    assert!(bearer_entries(&state, AuditEvent::AuthSuccess).is_empty());
}

#[tokio::test]
async fn oidc_policy_refusal_records_the_typed_error_as_reason() {
    let issuer = "https://audit-policy-idp.example/";
    let (jwks, token) = signed_jwt_fixture(issuer, "audit-api", 999);
    let jwks_uri = spawn_static_jwks(jwks).await;
    let state = state_with_registry(loopback_jwks_config()).await;
    ddl_ok(&state, &superuser(), "CREATE TENANT audit_policy ID 42").await;
    // The only mapping rule names another subject, so `alice` gets no default database.
    create_catalog_provider(&state, "policy_idp", issuer, "audit-api", &jwks_uri, "bob").await;

    let err = verify_bearer_token(&state, &token, PEER, SESSION)
        .await
        .expect_err("a token that maps to no database must be refused");
    assert!(matches!(err, nodedb::Error::OidcNoDefaultDatabase { .. }));

    let failures = bearer_entries(&state, AuditEvent::AuthFailure);
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert_eq!(
        failures[0].detail,
        format!(
            "bearer login refused: method=oidc_bearer provider=\"policy_idp\" sub=\"alice\" \
             reason={:?}",
            err.to_string()
        )
    );
    assert_eq!(failures[0].tenant_id.map(|t| t.as_u64()), Some(42));
}

/// A refused bearer token names an unverified `sub`, so it must never count against that account.
#[tokio::test]
async fn bearer_refusals_never_feed_lockout_or_login_rate_limits() {
    let issuer = "https://audit-lockout-idp.example/";
    let (jwks, valid_token) = signed_jwt_fixture(issuer, "audit-api", 999);
    let jwks_uri = spawn_static_jwks(jwks).await;
    let forged_token = forged_signature(&valid_token);
    let state = state_with_registry(loopback_jwks_config()).await;
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT audit_lockout ID 42").await;
    ddl_ok(&state, &su, "CREATE USER alice WITH PASSWORD 'pass'").await;
    create_catalog_provider(&state, "lockout_idp", issuer, "audit-api", &jwks_uri, "*").await;

    for _ in 0..25 {
        verify_bearer_token(&state, &forged_token, PEER, SESSION)
            .await
            .expect_err("a forged token must be refused");
    }

    assert_eq!(bearer_entries(&state, AuditEvent::AuthFailure).len(), 25);
    state
        .credentials
        .check_lockout("alice")
        .expect("bearer refusals must not lock the stored account named by the token");
    assert!(matches!(
        state.rate_limiter.check_login("203.0.113.10", "alice"),
        LoginRateLimitOutcome::Allowed
    ));
}

/// A state whose `[auth.jwt]` section holds the static provider `corp` for `issuer`.
async fn static_provider_state(
    issuer: &str,
    jwks_url: String,
) -> nodedb_test_support::pgwire_auth_helpers::BootedState {
    state_with_registry(JwtAuthConfig {
        providers: vec![JwtProviderConfig {
            name: "corp".into(),
            jwks_url,
            issuer: issuer.into(),
            audience: "corp-api".into(),
            tenant_id: 42,
        }],
        ..loopback_jwks_config()
    })
    .await
}

#[tokio::test]
async fn jwt_bearer_login_records_user_session_provider_and_sub() {
    let issuer = "https://static-audit-idp.example/";
    let (jwks, token) = signed_jwt_fixture(issuer, "corp-api", 999);
    let state = static_provider_state(issuer, spawn_static_jwks(jwks).await).await;

    let (identity, _claims) = authenticate_bearer_jwt(&state, &token, PEER, SESSION)
        .await
        .expect("static-provider JWT must validate");

    let successes = bearer_entries(&state, AuditEvent::AuthSuccess);
    assert_eq!(successes.len(), 1, "one login, one entry: {successes:?}");
    let entry = &successes[0];
    assert_eq!(
        entry.detail,
        "bearer login accepted: method=jwt_bearer provider=\"corp\" sub=\"alice\""
    );
    assert_eq!(entry.source, PEER);
    assert_eq!(entry.tenant_id.map(|t| t.as_u64()), Some(42));
    assert_eq!(entry.auth_user_id, identity.user_id.to_string());
    assert_eq!(entry.auth_user_name, identity.username);
    assert_eq!(entry.session_id, SESSION);
}

#[tokio::test]
async fn jwt_bearer_refusals_record_reason_provider_peer_and_claimed_sub() {
    let issuer = "https://static-refusal-idp.example/";
    let (jwks, valid_token) = signed_jwt_fixture(issuer, "corp-api", 999);
    let (_, wrong_audience_token) = signed_jwt_fixture(issuer, "other-api", 999);
    let forged_token = forged_signature(&valid_token);
    let state = static_provider_state(issuer, spawn_static_jwks(jwks).await).await;

    for token in [wrong_audience_token.as_str(), forged_token.as_str()] {
        assert!(
            authenticate_bearer_jwt(&state, token, PEER, SESSION)
                .await
                .is_none(),
            "the token must be refused"
        );
    }

    let failures = bearer_entries(&state, AuditEvent::AuthFailure);
    let details: Vec<&str> = failures.iter().map(|entry| entry.detail.as_str()).collect();
    assert_eq!(
        details,
        [
            "bearer login refused: method=jwt_bearer provider=<unknown> sub=\"alice\" \
             reason=\"JWT audience mismatch\"",
            "bearer login refused: method=jwt_bearer provider=\"corp\" sub=\"alice\" \
             reason=\"JWT signature verification failed\"",
        ]
    );
    assert!(failures.iter().all(|entry| entry.source == PEER));
    assert_eq!(failures[1].tenant_id.map(|t| t.as_u64()), Some(42));
    assert!(bearer_entries(&state, AuditEvent::AuthSuccess).is_empty());
}
