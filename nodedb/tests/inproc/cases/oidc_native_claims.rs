// SPDX-License-Identifier: BUSL-1.1

//! Verified OIDC claims must reach `$auth.*` on the native protocol.
//!
//! `verify_bearer_token` now returns the opaque [`VerifiedJwtClaims`] proof
//! alongside the `AuthenticatedIdentity`, and the native session
//! (`control::server::native::session::{auth, request}`) threads it into
//! every request's `RequestAuthScope` via
//! `RequestAuthScope::builder().with_verified_jwt(..)` — the same
//! constructor path the native session code takes. Standing up a full TCP
//! native session with a catalog OIDC provider + JWKS registry requires
//! mutating `NativeTestServer`'s `SharedState` after construction, which is
//! not possible once the listener/poller/event-plane tasks have each taken
//! their own `Arc` clone. These tests instead exercise the closest seam that
//! is both reachable and faithful to the real call chain: the same
//! `verify_bearer_token` -> `RequestAuthScope::builder().with_verified_jwt()`
//! pipeline `native::session::request::handle_request` drives, built
//! directly against a `SharedState` (no TCP needed to reach it).

use std::sync::Arc;

use base64::Engine;
use nodedb::config::auth::JwtAuthConfig;
use nodedb::control::security::jwks::registry::JwksRegistry;
use nodedb::control::security::oidc::verify_bearer_token;
use nodedb::control::security::request_scope::RequestAuthScope;

use super::oidc_authentication::{PEER, SESSION};
use nodedb_test_support::jwks_fixture::{loopback_egress, loopback_sources};
use nodedb_test_support::pgwire_auth_helpers::{
    ddl_ok, make_state_with_catalog_configured, superuser,
};

async fn spawn_static_jwks(body: String) -> String {
    let listener = tokio::net::TcpListener::bind("[::]:0")
        .await
        .expect("JWKS fixture must bind");
    let addr = listener
        .local_addr()
        .expect("JWKS fixture must expose its address");
    tokio::spawn(async move {
        loop {
            if let Ok((mut stream, _)) = listener.accept().await {
                use tokio::io::AsyncWriteExt;

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        }
    });
    format!("http://localhost:{}/jwks.json", addr.port())
}

/// Sign a JWT fixture carrying extra (non-authoritative) claims — `email`,
/// `org_id`, `groups`, `permissions` — plus an externally-asserted
/// `is_superuser: true` and a `superuser` role name, to exercise both claim
/// enrichment and the non-forgeability invariant in one token.
fn signed_jwt_fixture_with_extra_claims(issuer: &str, audience: &str) -> (String, String) {
    use rsa::pkcs1v15::SigningKey;
    use rsa::signature::{SignatureEncoding, Signer};
    use rsa::traits::PublicKeyParts;

    let mut rng = rsa::rand_core::OsRng;
    let private_key =
        rsa::RsaPrivateKey::new(&mut rng, 1024).expect("RSA fixture key generation must succeed");
    let public_key = rsa::RsaPublicKey::from(&private_key);
    let encode = |bytes: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    let jwks = format!(
        r#"{{"keys":[{{"kty":"RSA","kid":"native-claims-fixture","alg":"RS256","use":"sig","n":"{}","e":"{}"}}]}}"#,
        encode(&public_key.n().to_bytes_be()),
        encode(&public_key.e().to_bytes_be()),
    );
    let header = encode(br#"{"alg":"RS256","kid":"native-claims-fixture"}"#);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("fixture clock must be after epoch")
        .as_secs();
    let payload = encode(
        format!(
            r#"{{"iss":"{issuer}","aud":"{audience}","sub":"alice","tenant_id":999,"iat":{now},"exp":{},"user_id":42,"is_superuser":true,"roles":["superuser"],"email":"alice@example.test","org_id":"org-native","org_ids":["org-native","org-other"],"groups":["engineering"],"permissions":["documents:read"]}}"#,
            now + 3_600
        )
        .as_bytes(),
    );
    let signing_input = format!("{header}.{payload}");
    let signing_key = SigningKey::<sha2::Sha256>::new(private_key);
    let signature: rsa::pkcs1v15::Signature = signing_key.sign(signing_input.as_bytes());

    (
        jwks,
        format!("{signing_input}.{}", encode(&signature.to_bytes())),
    )
}

/// Set up a `SharedState` with a catalog OIDC provider bound to `tenant_id`
/// 999 and a `JwksRegistry`, then verify `token` against it. Mirrors the
/// setup `oidc_authentication.rs` uses, factored out so every test below
/// shares one code path to the real `verify_bearer_token` entry point.
async fn verify_via_catalog_provider(
    issuer: &str,
    audience: &str,
    jwks: String,
    token: &str,
) -> (
    nodedb_test_support::pgwire_auth_helpers::BootedState,
    nodedb::control::security::identity::AuthenticatedIdentity,
    nodedb::control::security::jwks::registry::VerifiedJwtClaims,
) {
    let jwks_uri = spawn_static_jwks(jwks).await;
    // The registry is set before the gateway install, as production boot
    // sets it.
    let registry = JwksRegistry::init(JwtAuthConfig::default(), loopback_egress())
        .await
        .expect("test JWKS registry must initialize");
    let state = make_state_with_catalog_configured(|state| {
        state.remote_sources = loopback_sources();
        state.jwks_registry = Some(Arc::new(registry));
    });
    let su = superuser();

    ddl_ok(&state, &su, "CREATE TENANT native_claims_tenant ID 999").await;
    ddl_ok(
        &state,
        &su,
        &format!(
            "CREATE OIDC PROVIDER native_claims_idp \
             ISSUER '{issuer}' \
             JWKS_URI '{jwks_uri}' \
             AUDIENCE '{audience}' \
             TENANT 999 \
             CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 0"
        ),
    )
    .await;

    let (identity, claims) = verify_bearer_token(&state, token, PEER, SESSION)
        .await
        .expect("catalog-backed OIDC token must validate");
    (state, identity, claims)
}

/// The `RequestAuthScope::builder().with_verified_jwt(..)` call is exactly
/// what `native::session::request::handle_request` runs for every request
/// on an OIDC-authenticated connection (see
/// `src/control/server/native/session/request.rs`). Claim-derived
/// enrichment (email, org, groups, permissions) must reach `$auth.*`
/// through it, not just through the discarded return value the gap left
/// behind.
#[tokio::test]
async fn native_request_scope_carries_verified_oidc_claim_enrichment() {
    let issuer = "https://native-claims-idp.example/";
    let audience = "native-claims-api";
    let (jwks, token) = signed_jwt_fixture_with_extra_claims(issuer, audience);
    let (state, identity, claims) =
        verify_via_catalog_provider(issuer, audience, jwks, &token).await;

    let scope = RequestAuthScope::builder(&identity, state.auth_stores())
        .with_verified_jwt(&claims)
        .build()
        .expect("build request scope");
    let auth = scope.auth();

    assert_eq!(auth.email.as_deref(), Some("alice@example.test"));
    assert_eq!(auth.org_id.as_deref(), Some("org-native"));
    assert_eq!(
        auth.org_ids,
        vec!["org-native".to_string(), "org-other".to_string()]
    );
    assert_eq!(auth.groups, vec!["engineering".to_string()]);
    assert_eq!(auth.permissions, vec!["documents:read".to_string()]);
}

/// Mirrors `auth_context::tests::from_jwt_removes_externally_asserted_superuser_authority`
/// through the real native-reachable pipeline: a token asserting
/// `is_superuser: true` and `roles: ["superuser"]` must not elevate the
/// resulting `AuthContext` — authority stays bound to the server-issued
/// `AuthenticatedIdentity` from `verify_bearer_token`, never the claims.
#[tokio::test]
async fn native_request_scope_does_not_forge_superuser_from_verified_oidc_claims() {
    let issuer = "https://native-superuser-forgery-idp.example/";
    let audience = "native-superuser-forgery-api";
    let (jwks, token) = signed_jwt_fixture_with_extra_claims(issuer, audience);
    let (state, identity, claims) =
        verify_via_catalog_provider(issuer, audience, jwks, &token).await;

    // The claim-mapping rule maps no roles for this token, so the
    // catalog-issued identity itself must not be a superuser either — the
    // fixture's `is_superuser`/`roles` claims never reach `identity`.
    assert!(
        !identity.is_superuser(),
        "verify_bearer_token must never derive superuser authority from claims"
    );

    let scope = RequestAuthScope::builder(&identity, state.auth_stores())
        .with_verified_jwt(&claims)
        .build()
        .expect("build request scope");

    assert!(
        !scope.auth().is_superuser(),
        "a verified JWT asserting superuser authority must not elevate the request's AuthContext"
    );
}

/// `native::session::request::handle_request` reuses the session's
/// established `session_id` (rather than generating a fresh one per
/// request) precisely so `$auth.session_id` stays stable across requests
/// on the same connection — see the comment at that call site. Rebuilding
/// the scope twice with the same explicit session id, once per "request",
/// must reproduce that stability even with verified-JWT enrichment applied
/// both times.
#[tokio::test]
async fn native_request_scope_session_id_stable_across_requests_with_verified_jwt() {
    let issuer = "https://native-session-stability-idp.example/";
    let audience = "native-session-stability-api";
    let (jwks, token) = signed_jwt_fixture_with_extra_claims(issuer, audience);
    let (state, identity, claims) =
        verify_via_catalog_provider(issuer, audience, jwks, &token).await;

    let session_id = "s_native_test_connection".to_string();

    let first = RequestAuthScope::builder(&identity, state.auth_stores())
        .with_session_id(session_id.clone())
        .with_verified_jwt(&claims)
        .build()
        .expect("build request scope");
    let second = RequestAuthScope::builder(&identity, state.auth_stores())
        .with_session_id(session_id.clone())
        .with_verified_jwt(&claims)
        .build()
        .expect("build request scope");

    assert_eq!(first.auth().session_id, session_id);
    assert_eq!(second.auth().session_id, session_id);
    assert_eq!(first.auth().session_id, second.auth().session_id);
}
