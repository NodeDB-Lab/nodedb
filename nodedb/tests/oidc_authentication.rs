// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for catalog-backed OIDC authentication.

mod common;

use std::sync::Arc;

use base64::Engine;
use common::pgwire_auth_helpers::{ddl_ok, make_state_with_catalog, superuser};
use nodedb::config::auth::{JwtAuthConfig, JwtProviderConfig};
use nodedb::control::security::jwks::registry::JwksRegistry;
use nodedb::control::security::oidc::{claim_mapping::apply_claim_mapping, verify_bearer_token};

// ── Catalog-backed bearer verification ─────────────────────────────────────

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

#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct LegacyOidcProvider {
    provider_name: String,
    issuer: String,
    jwks_uri: String,
    audience: Option<String>,
    claim_mapping: Vec<nodedb::control::security::catalog::oidc_providers::StoredClaimMappingRule>,
    created_at_lsn: u64,
}

fn forged_signature(token: &str) -> String {
    let (signing_input, signature) = token
        .rsplit_once('.')
        .expect("signed JWT fixture must have a signature");
    format!("{signing_input}.{}", "A".repeat(signature.len()))
}

fn assert_generic_oidc_authentication_failure(error: impl std::fmt::Display) -> String {
    let error = error.to_string();
    assert!(
        error.contains("OIDC authentication failed"),
        "expected the generic OIDC authentication failure, got: {error}"
    );
    let lowercased = error.to_lowercase();
    for detail in [
        "issuer",
        "audience",
        "provider",
        "signature",
        "tenant",
        "binding",
        "unavailable",
    ] {
        assert!(
            !lowercased.contains(detail),
            "unauthenticated token error must not disclose {detail}: {error}"
        );
    }
    error
}

fn signed_jwt_fixture(issuer: &str, audience: &str, tenant_id: u64) -> (String, String) {
    signed_jwt_fixture_with_expiry(issuer, audience, tenant_id, 9_999_999_999)
}

fn signed_jwt_fixture_with_expiry(
    issuer: &str,
    audience: &str,
    tenant_id: u64,
    exp: u64,
) -> (String, String) {
    use rsa::pkcs1v15::SigningKey;
    use rsa::signature::{SignatureEncoding, Signer};
    use rsa::traits::PublicKeyParts;

    let mut rng = rsa::rand_core::OsRng;
    let private_key =
        rsa::RsaPrivateKey::new(&mut rng, 1024).expect("RSA fixture key generation must succeed");
    let public_key = rsa::RsaPublicKey::from(&private_key);
    let encode = |bytes: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    let jwks = format!(
        r#"{{"keys":[{{"kty":"RSA","kid":"catalog-tenant-binding","alg":"RS256","use":"sig","n":"{}","e":"{}"}}]}}"#,
        encode(&public_key.n().to_bytes_be()),
        encode(&public_key.e().to_bytes_be()),
    );
    let header = encode(br#"{"alg":"RS256","kid":"catalog-tenant-binding"}"#);
    let payload = encode(
        format!(
            r#"{{"iss":"{issuer}","aud":"{audience}","sub":"alice","tenant_id":{tenant_id},"exp":{exp},"user_id":42}}"#
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

#[tokio::test]
async fn catalog_provider_tenant_binding_overrides_signed_tenant_claim() {
    let issuer = "https://catalog-idp.example/";
    let (jwks, token) = signed_jwt_fixture(issuer, "nodedb-api", 999);
    let jwks_uri = spawn_static_jwks(jwks).await;
    let mut state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;
    ddl_ok(
        &state,
        &su,
        &format!(
            "CREATE OIDC PROVIDER catalog_idp \
             ISSUER '{issuer}' \
             JWKS_URI '{jwks_uri}' \
             AUDIENCE 'nodedb-api' \
             TENANT 42 \
             CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 1"
        ),
    )
    .await;

    let registry = JwksRegistry::init(JwtAuthConfig {
        allow_http_jwks: true,
        allow_jwks_hosts: vec!["localhost".into()],
        allow_jwks_cidrs: vec!["127.0.0.0/8".into(), "::1/128".into()],
        ..JwtAuthConfig::default()
    })
    .await
    .expect("test JWKS registry must initialize");
    Arc::get_mut(&mut state)
        .expect("test state must remain uniquely owned")
        .jwks_registry = Some(Arc::new(registry));

    let identity = verify_bearer_token(&state, &token)
        .await
        .expect("catalog-backed OIDC token must validate");
    assert_eq!(
        identity.tenant_id.as_u64(),
        42,
        "the catalog provider TENANT binding, not the signed tenant_id claim, determines the identity tenant"
    );
}

#[tokio::test]
async fn catalog_provider_does_not_reuse_static_provider_cache_entry() {
    let catalog_issuer = "https://catalog-collision-idp.example/";
    let (static_jwks, token) = signed_jwt_fixture(catalog_issuer, "catalog-api", 999);
    let (catalog_jwks, _) = signed_jwt_fixture(catalog_issuer, "catalog-api", 999);
    let static_jwks_uri = spawn_static_jwks(static_jwks).await;
    let catalog_jwks_uri = spawn_static_jwks(catalog_jwks).await;
    let mut state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "CREATE TENANT catalog_tenant ID 42").await;
    ddl_ok(
        &state,
        &su,
        &format!(
            "CREATE OIDC PROVIDER colliding_idp \
             ISSUER '{catalog_issuer}' \
             JWKS_URI '{catalog_jwks_uri}' \
             AUDIENCE 'catalog-api' \
             TENANT 42 \
             CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 1"
        ),
    )
    .await;

    let registry = JwksRegistry::init(JwtAuthConfig {
        allow_http_jwks: true,
        allow_jwks_hosts: vec!["localhost".into()],
        allow_jwks_cidrs: vec!["127.0.0.0/8".into(), "::1/128".into()],
        providers: vec![JwtProviderConfig {
            // This is exactly the catalog cache identity generated before
            // static identities moved into their own generated domain.
            name: format!("catalog:colliding_idp:{catalog_jwks_uri}"),
            jwks_url: static_jwks_uri,
            issuer: "https://static-collision-idp.example/".into(),
            audience: "static-api".into(),
            tenant_id: 1,
        }],
        ..JwtAuthConfig::default()
    })
    .await
    .expect("test JWKS registry must initialize");
    Arc::get_mut(&mut state)
        .expect("test state must remain uniquely owned")
        .jwks_registry = Some(Arc::new(registry));

    let err = verify_bearer_token(&state, &token)
        .await
        .expect_err("catalog verification must not reuse the static provider key");
    assert_generic_oidc_authentication_failure(err);
}

#[tokio::test]
async fn catalog_authentication_failures_are_client_indistinguishable() {
    let issuer = "https://generic-auth-failure-idp.example/";
    let (jwks, valid_token) = signed_jwt_fixture(issuer, "expected-audience", 999);
    let jwks_uri = spawn_static_jwks(jwks).await;
    let (_, unknown_issuer_token) = signed_jwt_fixture(
        "https://unknown-auth-failure-idp.example/",
        "expected-audience",
        999,
    );
    let (_, wrong_audience_token) = signed_jwt_fixture(issuer, "wrong-audience", 999);
    let (expired_jwks, expired_token) =
        signed_jwt_fixture_with_expiry(issuer, "expired-audience", 999, 1);
    let expired_jwks_uri = spawn_static_jwks(expired_jwks).await;
    let mut state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "CREATE TENANT auth_failure ID 42").await;
    ddl_ok(
        &state,
        &su,
        &format!(
            "CREATE OIDC PROVIDER auth_failure_idp \
             ISSUER '{issuer}' \
             JWKS_URI '{jwks_uri}' \
             AUDIENCE 'expected-audience' \
             TENANT 42 \
             CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 1"
        ),
    )
    .await;
    ddl_ok(
        &state,
        &su,
        &format!(
            "CREATE OIDC PROVIDER expired_auth_failure_idp \
             ISSUER '{issuer}' \
             JWKS_URI '{expired_jwks_uri}' \
             AUDIENCE 'expired-audience' \
             TENANT 42 \
             CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 1"
        ),
    )
    .await;

    let registry = JwksRegistry::init(JwtAuthConfig {
        allow_http_jwks: true,
        allow_jwks_hosts: vec!["localhost".into()],
        allow_jwks_cidrs: vec!["127.0.0.0/8".into(), "::1/128".into()],
        ..JwtAuthConfig::default()
    })
    .await
    .expect("test JWKS registry must initialize");
    Arc::get_mut(&mut state)
        .expect("test state must remain uniquely owned")
        .jwks_registry = Some(Arc::new(registry));

    let errors = [
        verify_bearer_token(&state, &unknown_issuer_token)
            .await
            .expect_err("an unknown issuer must be rejected"),
        verify_bearer_token(&state, &wrong_audience_token)
            .await
            .expect_err("a wrong audience must be rejected"),
        verify_bearer_token(&state, &forged_signature(&valid_token))
            .await
            .expect_err("an invalid signature must be rejected"),
        verify_bearer_token(&state, &expired_token)
            .await
            .expect_err("an expired token must be rejected"),
    ]
    .map(assert_generic_oidc_authentication_failure);
    assert_eq!(
        errors[0], errors[1],
        "unknown issuer and wrong audience must expose the same client error"
    );
    assert_eq!(
        errors[1], errors[2],
        "wrong audience and invalid signature must expose the same client error"
    );
    assert_eq!(
        errors[2], errors[3],
        "invalid signature and expiry must expose the same client error"
    );
}

#[tokio::test]
async fn catalog_provider_cache_identity_frames_name_and_uri() {
    let issuer = "https://catalog-framing-idp.example/";
    let (first_jwks, first_token) = signed_jwt_fixture(issuer, "catalog-api", 999);
    let (second_jwks, second_token) = signed_jwt_fixture(issuer, "catalog-api", 999);
    let first_jwks_uri = spawn_static_jwks(first_jwks).await;
    let second_jwks_uri = spawn_static_jwks(second_jwks).await;
    let first_provider_name = "alpha";
    let first_provider_uri = format!("{first_jwks_uri}?redirect=:{second_jwks_uri}");
    let second_provider_name = format!("{first_provider_name}:{first_jwks_uri}?redirect=");

    assert_eq!(
        format!("catalog:{first_provider_name}:{first_provider_uri}"),
        format!("catalog:{second_provider_name}:{second_jwks_uri}"),
        "fixture tuples must collide under the legacy concatenated cache identity"
    );

    let registry = JwksRegistry::init(JwtAuthConfig {
        allow_http_jwks: true,
        allow_jwks_hosts: vec!["localhost".into()],
        allow_jwks_cidrs: vec!["127.0.0.0/8".into(), "::1/128".into()],
        ..JwtAuthConfig::default()
    })
    .await
    .expect("test JWKS registry must initialize");

    registry
        .validate_with_catalog_provider(first_provider_name, &first_provider_uri, &first_token)
        .await
        .expect("first catalog provider must validate with its own JWKS key");
    registry
        .validate_with_catalog_provider(&second_provider_name, &second_jwks_uri, &second_token)
        .await
        .expect("second catalog provider must not reuse the first provider cache entry");
}

#[tokio::test]
async fn authenticated_token_is_rejected_when_provider_tenant_was_dropped() {
    let issuer = "https://dropped-tenant-idp.example/";
    let (jwks, token) = signed_jwt_fixture(issuer, "nodedb-api", 999);
    let jwks_uri = spawn_static_jwks(jwks).await;
    let mut state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "CREATE TENANT removed_tenant ID 42").await;
    ddl_ok(
        &state,
        &su,
        &format!(
            "CREATE OIDC PROVIDER removed_tenant_idp \
             ISSUER '{issuer}' \
             JWKS_URI '{jwks_uri}' \
             AUDIENCE 'nodedb-api' \
             TENANT 42 \
             CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 1"
        ),
    )
    .await;
    ddl_ok(&state, &su, "DROP TENANT removed_tenant").await;

    let registry = JwksRegistry::init(JwtAuthConfig {
        allow_http_jwks: true,
        allow_jwks_hosts: vec!["localhost".into()],
        allow_jwks_cidrs: vec!["127.0.0.0/8".into(), "::1/128".into()],
        ..JwtAuthConfig::default()
    })
    .await
    .expect("test JWKS registry must initialize");
    Arc::get_mut(&mut state)
        .expect("test state must remain uniquely owned")
        .jwks_registry = Some(Arc::new(registry));

    let err = verify_bearer_token(&state, &forged_signature(&token))
        .await
        .expect_err("an unauthenticated token must fail before tenant-state validation");
    assert_generic_oidc_authentication_failure(err);

    let err = verify_bearer_token(&state, &token)
        .await
        .expect_err("a token bound to a dropped tenant must fail closed");
    assert!(matches!(
        err,
        nodedb::Error::OidcProviderTenantUnavailable { tenant_id: 42 }
    ));
}

#[tokio::test]
async fn catalog_providers_with_shared_issuer_route_by_audience() {
    let issuer = "https://shared-catalog-idp.example/";
    let (jwks, token) = signed_jwt_fixture(issuer, "tenant-b-api", 999);
    let jwks_uri = spawn_static_jwks(jwks).await;
    let mut state = make_state_with_catalog();
    let su = superuser();

    ddl_ok(&state, &su, "CREATE TENANT alpha ID 42").await;
    ddl_ok(&state, &su, "CREATE TENANT beta ID 43").await;
    for (name, audience, tenant_id) in [
        ("alpha_idp", "tenant-a-api", 42),
        ("beta_idp", "tenant-b-api", 43),
    ] {
        ddl_ok(
            &state,
            &su,
            &format!(
                "CREATE OIDC PROVIDER {name} \
                 ISSUER '{issuer}' \
                 JWKS_URI '{jwks_uri}' \
                 AUDIENCE '{audience}' \
                 TENANT {tenant_id} \
                 CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 0"
            ),
        )
        .await;
    }

    let registry = JwksRegistry::init(JwtAuthConfig {
        allow_http_jwks: true,
        allow_jwks_hosts: vec!["localhost".into()],
        allow_jwks_cidrs: vec!["127.0.0.0/8".into(), "::1/128".into()],
        ..JwtAuthConfig::default()
    })
    .await
    .expect("test JWKS registry must initialize");
    Arc::get_mut(&mut state)
        .expect("test state must remain uniquely owned")
        .jwks_registry = Some(Arc::new(registry));

    let identity = verify_bearer_token(&state, &token)
        .await
        .expect("issuer and audience must select the tenant-b provider");
    assert_eq!(identity.tenant_id.as_u64(), 43);
}

#[tokio::test]
async fn catalog_provider_without_tenant_binding_is_rejected() {
    let issuer = "https://legacy-idp.example/";
    let (jwks, token) = signed_jwt_fixture(issuer, "nodedb-api", 999);
    let jwks_uri = spawn_static_jwks(jwks).await;
    let legacy = LegacyOidcProvider {
        provider_name: "legacy_idp".into(),
        issuer: issuer.into(),
        jwks_uri,
        audience: Some("nodedb-api".into()),
        claim_mapping: vec![
            nodedb::control::security::catalog::oidc_providers::StoredClaimMappingRule {
                claim_name: "sub".into(),
                claim_value: "*".into(),
                default_database: Some(0),
                add_databases: vec![],
                add_roles: vec![],
            },
        ],
        created_at_lsn: 0,
    };
    let encoded = zerompk::to_msgpack_vec(&legacy).expect("legacy provider must serialize");
    let provider = zerompk::from_msgpack(&encoded).expect("legacy provider must deserialize");

    let mut state = make_state_with_catalog();
    state
        .credentials
        .catalog()
        .put_oidc_provider(&provider)
        .expect("legacy provider fixture must persist");
    let registry = JwksRegistry::init(JwtAuthConfig {
        allow_http_jwks: true,
        allow_jwks_hosts: vec!["localhost".into()],
        allow_jwks_cidrs: vec!["127.0.0.0/8".into(), "::1/128".into()],
        ..JwtAuthConfig::default()
    })
    .await
    .expect("test JWKS registry must initialize");
    Arc::get_mut(&mut state)
        .expect("test state must remain uniquely owned")
        .jwks_registry = Some(Arc::new(registry));

    let err = verify_bearer_token(&state, &forged_signature(&token))
        .await
        .expect_err("an unauthenticated token must fail before tenant-binding validation");
    assert_generic_oidc_authentication_failure(err);

    let err = verify_bearer_token(&state, &token)
        .await
        .expect_err("an unbound persisted provider must fail closed");
    assert!(matches!(err, nodedb::Error::OidcProviderTenantUnbound));
}

#[test]
fn claim_mapping_apply_function_is_public() {
    let _ = apply_claim_mapping;
}
