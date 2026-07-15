// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for OIDC provider DDL + claim mapping.

mod common;

use std::sync::Arc;

use base64::Engine;
use common::pgwire_auth_helpers::{
    ddl_err, ddl_ok, make_state_with_catalog, readonly_user, superuser,
};
use nodedb::config::auth::{JwtAuthConfig, JwtProviderConfig};
use nodedb::control::security::jwks::registry::JwksRegistry;
use nodedb::control::security::oidc::{claim_mapping::apply_claim_mapping, verify_bearer_token};
use nodedb::control::server::shared::ddl;
use nodedb::control::server::shared::ddl::result::DdlResult;
use nodedb::control::server::shared::session::DetachedTxnScope;

// ── CREATE OIDC PROVIDER ────────────────────────────────────────────────────

#[tokio::test]
async fn create_oidc_provider_persists_in_catalog() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER okta \
         ISSUER 'https://acme.okta.com' \
         JWKS_URI 'https://acme.okta.com/.well-known/jwks.json' \
         AUDIENCE 'nodedb' \
         TENANT 42",
    )
    .await;

    let cat = state.credentials.catalog();
    let stored = cat
        .get_oidc_provider("okta")
        .expect("catalog read must succeed")
        .expect("provider must exist after CREATE");
    assert_eq!(stored.issuer, "https://acme.okta.com");
    assert_eq!(stored.audience.as_deref(), Some("nodedb"));
    let encoded = sonic_rs::to_string(&stored).expect("provider must serialize");
    assert!(
        encoded.contains("\"tenant_id\":42"),
        "persisted provider must retain its tenant binding: {encoded}"
    );
}

#[tokio::test]
async fn create_oidc_provider_requires_superuser() {
    let state = make_state_with_catalog();
    let su = superuser();
    let viewer = readonly_user();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;
    let err = ddl_err(
        &state,
        &viewer,
        "CREATE OIDC PROVIDER bad \
         ISSUER 'https://x.example/' \
         JWKS_URI 'https://x.example/jwks' \
         TENANT 42",
    )
    .await;
    assert!(
        err.contains("42501") || err.contains("permission denied"),
        "expected permission denied, got: {err}"
    );
}

#[tokio::test]
async fn create_oidc_provider_rejects_unknown_tenant() {
    let state = make_state_with_catalog();
    let su = superuser();
    let err = ddl_err(
        &state,
        &su,
        "CREATE OIDC PROVIDER unknown_tenant \
         ISSUER 'https://unknown-tenant.example/' \
         JWKS_URI 'https://unknown-tenant.example/jwks' \
         TENANT 999",
    )
    .await;
    assert!(
        err.contains("does not exist") || err.contains("unknown tenant"),
        "expected unknown-tenant error, got: {err}"
    );
}

#[tokio::test]
async fn same_issuer_allows_distinct_nonempty_audiences_per_tenant() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT alpha ID 42").await;
    ddl_ok(&state, &su, "CREATE TENANT beta ID 43").await;
    ddl_ok(&state, &su, "CREATE TENANT gamma ID 44").await;

    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER alpha_idp \
         ISSUER 'https://shared.example/' \
         JWKS_URI 'https://shared.example/jwks' \
         AUDIENCE 'alpha-api' \
         TENANT 42",
    )
    .await;
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER beta_idp \
         ISSUER 'https://shared.example/' \
         JWKS_URI 'https://shared.example/jwks' \
         AUDIENCE 'beta-api' \
         TENANT 43",
    )
    .await;

    let missing_audience = ddl_err(
        &state,
        &su,
        "CREATE OIDC PROVIDER no_aud_route \
         ISSUER 'https://shared.example/' \
         JWKS_URI 'https://shared.example/jwks' \
         TENANT 44",
    )
    .await;
    assert!(
        missing_audience.to_lowercase().contains("audience"),
        "expected same-issuer registration without an audience to be rejected, got: {missing_audience}"
    );

    let err = ddl_err(
        &state,
        &su,
        "CREATE OIDC PROVIDER duplicate_route \
         ISSUER 'https://shared.example/' \
         JWKS_URI 'https://shared.example/jwks' \
         AUDIENCE 'alpha-api' \
         TENANT 43",
    )
    .await;
    assert!(
        err.contains("42710") || err.contains("already exists"),
        "expected duplicate issuer+audience error, got: {err}"
    );
}

// ── DROP OIDC PROVIDER ──────────────────────────────────────────────────────

#[tokio::test]
async fn drop_oidc_provider_removes_record() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT auth0_tenant ID 42").await;
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER auth0 \
         ISSUER 'https://x.auth0.com' \
         JWKS_URI 'https://x.auth0.com/.well-known/jwks.json' \
         TENANT 42",
    )
    .await;
    ddl_ok(&state, &su, "DROP OIDC PROVIDER auth0").await;

    let cat = state.credentials.catalog();
    let stored = cat
        .get_oidc_provider("auth0")
        .expect("catalog read must succeed");
    assert!(stored.is_none(), "provider must be absent after DROP");
}

#[tokio::test]
async fn drop_oidc_provider_unknown_returns_not_found() {
    let state = make_state_with_catalog();
    let su = superuser();
    let err = ddl_err(&state, &su, "DROP OIDC PROVIDER does_not_exist").await;
    assert!(
        err.contains("42704") || err.contains("does not exist"),
        "expected not-found error, got: {err}"
    );
}

#[tokio::test]
async fn drop_oidc_provider_if_exists_unknown_succeeds() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "DROP OIDC PROVIDER IF EXISTS does_not_exist").await;
}

// ── SHOW OIDC PROVIDERS ─────────────────────────────────────────────────────

#[tokio::test]
async fn show_oidc_providers_lists_registered() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT p1_tenant ID 42").await;
    ddl_ok(&state, &su, "CREATE TENANT p2_tenant ID 43").await;
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER p1 \
         ISSUER 'https://p1.example/' \
         JWKS_URI 'https://p1.example/jwks' \
         TENANT 42",
    )
    .await;
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER p2 \
         ISSUER 'https://p2.example/' \
         JWKS_URI 'https://p2.example/jwks' \
         TENANT 43",
    )
    .await;
    ddl_ok(&state, &su, "SHOW OIDC PROVIDERS").await;
}

#[tokio::test]
async fn show_oidc_providers_exposes_tenant_binding() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT acme ID 42").await;
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER acme_idp \
         ISSUER 'https://acme.example/' \
         JWKS_URI 'https://acme.example/jwks' \
         TENANT 42",
    )
    .await;

    let scope = DetachedTxnScope::new();
    let result = ddl::dispatch(
        &state,
        &su,
        "SHOW OIDC PROVIDERS",
        nodedb_types::id::DatabaseId::DEFAULT,
        &scope.ctx(),
    )
    .await
    .expect("SHOW OIDC PROVIDERS must be recognized")
    .expect("SHOW OIDC PROVIDERS must succeed");

    match &result[0] {
        DdlResult::Rows(rows) => {
            assert!(rows.columns.iter().any(|column| column == "tenant_id"));
            assert_eq!(
                rows.rows[0]
                    .get("tenant_id")
                    .and_then(serde_json::Value::as_str),
                Some("42")
            );
        }
        other => panic!("expected Rows response, got: {other:?}"),
    }
}

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

// ── Claim-mapping public surface smoke check ────────────────────────────────

#[test]
fn claim_mapping_apply_function_is_public() {
    // Verifies that `apply_claim_mapping` is accessible from integration tests.
    let _ = apply_claim_mapping;
}
