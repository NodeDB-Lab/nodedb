// SPDX-License-Identifier: BUSL-1.1

//! OIDC claim-mapping privilege-boundary integration tests.

use std::sync::Arc;

use base64::Engine;
use nodedb::config::auth::JwtAuthConfig;
use nodedb::control::security::identity::Role;
use nodedb::control::security::jwks::registry::JwksRegistry;
use nodedb::control::security::jwt::JwtError;
use nodedb::control::security::oidc::verify_bearer_token;

use super::oidc_authentication::{PEER, SESSION};
use nodedb_test_support::jwks_fixture::{loopback_egress, loopback_sources};
use nodedb_test_support::pgwire_auth_helpers::{
    ddl_err, ddl_ok, make_state_with_catalog, make_state_with_catalog_configured, superuser,
};
use nodedb_types::id::DatabaseId;

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

struct SignedJwtFixture {
    jwks: String,
    token: String,
    missing_expiration: String,
    mismatched_algorithm: String,
    none_algorithm: String,
}

fn signed_jwt_fixtures(roles: &[&str], is_superuser: bool) -> SignedJwtFixture {
    use rsa::pkcs1v15::SigningKey;
    use rsa::signature::{SignatureEncoding, Signer};
    use rsa::traits::PublicKeyParts;

    let mut rng = rsa::rand_core::OsRng;
    let private_key =
        rsa::RsaPrivateKey::new(&mut rng, 1024).expect("RSA fixture key generation must succeed");
    let public_key = rsa::RsaPublicKey::from(&private_key);
    let encode = |bytes: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    let jwks = format!(
        r#"{{"keys":[{{"kty":"RSA","kid":"oidc-role-boundary","alg":"RS256","use":"sig","n":"{}","e":"{}"}}]}}"#,
        encode(&public_key.n().to_bytes_be()),
        encode(&public_key.e().to_bytes_be()),
    );
    let roles = roles
        .iter()
        .map(|role| format!(r#""{role}""#))
        .collect::<Vec<_>>()
        .join(",");
    let issued_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("fixture clock must be after epoch")
        .as_secs();
    let expires_at = issued_at + 3_600;
    let claims = |include_expiration| {
        if include_expiration {
            format!(
                r#"{{"iss":"https://catalog-idp.example/","aud":"nodedb-api","sub":"alice","tenant_id":999,"roles":[{roles}],"is_superuser":{is_superuser},"iat":{issued_at},"exp":{expires_at},"user_id":42}}"#
            )
        } else {
            format!(
                r#"{{"iss":"https://catalog-idp.example/","aud":"nodedb-api","sub":"alice","tenant_id":999,"roles":[{roles}],"is_superuser":{is_superuser},"iat":{issued_at},"user_id":42}}"#
            )
        }
    };
    let sign = |header: &[u8], payload: String| {
        let header = encode(header);
        let payload = encode(payload.as_bytes());
        let signing_input = format!("{header}.{payload}");
        let signing_key = SigningKey::<sha2::Sha256>::new(private_key.clone());
        let signature: rsa::pkcs1v15::Signature = signing_key.sign(signing_input.as_bytes());
        format!("{signing_input}.{}", encode(&signature.to_bytes()))
    };
    let valid_claims = claims(true);
    let missing_expiration_claims = claims(false);
    let token = sign(
        br#"{"alg":"RS256","kid":"oidc-role-boundary"}"#,
        valid_claims.clone(),
    );
    let missing_expiration = sign(
        br#"{"alg":"RS256","kid":"oidc-role-boundary"}"#,
        missing_expiration_claims,
    );
    let mismatched_algorithm = sign(
        br#"{"alg":"HS256","kid":"oidc-role-boundary"}"#,
        valid_claims.clone(),
    );
    let none_algorithm = format!(
        "{}.{}.",
        encode(br#"{"alg":"none","kid":"oidc-role-boundary"}"#),
        encode(valid_claims.as_bytes()),
    );

    SignedJwtFixture {
        jwks,
        token,
        missing_expiration,
        mismatched_algorithm,
        none_algorithm,
    }
}

fn signed_jwt_fixture(roles: &[&str], is_superuser: bool) -> (String, String) {
    let fixture = signed_jwt_fixtures(roles, is_superuser);
    (fixture.jwks, fixture.token)
}

/// A catalog-backed state with a JWKS registry that fetches from the loopback
/// fixture server. The registry is set before the gateway install, as
/// production boot sets it.
async fn state_with_catalog_registry() -> nodedb_test_support::pgwire_auth_helpers::BootedState {
    let registry = JwksRegistry::init(JwtAuthConfig::default(), loopback_egress())
        .await
        .expect("test JWKS registry must initialize");
    make_state_with_catalog_configured(|state| {
        state.remote_sources = loopback_sources();
        state.jwks_registry = Some(Arc::new(registry));
    })
}

#[tokio::test]
async fn create_oidc_provider_rejects_superuser_claim_mapping() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT mapped_tenant ID 42").await;

    let error = ddl_err(
        &state,
        &su,
        "CREATE OIDC PROVIDER unsafe_mapping \
         ISSUER 'https://catalog-idp.example/' \
         JWKS_URI 'https://catalog-idp.example/jwks' \
         AUDIENCE 'nodedb-api' \
         TENANT 42 \
         CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 0 ADD ROLES ['superuser']",
    )
    .await;

    assert!(
        error.to_lowercase().contains("superuser"),
        "rejection must identify the non-assertable role: {error}"
    );
}

#[tokio::test]
async fn alter_oidc_provider_rejects_superuser_and_preserves_existing_mapping() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT mapped_tenant ID 42").await;
    ddl_ok(
        &state,
        &su,
        "CREATE OIDC PROVIDER safe_mapping \
         ISSUER 'https://catalog-idp.example/' \
         JWKS_URI 'https://catalog-idp.example/jwks' \
         AUDIENCE 'nodedb-api' \
         TENANT 42 \
         CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 0 ADD ROLES ['readonly']",
    )
    .await;

    let error = ddl_err(
        &state,
        &su,
        "ALTER OIDC PROVIDER safe_mapping SET CLAIM MAPPING \
         WHEN sub = '*' SET DEFAULT_DATABASE = 0 ADD ROLES ['superuser']",
    )
    .await;
    assert!(
        error.to_lowercase().contains("superuser"),
        "rejection must identify the non-assertable role: {error}"
    );

    let provider = state
        .credentials
        .catalog()
        .get_oidc_provider("safe_mapping")
        .expect("catalog read must succeed")
        .expect("provider must remain present");
    assert_eq!(provider.claim_mapping.len(), 1);
    assert_eq!(provider.claim_mapping[0].add_roles, vec!["readonly"]);
}

#[tokio::test]
async fn legacy_oidc_mapping_cannot_grant_superuser() {
    let (jwks, token) = signed_jwt_fixture(&[], false);
    let jwks_uri = spawn_static_jwks(jwks).await;
    let state = state_with_catalog_registry().await;
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT legacy_mapping_tenant ID 42").await;
    ddl_ok(
        &state,
        &su,
        &format!(
            "CREATE OIDC PROVIDER legacy_mapping \
             ISSUER 'https://catalog-idp.example/' \
             JWKS_URI '{jwks_uri}' \
             AUDIENCE 'nodedb-api' \
             TENANT 42 \
             CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 0 ADD ROLES ['readwrite']"
        ),
    )
    .await;

    let catalog = state.credentials.catalog();
    let mut provider = catalog
        .get_oidc_provider("legacy_mapping")
        .expect("catalog read must succeed")
        .expect("provider must exist");
    provider.claim_mapping[0]
        .add_roles
        .push("superuser".to_string());
    catalog
        .put_oidc_provider(&provider)
        .expect("legacy provider fixture must persist");

    let (identity, _claims) = verify_bearer_token(&state, &token, PEER, SESSION)
        .await
        .expect("legacy mapping must retain non-privileged authentication");
    assert_eq!(identity.tenant_id.as_u64(), 42);
    assert!(!identity.is_superuser);
    assert!(!identity.roles.contains(&Role::Superuser));
    assert!(identity.roles.contains(&Role::ReadWrite));
    assert_eq!(identity.default_database, Some(DatabaseId::DEFAULT));
    assert!(!identity.can_access_database(DatabaseId::new(9_999)));
}

async fn authenticate_catalog_token(
    token_roles: &[&str],
    is_superuser: bool,
) -> nodedb::control::security::identity::AuthenticatedIdentity {
    let (jwks, token) = signed_jwt_fixture(token_roles, is_superuser);
    let jwks_uri = spawn_static_jwks(jwks).await;
    let state = state_with_catalog_registry().await;
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT raw_claim_tenant ID 42").await;
    ddl_ok(
        &state,
        &su,
        &format!(
            "CREATE OIDC PROVIDER raw_claims \
             ISSUER 'https://catalog-idp.example/' \
             JWKS_URI '{jwks_uri}' \
             AUDIENCE 'nodedb-api' \
             TENANT 42 \
             CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 0 ADD ROLES ['readonly']"
        ),
    )
    .await;

    let (identity, _claims) = verify_bearer_token(&state, &token, PEER, SESSION)
        .await
        .expect("catalog-backed token must validate");
    identity
}

fn assert_catalog_identity_has_only_mapped_authority(
    identity: &nodedb::control::security::identity::AuthenticatedIdentity,
) {
    assert_eq!(identity.tenant_id.as_u64(), 42);
    assert!(!identity.is_superuser);
    assert!(!identity.roles.contains(&Role::Superuser));
    assert!(identity.roles.contains(&Role::ReadOnly));
    assert_eq!(identity.default_database, Some(DatabaseId::DEFAULT));
}

#[tokio::test]
async fn catalog_oidc_ignores_raw_superuser_flag_and_preserves_mapped_roles() {
    let identity = authenticate_catalog_token(&[], true).await;
    assert_catalog_identity_has_only_mapped_authority(&identity);
}

#[tokio::test]
async fn catalog_oidc_ignores_raw_superuser_role_and_preserves_mapped_roles() {
    let identity = authenticate_catalog_token(&["superuser"], false).await;
    assert_catalog_identity_has_only_mapped_authority(&identity);
}

#[tokio::test]
async fn catalog_oidc_rejects_algorithm_confusion_missing_expiration_and_none() {
    // One static JWKS response contains the RSA key for every token below.
    // The valid control token populates the cache, so the later
    // algorithm-mismatch assertion proves rejection even when its `kid` and
    // RSA key material are already available.
    let fixture = signed_jwt_fixtures(&[], false);
    let jwks_uri = spawn_static_jwks(fixture.jwks).await;
    let registry = Arc::new(
        JwksRegistry::init(
            JwtAuthConfig {
                allowed_algorithms: vec!["RS256".into(), "HS256".into()],
                ..JwtAuthConfig::default()
            },
            loopback_egress(),
        )
        .await
        .expect("test JWKS registry must initialize"),
    );
    let installed = Arc::clone(&registry);
    let state = make_state_with_catalog_configured(move |state| {
        state.remote_sources = loopback_sources();
        state.jwks_registry = Some(installed);
    });
    let su = superuser();
    ddl_ok(&state, &su, "CREATE TENANT negative_oidc_tenant ID 42").await;
    ddl_ok(
        &state,
        &su,
        &format!(
            "CREATE OIDC PROVIDER negative_cases \
             ISSUER 'https://catalog-idp.example/' \
             JWKS_URI '{jwks_uri}' \
             AUDIENCE 'nodedb-api' \
             TENANT 42 \
             CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 0 ADD ROLES ['readonly']"
        ),
    )
    .await;

    registry
        .validate_with_catalog_provider("negative_cases", &jwks_uri, &fixture.token)
        .await
        .expect("valid RS256 control token must fetch, cache, and validate against the JWKS");

    let missing_expiration = registry
        .validate_with_catalog_provider("negative_cases", &jwks_uri, &fixture.missing_expiration)
        .await;
    assert_eq!(missing_expiration.err(), Some(JwtError::MissingExpiration));

    let mismatched_algorithm = registry
        .validate_with_catalog_provider("negative_cases", &jwks_uri, &fixture.mismatched_algorithm)
        .await;
    assert_eq!(
        mismatched_algorithm.err(),
        Some(JwtError::UnsupportedAlgorithm)
    );

    let none_algorithm = registry
        .validate_with_catalog_provider("negative_cases", &jwks_uri, &fixture.none_algorithm)
        .await;
    assert_eq!(none_algorithm.err(), Some(JwtError::UnsupportedAlgorithm));

    let missing_expiration =
        verify_bearer_token(&state, &fixture.missing_expiration, PEER, SESSION).await;
    assert!(matches!(
        missing_expiration,
        Err(nodedb::Error::BadRequest { .. })
    ));

    let mismatched_algorithm =
        verify_bearer_token(&state, &fixture.mismatched_algorithm, PEER, SESSION).await;
    assert!(matches!(
        mismatched_algorithm,
        Err(nodedb::Error::BadRequest { .. })
    ));

    let none_algorithm = verify_bearer_token(&state, &fixture.none_algorithm, PEER, SESSION).await;
    assert!(matches!(
        none_algorithm,
        Err(nodedb::Error::BadRequest { .. })
    ));
}
