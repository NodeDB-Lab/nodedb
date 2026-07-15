// SPDX-License-Identifier: BUSL-1.1

//! OIDC claim-mapping privilege-boundary integration tests.

mod common;

use std::sync::Arc;

use base64::Engine;
use common::pgwire_auth_helpers::{ddl_err, ddl_ok, make_state_with_catalog, superuser};
use nodedb::config::auth::JwtAuthConfig;
use nodedb::control::security::identity::Role;
use nodedb::control::security::jwks::registry::JwksRegistry;
use nodedb::control::security::oidc::verify_bearer_token;
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

fn signed_jwt_fixture(roles: &[&str], is_superuser: bool) -> (String, String) {
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
    let header = encode(br#"{"alg":"RS256","kid":"oidc-role-boundary"}"#);
    let payload = encode(
        format!(
            r#"{{"iss":"https://catalog-idp.example/","aud":"nodedb-api","sub":"alice","tenant_id":999,"roles":[{roles}],"is_superuser":{is_superuser},"exp":9999999999,"user_id":42}}"#
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

async fn install_catalog_registry(state: &mut Arc<nodedb::control::state::SharedState>) {
    let registry = JwksRegistry::init(JwtAuthConfig {
        allow_http_jwks: true,
        allow_jwks_hosts: vec!["localhost".into()],
        allow_jwks_cidrs: vec!["127.0.0.0/8".into(), "::1/128".into()],
        ..JwtAuthConfig::default()
    })
    .await
    .expect("test JWKS registry must initialize");
    Arc::get_mut(state)
        .expect("test state must remain uniquely owned")
        .jwks_registry = Some(Arc::new(registry));
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
         CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 1 ADD ROLES ['superuser']",
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
         CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 1 ADD ROLES ['readonly']",
    )
    .await;

    let error = ddl_err(
        &state,
        &su,
        "ALTER OIDC PROVIDER safe_mapping SET CLAIM MAPPING \
         WHEN sub = '*' SET DEFAULT_DATABASE = 1 ADD ROLES ['superuser']",
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
    let mut state = make_state_with_catalog();
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
             CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 1 ADD ROLES ['readwrite']"
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
    install_catalog_registry(&mut state).await;

    let identity = verify_bearer_token(&state, &token)
        .await
        .expect("legacy mapping must retain non-privileged authentication");
    assert_eq!(identity.tenant_id.as_u64(), 42);
    assert!(!identity.is_superuser);
    assert!(!identity.roles.contains(&Role::Superuser));
    assert!(identity.roles.contains(&Role::ReadWrite));
    assert_eq!(identity.default_database, Some(DatabaseId::new(1)));
    assert!(!identity.can_access_database(DatabaseId::new(9_999)));
}

async fn authenticate_catalog_token(
    token_roles: &[&str],
    is_superuser: bool,
) -> nodedb::control::security::identity::AuthenticatedIdentity {
    let (jwks, token) = signed_jwt_fixture(token_roles, is_superuser);
    let jwks_uri = spawn_static_jwks(jwks).await;
    let mut state = make_state_with_catalog();
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
             CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 1 ADD ROLES ['readonly']"
        ),
    )
    .await;
    install_catalog_registry(&mut state).await;

    verify_bearer_token(&state, &token)
        .await
        .expect("catalog-backed token must validate")
}

fn assert_catalog_identity_has_only_mapped_authority(
    identity: &nodedb::control::security::identity::AuthenticatedIdentity,
) {
    assert_eq!(identity.tenant_id.as_u64(), 42);
    assert!(!identity.is_superuser);
    assert!(!identity.roles.contains(&Role::Superuser));
    assert!(identity.roles.contains(&Role::ReadOnly));
    assert_eq!(identity.default_database, Some(DatabaseId::new(1)));
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
