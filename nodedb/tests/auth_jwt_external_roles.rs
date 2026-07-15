// SPDX-License-Identifier: BUSL-1.1

//! External JWT role-boundary integration tests.

use base64::Engine;
use nodedb::config::auth::{JwtAuthConfig, JwtProviderConfig};
use nodedb::control::security::identity::Role;
use nodedb::control::security::jwks::registry::JwksRegistry;
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
        r#"{{"keys":[{{"kty":"RSA","kid":"external-role-boundary","alg":"RS256","use":"sig","n":"{}","e":"{}"}}]}}"#,
        encode(&public_key.n().to_bytes_be()),
        encode(&public_key.e().to_bytes_be()),
    );
    let roles = roles
        .iter()
        .map(|role| format!(r#""{role}""#))
        .collect::<Vec<_>>()
        .join(",");
    let header = encode(br#"{"alg":"RS256","kid":"external-role-boundary"}"#);
    let payload = encode(
        format!(
            r#"{{"iss":"https://static-idp.example/","aud":"nodedb-api","sub":"alice","tenant_id":999,"roles":[{roles}],"is_superuser":{is_superuser},"exp":9999999999,"user_id":42}}"#
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

async fn registry_for(jwks: String) -> JwksRegistry {
    let jwks_url = spawn_static_jwks(jwks).await;
    JwksRegistry::init(JwtAuthConfig {
        allow_http_jwks: true,
        allow_jwks_hosts: vec!["localhost".into()],
        allow_jwks_cidrs: vec!["127.0.0.0/8".into(), "::1/128".into()],
        providers: vec![JwtProviderConfig {
            name: "static-idp".into(),
            jwks_url,
            issuer: "https://static-idp.example/".into(),
            audience: "nodedb-api".into(),
            tenant_id: 42,
        }],
        ..JwtAuthConfig::default()
    })
    .await
    .expect("static provider configuration must initialize")
}

#[tokio::test]
async fn static_provider_ignores_externally_asserted_superuser_flag() {
    let (jwks, token) = signed_jwt_fixture(&["readwrite"], true);
    let identity = registry_for(jwks)
        .await
        .validate(&token)
        .await
        .expect("signed JWT must validate");

    assert_eq!(identity.tenant_id.as_u64(), 42);
    assert!(!identity.is_superuser);
    assert!(!identity.roles.contains(&Role::Superuser));
    assert!(identity.roles.contains(&Role::ReadWrite));
    assert!(!identity.can_access_database(DatabaseId::new(9_999)));
}

#[tokio::test]
async fn static_provider_filters_externally_asserted_superuser_role() {
    let (jwks, token) = signed_jwt_fixture(&["superuser", "readonly"], false);
    let identity = registry_for(jwks)
        .await
        .validate(&token)
        .await
        .expect("signed JWT must validate");

    assert_eq!(identity.tenant_id.as_u64(), 42);
    assert!(!identity.is_superuser);
    assert!(!identity.roles.contains(&Role::Superuser));
    assert!(identity.roles.contains(&Role::ReadOnly));
}
