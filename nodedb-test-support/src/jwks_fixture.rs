// SPDX-License-Identifier: BUSL-1.1

//! An in-process RS256 JWKS endpoint plus a matching token signer.
//!
//! Lets a test authenticate through the real `[auth.jwt]` verification
//! pipeline — signature, `(iss, aud)` routing, time claims, claim policy —
//! instead of a hand-built validator that no production path uses.

use base64::Engine;
use nodedb::config::auth::{JwtAuthConfig, JwtProviderConfig};
use rsa::pkcs1v15::SigningKey;
use rsa::signature::{SignatureEncoding, Signer};
use rsa::traits::PublicKeyParts;

/// `kid` published in the fixture's JWKS and stamped into every minted token.
pub const KEY_ID: &str = "nodedb-test-jwks";

/// A running JWKS endpoint and the private key its published key verifies.
pub struct JwksFixture {
    signing_key: SigningKey<sha2::Sha256>,
    jwks_url: String,
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

impl JwksFixture {
    /// Generate a key pair and serve its public half from a loopback endpoint.
    ///
    /// The endpoint answers every connection with the same JWKS document, so
    /// a registry may re-fetch it as often as it likes.
    pub async fn spawn() -> Self {
        // 1024-bit: the key is thrown away with the test, and generation cost
        // is paid on every run.
        let mut rng = rsa::rand_core::OsRng;
        let private_key = rsa::RsaPrivateKey::new(&mut rng, 1024)
            .expect("RSA fixture key generation must succeed");
        let public_key = rsa::RsaPublicKey::from(&private_key);
        let jwks = format!(
            r#"{{"keys":[{{"kty":"RSA","kid":"{KEY_ID}","alg":"RS256","use":"sig","n":"{}","e":"{}"}}]}}"#,
            b64(&public_key.n().to_bytes_be()),
            b64(&public_key.e().to_bytes_be()),
        );

        let listener = tokio::net::TcpListener::bind("[::]:0")
            .await
            .expect("JWKS fixture must bind");
        let addr = listener
            .local_addr()
            .expect("JWKS fixture must expose its address");
        let response = std::sync::Arc::new(format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            jwks.len(),
            jwks
        ));
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    continue;
                };
                let response = std::sync::Arc::clone(&response);
                // One task per connection: a fetcher that opens a second
                // connection while the first is still being served would
                // otherwise wait behind it in the accept loop.
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};

                    // Drain the request head before answering. Replying to a
                    // half-sent request and dropping the socket makes the peer
                    // observe a reset instead of the response, which surfaces
                    // as an unverifiable token rather than a transport error.
                    let mut request = Vec::new();
                    let mut buf = [0u8; 512];
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => request.extend_from_slice(&buf[..n]),
                        }
                    }

                    if stream.write_all(response.as_bytes()).await.is_ok() {
                        // Flush before the socket drops, so the peer reads a
                        // complete body rather than a truncated one.
                        let _ = stream.flush().await;
                        let _ = stream.shutdown().await;
                    }
                });
            }
        });

        Self {
            signing_key: SigningKey::<sha2::Sha256>::new(private_key),
            jwks_url: format!("http://localhost:{}/jwks.json", addr.port()),
        }
    }

    /// URL of this fixture's JWKS document.
    pub fn jwks_url(&self) -> &str {
        &self.jwks_url
    }

    /// Sign `claims` (a JSON object) as an RS256 token carrying [`KEY_ID`].
    ///
    /// The caller owns the whole claim set, including `iat` and `exp`, so a
    /// test can mint expired, premature, or policy-violating tokens as easily
    /// as valid ones.
    pub fn mint(&self, claims: &serde_json::Value) -> String {
        let header = b64(format!(r#"{{"alg":"RS256","kid":"{KEY_ID}"}}"#).as_bytes());
        let payload = b64(sonic_rs::to_string(claims)
            .expect("fixture claims must serialize")
            .as_bytes());
        let signing_input = format!("{header}.{payload}");
        let signature: rsa::pkcs1v15::Signature = self.signing_key.sign(signing_input.as_bytes());
        format!("{signing_input}.{}", b64(&signature.to_bytes()))
    }

    /// An `[auth.jwt]` section with one static provider pointing at this
    /// fixture, bound to `tenant_id`. Its registry fetches through
    /// [`loopback_egress`].
    pub fn auth_config(&self, issuer: &str, audience: &str, tenant_id: u64) -> JwtAuthConfig {
        JwtAuthConfig {
            providers: vec![JwtProviderConfig {
                name: "test-provider".into(),
                jwks_url: self.jwks_url.clone(),
                issuer: issuer.into(),
                audience: audience.into(),
                tenant_id,
            }],
            ..JwtAuthConfig::default()
        }
    }
}

/// The `[egress]` policy a JWKS fetch of a fixture needs: plaintext
/// `http://localhost` resolving to loopback. The strict default refuses both.
pub fn loopback_egress() -> std::sync::Arc<nodedb::control::security::egress::EgressPolicy> {
    std::sync::Arc::new(
        nodedb::control::security::egress::EgressPolicy::from_parts(
            true,
            &["localhost".to_string()],
            &["127.0.0.0/8".to_string(), "::1/128".to_string()],
        )
        .expect("the loopback egress policy is valid"),
    )
}

/// Remote sources under [`loopback_egress`], so `CREATE OIDC PROVIDER` admits
/// a fixture's `http://localhost` JWKS URI.
pub fn loopback_sources() -> nodedb::control::security::egress::RemoteSources {
    nodedb::control::security::egress::RemoteSources::new(loopback_egress())
        .expect("the loopback remote sources build")
}

/// Seconds since the UNIX epoch, for `iat` / `exp` claims.
pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("fixture clock must be after the epoch")
        .as_secs()
}
