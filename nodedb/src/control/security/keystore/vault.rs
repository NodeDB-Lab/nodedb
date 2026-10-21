// SPDX-License-Identifier: BUSL-1.1

//! HashiCorp Vault transit-engine key provider.
//!
//! ## Flow
//!
//! At startup NodeDB reads the wrapped DEK ciphertext blob from
//! `ciphertext_blob_path`, then calls:
//!
//! ```text
//! POST {addr}/v1/{mount}/decrypt/{key_name}
//! Body: { "ciphertext": "<vault:v1:...>" }
//! Header: X-Vault-Token: <token>
//! ```
//!
//! The `plaintext` field in the JSON response is base64-encoded 32 bytes.
//!
//! Key rotation calls `rewrap/{key_name}` with the old ciphertext blob and
//! persists the new ciphertext blob to disk before returning the new key.
//!
//! # Security checks
//!
//! Both `token_path` and `ciphertext_blob_path` are passed through
//! [`check_key_file`] before being read. Symlinks, world/group-readable
//! files, and files owned by a different UID are rejected.

use std::path::PathBuf;

use base64::Engine as _;
use tracing::info;
use zeroize::Zeroizing;

use crate::Result;

use super::KeyProvider;
use super::key_file_security::check_key_file;
use crate::control::security::egress::RemoteSources;

/// Cap on one Vault request, connect and read included.
const VAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Vault transit-engine key provider.
pub struct VaultKeyProvider {
    pub(super) addr: String,
    pub(super) token_path: PathBuf,
    pub(super) key_name: String,
    pub(super) mount: String,
    pub(super) ciphertext_blob_path: PathBuf,
    /// The outbound egress client of the server's `[egress]` policy.
    client: std::sync::Arc<reqwest::Client>,
}

impl VaultKeyProvider {
    /// A provider for the Vault at `addr`, reached through the outbound
    /// client of `sources`. The address is operator config: one the
    /// `[egress]` policy refuses is an error, so its host must be on the
    /// allow-list.
    pub(super) fn new(
        addr: String,
        token_path: PathBuf,
        key_name: String,
        mount: String,
        ciphertext_blob_path: PathBuf,
        sources: &RemoteSources,
    ) -> Result<Self> {
        sources
            .egress()
            .check_url(&addr)
            .map_err(|e| crate::Error::Config {
                detail: format!(
                    "Vault addr fails the [egress] policy: {e}; put its host on allow_hosts"
                ),
            })?;
        Ok(Self {
            addr,
            token_path,
            key_name,
            mount,
            ciphertext_blob_path,
            client: std::sync::Arc::clone(sources.http()),
        })
    }

    fn read_token(&self) -> Result<String> {
        check_key_file(&self.token_path)?;
        std::fs::read_to_string(&self.token_path)
            .map(|s| s.trim().to_owned())
            .map_err(|e| crate::Error::Encryption {
                detail: format!(
                    "failed to read Vault token from {}: {e}",
                    self.token_path.display()
                ),
            })
    }

    fn read_ciphertext_blob(&self) -> Result<String> {
        check_key_file(&self.ciphertext_blob_path)?;
        std::fs::read_to_string(&self.ciphertext_blob_path)
            .map(|s| s.trim().to_owned())
            .map_err(|e| crate::Error::Encryption {
                detail: format!(
                    "failed to read Vault ciphertext blob from {}: {e}",
                    self.ciphertext_blob_path.display()
                ),
            })
    }

    fn write_ciphertext_blob(&self, ciphertext: &str) -> Result<()> {
        std::fs::write(&self.ciphertext_blob_path, ciphertext).map_err(|e| {
            crate::Error::Encryption {
                detail: format!(
                    "failed to write Vault ciphertext blob to {}: {e}",
                    self.ciphertext_blob_path.display()
                ),
            }
        })
    }

    async fn decrypt_with_vault(&self, ciphertext: &str) -> Result<Zeroizing<[u8; 32]>> {
        let token = self.read_token()?;
        let url = format!(
            "{}/v1/{}/decrypt/{}",
            self.addr.trim_end_matches('/'),
            self.mount,
            self.key_name
        );

        let resp = self
            .client
            .post(&url)
            .header("X-Vault-Token", &token)
            .timeout(VAULT_TIMEOUT)
            .json(&serde_json::json!({ "ciphertext": ciphertext }))
            .send()
            .await
            .map_err(|e| crate::Error::Encryption {
                detail: format!("Vault HTTP request failed: {e}"),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(crate::Error::Encryption {
                detail: format!("Vault decrypt returned {status}: {body}"),
            });
        }

        let body: serde_json::Value = resp.json().await.map_err(|e| crate::Error::Encryption {
            detail: format!("Vault response JSON parse failed: {e}"),
        })?;

        let plaintext_b64 = body
            .pointer("/data/plaintext")
            .and_then(|v| v.as_str())
            .ok_or_else(|| crate::Error::Encryption {
                detail: "Vault response missing /data/plaintext field".into(),
            })?;

        decode_32_byte_b64(plaintext_b64)
    }

    async fn rewrap_with_vault(&self, old_ciphertext: &str) -> Result<String> {
        let token = self.read_token()?;
        let url = format!(
            "{}/v1/{}/rewrap/{}",
            self.addr.trim_end_matches('/'),
            self.mount,
            self.key_name
        );

        let resp = self
            .client
            .post(&url)
            .header("X-Vault-Token", &token)
            .timeout(VAULT_TIMEOUT)
            .json(&serde_json::json!({ "ciphertext": old_ciphertext }))
            .send()
            .await
            .map_err(|e| crate::Error::Encryption {
                detail: format!("Vault rewrap HTTP request failed: {e}"),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(crate::Error::Encryption {
                detail: format!("Vault rewrap returned {status}: {body}"),
            });
        }

        let body: serde_json::Value = resp.json().await.map_err(|e| crate::Error::Encryption {
            detail: format!("Vault rewrap response JSON parse failed: {e}"),
        })?;

        body.pointer("/data/ciphertext")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned())
            .ok_or_else(|| crate::Error::Encryption {
                detail: "Vault rewrap response missing /data/ciphertext field".into(),
            })
    }
}

#[async_trait::async_trait]
impl KeyProvider for VaultKeyProvider {
    async fn unwrap_key(&self) -> Result<Zeroizing<[u8; 32]>> {
        let ciphertext = self.read_ciphertext_blob()?;
        let key = self.decrypt_with_vault(&ciphertext).await?;
        info!(
            mount = %self.mount,
            key_name = %self.key_name,
            "Vault transit key unwrapped successfully"
        );
        Ok(key)
    }

    async fn rotate(&self) -> Result<Zeroizing<[u8; 32]>> {
        let old_ciphertext = self.read_ciphertext_blob()?;

        // Rewrap with Vault (no plaintext leaves Vault during rewrap).
        let new_ciphertext = self.rewrap_with_vault(&old_ciphertext).await?;
        self.write_ciphertext_blob(&new_ciphertext)?;

        // Decrypt the new ciphertext to get the new plaintext key.
        let key = self.decrypt_with_vault(&new_ciphertext).await?;
        info!(
            mount = %self.mount,
            key_name = %self.key_name,
            "Vault transit key rotated successfully"
        );
        Ok(key)
    }
}

fn decode_32_byte_b64(b64: &str) -> Result<Zeroizing<[u8; 32]>> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| crate::Error::Encryption {
            detail: format!("Vault plaintext base64 decode failed: {e}"),
        })?;
    if bytes.len() != 32 {
        return Err(crate::Error::Encryption {
            detail: format!(
                "Vault plaintext must be 32 bytes after base64 decode, got {}",
                bytes.len()
            ),
        });
    }
    let mut key = Zeroizing::new([0u8; 32]);
    key.copy_from_slice(&bytes);
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;

    /// Minimal mock HTTP server using `axum`.
    ///
    /// The mock responds to:
    ///   POST /v1/transit/decrypt/mykey  → 200 with plaintext
    ///   POST /v1/transit/decrypt/badkey → 403
    fn spawn_mock_vault(port: u16) -> tokio::task::JoinHandle<()> {
        use axum::{Router, routing::post};

        tokio::spawn(async move {
            let app = Router::new()
                .route(
                    "/v1/transit/decrypt/mykey",
                    post(|| async {
                        // 32 bytes of 0x42 base64-encoded.
                        let plaintext =
                            base64::engine::general_purpose::STANDARD.encode([0x42u8; 32]);
                        axum::Json(serde_json::json!({
                            "data": { "plaintext": plaintext }
                        }))
                    }),
                )
                .route(
                    "/v1/transit/decrypt/badkey",
                    post(|| async {
                        (
                            axum::http::StatusCode::FORBIDDEN,
                            axum::Json(serde_json::json!({
                                "errors": ["permission denied"]
                            })),
                        )
                    }),
                )
                .route(
                    "/v1/transit/rewrap/mykey",
                    post(|| async {
                        axum::Json(serde_json::json!({
                            "data": { "ciphertext": "vault:v1:newwrapped==" }
                        }))
                    }),
                );

            let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
                .await
                .unwrap();
            axum::serve(listener, app).await.unwrap();
        })
    }

    /// Remote sources whose `[egress]` policy admits plaintext
    /// `http://localhost` resolving to loopback, as an operator allow-lists a
    /// private Vault.
    fn loopback() -> RemoteSources {
        RemoteSources::new(std::sync::Arc::new(
            crate::control::security::egress::EgressPolicy::from_parts(
                true,
                &["localhost".to_string()],
                &["127.0.0.0/8".to_string(), "::1/128".to_string()],
            )
            .expect("valid policy"),
        ))
        .expect("remote sources")
    }

    /// A Vault address outside the `[egress]` policy is refused before any
    /// request: an IP literal always, and a host off the allow-list.
    #[test]
    fn a_vault_addr_outside_the_egress_policy_is_refused() {
        let strict = RemoteSources::new(std::sync::Arc::new(
            crate::control::security::egress::EgressPolicy::strict(),
        ))
        .expect("remote sources");
        for (addr, sources) in [
            ("http://127.0.0.1:8200", loopback()),
            ("http://vault.internal:8200", strict),
        ] {
            let refused = VaultKeyProvider::new(
                addr.into(),
                "token".into(),
                "mykey".into(),
                "transit".into(),
                "blob".into(),
                &sources,
            );
            let detail = refused.err().map(|e| e.to_string()).unwrap_or_default();
            assert!(detail.contains("[egress]"), "{addr}: {detail}");
        }
    }

    /// Write a file with secure Unix permissions (0o600) for use in tests.
    #[cfg(unix)]
    fn write_secure(path: &std::path::Path, content: &[u8]) {
        use std::io::Write as _;
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(content).unwrap();
        drop(f);
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn vault_unwrap_happy_path() {
        let port = 18201u16;
        let _srv = spawn_mock_vault(port);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let dir = tempfile::tempdir().unwrap();
        let token_path = dir.path().join("token");
        let blob_path = dir.path().join("blob");
        write_secure(&token_path, b"fake-token");
        write_secure(&blob_path, b"vault:v1:someciphertext==");

        let provider = VaultKeyProvider::new(
            format!("http://localhost:{port}"),
            token_path,
            "mykey".into(),
            "transit".into(),
            blob_path,
            &loopback(),
        )
        .expect("an allow-listed Vault");

        let key = provider.unwrap_key().await.unwrap();
        assert_eq!(*key, [0x42u8; 32]);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn vault_auth_error_path() {
        let port = 18202u16;
        let _srv = spawn_mock_vault(port);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let dir = tempfile::tempdir().unwrap();
        let token_path = dir.path().join("token");
        let blob_path = dir.path().join("blob");
        write_secure(&token_path, b"bad-token");
        write_secure(&blob_path, b"vault:v1:cipher==");

        let provider = VaultKeyProvider::new(
            format!("http://localhost:{port}"),
            token_path,
            "badkey".into(),
            "transit".into(),
            blob_path,
            &loopback(),
        )
        .expect("an allow-listed Vault");

        let err = provider.unwrap_key().await.unwrap_err();
        let detail = format!("{err:?}");
        assert!(
            detail.contains("403") || detail.contains("permission"),
            "expected auth error, got: {detail}"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn vault_rotate_updates_blob() {
        let port = 18203u16;
        let _srv = spawn_mock_vault(port);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let dir = tempfile::tempdir().unwrap();
        let token_path = dir.path().join("token");
        let blob_path = dir.path().join("blob");
        write_secure(&token_path, b"fake-token");
        write_secure(&blob_path, b"vault:v1:original==");

        let provider = VaultKeyProvider::new(
            format!("http://localhost:{port}"),
            token_path,
            "mykey".into(),
            "transit".into(),
            blob_path.clone(),
            &loopback(),
        )
        .expect("an allow-listed Vault");

        let key = provider.rotate().await.unwrap();
        assert_eq!(*key, [0x42u8; 32]);

        // Blob on disk should have been updated to the new ciphertext.
        let new_blob = std::fs::read_to_string(&blob_path).unwrap();
        assert_eq!(new_blob.trim(), "vault:v1:newwrapped==");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn vault_insecure_token_file_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let token_path = dir.path().join("token");
        let blob_path = dir.path().join("blob");

        use std::io::Write as _;
        // Token file with insecure permissions (0o644).
        let mut f = std::fs::File::create(&token_path).unwrap();
        f.write_all(b"some-token").unwrap();
        drop(f);
        std::fs::set_permissions(&token_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        write_secure(&blob_path, b"vault:v1:cipher==");

        let provider = VaultKeyProvider::new(
            "http://localhost:1".into(), // unreachable — error before HTTP
            token_path,
            "mykey".into(),
            "transit".into(),
            blob_path,
            &loopback(),
        )
        .expect("an allow-listed Vault");

        let err = provider.unwrap_key().await.unwrap_err();
        let detail = format!("{err:?}");
        assert!(
            detail.contains("insecure") || detail.contains("644"),
            "expected insecure-permissions error on token file, got: {detail}"
        );
    }
}
