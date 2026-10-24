// SPDX-License-Identifier: BUSL-1.1

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::control::security::keystore::KeyDerivation;

/// Encryption at rest settings: the WAL key, which also derives the segment
/// and checkpoint keys.
///
/// The key comes from exactly one source:
///
/// ```toml
/// [encryption]
/// # A 32-byte key file, the default source.
/// key_path = "/etc/nodedb/keys/wal.key"
/// ```
///
/// ```toml
/// [encryption.key_derivation]
/// # A key-management service: type = "vault", "aws_kms" or "file".
/// type = "vault"
/// addr = "https://vault.internal:8200"
/// token_path = "/etc/nodedb/keys/vault.token"
/// key_name = "nodedb-wal"
/// mount = "transit"
/// ciphertext_blob_path = "/var/lib/nodedb/.wal-key.wrapped"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptionSettings {
    /// Path to the 32-byte AES-256-GCM key file used to encrypt WAL records.
    /// Generate with: `head -c 32 /dev/urandom > /etc/nodedb/keys/wal.key`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_path: Option<PathBuf>,
    /// A key-management service that supplies the key in place of
    /// `key_path`. A Vault address goes through the `[egress]` policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_derivation: Option<KeyDerivation>,
}

impl EncryptionSettings {
    /// Settings that read the key from the file at `key_path`.
    pub fn file(key_path: PathBuf) -> Self {
        Self {
            key_path: Some(key_path),
            key_derivation: None,
        }
    }

    /// The key source these settings name. Exactly one of `key_path` and
    /// `key_derivation` must be set.
    pub fn source(&self) -> crate::Result<KeyDerivation> {
        match (&self.key_path, &self.key_derivation) {
            (Some(key_path), None) => Ok(KeyDerivation::File {
                key_path: key_path.clone(),
            }),
            (None, Some(derivation)) => Ok(derivation.clone()),
            (Some(_), Some(_)) => Err(crate::Error::Config {
                detail: "[encryption] sets both key_path and key_derivation; keep one".into(),
            }),
            (None, None) => Err(crate::Error::Config {
                detail: "[encryption] needs key_path or a [encryption.key_derivation] table".into(),
            }),
        }
    }

    /// The key file, when the key comes from a local file.
    pub fn key_file(&self) -> Option<&Path> {
        match (&self.key_path, &self.key_derivation) {
            (Some(key_path), _) => Some(key_path),
            (None, Some(KeyDerivation::File { key_path })) => Some(key_path),
            (None, _) => None,
        }
    }
}

/// Per-backup encryption settings.
///
/// When present, each backup envelope is encrypted with a per-backup DEK
/// that is itself wrapped by this backup KEK (AES-256-GCM). The backup KEK
/// MUST be distinct from the WAL KEK — a shared key is detected at startup
/// and surfaced as a [`tracing::warn!`].
///
/// Configure via the `[backup_encryption]` TOML section:
/// ```toml
/// [backup_encryption]
/// key_path = "/etc/nodedb/keys/backup.key"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupEncryptionSettings {
    /// Path to the 32-byte AES-256-GCM key file used to wrap per-backup DEKs.
    /// Generate with: `head -c 32 /dev/urandom > /etc/nodedb/keys/backup.key`
    pub key_path: PathBuf,
}

/// Client-facing TLS settings (distinct from inter-node mTLS in mtls.rs).
///
/// When this section is present, TLS is enabled for all protocols by default.
/// Use per-protocol flags to selectively disable TLS on individual listeners.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsSettings {
    /// Path to server certificate (PEM).
    pub cert_path: PathBuf,
    /// Path to server private key (PEM).
    pub key_path: PathBuf,
    /// Certificate hot-reload check interval (seconds). The server watches
    /// cert/key files for mtime changes and atomically swaps the TLS config.
    /// Default: 3600 (1 hour). Set to 0 to disable hot rotation.
    #[serde(default)]
    pub cert_reload_interval_secs: Option<u64>,

    /// Enable TLS on the native protocol listener (port 6433). Default: true.
    #[serde(default = "default_true")]
    pub native: bool,
    /// Enable TLS on the pgwire listener (port 6432). Default: true.
    #[serde(default = "default_true")]
    pub pgwire: bool,
    /// Enable TLS on the HTTP listener (port 6480). Default: true.
    #[serde(default = "default_true")]
    pub http: bool,
    /// Enable TLS on the RESP listener (port 6381). Default: true.
    #[serde(default = "default_true")]
    pub resp: bool,
    /// Enable TLS on the ILP listener (port 8086). Default: true.
    #[serde(default = "default_true")]
    pub ilp: bool,
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(body: &str) -> EncryptionSettings {
        toml::from_str(body).expect("the [encryption] section must deserialize")
    }

    #[test]
    fn key_path_selects_the_file_source() {
        let settings = parse(r#"key_path = "/k/wal.key""#);
        assert!(matches!(
            settings.source(),
            Ok(KeyDerivation::File { ref key_path }) if key_path == Path::new("/k/wal.key")
        ));
        assert_eq!(settings.key_file(), Some(Path::new("/k/wal.key")));
    }

    #[test]
    fn key_derivation_selects_vault_kms_or_file() {
        let vault = parse(
            r#"
            [key_derivation]
            type = "vault"
            addr = "https://vault.internal:8200"
            token_path = "/k/token"
            key_name = "wal"
            mount = "transit"
            ciphertext_blob_path = "/k/blob"
            "#,
        );
        assert!(matches!(vault.source(), Ok(KeyDerivation::Vault { .. })));
        assert_eq!(vault.key_file(), None);

        let kms = parse(
            r#"
            [key_derivation]
            type = "aws_kms"
            key_id = "arn:aws:kms:us-east-1:1:key/k"
            region = "us-east-1"
            ciphertext_blob_path = "/k/blob"
            "#,
        );
        assert!(matches!(kms.source(), Ok(KeyDerivation::AwsKms { .. })));
        assert_eq!(kms.key_file(), None);

        let file = parse(
            r#"
            [key_derivation]
            type = "file"
            key_path = "/k/wal.key"
            "#,
        );
        assert!(matches!(file.source(), Ok(KeyDerivation::File { .. })));
        assert_eq!(file.key_file(), Some(Path::new("/k/wal.key")));
    }

    #[test]
    fn exactly_one_source_is_required() {
        let both = parse(
            r#"
            key_path = "/k/wal.key"
            [key_derivation]
            type = "file"
            key_path = "/k/other.key"
            "#,
        );
        assert!(both.source().is_err());
        assert!(parse("").source().is_err());
        assert!(toml::from_str::<EncryptionSettings>(r#"keypath = "/k""#).is_err());
    }
}
