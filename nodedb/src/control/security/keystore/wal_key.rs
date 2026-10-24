// SPDX-License-Identifier: BUSL-1.1

//! Load the WAL key from the source `[encryption]` names: a key file, Vault,
//! or AWS KMS. The WAL key also derives the segment and checkpoint keys.

use std::sync::Arc;

use crate::config::server::EncryptionSettings;
use crate::control::security::egress::{EgressPolicy, RemoteSources};

/// The WAL key `settings` names, read from its file or unwrapped by its
/// key-management service. A Vault request goes through `egress`, the
/// server's `[egress]` policy.
pub async fn load_wal_key(
    settings: &EncryptionSettings,
    egress: Arc<EgressPolicy>,
) -> crate::Result<nodedb_wal::crypto::WalEncryptionKey> {
    let source = settings.source()?;
    let kind = source.kind();
    let sources = RemoteSources::new(egress).map_err(|e| crate::Error::Config {
        detail: format!("[encryption] {kind} key source: outbound HTTP client: {e}"),
    })?;
    let provider = source.into_provider(&sources).await?;
    let key = provider.unwrap_key().await?;
    nodedb_wal::crypto::WalEncryptionKey::from_bytes(&key).map_err(crate::Error::Wal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn a_key_file_is_the_default_source() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wal.key");
        std::fs::write(&path, [0x24u8; 32]).expect("write key");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("restrict key");
        let key = load_wal_key(
            &EncryptionSettings::file(path),
            Arc::new(EgressPolicy::strict()),
        )
        .await
        .expect("the file key loads");
        let expected = nodedb_wal::crypto::WalEncryptionKey::with_epoch(&[0x24u8; 32], [0u8; 4]);
        let sealed = expected
            .encrypt_aad(1, b"header", b"payload")
            .expect("encrypt with the expected key");
        let opened = key
            .decrypt_aad(&[0u8; 4], 1, b"header", &sealed)
            .expect("the loaded key opens what the file key sealed");
        assert_eq!(opened, b"payload");
    }

    #[tokio::test]
    async fn a_settings_block_with_no_source_is_refused() {
        let empty = EncryptionSettings {
            key_path: None,
            key_derivation: None,
        };
        let refused = load_wal_key(&empty, Arc::new(EgressPolicy::strict())).await;
        assert!(
            refused
                .err()
                .is_some_and(|e| e.to_string().contains("[encryption]"))
        );
    }
}
