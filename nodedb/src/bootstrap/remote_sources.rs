// SPDX-License-Identifier: BUSL-1.1

//! Wire `[credential_encryption]` into `SharedState`. `SharedState::open`
//! installs the `[egress]` policy and its outbound client.

use std::path::Path;
use std::sync::Arc;

use tracing::info;

use crate::ServerConfig;
use crate::control::security::source_credential::CredentialKek;
use crate::control::state::SharedState;

/// Install the credential key. A key file that fails its checks stops
/// startup, and so does a key the WAL or a backup also uses.
pub fn wire_remote_sources(
    shared: &mut Arc<SharedState>,
    config: &ServerConfig,
) -> crate::Result<()> {
    let credential_kek = match &config.credential_encryption {
        Some(settings) => {
            if key_path_is_reused(config, &settings.key_path) {
                return Err(reused_key_error(&settings.key_path));
            }
            let kek = CredentialKek::read(&settings.key_path)?;
            let wal_keys = shared
                .wal
                .encryption_ring()
                .map(|ring| (ring.current(), ring.previous()));
            if key_bytes_are_reused(&kek, wal_keys, shared.backup_kek.as_deref())? {
                return Err(reused_key_error(&settings.key_path));
            }
            info!(key_path = %settings.key_path.display(), "credential encryption enabled");
            Some(Arc::new(kek))
        }
        None => None,
    };
    let state = Arc::get_mut(shared).ok_or_else(|| crate::Error::Internal {
        detail: "remote source settings: shared state is already shared before wiring".into(),
    })?;
    state.remote_sources.credential_kek = credential_kek;
    Ok(())
}

fn reused_key_error(key_path: &Path) -> crate::Error {
    crate::Error::Config {
        detail: format!(
            "credential_encryption.key_path {} holds the WAL or backup key; give credentials a \
             key of their own",
            key_path.display()
        ),
    }
}

/// Whether `key_path` names the WAL or backup key file, through any spelling of the path.
fn key_path_is_reused(config: &ServerConfig, key_path: &Path) -> bool {
    let wal_key = config
        .encryption
        .as_ref()
        .and_then(|settings| settings.key_file());
    let backup_key = config
        .backup_encryption
        .as_ref()
        .map(|settings| settings.key_path.as_path());
    [wal_key, backup_key]
        .into_iter()
        .flatten()
        .any(|other| same_file(other, key_path))
}

/// Whether two paths name one file. A path that does not resolve compares as written.
fn same_file(left: &Path, right: &Path) -> bool {
    match (std::fs::canonicalize(left), std::fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        (Err(_), _) | (_, Err(_)) => left == right,
    }
}

/// Whether `kek` holds the same bytes as a WAL key or the backup key.
///
/// The WAL key can come from Vault or AWS KMS, where no path can show the reuse.
/// A copied key file also has a path of its own, so the bytes decide.
fn key_bytes_are_reused(
    kek: &CredentialKek,
    wal_keys: Option<(
        &nodedb_wal::crypto::WalEncryptionKey,
        Option<&nodedb_wal::crypto::WalEncryptionKey>,
    )>,
    backup_kek: Option<&[u8; 32]>,
) -> crate::Result<bool> {
    if backup_kek.is_some_and(|backup| kek.same_bytes(backup)) {
        return Ok(true);
    }
    let Some((current, previous)) = wal_keys else {
        return Ok(false);
    };
    if kek.same_key_as_wal(current)? {
        return Ok(true);
    }
    match previous {
        Some(previous) => kek.same_key_as_wal(previous),
        None => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_wal::crypto::WalEncryptionKey;

    use super::*;

    const KEY: [u8; 32] = [0x11; 32];
    const OTHER: [u8; 32] = [0x22; 32];

    fn wal_key(bytes: &[u8; 32]) -> WalEncryptionKey {
        WalEncryptionKey::with_epoch(bytes, [7u8; 4])
    }

    #[test]
    fn a_wal_key_from_any_source_is_found_by_its_bytes() {
        let kek = CredentialKek::from_bytes(KEY);
        let same = wal_key(&KEY);
        let other = wal_key(&OTHER);
        assert!(key_bytes_are_reused(&kek, Some((&same, None)), None).expect("compare"));
        assert!(
            key_bytes_are_reused(&kek, Some((&other, Some(&same))), None).expect("compare"),
            "the previous WAL key still reads old segments"
        );
        assert!(!key_bytes_are_reused(&kek, Some((&other, None)), None).expect("compare"));
        assert!(!key_bytes_are_reused(&kek, None, None).expect("compare"));
    }

    #[test]
    fn a_backup_key_is_found_by_its_bytes() {
        let kek = CredentialKek::from_bytes(KEY);
        assert!(key_bytes_are_reused(&kek, None, Some(&KEY)).expect("compare"));
        assert!(!key_bytes_are_reused(&kek, None, Some(&OTHER)).expect("compare"));
    }

    #[test]
    fn two_spellings_of_one_path_are_one_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("wal.key");
        std::fs::write(&file, KEY).expect("write key");
        let spelled = dir.path().join(".").join("wal.key");
        assert!(same_file(&file, &spelled));
        assert!(!same_file(&file, &dir.path().join("cred.key")));
    }
}
