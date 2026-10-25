// SPDX-License-Identifier: BUSL-1.1

//! Wire `[credential_encryption]` into `SharedState`. `SharedState::open`
//! installs the `[egress]` policy and its outbound client.

use std::sync::Arc;

use tracing::info;

use crate::ServerConfig;
use crate::control::security::source_credential::CredentialKek;
use crate::control::state::SharedState;

/// Install the credential key. A key file that fails its checks stops
/// startup.
pub fn wire_remote_sources(
    shared: &mut Arc<SharedState>,
    config: &ServerConfig,
) -> crate::Result<()> {
    let credential_kek = match &config.credential_encryption {
        Some(settings) => {
            let reused = config
                .encryption
                .as_ref()
                .is_some_and(|wal| wal.key_file() == Some(settings.key_path.as_path()))
                || config
                    .backup_encryption
                    .as_ref()
                    .is_some_and(|backup| backup.key_path == settings.key_path);
            if reused {
                return Err(crate::Error::Config {
                    detail: "credential_encryption.key_path names the WAL or backup key file; \
                             give credentials a key of their own"
                        .into(),
                });
            }
            let kek = CredentialKek::read(&settings.key_path)?;
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
