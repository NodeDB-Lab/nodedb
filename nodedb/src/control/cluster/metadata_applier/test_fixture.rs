// SPDX-License-Identifier: BUSL-1.1

//! Test fixture: an applier wired to a real `SharedState` over an on-disk
//! catalog.

use std::path::Path;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

use crate::bridge::dispatch::Dispatcher;
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;
use crate::wal::WalManager;

use super::MetadataCommitApplier;

/// One node session over the catalog `dir/system.redb`. `wal` names the WAL
/// file, so a second session over the same catalog can use a fresh WAL.
pub(super) fn applier_with_shared_at(
    dir: &Path,
    wal: &str,
) -> (MetadataCommitApplier, Arc<SharedState>) {
    applier_over(dir, wal, false, false)
}

/// [`applier_with_shared_at`] with the surrogate registry in cluster mode.
pub(super) fn cluster_applier_with_shared_at(
    dir: &Path,
    wal: &str,
) -> (MetadataCommitApplier, Arc<SharedState>) {
    applier_over(dir, wal, true, false)
}

/// [`applier_with_shared_at`] with a Data Plane core that acknowledges every
/// request. An entry whose post-apply registers a collection needs it: a
/// Register no core acknowledges stops the apply batch. The core holds the
/// state for the rest of the test, so the catalog cannot be reopened.
pub(super) fn applier_with_core_at(
    dir: &Path,
    wal: &str,
) -> (MetadataCommitApplier, Arc<SharedState>) {
    applier_over(dir, wal, false, true)
}

fn applier_over(
    dir: &Path,
    wal: &str,
    is_cluster: bool,
    answering: bool,
) -> (MetadataCommitApplier, Arc<SharedState>) {
    let wal = Arc::new(WalManager::open_for_testing(&dir.join(wal)).unwrap());
    let credentials = Arc::new(CredentialStore::open(&dir.join("system.redb")).unwrap());
    let (dispatcher, data_sides) = Dispatcher::new(1, 64);
    let state =
        SharedState::new_with_credentials(dispatcher, wal, credentials, is_cluster).unwrap();
    if answering {
        for side in data_sides {
            tokio::spawn(crate::control::state::test_core::acknowledge_every_request(
                Arc::clone(&state),
                side,
            ));
        }
    }
    let (tx, _rx) = broadcast::channel(16);
    let token_state = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let applier = MetadataCommitApplier::new(
        state.metadata_cache.clone(),
        tx,
        state.credentials.clone(),
        token_state,
    );
    applier.install_shared(Arc::downgrade(&state));
    (applier, state)
}
