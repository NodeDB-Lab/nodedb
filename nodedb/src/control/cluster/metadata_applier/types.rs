// SPDX-License-Identifier: BUSL-1.1

//! `MetadataCommitApplier` struct definition, construction, and the
//! `CatalogChangeEvent` it broadcasts.

use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

use tokio::sync::broadcast;

use nodedb_cluster::MetadataCache;

use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;

/// Broadcast channel capacity — small, because consumers are
/// internal subsystems that keep up or are lagged intentionally.
pub const CATALOG_CHANNEL_CAPACITY: usize = 64;

/// Event published on every committed metadata entry.
#[derive(Debug, Clone)]
pub struct CatalogChangeEvent {
    pub applied_index: u64,
}

/// Production `MetadataApplier` installed on the `RaftLoop`.
pub struct MetadataCommitApplier {
    pub(super) cache: Arc<RwLock<MetadataCache>>,
    pub(super) catalog_change_tx: broadcast::Sender<CatalogChangeEvent>,
    pub(super) credentials: Arc<CredentialStore>,
    pub(super) token_state: nodedb_cluster::SharedTokenStateMirror,
    pub(super) transport: OnceLock<Arc<nodedb_cluster::NexarTransport>>,
    /// Weak handle to `SharedState`. Installed by `start_raft` after
    /// construction so the applier can spawn async post-apply side
    /// effects (Data Plane register on `PutCollection`,
    /// `sequence_registry.create` on `PutSequence`, etc.). Weak to
    /// break the Arc cycle (SharedState → raft loop → applier →
    /// SharedState). `None` in unit tests.
    pub(super) shared: OnceLock<Weak<SharedState>>,
    /// `(raft_index, start, end)` of a surrogate carve whose persist failed.
    /// The re-delivered entry persists it and hands this exact batch to the
    /// allocator still waiting on it.
    pub(super) unpersisted_carve: Mutex<Option<(u64, u32, u32)>>,
}

impl MetadataCommitApplier {
    pub fn new(
        cache: Arc<RwLock<MetadataCache>>,
        catalog_change_tx: broadcast::Sender<CatalogChangeEvent>,
        credentials: Arc<CredentialStore>,
        token_state: nodedb_cluster::SharedTokenStateMirror,
    ) -> Self {
        Self {
            cache,
            catalog_change_tx,
            credentials,
            token_state,
            transport: OnceLock::new(),
            shared: OnceLock::new(),
            unpersisted_carve: Mutex::new(None),
        }
    }

    /// Whether the entry `payload` at `raft_index` owes its post-apply.
    ///
    /// The marker is a catalog row, so it survives a restart. The full-log
    /// replay at boot re-delivers the entry and finds it.
    pub(super) fn owes_post_apply(
        &self,
        raft_index: u64,
        payload: &[u8],
    ) -> Result<bool, crate::Error> {
        Ok(self
            .credentials
            .catalog()
            .load_owed_post_apply()?
            .is_some_and(|(index, owed)| index == raft_index && owed.as_slice() == payload))
    }

    /// Record durably that the entry `payload` at `raft_index` owes its
    /// post-apply. The row lands before the entry's catalog write, so a crash
    /// between that write and the end of the post-apply leaves it behind.
    pub(super) fn owe_post_apply(
        &self,
        raft_index: u64,
        payload: &[u8],
    ) -> Result<(), crate::Error> {
        self.credentials
            .catalog()
            .put_owed_post_apply(raft_index, payload)
    }

    /// Record durably that the entry `payload` at `raft_index` owes no
    /// post-apply. A marker of another entry stays.
    pub(super) fn settle_post_apply(
        &self,
        raft_index: u64,
        payload: &[u8],
    ) -> Result<(), crate::Error> {
        self.credentials
            .catalog()
            .remove_owed_post_apply(raft_index, payload)
    }

    pub fn install_transport(&self, transport: Arc<nodedb_cluster::NexarTransport>) {
        let _ = self.transport.set(transport);
    }

    /// Install a weak handle to `SharedState` so the applier can
    /// spawn post-apply side effects. Must be called **before** the
    /// raft loop starts ticking; `start_raft` does this as part of
    /// its construction sequence.
    pub fn install_shared(&self, shared: Weak<SharedState>) {
        let _ = self.shared.set(shared);
    }

    /// The installed `SharedState`.
    ///
    /// Missing means the applier runs before `start_raft` installed it, or
    /// after shutdown dropped it. The entry's host effects cannot land, so
    /// this is a transient error: the entry is re-delivered.
    pub(super) fn shared_state(&self) -> Result<Arc<SharedState>, crate::Error> {
        self.shared
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| crate::Error::Internal {
                detail: "metadata applier has no SharedState (not installed yet, or the node \
                         shut down); the entry is re-delivered"
                    .into(),
            })
    }
}
