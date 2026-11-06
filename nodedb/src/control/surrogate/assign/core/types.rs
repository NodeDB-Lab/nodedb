// SPDX-License-Identifier: BUSL-1.1

//! [`SurrogateAssigner`] struct definition, construction, and the small
//! lock/handle accessors shared by [`super::assign_ops`] and
//! [`super::flush`].

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock, Weak};

use nodedb_types::CollectionKey;
use tokio::sync::{Notify, oneshot};

use super::super::super::registry::SurrogateRegistry;
use super::super::super::wal_appender::SurrogateWalAppender;
use super::home::HomeSurrogateAuthority;
use crate::control::mirror::MirrorFence;
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;

/// Shared handle to the surrogate registry. Lives on `SharedState`
/// and is cloned (cheaply) into every CP path that allocates
/// surrogates.
///
/// The inner `RwLock` is held only for the duration of one
/// `assign_surrogate` call (write lock) — the registry's hot-path
/// `alloc_one` uses atomics, so the lock is uncontended.
pub type SurrogateRegistryHandle = Arc<RwLock<SurrogateRegistry>>;

/// CP-side surrogate assigner. Owning shape — bundles the registry,
/// the credential store (which exposes the catalog), and the WAL
/// appender so call sites only need to pass `(collection, pk_bytes)`.
///
/// Stored as `Arc<SurrogateAssigner>` on `SharedState`.
///
/// Fields are `pub(in crate::control::surrogate::assign)` so the
/// sibling [`super::super::cluster_reserve`] module's `impl` block (the
/// cross-node reservation methods) can reach them despite now sitting
/// one directory deeper than `cluster_reserve.rs`; they remain private
/// outside the `assign` module.
pub struct SurrogateAssigner {
    pub(in crate::control::surrogate::assign) registry: SurrogateRegistryHandle,
    pub(in crate::control::surrogate::assign) credential_store: Arc<CredentialStore>,
    pub(in crate::control::surrogate::assign) wal_appender: Arc<dyn SurrogateWalAppender>,
    /// Weak handle to SharedState for Raft-mediated HWM proposals.
    /// Set after SharedState construction to break the Arc cycle.
    /// When set and a Raft cluster is active, the flush path proposes
    /// `MetadataEntry::SurrogateAlloc { hwm }` in addition to the
    /// local WAL record so all followers advance their HWM.
    pub(in crate::control::surrogate::assign) shared: std::sync::OnceLock<Weak<SharedState>>,
    /// Pending cluster-mode batch reservations keyed by `request_id`.
    /// `reserve_batch` registers a oneshot here before proposing; the
    /// metadata applier removes + fires it via `complete_reservation`
    /// once the carved `[start, end)` range is known at apply time.
    pub(in crate::control::surrogate::assign) pending_reservations:
        Mutex<HashMap<u64, oneshot::Sender<(u32, u32)>>>,
    /// Monotonic source of unique `request_id`s for reservations on
    /// this node. Only ever read/incremented locally.
    pub(in crate::control::surrogate::assign) next_request_id: AtomicU64,
    /// Serializes in-flight reservations so at most one batch is being
    /// reserved at a time per node. Without this, a burst of allocators
    /// that all observe an empty batch each propose a reservation,
    /// over-reserving and wasting surrogate space.
    pub(in crate::control::surrogate::assign) reserve_gate: tokio::sync::Mutex<()>,
    /// Wakes the background refill loop. The hot path nudges it (via
    /// `notify_one`) whenever a draw fails or the batch dips below the
    /// low-watermark; the refiller then performs the blocking reservation
    /// OFF the latency-critical insert path. `Notify` coalesces: a nudge
    /// while the refiller is already running is remembered as one pending
    /// permit, so no top-up is ever lost.
    pub(in crate::control::surrogate::assign) refill_notify: Arc<Notify>,
    /// Wakes every async drawer waiting on an empty batch. Fired by
    /// `complete_reservation` once it installs a batch.
    pub(in crate::control::surrogate::assign) batch_ready: Notify,
    /// Where a cluster node obtains a key's bound surrogate from the key's
    /// collection home (see [`super::home`]). Installed once the node's state
    /// is built. Unset on a single node, which is every key's home.
    pub(in crate::control::surrogate::assign) home_authority:
        std::sync::OnceLock<Arc<dyn HomeSurrogateAuthority>>,
    /// The databases that refuse client writes. Installed when the node's
    /// state is assembled, on a single node and a cluster node alike.
    mirror_fence: std::sync::OnceLock<Arc<MirrorFence>>,
}

impl SurrogateAssigner {
    pub fn new(
        registry: SurrogateRegistryHandle,
        credential_store: Arc<CredentialStore>,
        wal_appender: Arc<dyn SurrogateWalAppender>,
    ) -> Self {
        Self {
            registry,
            credential_store,
            wal_appender,
            shared: std::sync::OnceLock::new(),
            pending_reservations: Mutex::new(HashMap::new()),
            next_request_id: AtomicU64::new(1),
            reserve_gate: tokio::sync::Mutex::new(()),
            refill_notify: Arc::new(Notify::new()),
            batch_ready: Notify::new(),
            home_authority: std::sync::OnceLock::new(),
            mirror_fence: std::sync::OnceLock::new(),
        }
    }

    /// Install the fence [`Self::refuse_mirror_bind`] reads.
    pub fn install_mirror_fence(&self, fence: Arc<MirrorFence>) {
        let _ = self.mirror_fence.set(fence);
    }

    /// Refuse a new binding for a write to a mirror that is not promoted.
    ///
    /// A client write binds its keys while it plans, ahead of every write
    /// gate. The gate then refuses the write and the binding stays, with no
    /// row behind it, on a database that holds only what its source wrote.
    /// So a write path runs this before it binds. The mirror applier binds
    /// nothing here: its bindings arrive in replicated entries.
    pub fn refuse_mirror_bind(&self, key: CollectionKey<'_>) -> crate::Result<()> {
        let read_only = self
            .mirror_fence
            .get()
            .and_then(|fence| fence.read_only_name(key.database_id()));
        match read_only {
            Some(database) => {
                // A client write refused here is routine. A bootstrap re-issue
                // refused here is a capture that lost a bind, so it names itself.
                let bootstrapping = self
                    .mirror_fence
                    .get()
                    .is_some_and(|fence| fence.is_bootstrapping(key.database_id()));
                if bootstrapping {
                    tracing::warn!(
                        database = %database,
                        collection = key.name(),
                        "a key with no bind reached a bootstrapping mirror; the write stops here"
                    );
                }
                Err(crate::Error::MirrorReadOnly {
                    database: database.to_string(),
                })
            }
            None => Ok(()),
        }
    }

    /// Install a weak SharedState handle so the flush path can
    /// propose to Raft when in cluster mode. Called by `start_raft`
    /// after SharedState is fully wired.
    pub fn install_shared(&self, shared: Weak<SharedState>) {
        let _ = self.shared.set(shared);
    }

    /// Install the home authority a cluster node resolves keys through.
    /// Called once the node's state is built.
    pub fn install_home_authority(&self, authority: Arc<dyn HomeSurrogateAuthority>) {
        let _ = self.home_authority.set(authority);
    }

    /// Expose the registry handle for read access by the Raft applier.
    ///
    /// The returned `Arc<RwLock<SurrogateRegistry>>` is used by
    /// `MetadataCommitApplier` to call `restore_hwm` when a
    /// `SurrogateAlloc` entry commits on a follower.
    pub fn registry_handle(&self) -> &SurrogateRegistryHandle {
        &self.registry
    }

    /// Acquire a write lock on the registry, converting a poisoned-lock
    /// error into the crate's typed `Internal` error.
    pub(super) fn registry_write(
        &self,
    ) -> crate::Result<std::sync::RwLockWriteGuard<'_, SurrogateRegistry>> {
        self.registry.write().map_err(|_| crate::Error::Internal {
            detail: "surrogate registry lock poisoned".into(),
        })
    }
}
