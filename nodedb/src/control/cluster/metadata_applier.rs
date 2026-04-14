//! Production metadata-group commit applier.
//!
//! Single branch for DDL: decode the opaque `CatalogDdl { payload }`
//! as a host-side [`CatalogEntry`], write through to `SystemCatalog`
//! redb via [`catalog_entry::apply_to`], and spawn the post-apply
//! side effects (Data Plane register, sequence registry sync, etc.).
//! All 16 per-DDL-object types are handled by adding a variant to
//! `CatalogEntry` — nothing in this file changes per type.
//!
//! The applier also advances the `AppliedIndexWatcher` (so sync
//! pgwire handlers unblock on commit) and broadcasts
//! `CatalogChangeEvent` (for future prepared-statement / catalog
//! cache invalidation).

use std::sync::{Arc, OnceLock, RwLock, Weak};

use tokio::sync::broadcast;
use tracing::{debug, warn};

use nodedb_cluster::{MetadataApplier, MetadataCache, MetadataEntry, decode_entry};

use crate::control::catalog_entry;
use crate::control::cluster::applied_index_watcher::AppliedIndexWatcher;
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
    cache: Arc<RwLock<MetadataCache>>,
    watcher: Arc<AppliedIndexWatcher>,
    catalog_change_tx: broadcast::Sender<CatalogChangeEvent>,
    credentials: Arc<CredentialStore>,
    /// Weak handle to `SharedState`. Installed by `start_raft` after
    /// construction so the applier can spawn async post-apply side
    /// effects (Data Plane register on `PutCollection`,
    /// `sequence_registry.create` on `PutSequence`, etc.). Weak to
    /// break the Arc cycle (SharedState → raft loop → applier →
    /// SharedState). `None` in unit tests.
    shared: OnceLock<Weak<SharedState>>,
}

impl MetadataCommitApplier {
    pub fn new(
        cache: Arc<RwLock<MetadataCache>>,
        watcher: Arc<AppliedIndexWatcher>,
        catalog_change_tx: broadcast::Sender<CatalogChangeEvent>,
        credentials: Arc<CredentialStore>,
    ) -> Self {
        Self {
            cache,
            watcher,
            catalog_change_tx,
            credentials,
            shared: OnceLock::new(),
        }
    }

    /// Install a weak handle to `SharedState` so the applier can
    /// spawn post-apply side effects. Must be called **before** the
    /// raft loop starts ticking; `start_raft` does this as part of
    /// its construction sequence.
    pub fn install_shared(&self, shared: Weak<SharedState>) {
        let _ = self.shared.set(shared);
    }

    /// Apply a single decoded `MetadataEntry`'s host-side effects.
    ///
    /// - `CatalogDdl` → decode payload as `CatalogEntry`, write
    ///   through to redb via `catalog_entry::apply_to`, spawn async
    ///   post-apply side effects if `SharedState` is reachable.
    /// - Non-DDL variants (topology, routing, lease, version) have
    ///   no host-side redb effects in this crate — the cluster crate
    ///   already tracks them in the `MetadataCache`.
    fn apply_host_side_effects(&self, entry: &MetadataEntry) {
        // Handle non-CatalogDdl variants that still have host-side
        // effects. Drain start/end land on `shared.lease_drain` on
        // every node so the next `force_refresh_lease` check sees
        // the replicated drain state.
        match entry {
            MetadataEntry::DescriptorDrainStart {
                descriptor_id,
                up_to_version,
                expires_at,
            } => {
                if let Some(weak) = self.shared.get()
                    && let Some(shared) = weak.upgrade()
                {
                    shared.lease_drain.install_start(
                        descriptor_id.clone(),
                        *up_to_version,
                        *expires_at,
                    );
                    debug!(
                        descriptor = ?descriptor_id,
                        up_to_version,
                        "drain_start applied to host tracker"
                    );
                }
                return;
            }
            MetadataEntry::DescriptorDrainEnd { descriptor_id } => {
                if let Some(weak) = self.shared.get()
                    && let Some(shared) = weak.upgrade()
                {
                    shared.lease_drain.install_end(descriptor_id);
                    debug!(
                        descriptor = ?descriptor_id,
                        "drain_end applied to host tracker"
                    );
                }
                return;
            }
            _ => {}
        }

        let Some(catalog) = self.credentials.catalog() else {
            return;
        };
        let MetadataEntry::CatalogDdl { payload } = entry else {
            return;
        };
        let catalog_entry = match catalog_entry::decode(payload) {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "metadata applier: failed to decode CatalogEntry payload");
                return;
            }
        };

        // Stamp `descriptor_version` + `modification_hlc` for every
        // `Put*` variant that carries them. Gated on the rolling
        // upgrade flag — in mixed-version clusters the older nodes
        // do not have the stamp logic, so we leave the entry's
        // sentinel `0` / `Hlc::ZERO` in place and let resolvers
        // treat it as "unknown, always re-fetch". Only the proposing
        // node's apply path observes the gate; followers run the
        // same applier and will reach the same conclusion because
        // every node observes the same live topology (same
        // `wire_version` on every NodeInfo, replicated via the
        // gossip path).
        let stamped = if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
        {
            let compat = !shared.cluster_version_view().can_activate_feature(
                crate::control::rolling_upgrade::DESCRIPTOR_VERSIONING_VERSION,
            );
            if compat {
                catalog_entry
            } else {
                catalog_entry::descriptor_stamp::stamp(catalog_entry, &shared.hlc_clock, catalog)
            }
        } else {
            // Unit tests construct the applier without a SharedState.
            // They don't care about descriptor versioning — leave
            // the entry unstamped so existing assertions on
            // `descriptor_version == 0` still hold.
            catalog_entry
        };

        debug!(kind = stamped.kind(), "catalog_entry: applying to redb");
        catalog_entry::apply::apply_to(&stamped, catalog);
        // Implicit drain clear: if the entry is a `Put*` for one
        // of the six stamped descriptor types, the DDL that was
        // waiting on drain has now committed — remove the drain
        // entry from every node's host tracker. Happens before
        // post_apply so a subsequent `acquire_lease` fired from
        // post_apply doesn't see a stale drain.
        if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
        {
            if let Some(drained_id) =
                crate::control::lease::drain_propose::descriptor_id_for_implicit_clear(&stamped)
            {
                shared.lease_drain.install_end(&drained_id);
            }
            // Run synchronous post-apply side effects INLINE so every
            // in-memory cache update (install_replicated_user,
            // install_replicated_owner, etc.) is visible before the
            // watcher bump. Any reader that observes `applied_index`
            // moving past `last` is guaranteed to see the sync side
            // effects of every entry up to `last`.
            //
            // The async tail (today: Data Plane Register dispatches
            // for PutCollection) is spawned separately and is NOT
            // part of the applied-index contract — it's a
            // performance optimisation, not a correctness gate.
            catalog_entry::post_apply::apply_post_apply_side_effects_sync(&stamped, &shared);
            catalog_entry::post_apply::spawn_post_apply_async_side_effects(stamped, shared);
        }
    }
}

impl MetadataApplier for MetadataCommitApplier {
    fn apply(&self, entries: &[(u64, Vec<u8>)]) -> u64 {
        let mut last = 0u64;
        for (index, data) in entries {
            last = *index;
            if data.is_empty() {
                continue;
            }
            let entry = match decode_entry(data) {
                Ok(e) => e,
                Err(e) => {
                    warn!(index = *index, error = %e, "metadata decode failed");
                    continue;
                }
            };
            // 1. Cluster-owned cache state (topology, routing,
            //    leases, catalog_entries_applied counter).
            {
                let mut guard = self.cache.write().unwrap_or_else(|p| p.into_inner());
                guard.apply(*index, &entry);
            }
            // 2. Host side effects (redb writeback + async post-apply).
            self.apply_host_side_effects(&entry);
        }
        if last > 0 {
            self.watcher.bump(last);
            let _ = self.catalog_change_tx.send(CatalogChangeEvent {
                applied_index: last,
            });
            debug!(applied_index = last, "metadata applier bumped watermark");
        }
        last
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::catalog_entry::CatalogEntry;
    use crate::control::security::catalog::StoredCollection;
    use nodedb_cluster::encode_entry;

    fn make_applier() -> (
        MetadataCommitApplier,
        Arc<RwLock<MetadataCache>>,
        Arc<AppliedIndexWatcher>,
        Arc<CredentialStore>,
        tempfile::TempDir,
    ) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let credentials =
            Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let watcher = Arc::new(AppliedIndexWatcher::new());
        let (tx, _rx) = broadcast::channel(16);
        let applier =
            MetadataCommitApplier::new(cache.clone(), watcher.clone(), tx, credentials.clone());
        (applier, cache, watcher, credentials, tmp)
    }

    fn put_collection_entry(name: &str) -> MetadataEntry {
        let stored = StoredCollection::new(7, name, "tester");
        let catalog_entry = CatalogEntry::PutCollection(Box::new(stored));
        MetadataEntry::CatalogDdl {
            payload: catalog_entry::encode(&catalog_entry).unwrap(),
        }
    }

    #[test]
    fn apply_put_collection_writes_through_to_redb() {
        let (applier, cache, watcher, credentials, _tmp) = make_applier();
        let bytes = encode_entry(&put_collection_entry("orders")).unwrap();
        assert_eq!(applier.apply(&[(11, bytes)]), 11);
        assert_eq!(watcher.current(), 11);

        let cache_guard = cache.read().unwrap();
        assert_eq!(cache_guard.applied_index, 11);
        assert_eq!(cache_guard.catalog_entries_applied, 1);
        drop(cache_guard);

        let loaded = credentials
            .catalog()
            .as_ref()
            .unwrap()
            .get_collection(7, "orders")
            .unwrap()
            .expect("present");
        assert_eq!(loaded.name, "orders");
        assert_eq!(loaded.owner, "tester");
    }

    #[test]
    fn apply_deactivate_preserves_record() {
        let (applier, _cache, _watcher, credentials, _tmp) = make_applier();

        // Seed.
        applier.apply(&[(1, encode_entry(&put_collection_entry("archived")).unwrap())]);

        let drop_entry = MetadataEntry::CatalogDdl {
            payload: catalog_entry::encode(&CatalogEntry::DeactivateCollection {
                tenant_id: 7,
                name: "archived".into(),
            })
            .unwrap(),
        };
        applier.apply(&[(2, encode_entry(&drop_entry).unwrap())]);

        let loaded = credentials
            .catalog()
            .as_ref()
            .unwrap()
            .get_collection(7, "archived")
            .unwrap()
            .expect("preserved");
        assert!(!loaded.is_active);
    }

    #[test]
    fn apply_empty_batch_is_noop() {
        let (applier, _cache, watcher, _credentials, _tmp) = make_applier();
        assert_eq!(applier.apply(&[]), 0);
        assert_eq!(watcher.current(), 0);
    }
}
