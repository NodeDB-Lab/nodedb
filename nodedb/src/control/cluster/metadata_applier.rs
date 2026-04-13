//! Production metadata-group commit applier.
//!
//! Owns the replicated [`nodedb_cluster::MetadataCache`] and, on every
//! committed metadata entry, additionally:
//!
//! 1. Decodes the entry's `host_payload` (when present) into its host
//!    type and writes the corresponding legacy record to
//!    `SystemCatalog` redb so every non-cache reader (`dispatch_utils`,
//!    the enforcement pipeline, `DESCRIBE`, `pg_catalog`) continues
//!    to see the descriptor on every node.
//! 2. Advances an [`AppliedIndexWatcher`] so synchronous pgwire
//!    handlers can block on `propose_metadata_and_wait` until the
//!    newly-committed entry is visible on THIS node.
//! 3. Publishes a `CatalogChangeEvent` on a `tokio::sync::broadcast`
//!    channel so the pgwire prepared-statement cache + HTTP catalog
//!    cache can invalidate.
//!
//! **Layering**: the host_payload is opaque to `nodedb-cluster`. Only
//! this applier (in the `nodedb` crate) knows that the payload carries
//! a `StoredCollection` for `CollectionDdl::Create`, that it should be
//! deactivated (not deleted) on `Drop`, and that `Alter` branches
//! carry a fresh full `StoredCollection` so the applier can overwrite
//! the record atomically.

use std::sync::{Arc, OnceLock, RwLock, Weak};

use tokio::sync::broadcast;
use tracing::{debug, warn};

use nodedb_cluster::metadata_group::actions::CollectionAction;
use nodedb_cluster::{MetadataApplier, MetadataCache, MetadataEntry, decode_entry};

use crate::control::cluster::applied_index_watcher::AppliedIndexWatcher;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;

/// Broadcast channel capacity — small, because consumers are internal
/// subsystems that keep up or are lagged intentionally.
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
    /// construction so the applier can spawn a `DocumentOp::Register`
    /// dispatch into this node's Data Plane whenever a replicated
    /// `CollectionDdl::Create` commits — the follower-side path that
    /// makes cross-node INSERTs work without waiting for lazy
    /// registration. Weak to break the Arc cycle (SharedState → raft
    /// loop → applier → SharedState). `None` in unit tests.
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
    /// spawn Data Plane `Register` dispatches after committing
    /// replicated `CollectionDdl::Create` entries. Must be called
    /// **before** the raft loop starts ticking — `start_raft` does
    /// this as part of its construction sequence.
    pub fn install_shared(&self, shared: Weak<SharedState>) {
        let _ = self.shared.set(shared);
    }

    /// Spawn a `DocumentOp::Register` for `stored` into this node's
    /// Data Plane. Called after a replicated `Create` has been
    /// persisted to the local `SystemCatalog` redb. Requires
    /// [`Self::install_shared`] to have been called; otherwise the
    /// spawn is silently skipped (unit-test mode).
    fn spawn_register(&self, stored: StoredCollection) {
        let Some(weak) = self.shared.get() else {
            return;
        };
        let Some(shared) = weak.upgrade() else {
            return;
        };
        let coll_name = stored.name.clone();
        tokio::spawn(async move {
            crate::control::server::pgwire::ddl::collection::create::dispatch_register_from_stored(
                &shared, &stored,
            )
            .await;
            debug!(
                collection = %coll_name,
                "metadata applier: Register dispatched to local Data Plane"
            );
        });
    }

    /// Apply the host-side side effects of a single decoded entry.
    ///
    /// Side effects:
    /// - `CollectionDdl::Create` with non-empty `host_payload` → decode
    ///   as [`StoredCollection`] and `put_collection` into the local
    ///   `SystemCatalog` redb.
    /// - `CollectionDdl::Drop` → mark the existing `StoredCollection`
    ///   as `is_active = false` (preserves the record for audit /
    ///   undrop; existing readers skip inactive records).
    /// - `CollectionDdl::Alter` with non-empty `host_payload` → decode
    ///   as a fresh `StoredCollection` and overwrite via
    ///   `put_collection`. Alter entries without a payload fall back
    ///   to the cache-only update already applied above.
    /// - All other variants: no host side effects in batch 1c. Each
    ///   non-collection DDL object type gains its host writeback in
    ///   its own pgwire handler migration batch.
    fn apply_host_side_effects(&self, entry: &MetadataEntry) {
        let Some(catalog) = self.credentials.catalog() else {
            // No system catalog configured — single-node in-memory
            // tests or a node that hasn't finished its boot catalog
            // open yet. Cache-only apply is correct in that mode.
            return;
        };
        // Non-collection variants: cache-only in batch 1c. Each
        // non-collection DDL object type gains its host writeback in
        // its own pgwire handler migration batch.
        let MetadataEntry::CollectionDdl {
            action,
            host_payload,
            ..
        } = entry
        else {
            return;
        };
        match action {
            CollectionAction::Create(desc) => {
                if host_payload.is_empty() {
                    return;
                }
                match zerompk::from_msgpack::<StoredCollection>(host_payload) {
                    Ok(stored) => {
                        if let Err(e) = catalog.put_collection(&stored) {
                            warn!(
                                collection = %desc.header.id.name,
                                error = %e,
                                "metadata applier: put_collection on Create failed"
                            );
                            return;
                        }
                        // Tell this node's Data Plane about the new
                        // collection so the first cross-node INSERT
                        // doesn't need to rediscover the storage mode.
                        self.spawn_register(stored);
                    }
                    Err(e) => warn!(
                        collection = %desc.header.id.name,
                        error = %e,
                        "metadata applier: failed to decode Create host_payload"
                    ),
                }
            }
            CollectionAction::Drop { id } => {
                // Preserve the record for audit & undrop: load it,
                // flip `is_active = false`, write back. If the
                // record is absent (fresh follower, or the node
                // was offline during Create), that's a no-op.
                match catalog.get_collection(id.tenant_id, &id.name) {
                    Ok(Some(mut stored)) => {
                        stored.is_active = false;
                        if let Err(e) = catalog.put_collection(&stored) {
                            warn!(
                                collection = %id.name,
                                error = %e,
                                "metadata applier: put_collection on Drop failed"
                            );
                        }
                    }
                    Ok(None) => {
                        debug!(
                            collection = %id.name,
                            "metadata applier: Drop with no local record (fresh follower)"
                        );
                    }
                    Err(e) => warn!(
                        collection = %id.name,
                        error = %e,
                        "metadata applier: get_collection on Drop failed"
                    ),
                }
            }
            CollectionAction::Alter { id, change } => {
                if !host_payload.is_empty() {
                    // Full-descriptor alter: overwrite the stored
                    // record and re-register with the local Data
                    // Plane so schema / enforcement changes are
                    // picked up immediately (new strict columns,
                    // updated retention, new materialized_sum, etc.).
                    match zerompk::from_msgpack::<StoredCollection>(host_payload) {
                        Ok(stored) => {
                            if let Err(e) = catalog.put_collection(&stored) {
                                warn!(
                                    collection = %id.name,
                                    error = %e,
                                    "metadata applier: put_collection on Alter failed"
                                );
                                return;
                            }
                            self.spawn_register(stored);
                            return;
                        }
                        Err(e) => {
                            warn!(
                                collection = %id.name,
                                error = %e,
                                "metadata applier: failed to decode Alter host_payload"
                            );
                            return;
                        }
                    }
                }
                // In-place alter via a specific `change` variant
                // (add_column, rename, etc.) without shipping a
                // whole fresh StoredCollection. The cache-side bump
                // already happened; redb stays at the previous
                // version until a handler migration attaches a
                // payload.
                debug!(
                    collection = %id.name,
                    ?change,
                    "metadata applier: Alter without host_payload — cache-only"
                );
            }
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
            // 1. Cache update (single source of truth for planners).
            {
                let mut guard = self.cache.write().unwrap_or_else(|p| p.into_inner());
                guard.apply(*index, &entry);
            }
            // 2. Host side effects (redb writeback, legacy readers).
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
    use nodedb_cluster::metadata_group::actions::CollectionAction;
    use nodedb_cluster::metadata_group::descriptors::collection::CollectionDescriptor;
    use nodedb_cluster::metadata_group::descriptors::common::{DescriptorHeader, DescriptorId};
    use nodedb_cluster::{DescriptorKind, encode_entry};
    use nodedb_types::Hlc;

    fn make_applier() -> (
        MetadataCommitApplier,
        Arc<RwLock<MetadataCache>>,
        Arc<AppliedIndexWatcher>,
    ) {
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let watcher = Arc::new(AppliedIndexWatcher::new());
        let (tx, _rx) = broadcast::channel(16);
        let credentials = Arc::new(CredentialStore::new());
        let applier = MetadataCommitApplier::new(cache.clone(), watcher.clone(), tx, credentials);
        (applier, cache, watcher)
    }

    fn coll_id(name: &str) -> DescriptorId {
        DescriptorId::new(1, DescriptorKind::Collection, name)
    }

    fn create_entry(name: &str, host_payload: Vec<u8>) -> MetadataEntry {
        MetadataEntry::CollectionDdl {
            tenant_id: 1,
            action: CollectionAction::Create(Box::new(CollectionDescriptor {
                header: DescriptorHeader::new_public(coll_id(name), 1, Hlc::new(1, 0)),
                collection_type: "document_schemaless".into(),
                columns: vec![],
                with_options: vec![],
                primary_key: None,
            })),
            host_payload,
        }
    }

    #[test]
    fn cache_updated_and_watcher_bumped() {
        let (applier, cache, watcher) = make_applier();
        let bytes = encode_entry(&create_entry("users", vec![])).unwrap();
        assert_eq!(applier.apply(&[(5, bytes)]), 5);
        assert_eq!(watcher.current(), 5);
        assert_eq!(cache.read().unwrap().collection_count(), 1);
    }

    #[test]
    fn empty_payload_skips_redb_writeback() {
        // No credentials catalog is configured in CredentialStore::new(),
        // so a host_payload of any shape would short-circuit. This test
        // just verifies the empty-payload path is a no-op.
        let (applier, _, _) = make_applier();
        let bytes = encode_entry(&create_entry("orders", vec![])).unwrap();
        assert_eq!(applier.apply(&[(7, bytes)]), 7);
    }

    #[test]
    fn empty_batch_returns_zero() {
        let (applier, _, watcher) = make_applier();
        assert_eq!(applier.apply(&[]), 0);
        assert_eq!(watcher.current(), 0);
    }

    /// The full production path: a `host_payload`-carrying Create
    /// flows through the applier, the cache picks up the descriptor,
    /// and the legacy `SystemCatalog` redb record appears so every
    /// downstream reader (dispatch, DESCRIBE, pg_catalog) can find
    /// the collection too.
    #[test]
    fn create_writes_through_to_system_catalog_redb() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let credentials =
            Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let watcher = Arc::new(AppliedIndexWatcher::new());
        let (tx, _rx) = broadcast::channel(16);
        let applier =
            MetadataCommitApplier::new(cache.clone(), watcher.clone(), tx, credentials.clone());

        let stored = StoredCollection::new(7, "invoices", "bob");
        let payload = zerompk::to_msgpack_vec(&stored).expect("encode");
        let bytes = encode_entry(&create_entry("invoices", payload)).unwrap();

        assert_eq!(applier.apply(&[(11, bytes)]), 11);
        assert_eq!(watcher.current(), 11);

        // Cache side (planner view).
        let cache_guard = cache.read().unwrap();
        assert_eq!(cache_guard.collection_count(), 1);
        let desc = cache_guard
            .collection(&coll_id("invoices"))
            .expect("present");
        assert_eq!(desc.header.id.tenant_id, 1); // descriptor tenant_id from entry
        drop(cache_guard);

        // Redb side (legacy readers).
        let catalog = credentials.catalog().as_ref().expect("catalog present");
        let loaded = catalog
            .get_collection(7, "invoices")
            .expect("read")
            .expect("found");
        assert_eq!(loaded.name, "invoices");
        assert_eq!(loaded.owner, "bob");
        assert_eq!(loaded.tenant_id, 7);
        assert!(loaded.is_active);
    }

    /// Drop marks the stored record `is_active = false` so the
    /// legacy `load_collections_for_tenant` filter hides it from
    /// readers, while `get_collection` still returns the inactive
    /// record for audit + undrop.
    #[test]
    fn drop_marks_stored_collection_inactive() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let credentials =
            Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let watcher = Arc::new(AppliedIndexWatcher::new());
        let (tx, _rx) = broadcast::channel(16);
        let applier =
            MetadataCommitApplier::new(cache.clone(), watcher.clone(), tx, credentials.clone());

        // Seed: create invoices.
        let stored = StoredCollection::new(7, "invoices", "bob");
        let payload = zerompk::to_msgpack_vec(&stored).unwrap();
        let create_bytes = encode_entry(&create_entry("invoices", payload)).unwrap();
        applier.apply(&[(1, create_bytes)]);

        // Drop: same id, empty host_payload — applier does a
        // get + mutate + put_collection.
        let drop_entry = MetadataEntry::CollectionDdl {
            tenant_id: 1,
            action: CollectionAction::Drop {
                id: DescriptorId::new(7, DescriptorKind::Collection, "invoices"),
            },
            host_payload: vec![],
        };
        let drop_bytes = encode_entry(&drop_entry).unwrap();
        applier.apply(&[(2, drop_bytes)]);

        let catalog = credentials.catalog().as_ref().unwrap();
        let loaded = catalog
            .get_collection(7, "invoices")
            .expect("read")
            .expect("record preserved for audit");
        assert!(!loaded.is_active, "drop should deactivate, not erase");
    }
}
