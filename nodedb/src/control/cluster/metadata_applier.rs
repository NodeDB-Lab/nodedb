//! Production metadata-group commit applier.
//!
//! Wraps the in-cluster [`nodedb_cluster::CacheApplier`] (which owns
//! the `MetadataCache` writes) and additionally:
//!
//! 1. Advances an [`AppliedIndexWatcher`] so synchronous pgwire
//!    handlers can block on `propose_metadata_and_wait` until the
//!    newly-committed entry is visible on THIS node.
//! 2. Publishes a `CatalogChangeEvent` on a `tokio::sync::broadcast`
//!    channel so subsystems like the pgwire prepared-statement cache
//!    and the HTTP `/catalog` endpoint can invalidate on schema
//!    change (consumers land with their respective migrations in
//!    batch 1c / Phase C).
//!
//! The production applier does NOT yet write back to `security.redb`
//! (the existing `SystemCatalog` redb file) on followers. That's the
//! pgwire collection-DDL handler migration in batch 1c — when we flip
//! the handler from "put_collection + propose" to "propose only", the
//! applier grows a redb-writeback branch for each DDL object type.
//! Until then, pgwire handler calls to `put_collection` write to the
//! leader's redb directly; followers rely on the in-memory cache for
//! reads (see `OriginCatalog::get_collection`).

use std::sync::Arc;

use tokio::sync::broadcast;
use tracing::debug;

use nodedb_cluster::{CacheApplier, MetadataApplier, MetadataCache};

use crate::control::cluster::applied_index_watcher::AppliedIndexWatcher;

/// Broadcast channel capacity — small, because consumers are internal
/// subsystems that keep up or are lagged intentionally (e.g. HTTP
/// caches that rebuild on any missed message).
pub const CATALOG_CHANNEL_CAPACITY: usize = 64;

/// Event published on every committed metadata entry.
#[derive(Debug, Clone)]
pub struct CatalogChangeEvent {
    /// Raft log index of the entry that was applied.
    pub applied_index: u64,
}

/// Production `MetadataApplier` installed on the `RaftLoop`.
///
/// Layered on top of the shared [`CacheApplier`] so the cache write
/// path stays in `nodedb-cluster` (one source of truth for cache
/// semantics), while nodedb-specific side effects — watcher bump,
/// broadcast publish — happen here.
pub struct MetadataCommitApplier {
    inner: CacheApplier,
    watcher: Arc<AppliedIndexWatcher>,
    catalog_change_tx: broadcast::Sender<CatalogChangeEvent>,
}

impl MetadataCommitApplier {
    pub fn new(
        cache: Arc<std::sync::RwLock<MetadataCache>>,
        watcher: Arc<AppliedIndexWatcher>,
        catalog_change_tx: broadcast::Sender<CatalogChangeEvent>,
    ) -> Self {
        Self {
            inner: CacheApplier::new(cache),
            watcher,
            catalog_change_tx,
        }
    }
}

impl MetadataApplier for MetadataCommitApplier {
    fn apply(&self, entries: &[(u64, Vec<u8>)]) -> u64 {
        let last = self.inner.apply(entries);
        if last > 0 {
            self.watcher.bump(last);
            // A lagging or disconnected subscriber is fine — we don't
            // block on delivery. The HTTP catalog cache and prepared
            // statement invalidator react on next access if they
            // missed a message.
            let _ = self.catalog_change_tx.send(CatalogChangeEvent {
                applied_index: last,
            });
            debug!(applied_index = last, "metadata applier bumped watermark");
        }
        last
    }
}
