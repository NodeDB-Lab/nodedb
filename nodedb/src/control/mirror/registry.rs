// SPDX-License-Identifier: BUSL-1.1

//! Registry of active cross-cluster links keyed by local `DatabaseId`.
//!
//! The registry is `Send + Sync` and lives on the Control Plane. Each mirror
//! database that is actively following a source cluster has one entry here.
//! When `ALTER DATABASE … PROMOTE` is issued the entry is removed and the
//! link is shut down before the catalog mutation lands.
//!
//! # Receive timestamp
//!
//! Each entry tracks `last_received_ms`: the wall-clock time at which the
//! mirror most recently received a source-originated frame over the link. The
//! receive-side wiring calls
//! [`MirrorLinkRegistry::record_received`] on every inbound frame; the lag
//! monitor reads the value via [`MirrorLinkRegistry::last_received_ms`] to
//! drive the `Degraded → Disconnected` transition.
//!
//! Storing this on the registry (rather than on `CrossClusterLink`) keeps
//! the cluster crate free of timing concerns and lets the Control Plane own
//! the single source of truth for status decisions.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nodedb_types::DatabaseId;
use tracing::info;

use nodedb_cluster::mirror::CrossClusterLink;

/// One registry entry: a link plus its most recent receive timestamp.
///
/// `last_received_ms` is initialised to the registration time so a freshly
/// inserted link does not immediately appear stale to the lag monitor.
struct MirrorLinkEntry {
    link: Arc<CrossClusterLink>,
    last_received_ms: AtomicU64,
}

impl MirrorLinkEntry {
    fn new(link: Arc<CrossClusterLink>) -> Self {
        Self {
            link,
            last_received_ms: AtomicU64::new(now_ms()),
        }
    }
}

/// Registry of active [`CrossClusterLink`] handles and their receive timestamps.
///
/// Keys are local mirror `DatabaseId`. All methods are lock-safe and
/// panic-free (poisoned lock ⇒ `into_inner`).
pub struct MirrorLinkRegistry {
    entries: RwLock<HashMap<u64, Arc<MirrorLinkEntry>>>,
}

impl Default for MirrorLinkRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MirrorLinkRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Register an active link for `db_id`. Overwrites any previous entry.
    ///
    /// `last_received_ms` is initialised to the current wall-clock time so a
    /// freshly registered link is treated as fresh until the next monitor tick.
    pub fn insert(&self, db_id: DatabaseId, link: Arc<CrossClusterLink>) {
        let mut w = self.entries.write().unwrap_or_else(|p| p.into_inner());
        w.insert(db_id.as_u64(), Arc::new(MirrorLinkEntry::new(link)));
    }

    /// Remove and return the link for `db_id`, if any.
    pub fn remove(&self, db_id: DatabaseId) -> Option<Arc<CrossClusterLink>> {
        let mut w = self.entries.write().unwrap_or_else(|p| p.into_inner());
        w.remove(&db_id.as_u64()).map(|e| e.link.clone())
    }

    /// Look up the link for `db_id` without removing it.
    pub fn get(&self, db_id: DatabaseId) -> Option<Arc<CrossClusterLink>> {
        let r = self.entries.read().unwrap_or_else(|p| p.into_inner());
        r.get(&db_id.as_u64()).map(|e| e.link.clone())
    }

    /// All database ids currently tracked, for iteration during server shutdown.
    pub fn all_ids(&self) -> Vec<DatabaseId> {
        let r = self.entries.read().unwrap_or_else(|p| p.into_inner());
        r.keys().map(|&id| DatabaseId::new(id)).collect()
    }

    /// Record that the apply loop just received a source-originated frame
    /// for `db_id`. Updates `last_received_ms` to the current wall-clock.
    ///
    /// Called from the apply receiver hot path. No-op if `db_id` has no
    /// registered link (a frame for an unknown link is dropped upstream).
    pub fn record_received(&self, db_id: DatabaseId) {
        let r = self.entries.read().unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = r.get(&db_id.as_u64()) {
            entry.last_received_ms.store(now_ms(), Ordering::Relaxed);
        }
    }

    /// Most recent receive timestamp for `db_id` in wall-clock milliseconds,
    /// or `None` if no link is registered.
    ///
    /// `None` means "no link in registry" — the lag monitor must treat that
    /// as "no signal" and fall back to the applied cut's time in the catalog
    /// `mirror_cut_state` row.
    pub fn last_received_ms(&self, db_id: DatabaseId) -> Option<u64> {
        let r = self.entries.read().unwrap_or_else(|p| p.into_inner());
        r.get(&db_id.as_u64())
            .map(|e| e.last_received_ms.load(Ordering::Relaxed))
    }

    /// Tear down the link for `db_id` if one is registered.
    ///
    /// The link entry is removed from the registry before any I/O so a
    /// concurrent caller cannot re-use a link that is mid-teardown. The
    /// actual QUIC close is best-effort: failure is logged but does not
    /// propagate to the caller because the operator's intent (stop following
    /// the source) must succeed regardless of network state.
    pub fn teardown_link(&self, db_id: DatabaseId) {
        if let Some(link) = self.remove(db_id) {
            info!(
                db_id = db_id.as_u64(),
                source_cluster = link.source_cluster_id(),
                "mirror link teardown: removing cross-cluster link"
            );
            // The link is `Arc`-wrapped; dropping the last Arc here causes
            // quinn to close the underlying QUIC connection gracefully on the
            // next I/O poll. Since this is called from a synchronous handler,
            // we cannot `.await` the close — best-effort drop is correct.
            drop(link);
        }
    }
}

/// Current wall-clock milliseconds since UNIX epoch.
///
/// Intentionally local to this module: the only callers are the registry's
/// own timestamp updates. The observer module has its own copy because both
/// the metric-update path and the status machine compute `now` independently.
/// The two differ by a few ms: the registry stamps the receive instant, and the
/// observer stamps the evaluation instant.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}
