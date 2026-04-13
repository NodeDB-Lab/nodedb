//! [`MetadataApplier`] trait: the contract raft_loop uses to dispatch
//! committed entries on the metadata group (group 0).

use std::sync::{Arc, RwLock};

use tracing::warn;

use crate::metadata_group::cache::MetadataCache;
use crate::metadata_group::codec::decode_entry;

/// Applies committed metadata entries to local state.
///
/// Implemented in the `nodedb-cluster` crate as [`CacheApplier`]
/// (tracks cluster-owned state only: topology/routing/leases/
/// version + a CatalogDdl counter) and wrapped by the production
/// applier in the `nodedb` crate to additionally decode the
/// `CatalogDdl` payload as a `CatalogEntry` and write through to
/// `SystemCatalog`.
pub trait MetadataApplier: Send + Sync + 'static {
    /// Apply a batch of committed raft entries. Entries with empty
    /// `data` (raft no-ops) are skipped. Returns the highest log
    /// index applied.
    fn apply(&self, entries: &[(u64, Vec<u8>)]) -> u64;
}

/// Default applier that writes committed entries to an in-memory
/// [`MetadataCache`]. The cache is shared with the rest of the
/// process via `Arc<RwLock<_>>`.
#[derive(Clone)]
pub struct CacheApplier {
    cache: Arc<RwLock<MetadataCache>>,
}

impl CacheApplier {
    pub fn new(cache: Arc<RwLock<MetadataCache>>) -> Self {
        Self { cache }
    }

    pub fn cache(&self) -> Arc<RwLock<MetadataCache>> {
        self.cache.clone()
    }
}

impl MetadataApplier for CacheApplier {
    fn apply(&self, entries: &[(u64, Vec<u8>)]) -> u64 {
        let mut last = 0u64;
        let mut guard = self
            .cache
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        for (index, data) in entries {
            last = *index;
            if data.is_empty() {
                continue;
            }
            match decode_entry(data) {
                Ok(entry) => guard.apply(*index, &entry),
                Err(e) => warn!(index = *index, error = %e, "metadata decode failed"),
            }
        }
        last
    }
}

/// No-op applier used by tests and subsystems that don't care about the
/// metadata stream. Still drains entries and returns the correct last
/// index so raft can advance its applied watermark.
pub struct NoopMetadataApplier;

impl MetadataApplier for NoopMetadataApplier {
    fn apply(&self, entries: &[(u64, Vec<u8>)]) -> u64 {
        entries.last().map(|(idx, _)| *idx).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_group::codec::encode_entry;
    use crate::metadata_group::entry::{MetadataEntry, TopologyChange};

    #[test]
    fn cache_applier_counts_catalog_ddl() {
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let applier = CacheApplier::new(cache.clone());

        let ddl = encode_entry(&MetadataEntry::CatalogDdl {
            payload: vec![1, 2, 3],
        })
        .unwrap();
        let topo = encode_entry(&MetadataEntry::TopologyChange(TopologyChange::Join {
            node_id: 7,
            addr: "10.0.0.7:9000".into(),
        }))
        .unwrap();

        let last = applier.apply(&[(1, ddl), (2, topo)]);
        assert_eq!(last, 2);

        let guard = cache.read().unwrap();
        assert_eq!(guard.applied_index, 2);
        assert_eq!(guard.catalog_entries_applied, 1);
        assert_eq!(guard.topology_log.len(), 1);
    }

    #[test]
    fn cache_applier_idempotent() {
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let applier = CacheApplier::new(cache.clone());

        let bytes = encode_entry(&MetadataEntry::CatalogDdl {
            payload: vec![9, 9],
        })
        .unwrap();
        applier.apply(&[(5, bytes.clone())]);
        applier.apply(&[(3, bytes)]); // Earlier index — ignored.

        let guard = cache.read().unwrap();
        assert_eq!(guard.applied_index, 5);
        assert_eq!(guard.catalog_entries_applied, 1);
    }

    #[test]
    fn noop_applier_advances_watermark() {
        let noop = NoopMetadataApplier;
        assert_eq!(noop.apply(&[(7, b"x".to_vec()), (9, b"y".to_vec())]), 9);
        assert_eq!(noop.apply(&[]), 0);
    }
}
