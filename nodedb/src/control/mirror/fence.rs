// SPDX-License-Identifier: BUSL-1.1

//! The set of databases that refuse client writes: every mirror that is not
//! promoted.
//!
//! Every write gate reads this set, so a write to any other database pays one
//! map lookup and no catalog read. The catalog is the source of truth. The set
//! follows it through the metadata applier's synchronous post-apply, which
//! runs before the applied index moves, so a reader at a metadata floor sees
//! the descriptor at that floor. Boot and a metadata image install rebuild the
//! whole set.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use nodedb_types::{DatabaseId, MirrorStatus};

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::database_types::DatabaseDescriptor;

fn is_bootstrapping(descriptor: &DatabaseDescriptor) -> bool {
    descriptor
        .mirror_origin
        .as_ref()
        .is_some_and(|origin| matches!(origin.status, MirrorStatus::Bootstrapping { .. }))
}

/// Names of the databases that refuse client writes, by id.
#[derive(Debug, Default)]
pub struct MirrorFence {
    read_only: RwLock<HashMap<DatabaseId, Arc<str>>>,
    /// The mirrors still taking their first capture. Their re-issue is the one
    /// unstamped writer a mirror admits.
    bootstrapping: RwLock<HashSet<DatabaseId>>,
}

/// The name a read-only database goes by, or `None` for a writable one.
fn read_only_name(descriptor: &DatabaseDescriptor) -> Option<Arc<str>> {
    let origin = descriptor.mirror_origin.as_ref()?;
    match origin.status {
        MirrorStatus::Promoted => None,
        MirrorStatus::Bootstrapping { .. }
        | MirrorStatus::Following
        | MirrorStatus::Degraded
        | MirrorStatus::Disconnected
        | MirrorStatus::Promoting { .. }
        | MirrorStatus::Failed { .. } => Some(Arc::from(descriptor.name.as_str())),
    }
}

impl MirrorFence {
    pub fn new() -> Self {
        Self::default()
    }

    /// The database's name when it refuses client writes.
    pub fn read_only_name(&self, database_id: DatabaseId) -> Option<Arc<str>> {
        self.read_only
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&database_id)
            .cloned()
    }

    /// Whether the database still takes its first capture.
    pub fn is_bootstrapping(&self, database_id: DatabaseId) -> bool {
        self.bootstrapping
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(&database_id)
    }

    fn follow_bootstrap(&self, descriptor: &DatabaseDescriptor) {
        let mut bootstrapping = self
            .bootstrapping
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if is_bootstrapping(descriptor) {
            bootstrapping.insert(descriptor.id);
        } else {
            bootstrapping.remove(&descriptor.id);
        }
    }

    /// Follow one applied descriptor.
    pub fn apply_descriptor(&self, descriptor: &DatabaseDescriptor) {
        self.follow_bootstrap(descriptor);
        let mut read_only = self
            .read_only
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match read_only_name(descriptor) {
            Some(name) => {
                read_only.insert(descriptor.id, name);
            }
            None => {
                read_only.remove(&descriptor.id);
            }
        }
    }

    /// Follow one applied database drop.
    pub fn remove(&self, database_id: DatabaseId) {
        self.read_only
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&database_id);
        self.bootstrapping
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&database_id);
    }

    /// Replace the set with what `catalog` holds.
    ///
    /// A read error leaves the set unchanged, and the caller refuses to
    /// continue: a set that misses a mirror admits writes to it.
    pub fn load_from_catalog(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let mut loaded = HashMap::new();
        let mut bootstrapping = HashSet::new();
        for descriptor in catalog.list_databases()? {
            if let Some(name) = read_only_name(&descriptor) {
                loaded.insert(descriptor.id, name);
            }
            if is_bootstrapping(&descriptor) {
                bootstrapping.insert(descriptor.id);
            }
        }
        *self
            .bootstrapping
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = bootstrapping;
        *self
            .read_only
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = loaded;
        Ok(())
    }

    /// Fence `database_id` with no catalog row behind it.
    #[cfg(test)]
    pub(crate) fn mark_read_only_for_test(&self, database_id: DatabaseId, name: &str) {
        self.read_only
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(database_id, Arc::from(name));
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use nodedb_types::{MirrorFailure, MirrorMode, MirrorOrigin};
    use tempfile::TempDir;

    use super::*;
    use crate::control::security::catalog::database_types::DatabaseStatus;

    fn descriptor(id: u64, status: Option<MirrorStatus>) -> DatabaseDescriptor {
        DatabaseDescriptor {
            id: DatabaseId::new(id),
            name: format!("db{id}"),
            status: DatabaseStatus::Mirroring,
            created_at_lsn: 0,
            quota_ref: 0,
            parent_clone: None,
            mirror_origin: status.map(|status| MirrorOrigin {
                source_cluster: "source".to_string(),
                source_database: DatabaseId::new(0),
                source_database_name: "source_db".to_string(),
                mode: MirrorMode::Async,
                status,
            }),
            audit_dml: nodedb_types::AuditDmlMode::None,
            idle_session_timeout_secs: 0,
        }
    }

    #[test]
    fn every_unpromoted_status_is_read_only() {
        let fence = MirrorFence::new();
        for (id, status) in [
            (
                11,
                MirrorStatus::Bootstrapping {
                    bytes_done: 0,
                    bytes_total: 0,
                },
            ),
            (12, MirrorStatus::Following),
            (13, MirrorStatus::Degraded),
            (14, MirrorStatus::Disconnected),
            (15, MirrorStatus::Promoting { final_cut: 3 }),
            (
                16,
                MirrorStatus::Failed {
                    failure: MirrorFailure::LogTruncated {
                        group: 1,
                        first_available: 2,
                    },
                },
            ),
        ] {
            fence.apply_descriptor(&descriptor(id, Some(status)));
            assert_eq!(
                fence.read_only_name(DatabaseId::new(id)).as_deref(),
                Some(format!("db{id}").as_str())
            );
        }
    }

    #[test]
    fn a_plain_database_is_writable() {
        let fence = MirrorFence::new();
        fence.apply_descriptor(&descriptor(20, None));
        assert!(fence.read_only_name(DatabaseId::new(20)).is_none());
    }

    #[test]
    fn promotion_clears_the_entry() {
        let fence = MirrorFence::new();
        fence.apply_descriptor(&descriptor(21, Some(MirrorStatus::Following)));
        fence.apply_descriptor(&descriptor(21, Some(MirrorStatus::Promoted)));
        assert!(fence.read_only_name(DatabaseId::new(21)).is_none());
    }

    #[test]
    fn a_drop_clears_the_entry() {
        let fence = MirrorFence::new();
        fence.apply_descriptor(&descriptor(22, Some(MirrorStatus::Following)));
        fence.remove(DatabaseId::new(22));
        assert!(fence.read_only_name(DatabaseId::new(22)).is_none());
    }

    #[test]
    fn a_catalog_load_replaces_the_set() {
        let dir = TempDir::new().expect("tempdir");
        let path: PathBuf = dir.path().join("system.redb");
        let catalog = SystemCatalog::open(&path).expect("open catalog");
        catalog
            .put_database(&descriptor(31, Some(MirrorStatus::Following)))
            .expect("put mirror");
        catalog
            .put_database(&descriptor(32, Some(MirrorStatus::Promoted)))
            .expect("put promoted");

        let fence = MirrorFence::new();
        fence.apply_descriptor(&descriptor(33, Some(MirrorStatus::Following)));
        fence.load_from_catalog(&catalog).expect("load");

        assert!(fence.read_only_name(DatabaseId::new(31)).is_some());
        assert!(fence.read_only_name(DatabaseId::new(32)).is_none());
        assert!(
            fence.read_only_name(DatabaseId::new(33)).is_none(),
            "a database the catalog does not hold leaves the set"
        );
    }
}
