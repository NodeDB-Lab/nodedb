// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree definition changes the metadata applier committed but the
//! permission cache has not taken yet.
//!
//! The applier runs synchronously and cannot take the cache's async lock, so
//! it queues each change here. The planning view and the lease coverage
//! apply the queue before they read the cache. Changes apply in commit
//! order.

use std::sync::Mutex;

use crate::control::security::catalog::StoredCollection;
use crate::control::security::permission_tree::{PermissionCache, PermissionTreeDef, SourceIndex};
use crate::types::DatabaseId;

/// One committed change to a collection's tree definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeDefChange {
    Register {
        tenant_id: u64,
        collection: String,
        def: PermissionTreeDef,
    },
    Unregister {
        tenant_id: u64,
        collection: String,
    },
}

impl TreeDefChange {
    /// The change a committed collection descriptor makes. Tree definitions
    /// live on default-database collections only, as the DDL writes them.
    /// An inactive collection governs nothing.
    pub fn from_collection(stored: &StoredCollection) -> crate::Result<Option<Self>> {
        if stored.database_id != DatabaseId::DEFAULT {
            return Ok(None);
        }
        let tenant_id = stored.tenant_id;
        let collection = stored.name.clone();
        let def = match (&stored.permission_tree_def, stored.is_active) {
            (Some(json), true) => json,
            _ => {
                return Ok(Some(Self::Unregister {
                    tenant_id,
                    collection,
                }));
            }
        };
        let def: PermissionTreeDef =
            sonic_rs::from_str(def).map_err(|e| crate::Error::Serialization {
                format: "json".into(),
                detail: format!("PERMISSION_TREE of collection '{collection}': {e}"),
            })?;
        Ok(Some(Self::Register {
            tenant_id,
            collection,
            def,
        }))
    }

    /// Record this committed change in the source index, ahead of the cache.
    pub fn note_committed(&self, sources: &SourceIndex) {
        match self {
            Self::Register {
                tenant_id,
                collection,
                def,
            } => sources.note_committed(*tenant_id, collection, Some(def)),
            Self::Unregister {
                tenant_id,
                collection,
            } => sources.note_committed(*tenant_id, collection, None),
        }
    }

    /// Apply this change to `cache`.
    pub fn apply(self, cache: &mut PermissionCache) {
        match self {
            Self::Register {
                tenant_id,
                collection,
                def,
            } => cache.register_tree_def(tenant_id, &collection, def),
            Self::Unregister {
                tenant_id,
                collection,
            } => cache.unregister_tree_def(tenant_id, &collection),
        }
    }
}

/// The queue of committed changes not yet in the cache.
#[derive(Debug, Default)]
pub struct PendingTreeDefs {
    changes: Mutex<Vec<TreeDefChange>>,
}

impl PendingTreeDefs {
    /// Queue a change the applier committed.
    pub fn push(&self, change: TreeDefChange) {
        self.changes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(change);
    }

    /// Whether a change waits.
    pub fn is_empty(&self) -> bool {
        self.changes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_empty()
    }

    /// Apply every queued change to `cache`. The caller holds the cache's
    /// write lock, so two callers cannot apply out of order.
    pub fn apply_to(&self, cache: &mut PermissionCache) {
        let changes = std::mem::take(&mut *self.changes.lock().unwrap_or_else(|p| p.into_inner()));
        for change in changes {
            change.apply(cache);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def() -> PermissionTreeDef {
        sonic_rs::from_str(
            r#"{"resource_column":"id","graph_index":"tree","permission_table":"grants"}"#,
        )
        .expect("tree def")
    }

    #[test]
    fn queued_changes_apply_in_commit_order() {
        let pending = PendingTreeDefs::default();
        pending.push(TreeDefChange::Register {
            tenant_id: 1,
            collection: "docs".into(),
            def: def(),
        });
        pending.push(TreeDefChange::Unregister {
            tenant_id: 1,
            collection: "docs".into(),
        });
        pending.push(TreeDefChange::Register {
            tenant_id: 1,
            collection: "notes".into(),
            def: def(),
        });
        assert!(!pending.is_empty());

        let mut cache = PermissionCache::new();
        pending.apply_to(&mut cache);

        assert!(pending.is_empty());
        assert!(cache.get_tree_def(1, "docs").is_none());
        assert_eq!(cache.get_tree_def(1, "notes"), Some(&def()));
    }
}
