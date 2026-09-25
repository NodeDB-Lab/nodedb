// SPDX-License-Identifier: BUSL-1.1

//! Which collections feed a permission tree, readable without the cache lock.
//!
//! A write path decides on every write whether the write changes
//! authorization state: it does when it writes a governed collection or a
//! permission table of a registered tree. That check runs on hot write paths,
//! so it reads this index under a plain read lock rather than the cache's
//! async lock.
//!
//! Two sets feed the answer, and a collection in either counts:
//!
//! - **Cached:** rebuilt by the cache whenever a tree definition changes.
//! - **Committed:** updated by the metadata applier as it commits a change,
//!   before the cache takes it from the queue.
//!
//! A write therefore counts as an authorization change from the moment its
//! tree definition committed on this node. A removed tree may count a little
//! longer, until both sets drop it, which only adds a barrier.
//!
//! Tree sources live in the default database, as the permission-tree DDL
//! writes them, so each source collection homes on one vShard.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use crate::types::{DatabaseId, VShardId};

use super::types::PermissionTreeDef;

#[derive(Debug, Default)]
struct SourceSet {
    collections: HashSet<String>,
    vshards: HashSet<u32>,
}

impl SourceSet {
    fn from_defs<'a>(
        defs: impl IntoIterator<Item = (&'a (u64, String), &'a PermissionTreeDef)>,
    ) -> Self {
        let mut set = Self::default();
        for ((_, governed), def) in defs {
            for collection in [governed.as_str(), def.permission_table.as_str()] {
                set.collections.insert(collection.to_owned());
                set.vshards.insert(
                    VShardId::from_collection_in_database(DatabaseId::DEFAULT, collection).as_u32(),
                );
            }
        }
        set
    }
}

#[derive(Debug, Default)]
struct Committed {
    defs: HashMap<(u64, String), PermissionTreeDef>,
    set: SourceSet,
}

/// The source collections of every registered or committed tree.
#[derive(Debug, Default)]
pub struct SourceIndex {
    cached: RwLock<SourceSet>,
    committed: RwLock<Committed>,
}

impl SourceIndex {
    /// Rebuild the cached set from the cache's tree definitions.
    pub(super) fn rebuild(&self, tree_defs: &HashMap<(u64, String), PermissionTreeDef>) {
        *self.cached.write().unwrap_or_else(|p| p.into_inner()) = SourceSet::from_defs(tree_defs);
    }

    /// Record a tree definition the metadata applier committed. `None`
    /// removes the tree of `(tenant_id, collection)`.
    pub fn note_committed(
        &self,
        tenant_id: u64,
        collection: &str,
        def: Option<&PermissionTreeDef>,
    ) {
        let mut committed = self.committed.write().unwrap_or_else(|p| p.into_inner());
        let key = (tenant_id, collection.to_owned());
        match def {
            Some(def) => {
                committed.defs.insert(key, def.clone());
            }
            None => {
                committed.defs.remove(&key);
            }
        }
        committed.set = SourceSet::from_defs(&committed.defs);
    }

    fn any(&self, test: impl Fn(&SourceSet) -> bool) -> bool {
        test(&self.cached.read().unwrap_or_else(|p| p.into_inner()))
            || test(&self.committed.read().unwrap_or_else(|p| p.into_inner()).set)
    }

    /// Whether no tree is registered or committed.
    pub fn is_empty(&self) -> bool {
        !self.any(|set| !set.collections.is_empty())
    }

    /// Whether `collection` feeds a tree.
    pub fn is_source_collection(&self, collection: &str) -> bool {
        self.any(|set| set.collections.contains(collection))
    }

    /// Whether `vshard_id` homes a collection that feeds a tree.
    pub fn is_source_vshard(&self, vshard_id: u32) -> bool {
        self.any(|set| set.vshards.contains(&vshard_id))
    }

    /// Every vShard that homes a source collection.
    pub fn source_vshards(&self) -> Vec<u32> {
        let mut vshards: HashSet<u32> = self
            .cached
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .vshards
            .clone();
        vshards.extend(
            self.committed
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .set
                .vshards
                .iter()
                .copied(),
        );
        vshards.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_index_names_governed_collections_and_permission_tables() {
        let def: PermissionTreeDef = sonic_rs::from_str(
            r#"{"resource_column":"id","graph_index":"tree","permission_table":"grants"}"#,
        )
        .expect("tree def");
        let mut defs = HashMap::new();
        defs.insert((1, "docs".to_owned()), def);
        let index = SourceIndex::default();
        assert!(index.is_empty());
        index.rebuild(&defs);
        assert!(index.is_source_collection("docs"));
        assert!(index.is_source_collection("grants"));
        assert!(!index.is_source_collection("other"));
        let grants_vshard =
            VShardId::from_collection_in_database(DatabaseId::DEFAULT, "grants").as_u32();
        assert!(index.is_source_vshard(grants_vshard));
        defs.clear();
        index.rebuild(&defs);
        assert!(index.is_empty());
        assert!(!index.is_source_vshard(grants_vshard));
    }

    #[test]
    fn a_committed_tree_counts_before_the_cache_takes_it() {
        let def: PermissionTreeDef = sonic_rs::from_str(
            r#"{"resource_column":"id","graph_index":"tree","permission_table":"grants"}"#,
        )
        .expect("tree def");
        let index = SourceIndex::default();
        index.note_committed(1, "docs", Some(&def));
        assert!(index.is_source_collection("grants"));
        assert!(index.is_source_collection("docs"));
        index.note_committed(1, "docs", None);
        assert!(index.is_empty());
    }
}
