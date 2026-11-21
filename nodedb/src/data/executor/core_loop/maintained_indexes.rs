// SPDX-License-Identifier: BUSL-1.1

//! The in-memory mirror of a core's maintained document index paths.
//!
//! Index maintenance and index reads use this mirror, never the registered
//! collection config. The config follows the metadata log, which reaches
//! each replica at its own position against the data log. The mirror
//! follows the data log: a build or purge changes the durable set in its own
//! transaction, and the mirror takes the committed set after the commit.
//! Core open loads it from the durable set.
//!
//! A collection in top-level mode also maintains the path of every
//! top-level key a document holds, in the top-level shape, unless an owner
//! is stored on that path.

use std::borrow::Cow;

use crate::engine::document::store::IndexPath;
use crate::engine::sparse::btree::{
    MaintainedIndexSet, MaintainedOwner, TOP_LEVEL_OWNER, is_top_level_path, top_level_path,
};
use crate::types::{DatabaseId, TenantId};

use super::doc_config_key::{DocConfigKey, DocConfigRef};

/// What one collection maintains on one core.
#[derive(Debug, Default, Clone)]
struct CollectionIndexes {
    /// Each path an owner is stored on, as its effective owner's spec. Its
    /// `unique` is whether a UNIQUE probe can judge the path's entries.
    paths: Vec<IndexPath>,
    /// Whether every other top-level path is maintained in the top-level
    /// shape.
    top_level: bool,
}

impl CollectionIndexes {
    fn stored(&self, path: &str) -> Option<&IndexPath> {
        self.paths.iter().find(|p| p.path == path)
    }
}

/// Every maintained path of every collection on one core, each path as the
/// spec of its effective owner.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct MaintainedIndexes {
    by_collection: hashbrown::HashMap<DocConfigKey, CollectionIndexes>,
}

impl MaintainedIndexes {
    /// The mirror of `sets`, as core open loads them.
    pub(in crate::data::executor) fn load(sets: Vec<MaintainedIndexSet>) -> crate::Result<Self> {
        let mut mirror = Self::default();
        for set in &sets {
            mirror.install(set)?;
        }
        Ok(mirror)
    }

    /// Take `set` as its collection's committed set. An empty set removes
    /// the collection.
    pub(in crate::data::executor) fn install(
        &mut self,
        set: &MaintainedIndexSet,
    ) -> crate::Result<()> {
        let prepared = PreparedSet::of(set)?;
        self.apply(prepared);
        Ok(())
    }

    /// Take a set prepared before its commit. Applying cannot fail, so a
    /// committed set always reaches the mirror.
    pub(in crate::data::executor) fn apply(&mut self, prepared: PreparedSet) {
        let PreparedSet { key, indexes } = prepared;
        if indexes.paths.is_empty() && !indexes.top_level {
            self.by_collection.remove(&key);
        } else {
            self.by_collection.insert(key, indexes);
        }
    }

    /// Forget one collection, as a collection purge does.
    pub(in crate::data::executor) fn remove(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) {
        self.by_collection.remove(&DocConfigRef {
            database_id: DatabaseId::new(database_id),
            tenant_id: TenantId::new(tenant_id),
            collection,
        });
    }

    fn collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> Option<&CollectionIndexes> {
        self.by_collection.get(&DocConfigRef {
            database_id: DatabaseId::new(database_id),
            tenant_id: TenantId::new(tenant_id),
            collection,
        })
    }

    /// Whether a write of `collection` maintains any path.
    pub(in crate::data::executor) fn maintains_any(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> bool {
        self.collection(database_id, tenant_id, collection)
            .is_some()
    }

    /// The paths an owner is stored on for `collection`. Empty when none.
    /// Top-level paths a set in top-level mode keeps are not listed.
    #[cfg(test)]
    pub(in crate::data::executor) fn stored_paths(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> &[IndexPath] {
        self.collection(database_id, tenant_id, collection)
            .map(|c| c.paths.as_slice())
            .unwrap_or_default()
    }

    /// Every path a write of `docs` into `collection` maintains: each
    /// stored path, then in top-level mode the path of each top-level key
    /// any of `docs` holds that no stored owner decides. A key that one
    /// document holds and another drops is listed, so its value moves.
    pub(in crate::data::executor) fn paths_for_docs(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        docs: &[&serde_json::Value],
    ) -> Vec<Cow<'_, IndexPath>> {
        let Some(indexes) = self.collection(database_id, tenant_id, collection) else {
            return Vec::new();
        };
        let mut out: Vec<Cow<'_, IndexPath>> = indexes.paths.iter().map(Cow::Borrowed).collect();
        if !indexes.top_level {
            return out;
        }
        let mut seen: hashbrown::HashSet<String> = hashbrown::HashSet::new();
        for doc in docs {
            let Some(object) = doc.as_object() else {
                continue;
            };
            for key in object.keys() {
                let Some(path) = top_level_path(key) else {
                    continue;
                };
                if indexes.stored(&path).is_some() || !seen.insert(path.clone()) {
                    continue;
                }
                out.push(Cow::Owned(top_level_index_path(path)));
            }
        }
        out
    }

    /// The maintained spec of one path. `None` when this core does not
    /// maintain it. `path` is the extraction path, without an array suffix.
    pub(in crate::data::executor) fn path(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        path: &str,
    ) -> Option<Cow<'_, IndexPath>> {
        let indexes = self.collection(database_id, tenant_id, collection)?;
        if let Some(stored) = indexes.stored(path) {
            return Some(Cow::Borrowed(stored));
        }
        (indexes.top_level && is_top_level_path(path))
            .then(|| Cow::Owned(top_level_index_path(path.to_owned())))
    }

    /// Maintain `paths` for `key` without a build, for a test that sets up
    /// a core's indexed state directly.
    #[cfg(test)]
    pub(in crate::data::executor) fn insert_for_test(
        &mut self,
        key: DocConfigKey,
        paths: Vec<IndexPath>,
    ) {
        self.by_collection.insert(
            key,
            CollectionIndexes {
                paths,
                top_level: false,
            },
        );
    }
}

/// The spec a write maintains top-level path `path` by.
fn top_level_index_path(path: String) -> IndexPath {
    IndexPath {
        name: path.clone(),
        path,
        is_array: TOP_LEVEL_OWNER.is_array,
        unique: TOP_LEVEL_OWNER.unique,
        case_insensitive: TOP_LEVEL_OWNER.case_insensitive,
        state: nodedb_physical::physical_plan::RegisteredIndexState::Ready,
        predicate: None,
    }
}

impl super::CoreLoop {
    /// Install a snapshot's maintained set row on this core, durable first
    /// and then in the mirror. A row that does not decode, or whose key does
    /// not name its set, is refused before any write.
    pub(in crate::data::executor) fn install_maintained_index_set(
        &mut self,
        key: &str,
        bytes: &[u8],
    ) -> crate::Result<()> {
        let prepared = PreparedSet::of(&MaintainedIndexSet::from_bytes(key, bytes)?)?;
        self.sparse.put_maintained_indexes_raw(key, bytes)?;
        self.maintained_indexes.apply(prepared);
        Ok(())
    }
}

#[cfg(test)]
impl super::CoreLoop {
    /// Maintain every index path the registered configs name, as a build of
    /// each path does. For a test that sets up indexed state through
    /// `doc_configs` and writes rows after it.
    pub(in crate::data::executor) fn maintain_registered_paths_for_test(&mut self) {
        for (key, config) in &self.doc_configs {
            self.maintained_indexes
                .insert_for_test(key.clone(), config.index_paths.clone());
        }
    }
}

/// One collection's set in the mirror's form, built before the set commits.
#[derive(Debug)]
pub(in crate::data::executor) struct PreparedSet {
    key: DocConfigKey,
    indexes: CollectionIndexes,
}

impl PreparedSet {
    /// The mirror form of `set`. Fails when a stored predicate does not
    /// parse.
    pub(in crate::data::executor) fn of(set: &MaintainedIndexSet) -> crate::Result<Self> {
        let mut paths = Vec::with_capacity(set.paths.len());
        for entry in &set.paths {
            if let Some(spec) = entry.effective() {
                paths.push(index_path(&entry.path, spec, entry.judges_unique())?);
            }
        }
        Ok(Self {
            key: (
                DatabaseId::new(set.database_id),
                TenantId::new(set.tenant_id),
                set.collection.clone(),
            ),
            indexes: CollectionIndexes {
                paths,
                top_level: set.top_level,
            },
        })
    }
}

/// The spec a write maintains `path` by, from its effective owner. A stored
/// predicate that does not parse is refused: maintaining the path without
/// it writes entries for rows the partial index excludes.
///
/// `unique` is whether a UNIQUE probe can judge the path's entries. It is
/// set when any owner of the path is unique with the effective shape, so a
/// newer plain index of the same shape keeps it.
fn index_path(path: &str, spec: &MaintainedOwner, unique: bool) -> crate::Result<IndexPath> {
    let predicate = match spec.predicate.as_deref() {
        Some(text) => Some(
            crate::engine::document::predicate::IndexPredicate::parse(text).ok_or_else(|| {
                crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!(
                        "maintained index path '{path}' holds a predicate that does not \
                         parse: {text}"
                    ),
                }
            })?,
        ),
        None => None,
    };
    let name = match &spec.owner {
        nodedb_physical::physical_plan::DocIndexOwner::Named(name) => name.clone(),
        nodedb_physical::physical_plan::DocIndexOwner::Declared
        | nodedb_physical::physical_plan::DocIndexOwner::DeclaredTopLevel => path.to_string(),
    };
    Ok(IndexPath {
        name,
        path: path.to_string(),
        is_array: spec.is_array,
        unique,
        case_insensitive: spec.case_insensitive,
        state: nodedb_physical::physical_plan::RegisteredIndexState::Ready,
        predicate,
    })
}
