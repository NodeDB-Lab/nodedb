// SPDX-License-Identifier: BUSL-1.1

//! [`ReadWriteSet`]: the read set or write set of a Calvin transaction.

use nodedb_types::id::{CollectionKey, DatabaseId, VShardId};
use serde::{Deserialize, Serialize};

use crate::error::CalvinError;

use super::primitives::EngineKeySet;

/// A set of keys spanning one or more engines, forming either the read set
/// or the write set of a Calvin transaction.
///
/// Cross-engine atomic transactions — e.g. a Document+Vector insert that must
/// land atomically — require all affected engines to appear in a single
/// `ReadWriteSet`. Decomposing by engine would break atomicity.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReadWriteSet(pub Vec<EngineKeySet>);

impl ReadWriteSet {
    pub fn new(sets: Vec<EngineKeySet>) -> Self {
        Self(sets)
    }

    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|s| s.is_empty())
    }

    /// Derive the set of vShards participating in this read/write set.
    ///
    /// For Document/Vector/KV/Unique entries, and a Collection entry that
    /// names no vShards, the vshard is derived from the collection name
    /// (collection-level routing, consistent with the per-vshard Raft groups
    /// that own each collection). KV collections are also assigned a single
    /// vshard at creation time. A Collection entry that names vShards
    /// participates on exactly those.
    ///
    /// For Edge entries the participating vShards are the edge's
    /// `home_vshards` (the `from_key(src)` / `from_key(dst)` key-hashed
    /// homes), NOT the collection name: a graph edge is dual-homed across
    /// its two endpoint vShards so it can be written atomically to both.
    ///
    /// This derivation is re-run on decode rather than serialized, so the
    /// serialized bytes remain deterministic regardless of how `VShardId`
    /// is computed.
    pub fn participating_vshards(&self) -> Result<Vec<VShardId>, CalvinError> {
        self.participating_vshards_in_database(DatabaseId::DEFAULT)
    }

    /// Derive participants using database-scoped collection homes.
    ///
    /// Key-set collection names are database-qualified, because the Data
    /// Plane reads storage by them. Each one is de-qualified into a
    /// [`CollectionKey`] before hashing, so the participant set matches the
    /// vShard every other path homes the collection to.
    pub fn participating_vshards_in_database(
        &self,
        database_id: DatabaseId,
    ) -> Result<Vec<VShardId>, CalvinError> {
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for engine_set in &self.0 {
            match engine_set {
                EngineKeySet::Edge {
                    home_vshards: homes,
                    ..
                }
                | EngineKeySet::Array { vshards: homes, .. } => {
                    for &home in homes.as_slice() {
                        let vshard = VShardId::new(home);
                        if seen.insert(vshard.as_u32()) {
                            result.push(vshard);
                        }
                    }
                }
                EngineKeySet::Collection { vshards: homes, .. } if !homes.is_empty() => {
                    for &home in homes.as_slice() {
                        let vshard = VShardId::new(home);
                        if seen.insert(vshard.as_u32()) {
                            result.push(vshard);
                        }
                    }
                }
                EngineKeySet::Document { .. }
                | EngineKeySet::Vector { .. }
                | EngineKeySet::Kv { .. }
                | EngineKeySet::Collection { .. }
                | EngineKeySet::Unique { .. } => {
                    let vshard =
                        CollectionKey::from_qualified_str(database_id, engine_set.collection())?
                            .vshard();
                    if seen.insert(vshard.as_u32()) {
                        result.push(vshard);
                    }
                }
            }
        }
        result.sort_by_key(|v| v.as_u32());
        Ok(result)
    }
}
