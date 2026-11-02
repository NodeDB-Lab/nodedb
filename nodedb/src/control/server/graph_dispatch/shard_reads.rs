// SPDX-License-Identifier: BUSL-1.1

//! The vShards a cross-shard graph read observed, for the transaction
//! read-set.
//!
//! A coordinator notes each vShard its legs read, then publishes the log
//! once the read finishes (`session::pending_shard_reads`). The request's
//! protocol records the published reads into the transaction read-set, so
//! the commit enlists and validates every vShard the read depended on.

use std::collections::BTreeSet;

use crate::control::server::shared::session::pending_shard_reads::{ShardReads, note};
use crate::control::server::shared::session::read_set::EngineTag;
use crate::types::{DatabaseId, TenantId, VShardId};

/// Every vShard a graph read observed.
#[derive(Debug, Default)]
pub(crate) struct ShardReadLog {
    vshards: BTreeSet<u32>,
}

impl ShardReadLog {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Note that a leg read `vshards`.
    pub(crate) fn note(&mut self, vshards: impl IntoIterator<Item = u32>) {
        self.vshards.extend(vshards);
    }

    /// Fold `other` into this log.
    pub(crate) fn merge(&mut self, other: ShardReadLog) {
        self.vshards.extend(other.vshards);
    }

    /// Hand the log to the running request for its transaction read-set.
    /// `collection` is the database-qualified collection the read scoped, or
    /// `None` when it walked every collection.
    pub(crate) fn publish(
        self,
        tenant_id: TenantId,
        database_id: DatabaseId,
        collection: Option<String>,
    ) {
        note(ShardReads {
            engine: EngineTag::Graph,
            tenant_id,
            database_id,
            collection,
            shards: self.vshards.into_iter().map(VShardId::new).collect(),
        });
    }

    #[cfg(test)]
    pub(crate) fn vshards(&self) -> &BTreeSet<u32> {
        &self.vshards
    }
}

/// The stored, database-qualified name of `bare` in `database_id`.
pub(crate) fn qualified(database_id: DatabaseId, bare: &str) -> String {
    nodedb_types::QualifiedCollection::new(database_id, bare)
        .as_str()
        .to_owned()
}

/// The key vShard of each node in `nodes`.
pub(crate) fn key_vshards<'a>(nodes: impl IntoIterator<Item = &'a String>) -> Vec<u32> {
    nodes
        .into_iter()
        .map(|node| VShardId::from_key(node.as_bytes()).as_u32())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_vshard_read_twice_is_noted_once() {
        let mut log = ShardReadLog::new();
        log.note([3, 4]);
        log.note([3]);
        assert_eq!(
            log.vshards().iter().copied().collect::<Vec<_>>(),
            vec![3, 4]
        );
    }

    #[test]
    fn merging_unions_the_vshards() {
        let mut left = ShardReadLog::new();
        left.note([1]);
        let mut right = ShardReadLog::new();
        right.note([1, 2]);
        left.merge(right);
        assert_eq!(
            left.vshards().iter().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );
    }
}
