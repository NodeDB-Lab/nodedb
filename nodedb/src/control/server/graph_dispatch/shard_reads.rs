// SPDX-License-Identifier: BUSL-1.1

//! The vShards a cross-shard graph read observed, for the transaction
//! read-set.
//!
//! A coordinator notes each vShard its legs read, at the version the serving
//! core reported, then publishes the log once the read finishes
//! (`session::pending_shard_reads`). The request's protocol records the published
//! reads into the transaction read-set, so commit validation checks every
//! vShard the read depended on. A version is the data-group log position of
//! the write that set it, the same on every replica, so a leg any replica
//! served compares with the vShard leader's versions.

use std::collections::BTreeMap;

use nodedb_types::WriteVersion;

use crate::control::server::shared::session::pending_shard_reads::{
    ShardObservation, ShardReads, note,
};
use crate::control::server::shared::session::read_set::EngineTag;
use crate::types::{DatabaseId, ReadVersions, TenantId, VShardId};

/// Every vShard a graph read observed, each at its earliest observed version.
#[derive(Debug, Default)]
pub(crate) struct ShardReadLog {
    versions: BTreeMap<u32, WriteVersion>,
}

impl ShardReadLog {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Note that a leg read `vshards` and reported `versions`. A vShard the
    /// leg reported no version of had no write its serving core knew of, so
    /// the leg observed it at `WriteVersion::ZERO`. A vShard read twice keeps
    /// its earlier version: the first observation is the one the result rests
    /// on.
    pub(crate) fn note(&mut self, vshards: impl IntoIterator<Item = u32>, versions: &ReadVersions) {
        for vshard in vshards {
            let version = versions.of(VShardId::new(vshard)).unwrap_or_default();
            self.note_version(vshard, version);
        }
    }

    fn note_version(&mut self, vshard: u32, version: WriteVersion) {
        self.versions
            .entry(vshard)
            .and_modify(|seen| *seen = (*seen).min(version))
            .or_insert(version);
    }

    /// Fold `other` into this log.
    pub(crate) fn merge(&mut self, other: ShardReadLog) {
        for (vshard, version) in other.versions {
            self.note_version(vshard, version);
        }
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
            shards: self
                .versions
                .into_iter()
                .map(|(vshard, version)| ShardObservation {
                    vshard: VShardId::new(vshard),
                    version,
                })
                .collect(),
        });
    }

    #[cfg(test)]
    pub(crate) fn versions(&self) -> &BTreeMap<u32, WriteVersion> {
        &self.versions
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

    fn at(index: u64) -> WriteVersion {
        WriteVersion::logged(1, index)
    }

    fn reported(pairs: &[(u32, u64)]) -> ReadVersions {
        let mut versions = ReadVersions::new();
        for (vshard, index) in pairs {
            versions.note(VShardId::new(*vshard), at(*index));
        }
        versions
    }

    #[test]
    fn a_vshard_read_twice_keeps_its_earliest_version() {
        let mut log = ShardReadLog::new();
        log.note([3, 4], &reported(&[(3, 20), (4, 20)]));
        log.note([3], &reported(&[(3, 10)]));
        log.note([4], &reported(&[(4, 30)]));
        assert_eq!(log.versions().get(&3), Some(&at(10)));
        assert_eq!(log.versions().get(&4), Some(&at(20)));
    }

    #[test]
    fn a_vshard_the_leg_reported_no_version_of_is_observed_at_zero() {
        let mut log = ShardReadLog::new();
        log.note([7], &reported(&[(2, 9)]));
        assert_eq!(log.versions().get(&7), Some(&WriteVersion::ZERO));
    }

    #[test]
    fn merging_keeps_the_earliest_version_per_vshard() {
        let mut left = ShardReadLog::new();
        left.note([1], &reported(&[(1, 8)]));
        let mut right = ShardReadLog::new();
        right.note([1, 2], &reported(&[(1, 4), (2, 6)]));
        left.merge(right);
        assert_eq!(left.versions().get(&1), Some(&at(4)));
        assert_eq!(left.versions().get(&2), Some(&at(6)));
    }
}
