// SPDX-License-Identifier: BUSL-1.1

//! The write versions one read observed, one per vShard it read.

use smallvec::SmallVec;

use nodedb_types::{ShardVersion, WriteVersion};

use super::VShardId;

/// The versions one read observed, one per vShard.
///
/// A version positions a write within its vShard only, so each one travels
/// with the vShard it belongs to. A read of one collection observes one
/// vShard, so the common case holds one version without allocating.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadVersions(SmallVec<[ShardVersion; 1]>);

impl ReadVersions {
    /// No versions: the response observed nothing a read validates.
    pub fn new() -> Self {
        Self::default()
    }

    /// One vShard observed at `version`.
    pub fn single(vshard: VShardId, version: WriteVersion) -> Self {
        let mut versions = Self::new();
        versions.note(vshard, version);
        versions
    }

    /// Note that the read observed `vshard` at `version`.
    ///
    /// Two answers for one vShard keep the higher. Only the core that holds a
    /// vShard records versions of it, so every other answer is lower.
    pub fn note(&mut self, vshard: VShardId, version: WriteVersion) {
        self.note_raw(vshard.as_u32(), version);
    }

    fn note_raw(&mut self, vshard: u32, version: WriteVersion) {
        match self.0.iter_mut().find(|shard| shard.vshard == vshard) {
            Some(shard) => shard.version = shard.version.max(version),
            None => self.0.push(ShardVersion { vshard, version }),
        }
    }

    /// Fold `other` in, keeping the higher version per vShard.
    pub fn merge(&mut self, other: &ReadVersions) {
        for shard in &other.0 {
            self.note_raw(shard.vshard, shard.version);
        }
    }

    /// The version the read observed on `vshard`, if it reported one.
    pub fn of(&self, vshard: VShardId) -> Option<WriteVersion> {
        self.0
            .iter()
            .find(|shard| shard.vshard == vshard.as_u32())
            .map(|shard| shard.version)
    }

    /// Every observed vShard, in the order first noted.
    pub fn iter(&self) -> impl Iterator<Item = &ShardVersion> {
        self.0.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The versions as they travel between nodes.
    pub fn to_wire(&self) -> Vec<ShardVersion> {
        self.0.to_vec()
    }

    /// The versions a node sent.
    pub fn from_wire(versions: &[ShardVersion]) -> Self {
        let mut read = Self::new();
        for shard in versions {
            read.note_raw(shard.vshard, shard.version);
        }
        read
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_vshard_noted_twice_keeps_the_higher_version() {
        let mut versions = ReadVersions::single(VShardId::new(3), WriteVersion::logged(0, 9));
        versions.note(VShardId::new(3), WriteVersion::logged(0, 4));
        versions.note(VShardId::new(5), WriteVersion::logged(1, 2));
        assert_eq!(
            versions.of(VShardId::new(3)),
            Some(WriteVersion::logged(0, 9))
        );
        assert_eq!(
            versions.of(VShardId::new(5)),
            Some(WriteVersion::logged(1, 2))
        );
        assert_eq!(versions.of(VShardId::new(6)), None);
    }

    #[test]
    fn versions_survive_the_wire() {
        let mut versions = ReadVersions::single(VShardId::new(3), WriteVersion::logged(0, 9));
        versions.merge(&ReadVersions::single(
            VShardId::new(8),
            WriteVersion::logged(2, 1),
        ));
        assert_eq!(ReadVersions::from_wire(&versions.to_wire()), versions);
    }
}
