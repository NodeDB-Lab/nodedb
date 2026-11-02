// SPDX-License-Identifier: BUSL-1.1

//! The vShards one read scanned.

use super::VShardId;

/// The vShards one read scanned, sorted and distinct.
///
/// The transaction read-set homes one entry of the read on each vShard, and
/// commit validates the entry there at the snapshot ordinal. Most reads scan
/// one vShard, so the set holds ids on the heap and keeps a `Response` small.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadShards(Vec<u32>);

impl ReadShards {
    /// No vShards: the response scanned nothing a read-set entry names.
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// One scanned vShard.
    pub fn single(vshard: VShardId) -> Self {
        Self(vec![vshard.as_u32()])
    }

    /// Note that the read scanned `vshard`.
    pub fn note(&mut self, vshard: VShardId) {
        let id = vshard.as_u32();
        if let Err(at) = self.0.binary_search(&id) {
            self.0.insert(at, id);
        }
    }

    /// Fold `other` in.
    pub fn merge(&mut self, other: &ReadShards) {
        for &id in &other.0 {
            self.note(VShardId::new(id));
        }
    }

    /// Whether the read scanned `vshard`.
    pub fn contains(&self, vshard: VShardId) -> bool {
        self.0.binary_search(&vshard.as_u32()).is_ok()
    }

    /// Every scanned vShard, in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = VShardId> + '_ {
        self.0.iter().map(|&id| VShardId::new(id))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The vShards as they travel between nodes, in ascending order.
    pub fn to_wire(&self) -> Vec<u32> {
        self.0.clone()
    }

    /// The vShards a node sent. An id at or above [`VShardId::COUNT`] names
    /// no vShard and is dropped.
    pub fn from_wire(vshards: &[u32]) -> Self {
        let mut shards = Self::new();
        for &id in vshards {
            if id < VShardId::COUNT {
                shards.note(VShardId::new(id));
            }
        }
        shards
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_vshard_noted_twice_is_held_once() {
        let mut shards = ReadShards::single(VShardId::new(3));
        shards.note(VShardId::new(3));
        shards.note(VShardId::new(1_000));
        assert_eq!(
            shards.iter().collect::<Vec<_>>(),
            vec![VShardId::new(3), VShardId::new(1_000)]
        );
        assert!(!shards.contains(VShardId::new(6)));
    }

    #[test]
    fn merging_unions_the_vshards() {
        let mut shards = ReadShards::single(VShardId::new(64));
        shards.merge(&ReadShards::single(VShardId::new(63)));
        assert_eq!(shards.to_wire(), vec![63, 64]);
        assert!(ReadShards::new().is_empty());
    }

    #[test]
    fn vshards_survive_the_wire() {
        let mut shards = ReadShards::single(VShardId::new(3));
        shards.note(VShardId::new(1_023));
        assert_eq!(ReadShards::from_wire(&shards.to_wire()), shards);
        assert_eq!(ReadShards::from_wire(&[5_000]), ReadShards::new());
    }
}
