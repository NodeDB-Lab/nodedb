// SPDX-License-Identifier: BUSL-1.1

//! The vShards a stored record lives on.
//!
//! Live writes, the Raft snapshot builder, backup and MOVE TENANT capture, and
//! the restore re-issue all place a record by this one rule:
//!
//! - A collection row lives on its collection's home vShard.
//! - A graph edge lives on the key vShard of each endpoint. `from_key(src)`
//!   holds it for forward traversal and `from_key(dst)` for reverse traversal.
//! - A PK→surrogate bind lives on its collection home, the one place a key's
//!   surrogate is minted (`surrogate_exchange::authority`). In a collection
//!   that holds edges it also lives on the key vShard of its key: a live edge
//!   write carries each endpoint's surrogate from the collection home, and
//!   every replica of each endpoint home binds it on apply.
//! - An array cell lives on the vShard its tile key routes to.
//! - A node's labels and label versions live on the key vShard of the node,
//!   where every label write of the node routes.
//!
//! Every collection is collection-homed. Key-homed data is graph data, whose
//! partition key is the node id.

use std::collections::HashSet;

use nodedb_types::CollectionKey;

use super::VShardId;

/// A record, named by what decides where it lives.
#[derive(Debug, Clone, Copy)]
pub enum HomedRecord<'a> {
    /// A row of a collection.
    Row(CollectionKey<'a>),
    /// A graph edge between the node keys `src` and `dst`.
    Edge { src: &'a str, dst: &'a str },
    /// The PK→surrogate bind of `key` in `collection`. `holds_edges` is the
    /// collection's edge-bearing flag.
    Bind {
        collection: CollectionKey<'a>,
        key: &'a [u8],
        holds_edges: bool,
    },
    /// Array cells whose tile keys route to `vshard`.
    ArrayCells { vshard: VShardId },
    /// The labels and label versions of the graph node `node`.
    NodeLabels { node: &'a str },
}

/// The one or two vShards a record lives on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHomes {
    owner: VShardId,
    peer: Option<VShardId>,
}

impl RecordHomes {
    /// The homes of `record`.
    pub fn of(record: HomedRecord<'_>) -> Self {
        match record {
            HomedRecord::Row(key) => Self {
                owner: VShardId::from_collection(key),
                peer: None,
            },
            HomedRecord::Edge { src, dst } => Self::edge(src, dst),
            HomedRecord::ArrayCells { vshard } => Self {
                owner: vshard,
                peer: None,
            },
            HomedRecord::NodeLabels { node } => Self {
                owner: VShardId::from_key(node.as_bytes()),
                peer: None,
            },
            HomedRecord::Bind {
                collection,
                key,
                holds_edges,
            } => {
                let owner = VShardId::from_collection(collection);
                let key_home = VShardId::from_key(key);
                Self {
                    owner,
                    peer: (holds_edges && key_home != owner).then_some(key_home),
                }
            }
        }
    }

    /// The homes of the edge `src -> dst`.
    pub fn edge(src: &str, dst: &str) -> Self {
        let owner = Self::edge_owner(src);
        let dst_home = VShardId::from_key(dst.as_bytes());
        Self {
            owner,
            peer: (dst_home != owner).then_some(dst_home),
        }
    }

    /// The owner home of every edge whose source endpoint is `src`.
    pub fn edge_owner(src: &str) -> VShardId {
        VShardId::from_key(src.as_bytes())
    }

    /// The home that counts the record once cluster-wide: a row's collection
    /// home, or an edge's source-endpoint home.
    pub fn owner(self) -> VShardId {
        self.owner
    }

    /// The other home: an edge's destination-endpoint home, or the owner when
    /// the record has one home.
    pub fn second(self) -> VShardId {
        self.peer.unwrap_or(self.owner)
    }

    /// Whether the record lives on one vShard.
    pub fn is_single(self) -> bool {
        self.peer.is_none()
    }

    /// Every distinct home, owner first.
    pub fn iter(self) -> impl Iterator<Item = VShardId> {
        std::iter::once(self.owner).chain(self.peer)
    }

    /// The homes a write of the record goes to. With `dual_home` false, the
    /// owner takes the whole write: it stores both edge directions itself.
    pub fn write_homes(self, dual_home: bool) -> impl Iterator<Item = VShardId> {
        std::iter::once(self.owner).chain(self.peer.filter(|_| dual_home))
    }

    /// Whether any home is in `vshards`.
    pub fn intersects(self, vshards: &HashSet<u32>) -> bool {
        self.iter().any(|home| vshards.contains(&home.as_u32()))
    }

    /// Whether the owner is in `vshards`.
    pub fn owned_by(self, vshards: &HashSet<u32>) -> bool {
        vshards.contains(&self.owner.as_u32())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::DatabaseId;

    /// Two node keys whose key vShards differ.
    fn cross_shard_pair() -> (String, String) {
        let src = "n0".to_string();
        let home = VShardId::from_key(src.as_bytes());
        let dst = (1..4096)
            .map(|i| format!("n{i}"))
            .find(|k| VShardId::from_key(k.as_bytes()) != home)
            .expect("a key on another vShard");
        (src, dst)
    }

    #[test]
    fn a_row_lives_on_its_collection_home() {
        let key = CollectionKey::from_bare(DatabaseId::new(1025), "orders");
        let homes = RecordHomes::of(HomedRecord::Row(key));
        assert_eq!(homes.owner(), key.vshard());
        assert!(homes.is_single());
        assert_eq!(homes.second(), key.vshard());
        assert_eq!(homes.iter().collect::<Vec<_>>(), vec![key.vshard()]);
    }

    #[test]
    fn a_cross_shard_edge_lives_on_both_endpoint_homes() {
        let (src, dst) = cross_shard_pair();
        let src_home = VShardId::from_key(src.as_bytes());
        let dst_home = VShardId::from_key(dst.as_bytes());

        let homes = RecordHomes::of(HomedRecord::Edge {
            src: &src,
            dst: &dst,
        });
        assert_eq!(homes, RecordHomes::edge(&src, &dst));
        assert_eq!(homes.owner(), src_home);
        assert_eq!(homes.second(), dst_home);
        assert!(!homes.is_single());
        assert_eq!(homes.iter().collect::<Vec<_>>(), vec![src_home, dst_home]);
    }

    #[test]
    fn a_same_shard_edge_lives_on_one_home() {
        let homes = RecordHomes::edge("a", "a");
        assert!(homes.is_single());
        assert_eq!(homes.iter().count(), 1);
        assert_eq!(homes.write_homes(true).count(), 1);
    }

    #[test]
    fn write_homes_drop_the_peer_without_dual_homing() {
        let (src, dst) = cross_shard_pair();
        let homes = RecordHomes::edge(&src, &dst);
        assert_eq!(homes.write_homes(true).count(), 2);
        assert_eq!(
            homes.write_homes(false).collect::<Vec<_>>(),
            vec![homes.owner()]
        );
    }

    #[test]
    fn a_bind_in_an_edge_collection_also_lives_on_its_key_home() {
        let collection = CollectionKey::from_bare(DatabaseId::DEFAULT, "follows");
        let key = (0..4096)
            .map(|i| format!("n{i}"))
            .find(|k| VShardId::from_key(k.as_bytes()) != collection.vshard())
            .expect("a key off the collection home");
        let bind = |holds_edges| {
            RecordHomes::of(HomedRecord::Bind {
                collection,
                key: key.as_bytes(),
                holds_edges,
            })
        };
        let edges = bind(true);
        assert_eq!(edges.owner(), collection.vshard());
        assert_eq!(edges.second(), VShardId::from_key(key.as_bytes()));
        assert_eq!(
            edges.second(),
            RecordHomes::edge(&key, "x").owner(),
            "the bind home is the home a live edge write binds the endpoint on"
        );
        let rows_only = bind(false);
        assert!(rows_only.is_single());
        assert_eq!(rows_only.owner(), collection.vshard());
    }

    #[test]
    fn node_labels_live_on_the_node_key_home() {
        let (node, other) = cross_shard_pair();
        let homes = RecordHomes::of(HomedRecord::NodeLabels { node: &node });
        assert!(homes.is_single());
        assert_eq!(homes.owner(), VShardId::from_key(node.as_bytes()));
        assert_eq!(
            homes.owner(),
            RecordHomes::edge(&node, &other).owner(),
            "a node's labels live where its outgoing edges are owned"
        );
        let home: HashSet<u32> = [homes.owner().as_u32()].into();
        assert!(homes.owned_by(&home));
        let elsewhere: HashSet<u32> = [VShardId::from_key(other.as_bytes()).as_u32()].into();
        assert!(!homes.owned_by(&elsewhere));
    }

    #[test]
    fn membership_checks_use_every_home_or_the_owner() {
        let (src, dst) = cross_shard_pair();
        let homes = RecordHomes::edge(&src, &dst);
        let dst_only: HashSet<u32> = [homes.second().as_u32()].into_iter().collect();
        let src_only: HashSet<u32> = [homes.owner().as_u32()].into_iter().collect();
        assert!(homes.intersects(&dst_only));
        assert!(!homes.owned_by(&dst_only));
        assert!(homes.intersects(&src_only));
        assert!(homes.owned_by(&src_only));
        assert!(!homes.intersects(&HashSet::new()));
    }
}
