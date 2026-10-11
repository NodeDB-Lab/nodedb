// SPDX-License-Identifier: BUSL-1.1

//! The accumulated Calvin write keys of a task slice.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_cluster::calvin::types::{EngineKeySet, SortedVec};

use super::super::shared::node_lock_pair;
use crate::types::{RecordHomes, VShardId};

/// The lock key of the row `document_id` names, bound or not: the bytes its
/// surrogate binds under (`bind_plan`).
///
/// Every point writer of a document row, Document or CRDT, and every
/// presence guard that names it, locks this key beside any surrogate key.
/// So a delete planned while the key was unbound waits for an insert that
/// binds it, and the scheduler's dispatch-time rebind of that delete runs
/// after the insert's binding landed, alike on every replica.
pub fn row_id_key(document_id: &str) -> Vec<u8> {
    document_id.as_bytes().to_vec()
}

/// The lock pairs and routing homes of one edge collection.
#[derive(Debug, Default)]
struct EdgeKeys {
    pairs: Vec<(u32, u32)>,
    homes: Vec<u32>,
}

/// Write keys by engine and collection. Every map is ordered, so the key
/// sets it yields are identical on every node that builds them.
#[derive(Debug, Default)]
pub struct WriteKeys {
    documents: BTreeMap<String, Vec<u32>>,
    vectors: BTreeMap<String, Vec<u32>>,
    kv: BTreeMap<String, Vec<Vec<u8>>>,
    edges: BTreeMap<String, EdgeKeys>,
    /// Array collection to the vShards its written cells live on.
    arrays: BTreeMap<String, Vec<u32>>,
    /// Collections a write covers whole.
    whole: BTreeSet<String>,
}

impl WriteKeys {
    /// Lock the document rows `surrogates` of `collection`. An empty list
    /// still enlists the collection's vShard.
    pub fn rows(&mut self, collection: &str, surrogates: impl IntoIterator<Item = u32>) {
        self.documents
            .entry(collection.to_owned())
            .or_default()
            .extend(surrogates);
    }

    /// Lock `collection` whole, exclusively: a write with no row identity,
    /// such as a predicate, bulk, or truncate write. It orders against every
    /// row writer and every reader of the collection.
    pub fn whole_collection(&mut self, collection: &str) {
        self.whole.insert(collection.to_owned());
    }

    /// Lock the vector rows `surrogates` of `collection`. An empty list
    /// still enlists the collection's vShard.
    pub fn vector_rows(&mut self, collection: &str, surrogates: impl IntoIterator<Item = u32>) {
        self.vectors
            .entry(collection.to_owned())
            .or_default()
            .extend(surrogates);
    }

    /// Lock the raw keys `keys` of `collection`. An empty list still enlists
    /// the collection's vShard.
    pub fn kv_keys(&mut self, collection: &str, keys: impl IntoIterator<Item = Vec<u8>>) {
        self.kv
            .entry(collection.to_owned())
            .or_default()
            .extend(keys);
    }

    /// Lock row `document_id` of `collection` by [`row_id_key`].
    pub fn row_id(&mut self, collection: &str, document_id: &str) {
        self.kv_keys(collection, [row_id_key(document_id)]);
    }

    /// Lock the edge `src_id -> dst_id` of `collection` by its surrogate
    /// pair, and both endpoints' node lock pairs, on both endpoint homes.
    pub fn edge(&mut self, collection: &str, src_id: &str, dst_id: &str, pair: (u32, u32)) {
        let keys = self.edges.entry(collection.to_owned()).or_default();
        keys.pairs
            .extend([pair, node_lock_pair(src_id), node_lock_pair(dst_id)]);
        keys.homes.extend(
            RecordHomes::edge(src_id, dst_id)
                .iter()
                .map(VShardId::as_u32),
        );
    }

    /// Lock node `node_id` of `collection` on the node's key home.
    pub fn node(&mut self, collection: &str, node_id: &str) {
        let keys = self.edges.entry(collection.to_owned()).or_default();
        keys.pairs.push(node_lock_pair(node_id));
        keys.homes
            .push(VShardId::from_key(node_id.as_bytes()).as_u32());
    }

    /// Enlist `vshard` for `collection` without locking a key.
    pub fn home(&mut self, collection: &str, vshard: u32) {
        self.edges
            .entry(collection.to_owned())
            .or_default()
            .homes
            .push(vshard);
    }

    /// Lock the whole array `collection` on `vshard`, the vShard the written
    /// cells' tiles live on, and enlist that vShard.
    pub fn array_cells(&mut self, collection: &str, vshard: u32) {
        self.arrays
            .entry(collection.to_owned())
            .or_default()
            .push(vshard);
    }

    /// Drop every document row key of `collection`. A dependent
    /// transaction replaces them with the rows its reconnaissance predicted.
    pub fn drop_documents(&mut self, collection: &str) {
        self.documents.remove(collection);
    }

    /// The key sets, one per engine and collection, ordered by collection.
    pub fn into_key_sets(self) -> Vec<EngineKeySet> {
        let mut sets: Vec<EngineKeySet> = Vec::new();
        sets.extend(
            self.documents
                .into_iter()
                .map(|(collection, rows)| EngineKeySet::Document {
                    collection,
                    surrogates: SortedVec::new(rows),
                }),
        );
        sets.extend(
            self.vectors
                .into_iter()
                .map(|(collection, rows)| EngineKeySet::Vector {
                    collection,
                    surrogates: SortedVec::new(rows),
                }),
        );
        sets.extend(
            self.kv
                .into_iter()
                .map(|(collection, keys)| EngineKeySet::Kv {
                    collection,
                    keys: SortedVec::new(keys),
                }),
        );
        sets.extend(
            self.edges
                .into_iter()
                .map(|(collection, keys)| EngineKeySet::Edge {
                    collection,
                    edges: SortedVec::new(keys.pairs),
                    home_vshards: SortedVec::new(keys.homes),
                }),
        );
        sets.extend(
            self.arrays
                .into_iter()
                .map(|(collection, vshards)| EngineKeySet::Array {
                    collection,
                    vshards: SortedVec::new(vshards),
                }),
        );
        sets.extend(
            self.whole
                .into_iter()
                .map(|collection| EngineKeySet::Collection {
                    collection,
                    vshards: SortedVec::new(Vec::new()),
                }),
        );
        sets.sort_by(|a, b| a.collection().cmp(b.collection()));
        sets
    }
}
