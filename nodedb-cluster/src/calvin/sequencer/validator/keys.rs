// SPDX-License-Identifier: BUSL-1.1

//! Flattened per-key view of a transaction's read and write sets, and the
//! one predicate that decides whether a read key and a write key conflict.

use std::collections::BTreeSet;

use crate::calvin::types::{EngineKeySet, ReadWriteSet};

/// Canonical, order-comparable identity of a single key across engines:
/// `(engine_discriminant, collection, key_bytes)`. Used as the `BTreeMap` key
/// for the reader/writer maps so all iteration feeding output is deterministic.
pub(super) type KeyId = (u8, String, Vec<u8>);

/// Discriminant of an array key: one array on one vShard.
const ARRAY: u8 = 4;

/// How much of its collection one key covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KeyScope {
    /// One row: a surrogate, a KV key, an edge pair or a UNIQUE value.
    Row,
    /// Every row: a whole-collection key set, or an array on one vShard.
    Collection,
}

/// A single flattened key drawn from one transaction's read or write set.
#[derive(Debug)]
pub(super) struct FlatKey {
    /// Discriminant tag for the key-set variant (Document=0, Vector=1, Kv=2,
    /// Edge=3, Array=4, Collection=5, Unique=6).
    pub(super) discriminant: u8,
    /// Static engine name used when building conflict-context keys.
    pub(super) engine_name: &'static str,
    /// Collection name.
    pub(super) collection: String,
    /// Serialized key bytes.
    pub(super) key_bytes: Vec<u8>,
    /// How much of the collection the key covers.
    pub(super) scope: KeyScope,
}

impl FlatKey {
    /// The order-comparable identity of this key.
    pub(super) fn key_id(&self) -> KeyId {
        (
            self.discriminant,
            self.collection.clone(),
            self.key_bytes.clone(),
        )
    }
}

/// Whether a reader of `read` and a writer of `write` conflict, so the
/// reader must take the lower position.
///
/// Mirrors the scheduler's lock compatibility:
/// - One key conflicts with itself.
/// - A collection-scope key conflicts with every key of its collection. A row
///   key announces itself on its collection, and a whole-collection key
///   excludes every row announcement.
/// - Two array keys of one collection on distinct vShards are disjoint: each
///   vShard's lock table holds its own part of the array.
/// - Two distinct row keys never conflict.
pub(super) fn conflicts(read: &FlatKey, write: &FlatKey) -> bool {
    if read.collection != write.collection {
        return false;
    }
    if read.discriminant == write.discriminant && read.key_bytes == write.key_bytes {
        return true;
    }
    match (read.scope, write.scope) {
        (KeyScope::Row, KeyScope::Row) => false,
        (KeyScope::Collection, KeyScope::Collection)
            if read.discriminant == ARRAY && write.discriminant == ARRAY =>
        {
            false
        }
        (KeyScope::Collection, KeyScope::Row | KeyScope::Collection)
        | (KeyScope::Row, KeyScope::Collection) => true,
    }
}

/// Flatten one read-or-write set into its constituent per-key entries.
pub(super) fn flatten_set(set: &ReadWriteSet) -> Vec<FlatKey> {
    let mut out = Vec::new();
    for key_set in &set.0 {
        match key_set {
            EngineKeySet::Document {
                collection,
                surrogates,
            } => {
                for &s in surrogates.as_slice() {
                    out.push(FlatKey {
                        discriminant: 0,
                        engine_name: "document",
                        collection: collection.clone(),
                        key_bytes: s.to_le_bytes().to_vec(),
                        scope: KeyScope::Row,
                    });
                }
            }
            EngineKeySet::Vector {
                collection,
                surrogates,
            } => {
                for &s in surrogates.as_slice() {
                    out.push(FlatKey {
                        discriminant: 1,
                        engine_name: "vector",
                        collection: collection.clone(),
                        key_bytes: s.to_le_bytes().to_vec(),
                        scope: KeyScope::Row,
                    });
                }
            }
            EngineKeySet::Kv { collection, keys } => {
                for k in keys.as_slice() {
                    out.push(FlatKey {
                        discriminant: 2,
                        engine_name: "kv",
                        collection: collection.clone(),
                        key_bytes: k.clone(),
                        scope: KeyScope::Row,
                    });
                }
            }
            // One key per distinct pair: a pair placed on two homes is one key.
            EngineKeySet::Edge {
                collection, edges, ..
            } => {
                let pairs: BTreeSet<(u32, u32)> = edges.iter().map(|p| p.pair).collect();
                for (src, dst) in pairs {
                    let mut key_bytes = src.to_le_bytes().to_vec();
                    key_bytes.extend_from_slice(&dst.to_le_bytes());
                    out.push(FlatKey {
                        discriminant: 3,
                        engine_name: "edge",
                        collection: collection.clone(),
                        key_bytes,
                        scope: KeyScope::Row,
                    });
                }
            }
            // The whole array on each vShard it names.
            EngineKeySet::Array {
                collection,
                vshards,
            } => {
                for &vshard in vshards.as_slice() {
                    out.push(FlatKey {
                        discriminant: ARRAY,
                        engine_name: "array",
                        collection: collection.clone(),
                        key_bytes: vshard.to_le_bytes().to_vec(),
                        scope: KeyScope::Collection,
                    });
                }
            }
            // The whole collection: one key with no row bytes.
            EngineKeySet::Collection { collection, .. } => {
                out.push(FlatKey {
                    discriminant: 5,
                    engine_name: "collection",
                    collection: collection.clone(),
                    key_bytes: Vec::new(),
                    scope: KeyScope::Collection,
                });
            }
            // One key per claimed value: the index name, a zero byte, the value.
            EngineKeySet::Unique {
                collection,
                index,
                values,
            } => {
                for value in values.as_slice() {
                    let mut key_bytes = Vec::with_capacity(index.len() + 1 + value.len());
                    key_bytes.extend_from_slice(index.as_bytes());
                    key_bytes.push(0);
                    key_bytes.extend_from_slice(value);
                    out.push(FlatKey {
                        discriminant: 6,
                        engine_name: "unique",
                        collection: collection.clone(),
                        key_bytes,
                        scope: KeyScope::Row,
                    });
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(discriminant: u8, collection: &str, bytes: &[u8], scope: KeyScope) -> FlatKey {
        FlatKey {
            discriminant,
            engine_name: "test",
            collection: collection.to_owned(),
            key_bytes: bytes.to_vec(),
            scope,
        }
    }

    fn row(collection: &str, surrogate: u32) -> FlatKey {
        key(0, collection, &surrogate.to_le_bytes(), KeyScope::Row)
    }

    fn whole(collection: &str) -> FlatKey {
        key(5, collection, &[], KeyScope::Collection)
    }

    fn array(collection: &str, vshard: u32) -> FlatKey {
        key(
            ARRAY,
            collection,
            &vshard.to_le_bytes(),
            KeyScope::Collection,
        )
    }

    #[test]
    fn a_key_conflicts_with_itself_only_among_rows() {
        assert!(conflicts(&row("c", 1), &row("c", 1)));
        assert!(!conflicts(&row("c", 1), &row("c", 2)));
        assert!(!conflicts(&row("c", 1), &row("d", 1)));
    }

    #[test]
    fn a_collection_scope_key_conflicts_with_every_key_of_its_collection() {
        assert!(conflicts(&whole("c"), &row("c", 1)));
        assert!(conflicts(&row("c", 1), &whole("c")));
        assert!(conflicts(&whole("c"), &whole("c")));
        assert!(conflicts(&array("c", 3), &row("c", 1)));
        assert!(conflicts(&row("c", 1), &array("c", 3)));
        assert!(conflicts(&whole("c"), &array("c", 3)));
        assert!(!conflicts(&whole("c"), &row("d", 1)));
        assert!(!conflicts(&row("d", 1), &whole("c")));
    }

    #[test]
    fn array_keys_on_distinct_vshards_are_disjoint() {
        assert!(conflicts(&array("c", 3), &array("c", 3)));
        assert!(!conflicts(&array("c", 3), &array("c", 4)));
    }
}
