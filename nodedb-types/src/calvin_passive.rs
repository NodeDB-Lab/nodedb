// SPDX-License-Identifier: Apache-2.0

//! Identity of one row a passive Calvin participant reads.
//!
//! A dependent-read transaction names the rows its passive vShards read. Each
//! passive vShard reads them at its lock grant and broadcasts one value per
//! row through the data-group log of every active vShard. The active vShard
//! matches each value to its row by [`PassiveReadKeyId`].
//!
//! The identity is exact: a key-value row keeps its key bytes, so two keys
//! never share an identity.

use serde::{Deserialize, Serialize};

use crate::QualifiedCollection;

/// The row within a collection a passive read names.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum PassiveKey {
    /// A document row, by its surrogate.
    Surrogate { surrogate: u32 },
    /// A key-value row, by its key bytes.
    Kv { key: Vec<u8> },
}

/// Identity of one row a passive participant reads.
///
/// The map key of every passive read value. `Ord` is derived, collection
/// first, so every replica iterates a map of them in one order.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PassiveReadKeyId {
    /// The database-qualified collection of the row.
    pub collection: QualifiedCollection,
    /// The row within the collection.
    pub key: PassiveKey,
}

impl PassiveReadKeyId {
    /// The identity of document row `surrogate` of `collection`.
    pub fn surrogate(collection: QualifiedCollection, surrogate: u32) -> Self {
        Self {
            collection,
            key: PassiveKey::Surrogate { surrogate },
        }
    }

    /// The identity of key-value row `key` of `collection`.
    pub fn kv(collection: QualifiedCollection, key: Vec<u8>) -> Self {
        Self {
            collection,
            key: PassiveKey::Kv { key },
        }
    }

    /// Bytes this identity adds to a replicated entry, estimated.
    pub fn serialized_size_hint(&self) -> usize {
        let key = match &self.key {
            PassiveKey::Surrogate { .. } => 4,
            PassiveKey::Kv { key } => key.len(),
        };
        self.collection.as_str().len() + key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two key-value rows keep two identities, whatever their keys hash to.
    #[test]
    fn kv_rows_keep_distinct_identities() {
        let collection = QualifiedCollection::from_stored("items".to_owned());
        let a = PassiveReadKeyId::kv(collection.clone(), b"alice:sword".to_vec());
        let b = PassiveReadKeyId::kv(collection, b"alice:shield".to_vec());
        assert_ne!(a, b);
    }

    /// An identity survives a msgpack round trip.
    #[test]
    fn identity_round_trips_through_msgpack() {
        let id = PassiveReadKeyId::surrogate(QualifiedCollection::from_stored("o".to_owned()), 9);
        let bytes = zerompk::to_msgpack_vec(&id).expect("encode");
        let back: PassiveReadKeyId = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, id);
    }
}
