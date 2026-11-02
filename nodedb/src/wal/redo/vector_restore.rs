// SPDX-License-Identifier: BUSL-1.1

//! Payload of one restored vector row: a `RecordType::VectorPut` sub-record
//! of a RESTORE batch's redo.
//!
//! A backup holds every node of a row of a vector index with its system-time
//! span: the live node, or every live node of a multi-vector document, and
//! every ended node a cut still reads. The sub-record carries the nodes of
//! one row, so every replica and every replay installs each at exactly its
//! span, and a search at any cut answers as the source answered.
//!
//! A RESTORE is a write in the Calvin sequence. The resolve reads the
//! destination's newest ordinal for the row and rewrites the record to the
//! exact nodes the install writes (`restore_key_plan`). The install first
//! ends the row's live nodes at `end_live_at`, so no two versions of the row
//! overlap. It reads nothing to choose, so every replica writes alike.
//!
//! The payload is the pair `("vector_restore", VectorRestoreRedo)`. The
//! `VectorPut` insert shapes are tuples that open with the collection name,
//! never with this discriminator followed by a map.

use serde::{Deserialize, Serialize};

/// The discriminator a restored vector row's payload opens with.
pub const VECTOR_RESTORE: &str = "vector_restore";

/// One ended node of a restored row, visible at every ordinal `S` with
/// `sys_from <= S < sys_to`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct VectorRestoreNode {
    pub vector: Vec<f32>,
    pub sys_from: i64,
    pub sys_to: i64,
}

/// The live nodes of a restored row: one vector, or every vector of a
/// multi-vector document.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct VectorRestoreLive {
    pub vectors: Vec<Vec<f32>>,
    /// The ordinal the live nodes start at.
    pub sys_from: i64,
}

/// Every node of one restored vector row.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct VectorRestoreRedo {
    /// The collection, as the destination Data Plane stores it.
    pub collection: String,
    /// The vector field. Empty for the collection's default index.
    pub field_name: String,
    /// The row's identity in the destination database. Every node installs
    /// under it.
    pub surrogate: u32,
    /// The index dimension.
    pub dim: u32,
    /// Whether the row is a multi-vector document.
    pub multi: bool,
    /// Every ended node to install, ascending by `sys_from`.
    pub ended: Vec<VectorRestoreNode>,
    /// The live nodes to install. `None` when the row installs no live node.
    pub live: Option<VectorRestoreLive>,
    /// The ordinal the destination's live nodes of the row end at before
    /// any node installs. `None` ends none.
    pub end_live_at: Option<i64>,
    /// Whether the collection is vector-primary: its rows publish change
    /// entries by their surrogate's identity. A vector index on a document
    /// collection publishes none: the document row does.
    pub vector_primary: bool,
}

impl VectorRestoreRedo {
    /// Encode the sub-record payload.
    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(&(VECTOR_RESTORE, self)).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal vector restore of '{}': {e}", self.collection),
        })
    }

    /// Decode a sub-record payload. `None` when it is no restored vector row.
    pub fn decode(payload: &[u8]) -> Option<Self> {
        let (disc, redo) = zerompk::from_msgpack::<(String, Self)>(payload).ok()?;
        (disc == VECTOR_RESTORE).then_some(redo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> VectorRestoreRedo {
        VectorRestoreRedo {
            collection: "docs".into(),
            field_name: "emb".into(),
            surrogate: 3,
            dim: 2,
            multi: false,
            ended: vec![VectorRestoreNode {
                vector: vec![1.0, 0.0],
                sys_from: 10,
                sys_to: 20,
            }],
            live: Some(VectorRestoreLive {
                vectors: vec![vec![0.0, 1.0]],
                sys_from: 20,
            }),
            end_live_at: Some(10),
            vector_primary: true,
        }
    }

    #[test]
    fn a_restored_row_round_trips() {
        let bytes = sample().encode().expect("encode");
        assert_eq!(VectorRestoreRedo::decode(&bytes), Some(sample()));
    }

    #[test]
    fn a_vector_put_tuple_is_no_restored_row() {
        let put = zerompk::to_msgpack_vec(&(
            "docs".to_string(),
            vec![1.0f32, 0.0],
            2usize,
            "emb".to_string(),
            3u32,
            Option::<u8>::None,
        ))
        .expect("encode");
        assert!(VectorRestoreRedo::decode(&put).is_none());
    }
}
