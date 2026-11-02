// SPDX-License-Identifier: BUSL-1.1

//! Snapshot sections that carry the system-time history of the in-memory
//! indexes: dense vector spans, sparse vector indexes, and spatial R-trees.
//! A follower that installs them answers a read at any retained cut as the
//! source does.

/// The history of one dense vector index, keyed in the snapshot like its
/// `vectors` entry.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct VectorIndexHistory {
    /// `(exported node id, sys_from)` of every live node a cut can read
    /// below. The id names a row of the index's `vectors` entry. A live row
    /// not listed is visible at every cut.
    pub live_starts: Vec<(u32, i64)>,
    /// Every ended node a cut still reads.
    pub ended: Vec<nodedb_vector::collection::EndedNodeExport>,
}

/// One field index of one collection, as its engine encodes it for a
/// checkpoint: a sparse vector index or a spatial R-tree.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct FieldIndexImage {
    pub database_id: u64,
    pub tenant_id: u64,
    /// The collection as the Data Plane stores it.
    pub collection: String,
    pub field: String,
    /// The index's checkpoint bytes, history included.
    pub index: Vec<u8>,
    /// `(entry id, document id)` of every R-tree entry. Empty for a sparse
    /// vector index.
    pub doc_entries: Vec<(u64, String)>,
}

impl FieldIndexImage {
    /// The section key the image travels under: `"{db}:{tid}:{collection}"`,
    /// which routes it to its collection's core.
    pub fn section_key(&self) -> String {
        format!(
            "{}:{}:{}",
            self.database_id, self.tenant_id, self.collection
        )
    }
}
