// SPDX-License-Identifier: BUSL-1.1

//! Parameter and delta types for the vector side-effects of `apply_point_put`.

/// Inputs to `apply_point_put_vector_indexes` for one document write.
///
/// `wal_lsn` is the WAL LSN of the document write driving this indexing (`0`
/// when unassigned); it advances each touched collection's checkpoint watermark
/// and, on replay, gates a record the collection's checkpoint already absorbed.
pub(in crate::data::executor) struct VectorIndexPutParams<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub storage_key: crate::engine::document::store::StorageKey,
    pub value: &'a [u8],
    pub wal_lsn: u64,
    /// The system-time ordinal of the row version this write stores. The
    /// row's prior vector nodes end there and its new ones start there.
    pub sys_from: i64,
    /// Whether the write replaces a stored row. Only then does a vector field
    /// the body lacks end.
    pub replaces_row: bool,
}

/// How a removal found the node, or nodes, it ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::data::executor) enum VectorRemoval {
    /// The `vector_doc_map` entry of the document's field named the node.
    /// `node_deleted` is `false` when the node was dead already and only the
    /// map entry went.
    Recorded { node_deleted: bool },
    /// The node bound to the row's surrogate, with no `vector_doc_map` entry:
    /// a node a direct vector write bound, or a document node whose map entry
    /// a restart or a snapshot install did not carry.
    Bound,
    /// The multi-vector document of the row's surrogate: its `members`, and
    /// the ids among them the removal ended.
    MultiVector { members: Vec<u32>, ended: Vec<u32> },
}

/// Capture of one vector index removal, carrying everything needed to key
/// the `VectorCollection` (`index_key`, `vector_id`) AND reverse the paired
/// `vector_doc_map` entry or multi-vector membership on rollback
/// (`collection`, `field`, `doc_id`, `removal`).
#[derive(Clone)]
pub(in crate::data::executor) struct VectorIndexDelta {
    pub index_key: (nodedb_types::DatabaseId, crate::types::TenantId, String),
    /// The ended node. The first ended member for a multi-vector document.
    pub vector_id: u32,
    pub collection: String,
    pub field: String,
    pub doc_id: crate::engine::document::store::StorageKey,
    pub removal: VectorRemoval,
}

impl VectorIndexDelta {
    /// The undo entry that reverses this removal.
    pub(in crate::data::executor) fn into_delete_undo(
        self,
    ) -> crate::data::executor::handlers::transaction::undo::UndoEntry {
        use crate::data::executor::handlers::transaction::undo::UndoEntry;
        match self.removal {
            VectorRemoval::Recorded { node_deleted } => UndoEntry::DeleteVector {
                index_key: self.index_key,
                vector_id: self.vector_id,
                collection: self.collection,
                field: self.field,
                doc_id: Some(self.doc_id),
                node_deleted,
            },
            VectorRemoval::Bound => UndoEntry::DisplacedVector {
                index_key: self.index_key,
                vector_id: self.vector_id,
                surrogate: self.doc_id.surrogate(),
            },
            VectorRemoval::MultiVector { members, ended } => UndoEntry::EndedMultiVector {
                index_key: self.index_key,
                document: self.doc_id.surrogate(),
                members,
                ended,
            },
        }
    }
}

/// Inputs to `remove_then_insert_vector_field`, the shared per-field
/// remove-before-insert tail of `apply_point_put_vector_indexes`'s strict and
/// schemaless arms, once each has resolved its own `index_key` and extracted
/// `floats` for `field_name`. `storage_key` carries the row's surrogate: call
/// `storage_key.surrogate()` rather than threading a second surrogate field.
pub(super) struct VectorFieldInsert<'a> {
    pub(super) database_id: u64,
    pub(super) tid: u64,
    pub(super) index_key: (nodedb_types::DatabaseId, crate::types::TenantId, String),
    pub(super) collection: &'a str,
    pub(super) field_name: &'a str,
    pub(super) storage_key: crate::engine::document::store::StorageKey,
    pub(super) floats: Vec<f32>,
    /// The row version's system-time ordinal (see `VectorIndexPutParams`).
    pub(super) sys_from: i64,
}
