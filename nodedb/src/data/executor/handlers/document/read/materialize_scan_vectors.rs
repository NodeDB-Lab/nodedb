// SPDX-License-Identifier: BUSL-1.1

//! The vector nodes a raw materialize scan carries with each row.
//!
//! A clone copy installs a source row whole: its stored body and its node in
//! every vector index of the collection. A document row's vector lives only
//! in the index, and a vector-primary row's vector lives only on its node,
//! so the scan reads each row's nodes as a read at the scan's cut sees them.

use nodedb_types::Surrogate;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::vector_direct_row::VectorIndexKey;
use crate::types::{CopiedVectorNodes, DatabaseId, TenantId};

/// One vector index of a collection, with its field.
pub(super) struct CollectionVectorIndex {
    pub key: VectorIndexKey,
    /// Empty for the collection's default index.
    pub field_name: String,
}

impl CoreLoop {
    /// Every vector index this core holds for `collection`, ordered by field.
    pub(super) fn collection_vector_indexes(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Vec<CollectionVectorIndex> {
        let database = DatabaseId::new(database_id);
        let tenant = TenantId::new(tid);
        let mut indexes: Vec<CollectionVectorIndex> = self
            .vector_collections
            .keys()
            .filter(|(db, t, _)| *db == database && *t == tenant)
            .filter_map(|key| {
                let field_name = match key.2.strip_prefix(collection)? {
                    "" => String::new(),
                    rest => rest.strip_prefix(':')?.to_string(),
                };
                Some(CollectionVectorIndex {
                    key: key.clone(),
                    field_name,
                })
            })
            .collect();
        indexes.sort_by(|a, b| a.field_name.cmp(&b.field_name));
        indexes
    }

    /// The encoded nodes of row `surrogate` in each of `indexes`, as a read
    /// at `system_as_of` sees them, or the live nodes when unset.
    pub(super) fn copied_vector_nodes(
        &self,
        indexes: &[CollectionVectorIndex],
        surrogate: Surrogate,
        system_as_of: Option<i64>,
    ) -> Result<Vec<u8>, ErrorCode> {
        let mut nodes = Vec::new();
        for index in indexes {
            let Some(coll) = self.vector_collections.get(&index.key) else {
                continue;
            };
            let exported = coll
                .export_row_nodes(surrogate, system_as_of)
                .map_err(|e| ErrorCode::from(crate::Error::from(e)))?;
            if let Some(row) = exported {
                nodes.push(CopiedVectorNodes {
                    field_name: index.field_name.clone(),
                    vectors: row.vectors,
                    sys_from: row.sys_from,
                    multi: row.multi,
                });
            }
        }
        CopiedVectorNodes::encode(&nodes).map_err(ErrorCode::from)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_vector::collection::VectorCollection;
    use nodedb_vector::hnsw::HnswParams;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    /// A row carries its node in each index of its collection, and an
    /// index of another collection whose name it prefixes is no index of
    /// it.
    #[test]
    fn a_row_carries_its_node_in_each_index_of_its_collection() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let row = Surrogate::new(5);
        for (collection, field, vector) in [
            ("docs", "emb", vec![1.0, 0.0]),
            ("docs", "", vec![0.0, 1.0]),
            ("docs2", "emb", vec![0.5, 0.5]),
        ] {
            let mut coll = VectorCollection::new(2, HnswParams::default());
            coll.insert_at(vector, row, 10).expect("insert");
            core.vector_collections
                .insert(CoreLoop::vector_index_key(0, 1, collection, field), coll);
        }
        let indexes = core.collection_vector_indexes(0, 1, "docs");
        let fields: Vec<&str> = indexes.iter().map(|i| i.field_name.as_str()).collect();
        assert_eq!(fields, vec!["", "emb"]);

        let bytes = core
            .copied_vector_nodes(&indexes, row, None)
            .expect("export");
        let nodes = CopiedVectorNodes::decode(&bytes).expect("decode");
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[1].field_name, "emb");
        assert_eq!(nodes[1].vectors, vec![vec![1.0, 0.0]]);
        assert_eq!(nodes[1].sys_from, 10);
        assert!(
            core.copied_vector_nodes(&indexes, row, Some(5))
                .expect("export")
                .is_empty(),
            "a cut before the row's node carries none"
        );
    }
}
