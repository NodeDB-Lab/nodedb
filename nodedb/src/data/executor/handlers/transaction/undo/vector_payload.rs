// SPDX-License-Identifier: BUSL-1.1

//! Undo of the payload bitmap change a restored vector-primary row's
//! install makes.
//!
//! The install takes the bitmap row of the row's live node before it and
//! adds the bitmap row of the row's live node after it. The undo removes the
//! added row and puts the taken row back. It runs before the undo of the
//! install's nodes, so every node id it names is still the node it indexed.
//! An index the rollback already removed holds no bitmap to change.

use std::collections::HashMap;

use nodedb_types::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::vector_direct_row::VectorIndexKey;

/// One payload bitmap row: a live node and the lower-cased sidecar fields
/// its entries index.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::data::executor) struct PayloadRow {
    pub node_id: u32,
    pub fields: HashMap<String, Value>,
}

/// The payload bitmap change of one restored row.
#[derive(Debug)]
pub(in crate::data::executor) struct VectorPayloadRowsUndo {
    pub index_key: VectorIndexKey,
    /// The row the install took out.
    pub removed: Option<PayloadRow>,
    /// The row the install added.
    pub inserted: Option<PayloadRow>,
}

impl CoreLoop {
    /// Reverse one restored row's payload bitmap change.
    pub(super) fn apply_undo_vector_payload_rows(&mut self, undo: VectorPayloadRowsUndo) {
        let VectorPayloadRowsUndo {
            index_key,
            removed,
            inserted,
        } = undo;
        let Some(coll) = self.vector_collections.get_mut(&index_key) else {
            return;
        };
        if let Some(row) = &inserted {
            coll.payload.delete_row(row.node_id, &row.fields);
        }
        if let Some(row) = &removed {
            coll.payload.insert_row(row.node_id, &row.fields);
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{PayloadIndexKind, Surrogate};
    use nodedb_vector::collection::{FilterPredicate, VectorCollection};
    use nodedb_vector::hnsw::HnswParams;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    fn tag(value: &str) -> HashMap<String, Value> {
        HashMap::from([("tag".to_string(), Value::String(value.into()))])
    }

    fn admitted(core: &CoreLoop, key: &VectorIndexKey, value: &str) -> Vec<u32> {
        core.vector_collections[key]
            .payload
            .pre_filter(&FilterPredicate::Eq {
                field: "tag".into(),
                value: Value::String(value.into()),
            })
            .expect("an indexed field")
            .iter()
            .collect()
    }

    /// The undo removes the added row and puts the taken row back.
    #[test]
    fn the_undo_restores_the_taken_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let key = CoreLoop::vector_index_key(0, 1, "vp", "emb");
        let mut coll = VectorCollection::new(2, HnswParams::default());
        coll.payload
            .add_index("tag".to_string(), PayloadIndexKind::Equality);
        let old = coll
            .insert_at(vec![1.0, 0.0], Surrogate::new(3), 10)
            .expect("old node");
        let new = coll
            .insert_at(vec![0.0, 1.0], Surrogate::new(3), 20)
            .expect("new node");
        coll.payload.insert_row(new, &tag("b"));
        core.vector_collections.insert(key.clone(), coll);

        core.apply_undo_vector_payload_rows(VectorPayloadRowsUndo {
            index_key: key.clone(),
            removed: Some(PayloadRow {
                node_id: old,
                fields: tag("a"),
            }),
            inserted: Some(PayloadRow {
                node_id: new,
                fields: tag("b"),
            }),
        });
        assert_eq!(admitted(&core, &key, "a"), vec![old]);
        assert!(admitted(&core, &key, "b").is_empty());
    }
}
