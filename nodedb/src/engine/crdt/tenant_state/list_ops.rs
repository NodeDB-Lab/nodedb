// SPDX-License-Identifier: BUSL-1.1

//! Safe per-collection block-list operations.
//!
//! These methods intentionally delegate through `CrdtState` rather than
//! returning Loro's raw document handle. That keeps auto-commit state owned by
//! the Data Plane engine and prevents callers from racing delta preview's
//! pending-transaction check with an out-of-band document mutation.

use loro::LoroValue;

use super::TenantCrdtEngine;
use super::core::loro_root;

impl TenantCrdtEngine {
    /// Insert a populated block map into a row-owned movable list.
    pub fn list_insert_fields(
        &mut self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        index: usize,
        fields: &[(String, LoroValue)],
    ) -> crate::Result<()> {
        self.state_mut(collection)?
            .list_insert_fields(loro_root(collection), row_id, list_path, index, fields)
            .map_err(crate::Error::Crdt)
    }

    /// Delete one block from a row-owned movable list.
    pub fn list_delete(
        &mut self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        index: usize,
    ) -> crate::Result<()> {
        self.state_mut(collection)?
            .list_delete(loro_root(collection), row_id, list_path, index)
            .map_err(crate::Error::Crdt)
    }

    /// Move one block within a row-owned movable list.
    pub fn list_move(
        &mut self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        from_index: usize,
        to_index: usize,
    ) -> crate::Result<()> {
        self.state_mut(collection)?
            .list_move(
                loro_root(collection),
                row_id,
                list_path,
                from_index,
                to_index,
            )
            .map_err(crate::Error::Crdt)
    }

    /// Return one row-owned movable list's length without exposing its raw doc.
    pub fn list_length(
        &mut self,
        collection: &str,
        row_id: &str,
        list_path: &str,
    ) -> crate::Result<usize> {
        self.state_mut(collection)?
            .list_length(loro_root(collection), row_id, list_path)
            .map_err(crate::Error::Crdt)
    }

    /// Read one row-owned movable-list value without exposing its raw doc.
    pub fn list_get(
        &mut self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        index: usize,
    ) -> crate::Result<Option<LoroValue>> {
        self.state_mut(collection)?
            .list_get(loro_root(collection), row_id, list_path, index)
            .map_err(crate::Error::Crdt)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_crdt::constraint::ConstraintSet;

    use crate::types::TenantId;

    use super::*;

    #[test]
    fn list_operations_stay_behind_engine_methods() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
        engine
            .doc_upsert(
                "pages",
                "one",
                &[("title", LoroValue::String("draft".into()))],
            )
            .unwrap();
        engine
            .list_insert_fields(
                "pages",
                "one",
                "blocks",
                0,
                &[("id".into(), LoroValue::String("block-0".into()))],
            )
            .unwrap();
        engine
            .list_insert_fields(
                "pages",
                "one",
                "blocks",
                1,
                &[("id".into(), LoroValue::String("block-1".into()))],
            )
            .unwrap();
        engine.list_move("pages", "one", "blocks", 1, 0).unwrap();

        assert_eq!(engine.list_length("pages", "one", "blocks").unwrap(), 2);
        let Some(LoroValue::Map(first)) = engine.list_get("pages", "one", "blocks", 0).unwrap()
        else {
            panic!("first list value must be a block map");
        };
        assert_eq!(first.get("id"), Some(&LoroValue::String("block-1".into())));
        engine.list_delete("pages", "one", "blocks", 0).unwrap();
        assert_eq!(engine.list_length("pages", "one", "blocks").unwrap(), 1);
    }
}
