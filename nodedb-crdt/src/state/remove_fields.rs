// SPDX-License-Identifier: Apache-2.0

//! Removing named scalar fields from one row, leaving every other key intact.

use loro::{LoroMap, ValueOrContainer};

use crate::error::{CrdtError, Result};

use super::core::CrdtState;

impl CrdtState {
    /// Delete the named scalar `fields` from a row. Returns how many were
    /// present and deleted.
    ///
    /// The inverse of `set_fields`: untouched keys keep their values. An
    /// absent row or an absent field is a no-op and authors no operation.
    ///
    /// A container-valued key is refused with `ScalarFieldShadowsContainer`,
    /// as `set_fields` refuses it: deleting it discards its nested CRDT state
    /// (e.g. a row's block list). Every field is checked before any is
    /// deleted, so a refused call deletes nothing.
    pub fn remove_fields(&self, collection: &str, row_id: &str, fields: &[&str]) -> Result<usize> {
        let coll = self.doc.get_map(collection);
        let row: LoroMap = match coll.get(row_id) {
            Some(ValueOrContainer::Container(loro::Container::Map(m))) => m,
            _ => return Ok(0),
        };
        if let Some(field) = fields
            .iter()
            .find(|field| matches!(row.get(field), Some(ValueOrContainer::Container(_))))
        {
            return Err(CrdtError::ScalarFieldShadowsContainer {
                collection: collection.to_string(),
                row_id: row_id.to_string(),
                field: (*field).to_string(),
            });
        }
        let mut removed = 0;
        for field in fields {
            if row.get(field).is_some() {
                row.delete(field)
                    .map_err(|e| CrdtError::Loro(e.to_string()))?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;

    use super::*;

    #[test]
    fn remove_fields_keeps_untouched_fields() {
        let state = CrdtState::new(1).expect("state");
        state
            .upsert(
                "c",
                "r",
                &[("a", LoroValue::I64(1)), ("b", LoroValue::I64(2))],
            )
            .expect("upsert");
        assert_eq!(state.remove_fields("c", "r", &["a"]).expect("remove"), 1);
        assert_eq!(state.read_field("c", "r", "a"), None);
        assert_eq!(state.read_field("c", "r", "b"), Some(LoroValue::I64(2)));
    }

    #[test]
    fn remove_fields_on_absent_row_authors_nothing() {
        let state = CrdtState::new(1).expect("state");
        let before = state.local_op_counter();
        assert_eq!(
            state.remove_fields("c", "missing", &["a"]).expect("remove"),
            0
        );
        assert_eq!(state.local_op_counter(), before);
        assert!(!state.row_exists("c", "missing"));
    }

    #[test]
    fn remove_fields_refuses_a_container_key_and_deletes_nothing() {
        let state = CrdtState::new(1).expect("state");
        state
            .upsert("c", "r", &[("a", LoroValue::I64(1))])
            .expect("upsert");
        let row = match state.doc.get_map("c").get("r") {
            Some(ValueOrContainer::Container(loro::Container::Map(m))) => m,
            other => panic!("expected a row map, got {other:?}"),
        };
        row.insert_container("blocks", loro::LoroList::new())
            .expect("nested list");
        match state.remove_fields("c", "r", &["a", "blocks"]) {
            Err(CrdtError::ScalarFieldShadowsContainer { field, .. }) => {
                assert_eq!(field, "blocks");
            }
            other => panic!("expected ScalarFieldShadowsContainer, got {other:?}"),
        }
        assert_eq!(state.read_field("c", "r", "a"), Some(LoroValue::I64(1)));
    }

    #[test]
    fn remove_fields_counts_only_present_fields() {
        let state = CrdtState::new(1).expect("state");
        state
            .upsert(
                "c",
                "r",
                &[("a", LoroValue::I64(1)), ("b", LoroValue::Null)],
            )
            .expect("upsert");
        assert_eq!(
            state
                .remove_fields("c", "r", &["a", "b", "missing", "a"])
                .expect("remove"),
            2
        );
        assert_eq!(state.read_field("c", "r", "a"), None);
        assert_eq!(state.read_field("c", "r", "b"), None);
    }

    #[test]
    fn remove_fields_with_only_absent_fields_authors_nothing() {
        let state = CrdtState::new(1).expect("state");
        state
            .upsert("c", "r", &[("a", LoroValue::I64(1))])
            .expect("upsert");
        let before = state.local_op_counter();
        assert_eq!(state.remove_fields("c", "r", &["zz"]).expect("remove"), 0);
        assert_eq!(state.local_op_counter(), before);
    }
}
