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
    /// A container-valued key is never deleted, because deleting it discards
    /// its nested CRDT state (e.g. a row's block list).
    pub fn remove_fields(&self, collection: &str, row_id: &str, fields: &[&str]) -> Result<usize> {
        let coll = self.doc.get_map(collection);
        let row: LoroMap = match coll.get(row_id) {
            Some(ValueOrContainer::Container(loro::Container::Map(m))) => m,
            _ => return Ok(0),
        };
        let mut removed = 0;
        for field in fields {
            if matches!(row.get(field), Some(ValueOrContainer::Value(_))) {
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
}
