// SPDX-License-Identifier: Apache-2.0

//! The typed-value check a write applies to a row.
//!
//! The check reads the row deep (`read_row_deep`, in `materialize.rs`), so a
//! value inside a block list or a nested map is checked too.

use crate::error::Result;
use crate::loro_value::loro_to_value;

use super::core::CrdtState;

impl CrdtState {
    /// Refuse the row `row_id` of `collection` when any value in it, at any
    /// depth, does not decode through [`crate::loro_value`]: a Loro binary
    /// that is not a tagged value. An absent row holds no value to refuse.
    pub fn check_row_values(&self, collection: &str, row_id: &str) -> Result<()> {
        match self.read_row_deep(collection, row_id) {
            Some(row) => loro_to_value(&row).map(|_| ()),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;

    use super::*;
    use crate::error::CrdtError;

    #[test]
    fn a_deep_read_resolves_the_block_list() {
        let state = CrdtState::new(1).expect("state");
        state
            .upsert("pages", "p1", &[("title", LoroValue::String("t".into()))])
            .expect("row");
        state
            .list_insert_fields(
                "pages",
                "p1",
                "blocks",
                0,
                &[("text".to_string(), LoroValue::String("a".into()))],
            )
            .expect("block");
        let Some(LoroValue::Map(row)) = state.read_row_deep("pages", "p1") else {
            panic!("the row reads as a map");
        };
        let Some(LoroValue::List(blocks)) = row.get("blocks") else {
            panic!("the block list resolves to a list");
        };
        assert_eq!(blocks.len(), 1);
    }

    #[test]
    fn an_undecodable_binary_is_refused() {
        let state = CrdtState::new(1).expect("state");
        state
            .upsert(
                "pages",
                "p1",
                &[("raw", LoroValue::Binary(vec![0xc1].into()))],
            )
            .expect("row");
        assert!(matches!(
            state.check_row_values("pages", "p1"),
            Err(CrdtError::TypedValueMalformed { .. })
        ));
        assert!(state.check_row_values("pages", "absent").is_ok());
    }
}
