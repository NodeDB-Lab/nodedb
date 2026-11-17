// SPDX-License-Identifier: Apache-2.0

//! The deep row a scalar field write leaves, computed from a row value alone.
//!
//! `Upsert` and `SetFields` write scalar fields only. Neither creates nor
//! removes a nested container, so a row's container keys hold across a run
//! of such writes. A caller that predicts several writes to one row before
//! any of them runs folds each prediction into the base of the next one
//! with [`ScalarRowBase::after_write`].

use std::collections::{HashMap, HashSet};

use loro::{LoroMap, LoroValue, ValueOrContainer};

use crate::error::{CrdtError, Result};

use super::core::CrdtState;
use super::row_write::map_entries;

/// A row as a scalar field write reads it: its deep entries and the keys
/// that hold a nested container.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ScalarRowBase {
    entries: HashMap<String, LoroValue>,
    containers: HashSet<String>,
}

impl ScalarRowBase {
    /// The base the next write reads once a write over `prior` left
    /// `written`. A scalar write keeps every container key of `prior`. An
    /// absent `prior` is a new row, which holds no container.
    pub fn after_write(prior: Option<&Self>, written: &LoroValue) -> Self {
        Self {
            entries: map_entries(written.clone()),
            containers: prior
                .map(|base| base.containers.clone())
                .unwrap_or_default(),
        }
    }

    /// The deep row a write of `fields` to row `row_id` of `collection`
    /// leaves over `base`, with the error the write raises.
    ///
    /// With `replace` (`Upsert`) a scalar key the write does not name goes
    /// and a container-valued key stays. Without it (`SetFields`) every other
    /// key stays. An absent `base` is a new row. A field that names a
    /// container-valued key is refused.
    pub fn predict(
        base: Option<&Self>,
        collection: &str,
        row_id: &str,
        fields: &[(&str, LoroValue)],
        replace: bool,
    ) -> Result<LoroValue> {
        let empty = Self::default();
        let base = base.unwrap_or(&empty);
        if let Some((field, _)) = fields
            .iter()
            .find(|(field, _)| base.containers.contains(*field))
        {
            return Err(CrdtError::ScalarFieldShadowsContainer {
                collection: collection.to_string(),
                row_id: row_id.to_string(),
                field: (*field).to_string(),
            });
        }
        let mut row = base.entries.clone();
        if replace {
            row.retain(|key, _| base.containers.contains(key));
        }
        for (field, value) in fields {
            row.insert((*field).to_string(), value.clone());
        }
        Ok(LoroValue::Map(row.into()))
    }
}

/// The keys of `row` that hold a nested container.
fn container_keys(row: &LoroMap) -> HashSet<String> {
    row.keys()
        .filter(|key| matches!(row.get(key), Some(ValueOrContainer::Container(_))))
        .map(|key| key.to_string())
        .collect()
}

impl CrdtState {
    /// The base a scalar write of row `row_id` of `collection` reads. `None`
    /// when the row is absent or not a map: the write makes a new row.
    pub fn scalar_row_base(&self, collection: &str, row_id: &str) -> Option<ScalarRowBase> {
        match self.doc.get_map(collection).get(row_id) {
            Some(ValueOrContainer::Container(loro::Container::Map(map))) => Some(ScalarRowBase {
                entries: map_entries(map.get_deep_value()),
                containers: container_keys(&map),
            }),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::RowWrite;

    /// A row with a scalar `title` and a block list under `blocks`.
    fn state() -> CrdtState {
        let state = CrdtState::new(7).expect("state");
        state
            .upsert("pages", "p1", &[("title", LoroValue::String("t".into()))])
            .expect("row");
        state
            .list_insert_fields(
                "pages",
                "p1",
                "blocks",
                0,
                &[("id".to_string(), LoroValue::String("b0".into()))],
            )
            .expect("block");
        state
    }

    /// Writes predicted in a run from the folded base leave the row the
    /// same writes leave when each runs in turn.
    #[test]
    fn a_folded_run_of_writes_predicts_the_row_the_run_leaves() {
        let set = [("n", LoroValue::I64(1))];
        let upsert = [("title", LoroValue::String("u".into()))];
        let run = [RowWrite::SetFields(&set), RowWrite::Upsert(&upsert)];

        let state = state();
        let mut base = state.scalar_row_base("pages", "p1");
        let mut predicted = None;
        for write in &run {
            let (fields, replace) = match *write {
                RowWrite::SetFields(fields) => (fields, false),
                RowWrite::Upsert(fields) => (fields, true),
                _ => unreachable!("the run holds scalar writes only"),
            };
            let row = ScalarRowBase::predict(base.as_ref(), "pages", "p1", fields, replace)
                .expect("predict");
            base = Some(ScalarRowBase::after_write(base.as_ref(), &row));
            predicted = Some(row);
        }
        for write in &run {
            state.apply_row_write("pages", "p1", write).expect("write");
        }
        assert_eq!(predicted, state.read_row_deep("pages", "p1"));
    }

    #[test]
    fn a_field_naming_a_container_is_refused() {
        let base = state().scalar_row_base("pages", "p1");
        let fields = [("blocks", LoroValue::I64(1))];
        assert!(matches!(
            ScalarRowBase::predict(base.as_ref(), "pages", "p1", &fields, false),
            Err(CrdtError::ScalarFieldShadowsContainer { .. })
        ));
    }

    #[test]
    fn an_absent_row_has_no_base() {
        assert!(state().scalar_row_base("pages", "absent").is_none());
        let fields = [("a", LoroValue::I64(1))];
        let row = ScalarRowBase::predict(None, "pages", "absent", &fields, true).expect("new row");
        assert_eq!(map_entries(row).get("a"), Some(&LoroValue::I64(1)));
    }
}
