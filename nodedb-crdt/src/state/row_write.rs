// SPDX-License-Identifier: Apache-2.0

//! One-row writes whose result is known before they run.
//!
//! [`CrdtState::predict_row`] computes the deep row a [`RowWrite`] leaves,
//! with the same errors the write raises, and changes nothing. It reads only
//! the row: its cost is the row's size, never the collection's. A caller
//! projects the predicted row first and runs the write with
//! [`CrdtState::apply_row_write`] only once the projection has landed. A
//! projection that fails leaves the document, its version vector and its
//! exported bytes exactly as they were.
//!
//! The prediction mirrors each write's semantics:
//!
//! - `Upsert` replaces the row's scalar fields: a scalar key the write does
//!   not name goes, a container-valued key stays. A row that is absent or not
//!   a map becomes a new map.
//! - `SetFields` writes the named scalar fields and keeps every other key.
//! - Both refuse a field that names a container-valued key.
//! - `ListInsert` needs the row. It creates missing intermediate maps and the
//!   list, and places the block at the smaller of `index` and the length.
//! - `ListDelete` and `ListMove` need the list, and an index inside it. A
//!   move onto its own index needs the row only, and changes nothing.

use std::collections::{HashMap, HashSet};

use loro::{LoroMap, LoroValue, ValueOrContainer};

use crate::error::{CrdtError, Result};
use crate::list_ops::{block_list_row, movable_list_at, path_unresolved, split_list_path};

use super::core::CrdtState;

/// A write to one row of a collection.
#[derive(Debug, Clone, Copy)]
pub enum RowWrite<'a> {
    /// Replace the row's scalar fields (`CrdtState::upsert`).
    Upsert(&'a [(&'a str, LoroValue)]),
    /// Write the named scalar fields (`CrdtState::set_fields`).
    SetFields(&'a [(&'a str, LoroValue)]),
    /// Insert a block of `fields` into the list at `list_path`.
    ListInsert {
        list_path: &'a str,
        index: usize,
        fields: &'a [(String, LoroValue)],
    },
    /// Delete the block at `index` of the list at `list_path`.
    ListDelete { list_path: &'a str, index: usize },
    /// Move the block at `from` to `to` in the list at `list_path`.
    ListMove {
        list_path: &'a str,
        from: usize,
        to: usize,
    },
}

impl RowWrite<'_> {
    /// Whether the write leaves its row exactly as it was: a move onto its
    /// own index. Such a write projects nothing and records no operation.
    pub fn changes_nothing(&self) -> bool {
        matches!(*self, RowWrite::ListMove { from, to, .. } if from == to)
    }
}

/// The entries of a map value, owned.
fn entries(value: LoroValue) -> HashMap<String, LoroValue> {
    match value {
        LoroValue::Map(map) => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        _ => HashMap::new(),
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
    /// The deep row `row_id` of `collection` after `write`, with the error
    /// `write` would raise. Changes nothing.
    pub fn predict_row(
        &self,
        collection: &str,
        row_id: &str,
        write: &RowWrite<'_>,
    ) -> Result<LoroValue> {
        match *write {
            RowWrite::Upsert(fields) => self.predict_scalars(collection, row_id, fields, true),
            RowWrite::SetFields(fields) => self.predict_scalars(collection, row_id, fields, false),
            RowWrite::ListInsert {
                list_path,
                index,
                fields,
            } => self.predict_list(collection, row_id, list_path, true, |list| {
                let block = fields.iter().cloned().collect::<HashMap<_, _>>();
                let at = index.min(list.len());
                list.insert(at, LoroValue::Map(block.into()));
                Ok(())
            }),
            RowWrite::ListDelete { list_path, index } => {
                self.predict_list(collection, row_id, list_path, false, |list| {
                    check_index(list_path, index, list.len())?;
                    list.remove(index);
                    Ok(())
                })
            }
            RowWrite::ListMove {
                list_path,
                from,
                to,
            } => {
                if from == to {
                    // The row must exist, as for any list write.
                    return Ok(block_list_row(&self.doc, collection, row_id)?.get_deep_value());
                }
                self.predict_list(collection, row_id, list_path, false, |list| {
                    for index in [from, to] {
                        check_index(list_path, index, list.len())?;
                    }
                    let block = list.remove(from);
                    list.insert(to, block);
                    Ok(())
                })
            }
        }
    }

    /// Run `write` on row `row_id` of `collection`. The write commits on
    /// return.
    pub fn apply_row_write(
        &self,
        collection: &str,
        row_id: &str,
        write: &RowWrite<'_>,
    ) -> Result<()> {
        match *write {
            RowWrite::Upsert(fields) => self.upsert(collection, row_id, fields),
            RowWrite::SetFields(fields) => self.set_fields(collection, row_id, fields),
            RowWrite::ListInsert {
                list_path,
                index,
                fields,
            } => self.list_insert_fields(collection, row_id, list_path, index, fields),
            RowWrite::ListDelete { list_path, index } => {
                self.list_delete(collection, row_id, list_path, index)
            }
            RowWrite::ListMove {
                list_path,
                from,
                to,
            } => self.list_move(collection, row_id, list_path, from, to),
        }
    }

    fn predict_scalars(
        &self,
        collection: &str,
        row_id: &str,
        fields: &[(&str, LoroValue)],
        replace: bool,
    ) -> Result<LoroValue> {
        let (mut row, containers) = match self.doc.get_map(collection).get(row_id) {
            Some(ValueOrContainer::Container(loro::Container::Map(map))) => {
                (entries(map.get_deep_value()), container_keys(&map))
            }
            // An absent or non-map row becomes a new map.
            _ => (HashMap::new(), HashSet::new()),
        };
        if let Some((field, _)) = fields.iter().find(|(field, _)| containers.contains(*field)) {
            return Err(CrdtError::ScalarFieldShadowsContainer {
                collection: collection.to_string(),
                row_id: row_id.to_string(),
                field: (*field).to_string(),
            });
        }
        if replace {
            row.retain(|key, _| containers.contains(key));
        }
        for (field, value) in fields {
            row.insert((*field).to_string(), value.clone());
        }
        Ok(LoroValue::Map(row.into()))
    }

    /// The deep row with `edit` applied to the list at `list_path`. `create`
    /// admits a missing list and missing intermediate maps, as an insert
    /// creates them.
    fn predict_list(
        &self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        create: bool,
        edit: impl FnOnce(&mut Vec<LoroValue>) -> Result<()>,
    ) -> Result<LoroValue> {
        let row = block_list_row(&self.doc, collection, row_id)?;
        let (parents, last) = split_list_path(list_path);
        let mut current = Some(row.clone());
        for segment in &parents {
            current = match current {
                Some(map) => match map.get(segment) {
                    Some(ValueOrContainer::Container(loro::Container::Map(m))) => Some(m),
                    None if create => None,
                    _ => return Err(path_unresolved(list_path, segment)),
                },
                None => None,
            };
        }
        let existing = match current {
            Some(map) => movable_list_at(map.get(last), list_path, last)?,
            None => None,
        };
        let mut list = match (existing, create) {
            (Some(list), _) => match list.get_deep_value() {
                LoroValue::List(items) => items.iter().cloned().collect(),
                _ => Vec::new(),
            },
            (None, true) => Vec::new(),
            (None, false) => return Err(path_unresolved(list_path, last)),
        };
        edit(&mut list)?;

        let mut deep = entries(row.get_deep_value());
        set_at_path(&mut deep, &parents, last, LoroValue::List(list.into()));
        Ok(LoroValue::Map(deep.into()))
    }
}

/// Refuse `index` outside a list of `len` blocks.
fn check_index(list_path: &str, index: usize, len: usize) -> Result<()> {
    if index >= len {
        return Err(CrdtError::BlockListIndexOutOfBounds {
            list_path: list_path.to_string(),
            index,
            len,
        });
    }
    Ok(())
}

/// Set `value` at `parents.last` inside `map`, creating missing maps.
fn set_at_path(
    map: &mut HashMap<String, LoroValue>,
    parents: &[&str],
    last: &str,
    value: LoroValue,
) {
    match parents.split_first() {
        None => {
            map.insert(last.to_string(), value);
        }
        Some((segment, rest)) => {
            let mut child = map.remove(*segment).map(entries).unwrap_or_default();
            set_at_path(&mut child, rest, last, value);
            map.insert((*segment).to_string(), LoroValue::Map(child.into()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> CrdtState {
        let state = CrdtState::new(5).expect("state");
        state
            .upsert(
                "pages",
                "p1",
                &[
                    ("title", LoroValue::String("t".into())),
                    ("n", LoroValue::I64(1)),
                ],
            )
            .expect("row");
        for (index, id) in ["b0", "b1", "b2"].iter().enumerate() {
            state
                .list_insert_fields(
                    "pages",
                    "p1",
                    "blocks",
                    index,
                    &[("id".to_string(), LoroValue::String((*id).into()))],
                )
                .expect("block");
        }
        state
    }

    /// Every write's prediction equals the row the write leaves.
    #[test]
    fn a_prediction_equals_the_written_row() {
        let upsert = [("title", LoroValue::String("u".into()))];
        let set = [("extra", LoroValue::Bool(true))];
        let block = [("id".to_string(), LoroValue::String("new".into()))];
        let writes = [
            RowWrite::Upsert(&upsert),
            RowWrite::SetFields(&set),
            RowWrite::ListInsert {
                list_path: "blocks",
                index: 1,
                fields: &block,
            },
            RowWrite::ListInsert {
                list_path: "blocks",
                index: 99,
                fields: &block,
            },
            RowWrite::ListInsert {
                list_path: "outline.items",
                index: 0,
                fields: &block,
            },
            RowWrite::ListDelete {
                list_path: "blocks",
                index: 1,
            },
            RowWrite::ListMove {
                list_path: "blocks",
                from: 0,
                to: 2,
            },
            RowWrite::ListMove {
                list_path: "blocks",
                from: 2,
                to: 0,
            },
        ];
        for write in writes {
            let state = state();
            let predicted = state.predict_row("pages", "p1", &write).expect("predict");
            state.apply_row_write("pages", "p1", &write).expect("write");
            assert_eq!(
                Some(predicted),
                state.read_row_deep("pages", "p1"),
                "{write:?}"
            );
        }
    }

    /// A prediction raises the error the write raises, and changes nothing.
    #[test]
    fn a_refused_prediction_changes_nothing() {
        let shadow = [("blocks", LoroValue::I64(1))];
        for write in [
            RowWrite::Upsert(&shadow),
            RowWrite::ListDelete {
                list_path: "blocks",
                index: 3,
            },
            RowWrite::ListMove {
                list_path: "missing",
                from: 0,
                to: 1,
            },
            RowWrite::ListDelete {
                list_path: "title",
                index: 0,
            },
        ] {
            let state = state();
            let before = state.oplog_version_vector();
            let predicted = state.predict_row("pages", "p1", &write);
            assert!(predicted.is_err(), "{write:?}");
            assert_eq!(state.oplog_version_vector(), before, "{write:?}");
            let written = state.apply_row_write("pages", "p1", &write);
            assert_eq!(
                std::mem::discriminant(&predicted.expect_err("refused")),
                std::mem::discriminant(&written.expect_err("the write refuses too")),
                "{write:?}"
            );
        }
    }

    /// A move onto its own index on an absent row is refused like any list
    /// write. On an existing row it predicts the row unchanged, and the write
    /// records no operation.
    #[test]
    fn a_move_onto_its_own_index_needs_the_row_and_changes_nothing() {
        let state = state();
        let write = RowWrite::ListMove {
            list_path: "blocks",
            from: 1,
            to: 1,
        };
        assert!(write.changes_nothing());
        assert!(matches!(
            state.predict_row("pages", "absent", &write),
            Err(CrdtError::BlockListRowAbsent { .. })
        ));
        assert!(matches!(
            state.apply_row_write("pages", "absent", &write),
            Err(CrdtError::BlockListRowAbsent { .. })
        ));

        let before = state.oplog_version_vector();
        let predicted = state.predict_row("pages", "p1", &write).expect("predict");
        assert_eq!(Some(predicted), state.read_row_deep("pages", "p1"));
        state.apply_row_write("pages", "p1", &write).expect("write");
        assert_eq!(
            state.oplog_version_vector(),
            before,
            "no operation recorded"
        );
        assert!(
            !RowWrite::ListMove {
                list_path: "blocks",
                from: 0,
                to: 1,
            }
            .changes_nothing()
        );
    }

    /// A prediction reads the row alone: in a collection of many rows it
    /// produces exactly the bytes it produces in a collection of one, and the
    /// document's exported size does not move.
    #[test]
    fn a_prediction_is_independent_of_the_other_rows() {
        let block = [("id".to_string(), LoroValue::String("new".into()))];
        let write = RowWrite::ListInsert {
            list_path: "blocks",
            index: 0,
            fields: &block,
        };
        let single = state();
        let crowded = state();
        for n in 0..500 {
            crowded
                .upsert(
                    "pages",
                    &format!("other-{n}"),
                    &[("body", LoroValue::String("x".repeat(64).into()))],
                )
                .expect("other row");
        }
        let encode = |state: &CrdtState| {
            let predicted = state.predict_row("pages", "p1", &write).expect("predict");
            zerompk::to_msgpack_vec(&crate::loro_value::loro_to_value(&predicted).expect("decode"))
                .expect("encode")
        };
        let crowded_size = crowded.export_snapshot().expect("snapshot").len();
        assert_eq!(encode(&single), encode(&crowded));
        assert_eq!(
            crowded.export_snapshot().expect("snapshot").len(),
            crowded_size,
            "a prediction writes nothing"
        );
    }
}
