// SPDX-License-Identifier: Apache-2.0

//! LoroMovableList operations for block document model.
//!
//! A block document is a LoroMap with a `blocks` field that is a
//! **LoroMovableList** of LoroMaps. LoroMovableList (not LoroList) is used
//! because it supports native `mov()` — concurrent block reordering converges
//! deterministically without duplicating elements. LoroList only supports
//! insert/delete; reordering via delete+insert loses CRDT container identity.
//!
//! Each block LoroMap has: `id` (string), `type` (string), `content`
//! (string or LoroText), and optional `children` (nested LoroMovableList).

use loro::{LoroDoc, LoroMap, LoroMovableList, LoroValue, ValueOrContainer};

use crate::error::{CrdtError, Result};

/// Insert a value into a block list at the specified index.
pub fn list_insert(
    doc: &LoroDoc,
    collection: &str,
    row_id: &str,
    list_path: &str,
    index: usize,
    value: LoroValue,
) -> Result<()> {
    let list = get_or_create_movable_list(doc, collection, row_id, list_path)?;
    let idx = index.min(list.len());
    list.insert(idx, value)
        .map_err(|e| CrdtError::Loro(format!("list insert at {idx}: {e}")))?;
    Ok(())
}

/// Insert a LoroMap container into a block list at the specified index.
///
/// Returns the created LoroMap so callers can populate its fields.
pub fn list_insert_container(
    doc: &LoroDoc,
    collection: &str,
    row_id: &str,
    list_path: &str,
    index: usize,
) -> Result<LoroMap> {
    let list = get_or_create_movable_list(doc, collection, row_id, list_path)?;
    let idx = index.min(list.len());
    list.insert_container(idx, LoroMap::new())
        .map_err(|e| CrdtError::Loro(format!("list insert container at {idx}: {e}")))
}

/// Delete an element from a block list at the specified index.
pub fn list_delete(
    doc: &LoroDoc,
    collection: &str,
    row_id: &str,
    list_path: &str,
    index: usize,
) -> Result<()> {
    let list = get_movable_list(doc, collection, row_id, list_path)?;
    if index >= list.len() {
        return Err(CrdtError::BlockListIndexOutOfBounds {
            list_path: list_path.to_string(),
            index,
            len: list.len(),
        });
    }
    list.delete(index, 1)
        .map_err(|e| CrdtError::Loro(format!("list delete at {index}: {e}")))?;
    Ok(())
}

/// Move an element within a block list from one index to another.
///
/// Uses LoroMovableList's native `mov()` — CRDT-safe, concurrent moves
/// converge deterministically without duplicating elements.
pub fn list_move(
    doc: &LoroDoc,
    collection: &str,
    row_id: &str,
    list_path: &str,
    from_index: usize,
    to_index: usize,
) -> Result<()> {
    if from_index == to_index {
        // A move onto its own index changes nothing, but the row must exist.
        block_list_row(doc, collection, row_id)?;
        return Ok(());
    }
    let list = get_movable_list(doc, collection, row_id, list_path)?;
    let len = list.len();
    for index in [from_index, to_index] {
        if index >= len {
            return Err(CrdtError::BlockListIndexOutOfBounds {
                list_path: list_path.to_string(),
                index,
                len,
            });
        }
    }
    list.mov(from_index, to_index)
        .map_err(|e| CrdtError::Loro(format!("list move {from_index}→{to_index}: {e}")))?;
    Ok(())
}

/// Get the length of a block list.
pub fn list_length(
    doc: &LoroDoc,
    collection: &str,
    row_id: &str,
    list_path: &str,
) -> Result<usize> {
    let list = get_movable_list(doc, collection, row_id, list_path)?;
    Ok(list.len())
}

/// Get a value at a specific index in a block list.
pub fn list_get(
    doc: &LoroDoc,
    collection: &str,
    row_id: &str,
    list_path: &str,
    index: usize,
) -> Result<Option<LoroValue>> {
    let list = get_movable_list(doc, collection, row_id, list_path)?;
    Ok(match list.get(index) {
        Some(ValueOrContainer::Value(v)) => Some(v),
        Some(ValueOrContainer::Container(loro::Container::Map(m))) => Some(m.get_value()),
        Some(ValueOrContainer::Container(loro::Container::List(l))) => Some(l.get_value()),
        Some(ValueOrContainer::Container(loro::Container::MovableList(l))) => Some(l.get_value()),
        Some(ValueOrContainer::Container(_)) => Some(LoroValue::Null),
        None => None,
    })
}

/// The row map `list_path` is resolved in. A block list lives inside an
/// existing row map, so an absent or non-map row is refused.
pub(crate) fn block_list_row(doc: &LoroDoc, collection: &str, row_id: &str) -> Result<LoroMap> {
    match doc.get_map(collection).get(row_id) {
        Some(ValueOrContainer::Container(loro::Container::Map(m))) => Ok(m),
        _ => Err(CrdtError::BlockListRowAbsent {
            collection: collection.to_string(),
            row_id: row_id.to_string(),
        }),
    }
}

/// Split `list_path` into its parent map segments and the list segment.
pub(crate) fn split_list_path(list_path: &str) -> (Vec<&str>, &str) {
    match list_path.rsplit_once('.') {
        Some((parents, last)) => (parents.split('.').collect(), last),
        None => (Vec::new(), list_path),
    }
}

/// The movable list held by `value` at the last segment of `list_path`.
pub(crate) fn movable_list_at(
    value: Option<ValueOrContainer>,
    list_path: &str,
    segment: &str,
) -> Result<Option<LoroMovableList>> {
    match value {
        Some(ValueOrContainer::Container(loro::Container::MovableList(l))) => Ok(Some(l)),
        Some(ValueOrContainer::Container(loro::Container::List(_))) => {
            Err(CrdtError::BlockListNotMovable {
                list_path: list_path.to_string(),
            })
        }
        None => Ok(None),
        Some(_) => Err(path_unresolved(list_path, segment)),
    }
}

pub(crate) fn path_unresolved(list_path: &str, segment: &str) -> CrdtError {
    CrdtError::BlockListPathUnresolved {
        list_path: list_path.to_string(),
        segment: segment.to_string(),
    }
}

/// Navigate to a LoroMovableList at `collection/row_id/list_path`.
///
/// `list_path` is a dot-separated field path within the row LoroMap.
/// For a simple blocks array: `"blocks"`.
/// For nested: `"content.blocks"` (navigates through LoroMap fields).
fn get_movable_list(
    doc: &LoroDoc,
    collection: &str,
    row_id: &str,
    list_path: &str,
) -> Result<LoroMovableList> {
    let (parents, last) = split_list_path(list_path);
    let mut current_map = block_list_row(doc, collection, row_id)?;
    for segment in parents {
        current_map = match current_map.get(segment) {
            Some(ValueOrContainer::Container(loro::Container::Map(m))) => m,
            _ => return Err(path_unresolved(list_path, segment)),
        };
    }
    movable_list_at(current_map.get(last), list_path, last)?
        .ok_or_else(|| path_unresolved(list_path, last))
}

/// Navigate to a LoroMovableList at `collection/row_id/list_path`, creating
/// it (and any missing intermediate LoroMaps along `list_path`) if absent.
///
/// The row itself must already exist — this function never creates rows,
/// only the containers along the path within an existing row. Auto-vivify
/// happens only on the insert path: inserting the first block into a fresh
/// row bootstraps the list, without requiring a separate "create container"
/// step that no caller in either repo could otherwise reach.
///
/// A segment occupied by the wrong type (a scalar, or a `Container::List`
/// at the last segment) is never silently replaced — it returns the same
/// typed error `get_movable_list` returns for that case.
///
/// Deterministic under replay: this walks the same containers in the same
/// order for the same op sequence, so replaying at the same LSN on the same
/// peer_id reconstructs identical structure.
fn get_or_create_movable_list(
    doc: &LoroDoc,
    collection: &str,
    row_id: &str,
    list_path: &str,
) -> Result<LoroMovableList> {
    let (parents, last) = split_list_path(list_path);
    let mut current_map = block_list_row(doc, collection, row_id)?;
    for segment in parents {
        current_map = match current_map.get(segment) {
            Some(ValueOrContainer::Container(loro::Container::Map(m))) => m,
            None => current_map
                .insert_container(segment, LoroMap::new())
                .map_err(|e| {
                    CrdtError::Loro(format!(
                        "create intermediate map at '{list_path}' segment '{segment}': {e}"
                    ))
                })?,
            Some(_) => return Err(path_unresolved(list_path, segment)),
        };
    }
    match movable_list_at(current_map.get(last), list_path, last)? {
        Some(list) => Ok(list),
        None => current_map
            .insert_container(last, LoroMovableList::new())
            .map_err(|e| CrdtError::Loro(format!("create movable list at '{list_path}': {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    trait TestDoc {
        fn doc(&self) -> &LoroDoc;
    }

    impl TestDoc for LoroDoc {
        fn doc(&self) -> &LoroDoc {
            self
        }
    }

    fn setup_doc_with_blocks() -> LoroDoc {
        let state = LoroDoc::new();
        let coll = state.doc().get_map("pages");
        let row = coll.insert_container("doc-1", LoroMap::new()).unwrap();

        row.insert("title", LoroValue::String("Test".into()))
            .unwrap();
        // Use LoroMovableList for blocks (supports native mov).
        let blocks = row
            .insert_container("blocks", LoroMovableList::new())
            .unwrap();

        let blk0 = blocks.insert_container(0, LoroMap::new()).unwrap();
        blk0.insert("id", LoroValue::String("blk-0".into()))
            .unwrap();
        blk0.insert("type", LoroValue::String("heading".into()))
            .unwrap();
        blk0.insert("content", LoroValue::String("Hello".into()))
            .unwrap();

        let blk1 = blocks.insert_container(1, LoroMap::new()).unwrap();
        blk1.insert("id", LoroValue::String("blk-1".into()))
            .unwrap();
        blk1.insert("type", LoroValue::String("paragraph".into()))
            .unwrap();
        blk1.insert("content", LoroValue::String("World".into()))
            .unwrap();

        state
    }

    #[test]
    fn list_length_works() {
        let state = setup_doc_with_blocks();
        let len = list_length(state.doc(), "pages", "doc-1", "blocks").unwrap();
        assert_eq!(len, 2);
    }

    #[test]
    fn list_get_block() {
        let state = setup_doc_with_blocks();
        let val = list_get(state.doc(), "pages", "doc-1", "blocks", 0)
            .unwrap()
            .unwrap();
        if let LoroValue::Map(map) = val {
            assert_eq!(map.get("type"), Some(&LoroValue::String("heading".into())));
        } else {
            panic!("expected map, got {val:?}");
        }
    }

    #[test]
    fn list_insert_and_get() {
        let state = setup_doc_with_blocks();
        list_insert(
            state.doc(),
            "pages",
            "doc-1",
            "blocks",
            1,
            LoroValue::String("inserted".into()),
        )
        .unwrap();
        assert_eq!(
            list_length(state.doc(), "pages", "doc-1", "blocks").unwrap(),
            3
        );
    }

    #[test]
    fn list_insert_container_works() {
        let state = setup_doc_with_blocks();
        let new_block = list_insert_container(state.doc(), "pages", "doc-1", "blocks", 2).unwrap();
        new_block
            .insert("id", LoroValue::String("blk-2".into()))
            .unwrap();
        new_block
            .insert("type", LoroValue::String("code".into()))
            .unwrap();
        assert_eq!(
            list_length(state.doc(), "pages", "doc-1", "blocks").unwrap(),
            3
        );
    }

    #[test]
    fn list_delete_works() {
        let state = setup_doc_with_blocks();
        list_delete(state.doc(), "pages", "doc-1", "blocks", 0).unwrap();
        assert_eq!(
            list_length(state.doc(), "pages", "doc-1", "blocks").unwrap(),
            1
        );
    }

    #[test]
    fn list_move_native() {
        let state = setup_doc_with_blocks();
        // Move block 0 to position 1 (native LoroMovableList::mov).
        list_move(state.doc(), "pages", "doc-1", "blocks", 0, 1).unwrap();

        // After mov(0,1): [blk-0, blk-1] → [blk-1, blk-0]
        // (Loro's mov(0,2) semantics: element at 0 goes AFTER element at 1)
        let first = list_get(state.doc(), "pages", "doc-1", "blocks", 0)
            .unwrap()
            .unwrap();
        if let LoroValue::Map(map) = first {
            assert_eq!(map.get("id"), Some(&LoroValue::String("blk-1".into())));
        } else {
            panic!("expected map");
        }
    }

    #[test]
    fn list_move_preserves_container_identity() {
        let state = setup_doc_with_blocks();
        // Move block 0 to position 1.
        list_move(state.doc(), "pages", "doc-1", "blocks", 0, 1).unwrap();

        // The moved block should still be a live CRDT container.
        // Verify by checking that list_get returns a Map with all fields intact.
        let moved = list_get(state.doc(), "pages", "doc-1", "blocks", 1)
            .unwrap()
            .unwrap();
        if let LoroValue::Map(map) = moved {
            assert_eq!(map.get("id"), Some(&LoroValue::String("blk-0".into())));
            assert_eq!(map.get("content"), Some(&LoroValue::String("Hello".into())));
        } else {
            panic!("expected map with container identity preserved");
        }
    }

    #[test]
    fn list_delete_out_of_bounds() {
        let state = setup_doc_with_blocks();
        let err = list_delete(state.doc(), "pages", "doc-1", "blocks", 99);
        assert!(matches!(
            err,
            Err(CrdtError::BlockListIndexOutOfBounds { index: 99, .. })
        ));
    }

    #[test]
    fn get_list_wrong_path_errors() {
        let state = setup_doc_with_blocks();
        let err = list_length(state.doc(), "pages", "doc-1", "nonexistent");
        assert!(matches!(
            err,
            Err(CrdtError::BlockListPathUnresolved { ref segment, .. }) if segment == "nonexistent"
        ));
        let err = list_length(state.doc(), "pages", "missing-row", "blocks");
        assert!(matches!(err, Err(CrdtError::BlockListRowAbsent { .. })));
    }

    /// A row with no list at `list_path` yet — the setup previous agents
    /// couldn't reach without `get_or_create_movable_list`.
    fn setup_bare_row(row_id: &str) -> LoroDoc {
        let state = LoroDoc::new();
        let coll = state.doc().get_map("pages");
        let row = coll.insert_container(row_id, LoroMap::new()).unwrap();
        row.insert("title", LoroValue::String("Bare".into()))
            .unwrap();
        state
    }

    #[test]
    fn list_insert_auto_vivifies_missing_list() {
        let state = setup_bare_row("doc-2");
        list_insert(
            state.doc(),
            "pages",
            "doc-2",
            "blocks",
            0,
            LoroValue::String("first".into()),
        )
        .unwrap();
        assert_eq!(
            list_length(state.doc(), "pages", "doc-2", "blocks").unwrap(),
            1
        );
    }

    #[test]
    fn list_insert_container_auto_vivifies_missing_list() {
        let state = setup_bare_row("doc-2");
        let block = list_insert_container(state.doc(), "pages", "doc-2", "blocks", 0).unwrap();
        block
            .insert("id", LoroValue::String("blk-0".into()))
            .unwrap();
        assert_eq!(
            list_length(state.doc(), "pages", "doc-2", "blocks").unwrap(),
            1
        );
    }

    #[test]
    fn list_insert_auto_vivifies_nested_intermediate_maps() {
        let state = setup_bare_row("doc-2");
        // Neither "content" nor "content.blocks" exists yet.
        list_insert(
            state.doc(),
            "pages",
            "doc-2",
            "content.blocks",
            0,
            LoroValue::String("first".into()),
        )
        .unwrap();
        assert_eq!(
            list_length(state.doc(), "pages", "doc-2", "content.blocks").unwrap(),
            1
        );
    }

    #[test]
    fn list_insert_scalar_segment_errors_instead_of_replacing() {
        let state = setup_bare_row("doc-2");
        let coll = state.doc().get_map("pages");
        let row = match coll.get("doc-2") {
            Some(ValueOrContainer::Container(loro::Container::Map(m))) => m,
            _ => panic!("expected row map"),
        };
        row.insert("blocks", LoroValue::String("not-a-list".into()))
            .unwrap();

        let err = list_insert(
            state.doc(),
            "pages",
            "doc-2",
            "blocks",
            0,
            LoroValue::String("x".into()),
        );
        assert!(matches!(
            err,
            Err(CrdtError::BlockListPathUnresolved { ref segment, .. }) if segment == "blocks"
        ));
        // The scalar must survive untouched — no silent replacement.
        match row.get("blocks") {
            Some(ValueOrContainer::Value(v)) => {
                assert_eq!(v, LoroValue::String("not-a-list".into()));
            }
            other => panic!("expected untouched scalar, got {other:?}"),
        }
    }

    #[test]
    fn list_delete_on_missing_list_still_errors() {
        let state = setup_bare_row("doc-2");
        let err = list_delete(state.doc(), "pages", "doc-2", "blocks", 0);
        assert!(matches!(
            err,
            Err(CrdtError::BlockListPathUnresolved { .. })
        ));
    }

    #[test]
    fn list_move_on_missing_list_still_errors() {
        let state = setup_bare_row("doc-2");
        let err = list_move(state.doc(), "pages", "doc-2", "blocks", 0, 1);
        assert!(matches!(
            err,
            Err(CrdtError::BlockListPathUnresolved { .. })
        ));
    }
}
