// SPDX-License-Identifier: Apache-2.0

//! Write-set extraction from an import.
//!
//! An import reports the rows it *actually* wrote, independent of any row id
//! the sender claimed, and a committed row is assembled into a
//! [`ProposedChange`] so it can be re-checked against installed constraints.
//!
//! The rows come from the operations the import added to the oplog. Loro adds
//! an operation to the oplog only once its causal predecessors are present,
//! and the oplog version vector also counts an open local transaction. So the
//! version vector difference across the import holds exactly:
//! - the ready operations of this blob.
//! - earlier pending operations this blob unblocked.
//!
//! It never holds an operation the document already had, nor a local write.
//! `ImportStatus::success` is not used: it also covers ranges the document
//! already knew.
//!
//! Each new span is read back with `LoroDoc::export_json_in_id_span`:
//! - an operation on a root map names the row key it set or deleted.
//! - an operation on any other container names the row its container path
//!   passes through (`LoroDoc::get_path_to_container`).
//!
//! A container whose row the same import deleted has no path. The root-map
//! delete already names that row. Nothing subscribes to the document, so Loro
//! never turns on diff recording, and later local writes cost the same as on a
//! document that never ran a tracked import.
//!
//! An import into an empty applied state with no open local transaction makes
//! every present row new, so the rows are read from the state directly. That
//! also covers a shallow snapshot, whose history before its shallow root cannot
//! be read back.
//!
//! A row is reported when an applied operation wrote it, also when that write
//! lost a concurrent conflict and left the visible value unchanged.
//!
//! The write-set is row-granular only: collection + row-id pairs. The
//! validator re-reads the full row, so field-level detail is unnecessary here.
//! Ordering is deterministic (`BTreeSet` for the write-set, sorted field vec
//! for the change) because every replica must agree on the same result.
//!
//! Every collection is a root map of row maps. A write outside that shape
//! names no row the validator can check, so the write-set refuses the import:
//! - an operation on a root text, list, movable list, tree, or counter, or on
//!   a container under one, is [`CrdtError::NonMapRootContainer`].
//! - a root-map row set to a scalar or a non-map container, or an operation
//!   on such a row container, is [`CrdtError::NonMapRowValue`].

use std::collections::{BTreeSet, HashMap};

use loro::{
    Container, ContainerID, ContainerType, Frontiers, IdSpan, Index, JsonMapOp, JsonOp,
    JsonOpContent, LoroDoc, LoroValue, ValueOrContainer,
};
use nodedb_types::Surrogate;

use crate::error::{CrdtError, Result};
use crate::validator::ProposedChange;

use super::core::CrdtState;
use super::import_admission::ImportAdmission;

/// The outcome of an import and the rows it wrote.
///
/// `write_set` is filled for every outcome. A blob can apply its ready
/// changes and still return `ImportPendingDependencies`.
#[derive(Debug)]
#[must_use]
pub struct WriteSetImport {
    /// The import result, as [`CrdtState::import`] returns it.
    pub outcome: Result<ImportAdmission>,
    /// Sorted `(collection, row_id)` pairs an applied operation of the import
    /// wrote: rows added, removed, replaced, or changed in any nested
    /// container. An error when an applied operation breaks the collection
    /// shape: the caller must refuse the import.
    pub write_set: Result<Vec<(String, String)>>,
}

/// A write outside the root-map-of-row-maps shape.
///
/// Ordered so every replica reports the same fault for one import.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::state) enum ShapeFault {
    /// A root container that is not a map, or a container under one.
    NonMapRoot {
        container: String,
        container_type: String,
    },
    /// A root-map row whose value is not a map.
    NonMapRow {
        collection: String,
        row_id: String,
        value: String,
    },
}

impl ShapeFault {
    fn into_error(self) -> CrdtError {
        match self {
            Self::NonMapRoot {
                container,
                container_type,
            } => CrdtError::NonMapRootContainer {
                container,
                container_type,
            },
            Self::NonMapRow {
                collection,
                row_id,
                value,
            } => CrdtError::NonMapRowValue {
                collection,
                row_id,
                value,
            },
        }
    }
}

/// The rows an import wrote and the shape faults among its writes.
#[derive(Debug, Default)]
pub(in crate::state) struct ImportedRows {
    pub(in crate::state) rows: BTreeSet<(String, String)>,
    pub(in crate::state) faults: BTreeSet<ShapeFault>,
}

/// Run `import` on `doc` and collect the `(collection, row_id)` pairs its
/// applied operations wrote.
///
/// `scope` limits the pairs to one collection. `None` covers every root map.
/// Shape faults are collected for every collection.
pub(in crate::state) fn collect_imported_rows<T>(
    doc: &LoroDoc,
    scope: Option<&str>,
    import: impl FnOnce() -> T,
) -> (T, ImportedRows) {
    if doc.state_frontiers().is_empty() && doc.get_pending_txn_len() == 0 {
        let outcome = import();
        return (outcome, present_rows(doc, scope));
    }

    let before = doc.oplog_vv();
    let outcome = import();
    let after = doc.oplog_vv();
    let mut reader = RowReader {
        doc,
        scope,
        imported: ImportedRows::default(),
        container_places: HashMap::new(),
    };
    for (peer, end) in after.iter() {
        let start = before.get(peer).copied().unwrap_or(0);
        if *end <= start {
            continue;
        }
        for change in doc.export_json_in_id_span(IdSpan::new(*peer, start, *end)) {
            for op in &change.ops {
                reader.read(op);
            }
        }
    }
    (outcome, reader.imported)
}

/// Run `import` on `doc` and collect the sorted `(collection, row_id)` pairs
/// it wrote across every collection, or the first shape fault.
pub(in crate::state) fn collect_write_set<T>(
    doc: &LoroDoc,
    import: impl FnOnce() -> T,
) -> (T, Result<Vec<(String, String)>>) {
    let (outcome, imported) = collect_imported_rows(doc, None, import);
    let write_set = match imported.faults.into_iter().next() {
        Some(fault) => Err(fault.into_error()),
        None => Ok(imported.rows.into_iter().collect()),
    };
    (outcome, write_set)
}

/// Every row present in `scope`, or in every root map when `scope` is `None`.
fn present_rows(doc: &LoroDoc, scope: Option<&str>) -> ImportedRows {
    let mut imported = ImportedRows::default();
    let mut add_collection = |collection: &str| {
        let map = doc.get_map(collection);
        for row_id in map.keys() {
            let row_id = row_id.to_string();
            if let Some(value) = map.get(&row_id).and_then(|row| non_map_row(&row)) {
                imported.faults.insert(ShapeFault::NonMapRow {
                    collection: collection.to_owned(),
                    row_id: row_id.clone(),
                    value,
                });
            }
            imported.rows.insert((collection.to_owned(), row_id));
        }
    };
    let mut root_faults = BTreeSet::new();
    if let LoroValue::Map(roots) = doc.get_value() {
        for root in roots.values() {
            let LoroValue::Container(id) = root else {
                continue;
            };
            let ContainerID::Root {
                name,
                container_type,
            } = id
            else {
                continue;
            };
            if id.is_mergeable() {
                continue;
            }
            if *container_type != ContainerType::Map {
                root_faults.insert(ShapeFault::NonMapRoot {
                    container: name.to_string(),
                    container_type: format!("{container_type:?}"),
                });
            } else if scope.is_none() {
                add_collection(name.as_str());
            }
        }
    }
    if let Some(collection) = scope {
        add_collection(collection);
    }
    imported.faults.extend(root_faults);
    imported
}

/// A description of a row value that is not a map, or `None` for a map.
fn non_map_row(row: &ValueOrContainer) -> Option<String> {
    match row {
        ValueOrContainer::Container(Container::Map(_)) => None,
        ValueOrContainer::Container(other) => Some(format!("a {:?} container", other.get_type())),
        ValueOrContainer::Value(value) => Some(format!("the scalar {value:?}")),
    }
}

/// A description of a map-insert value that is not a map container, or
/// `None` for a map container.
fn non_map_inserted(value: &LoroValue) -> Option<String> {
    match value {
        LoroValue::Container(id) if id.container_type() == ContainerType::Map => None,
        LoroValue::Container(id) => Some(format!("a {:?} container", id.container_type())),
        other => Some(format!("the scalar {other:?}")),
    }
}

/// Where a non-root container sits.
#[derive(Debug, Clone)]
enum ContainerPlace {
    /// Inside row `row_id` of the root map `collection`.
    Row(String, String),
    /// No live path: the import deleted the row that held it.
    Detached,
    /// Outside a root-map row.
    Misplaced(ShapeFault),
}

/// Maps imported operations to the rows they wrote.
struct RowReader<'a> {
    doc: &'a LoroDoc,
    scope: Option<&'a str>,
    imported: ImportedRows,
    /// Place of each non-root container already resolved.
    container_places: HashMap<ContainerID, ContainerPlace>,
}

impl RowReader<'_> {
    fn read(&mut self, op: &JsonOp) {
        let scope = self.scope;
        let in_scope = |collection: &str| scope.is_none_or(|wanted| wanted == collection);

        if let ContainerID::Root {
            name,
            container_type,
        } = &op.container
            && !op.container.is_mergeable()
        {
            if *container_type != ContainerType::Map {
                self.imported.faults.insert(ShapeFault::NonMapRoot {
                    container: name.to_string(),
                    container_type: format!("{container_type:?}"),
                });
                return;
            }
            let row = match &op.content {
                JsonOpContent::Map(JsonMapOp::Insert { key, value }) => {
                    if let Some(value) = non_map_inserted(value) {
                        self.imported.faults.insert(ShapeFault::NonMapRow {
                            collection: name.to_string(),
                            row_id: key.clone(),
                            value,
                        });
                    }
                    Some(key)
                }
                JsonOpContent::Map(JsonMapOp::Delete { key }) => Some(key),
                _ => None,
            };
            if let Some(row_id) = row
                && in_scope(name.as_str())
            {
                self.imported
                    .rows
                    .insert((name.to_string(), row_id.clone()));
            }
            return;
        }

        let doc = self.doc;
        let place = self
            .container_places
            .entry(op.container.clone())
            .or_insert_with(|| place_of_container(doc, &op.container));
        match place {
            ContainerPlace::Row(collection, row_id) => {
                if in_scope(collection.as_str()) {
                    self.imported
                        .rows
                        .insert((collection.clone(), row_id.clone()));
                }
            }
            ContainerPlace::Detached => {}
            ContainerPlace::Misplaced(fault) => {
                self.imported.faults.insert(fault.clone());
            }
        }
    }
}

/// Where a non-root container sits.
///
/// The path runs from the root: `[(root, Key(collection)), (row,
/// Key(row_id)), ...]`. The row container must be a map. A container under
/// any other root kind is misplaced.
fn place_of_container(doc: &LoroDoc, container: &ContainerID) -> ContainerPlace {
    let Some(path) = doc.get_path_to_container(container) else {
        return ContainerPlace::Detached;
    };
    match path.as_slice() {
        [
            (
                ContainerID::Root {
                    name,
                    container_type: ContainerType::Map,
                },
                _,
            ),
            (row, Index::Key(row_id)),
            ..,
        ] => {
            if row.container_type() != ContainerType::Map {
                return ContainerPlace::Misplaced(ShapeFault::NonMapRow {
                    collection: name.to_string(),
                    row_id: row_id.to_string(),
                    value: format!("a {:?} container", row.container_type()),
                });
            }
            ContainerPlace::Row(name.to_string(), row_id.to_string())
        }
        [
            (
                root @ ContainerID::Root {
                    name,
                    container_type,
                },
                _,
            ),
            ..,
        ] if *container_type != ContainerType::Map && !root.is_mergeable() => {
            ContainerPlace::Misplaced(ShapeFault::NonMapRoot {
                container: name.to_string(),
                container_type: format!("{container_type:?}"),
            })
        }
        // A path that does not reach a root-map row names no row.
        _ => ContainerPlace::Detached,
    }
}

impl CrdtState {
    /// Current applied-state frontier. Equal frontiers mean equal applied
    /// state for two documents built from the same history.
    pub fn frontier(&self) -> Frontiers {
        self.doc.state_frontiers()
    }

    /// [`Self::import`] that also reports the `(collection, row_id)` pairs
    /// the import wrote.
    pub fn import_with_write_set(&self, data: &[u8]) -> WriteSetImport {
        let (outcome, write_set) = collect_write_set(&self.doc, || self.import(data));
        WriteSetImport { outcome, write_set }
    }

    /// Assemble a [`ProposedChange`] from a committed row's current fields.
    ///
    /// Returns `None` when the row is absent (a pure delete leaves nothing to
    /// validate). The field vec is sorted by key so the change is byte-identical
    /// across replicas (the underlying map iterates in nondeterministic order).
    pub fn build_change_from_row(
        &self,
        collection: &str,
        row_id: &str,
        surrogate: Surrogate,
    ) -> Option<ProposedChange> {
        match self.read_row(collection, row_id)? {
            LoroValue::Map(m) => {
                let mut fields: Vec<(String, LoroValue)> =
                    m.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();
                fields.sort_by(|a, b| a.0.cmp(&b.0));
                Some(ProposedChange {
                    collection: collection.to_string(),
                    row_id: row_id.to_string(),
                    surrogate,
                    fields,
                })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> LoroValue {
        LoroValue::String(v.into())
    }

    fn n(v: i64) -> LoroValue {
        LoroValue::I64(v)
    }

    fn pair(collection: &str, row: &str) -> (String, String) {
        (collection.to_string(), row.to_string())
    }

    fn write_set_of(dst: &CrdtState, data: &[u8]) -> Vec<(String, String)> {
        let imported = dst.import_with_write_set(data);
        imported.outcome.as_ref().expect("import");
        imported.write_set.expect("write set")
    }

    // (1) A single-row delta surfaces exactly that row.
    #[test]
    fn single_insert_write_set() {
        let src = CrdtState::new(2).unwrap();
        src.upsert("users", "u1", &[("name", s("Alice"))]).unwrap();
        let delta = src
            .export_updates_since(&loro::VersionVector::default())
            .unwrap();

        let dst = CrdtState::new(1).unwrap();
        assert_eq!(write_set_of(&dst, &delta), vec![pair("users", "u1")]);
    }

    // (2) A two-row blob imported from a peer surfaces both rows, sorted.
    #[test]
    fn two_row_blob_write_set() {
        let src = CrdtState::new(2).unwrap();
        src.upsert("users", "a", &[("x", n(1))]).unwrap();
        src.upsert("users", "b", &[("x", n(2))]).unwrap();
        let snapshot = src.export_snapshot().unwrap();

        let dst = CrdtState::new(1).unwrap();
        assert_eq!(
            write_set_of(&dst, &snapshot),
            vec![pair("users", "a"), pair("users", "b")]
        );
    }

    // (3) The write-set reflects the row a delta REALLY wrote, not a claimed id.
    #[test]
    fn write_set_reveals_real_row_not_claimed() {
        let src = CrdtState::new(2).unwrap();
        src.upsert("orders", "real-1", &[("amt", n(10))]).unwrap();
        let snapshot = src.export_snapshot().unwrap();

        let dst = CrdtState::new(1).unwrap();
        let ws = write_set_of(&dst, &snapshot);
        assert_eq!(ws, vec![pair("orders", "real-1")]);
        assert!(!ws.contains(&pair("orders", "fake-claimed")));
    }

    // (4) A cross-collection blob surfaces the real collection it wrote into.
    #[test]
    fn write_set_reveals_real_collection() {
        let src = CrdtState::new(2).unwrap();
        src.upsert("secret", "s1", &[("v", n(9))]).unwrap();
        let snapshot = src.export_snapshot().unwrap();

        let dst = CrdtState::new(1).unwrap();
        assert_eq!(write_set_of(&dst, &snapshot), vec![pair("secret", "s1")]);
    }

    // (5) A peer delta that merges ONE field of an existing row surfaces that
    //     one row, and the post-import row still carries ALL fields (so NOT
    //     NULL can see the untouched required fields). Modeled via a
    //     field-level merge (`insert` on the existing row container) exported
    //     as an incremental delta, not an `upsert`, which is whole-row
    //     replace and would wipe the untouched field.
    #[test]
    fn update_only_write_set_and_full_change() {
        let src = CrdtState::new(2).unwrap();
        src.upsert("users", "u1", &[("email", s("a")), ("name", s("x"))])
            .unwrap();
        let snapshot = src.export_snapshot().unwrap();

        let dst = CrdtState::new(1).unwrap();
        dst.import(&snapshot).unwrap();

        // Field-level merge of only `name` on the EXISTING row container.
        let src_vv = src.doc.oplog_vv();
        match src.doc.get_map("users").get("u1") {
            Some(loro::ValueOrContainer::Container(loro::Container::Map(row))) => {
                row.insert("name", s("y")).unwrap();
            }
            _ => panic!("row container missing"),
        }
        let delta = src.export_updates_since(&src_vv).unwrap();
        assert_eq!(write_set_of(&dst, &delta), vec![pair("users", "u1")]);

        let change = dst
            .build_change_from_row("users", "u1", Surrogate::ZERO)
            .unwrap();
        let field_names: Vec<String> = change.fields.iter().map(|(k, _)| k.clone()).collect();
        // Sorted: email before name — both present despite only name merging.
        assert_eq!(field_names, vec!["email".to_string(), "name".to_string()]);
        assert!(change.fields.contains(&("name".to_string(), s("y"))));
        assert!(change.fields.contains(&("email".to_string(), s("a"))));
    }

    // (6) The output is deterministic across repeated runs on fresh docs.
    #[test]
    fn write_set_is_deterministic() {
        fn run() -> Vec<(String, String)> {
            let src = CrdtState::new(2).unwrap();
            src.upsert("users", "b", &[("x", n(2))]).unwrap();
            src.upsert("users", "a", &[("x", n(1))]).unwrap();
            src.upsert("secret", "s1", &[("v", n(9))]).unwrap();
            let snapshot = src.export_snapshot().unwrap();

            let dst = CrdtState::new(1).unwrap();
            write_set_of(&dst, &snapshot)
        }

        let first = run();
        let second = run();
        assert_eq!(first, second);
        assert_eq!(
            first,
            vec![pair("secret", "s1"), pair("users", "a"), pair("users", "b")]
        );
    }

    // (7) A deleted row yields no change to validate.
    #[test]
    fn deleted_row_yields_no_change() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert("users", "u1", &[("name", s("Alice"))])
            .unwrap();
        state.delete("users", "u1").unwrap();

        assert!(
            state
                .build_change_from_row("users", "u1", Surrogate::ZERO)
                .is_none()
        );
    }

    // (8) A delta that only changes a list nested under a row names that row.
    #[test]
    fn nested_only_change_is_in_the_write_set() {
        let src = CrdtState::new(2).unwrap();
        src.upsert("pages", "p1", &[("title", s("t"))]).unwrap();
        src.list_insert_fields("pages", "p1", "blocks", 0, &[("text".to_owned(), s("one"))])
            .unwrap();
        let dst = CrdtState::new(1).unwrap();
        dst.import(&src.export_snapshot().unwrap()).unwrap();

        let before = src.oplog_version_vector();
        match src.doc.get_map("pages").get("p1") {
            Some(loro::ValueOrContainer::Container(loro::Container::Map(row))) => {
                match row.get("blocks") {
                    Some(loro::ValueOrContainer::Container(loro::Container::MovableList(list))) => {
                        match list.get(0) {
                            Some(loro::ValueOrContainer::Container(loro::Container::Map(
                                block,
                            ))) => {
                                block.insert("text", s("two")).unwrap();
                            }
                            _ => panic!("block map missing"),
                        }
                    }
                    _ => panic!("block list missing"),
                }
            }
            _ => panic!("row container missing"),
        }
        let delta = src.export_updates_since(&before).unwrap();

        assert_eq!(write_set_of(&dst, &delta), vec![pair("pages", "p1")]);
    }

    // (9) A shallow source works for the first import and for later deltas.
    #[test]
    fn shallow_source_reports_its_rows() {
        let mut src = CrdtState::new(2).unwrap();
        src.upsert("users", "a", &[("x", n(1))]).unwrap();
        src.upsert("users", "b", &[("x", n(2))]).unwrap();
        src.compact_history().unwrap();
        let snapshot = src.export_snapshot().unwrap();

        let dst = CrdtState::new(1).unwrap();
        let first = dst.import_with_write_set(&snapshot);
        first.outcome.as_ref().expect("shallow import");
        assert!(dst.doc.is_shallow());
        assert_eq!(
            first.write_set.expect("write set"),
            vec![pair("users", "a"), pair("users", "b")]
        );

        let before = src.oplog_version_vector();
        src.upsert("users", "c", &[("x", n(3))]).unwrap();
        src.delete("users", "a").unwrap();
        let delta = src.export_updates_since(&before).unwrap();
        assert_eq!(
            write_set_of(&dst, &delta),
            vec![pair("users", "a"), pair("users", "c")]
        );
    }

    // (10) Rows of each collection are attributed to that collection.
    #[test]
    fn rows_are_attributed_to_their_own_collection() {
        let src = CrdtState::new(2).unwrap();
        src.upsert("users", "u1", &[("x", n(1))]).unwrap();
        src.upsert("orders", "o1", &[("amt", n(1))]).unwrap();
        let dst = CrdtState::new(1).unwrap();
        dst.import(&src.export_snapshot().unwrap()).unwrap();

        let before = src.oplog_version_vector();
        src.upsert("users", "u2", &[("x", n(2))]).unwrap();
        src.upsert("orders", "u1", &[("amt", n(5))]).unwrap();
        src.list_insert_fields("orders", "o1", "lines", 0, &[("sku".to_owned(), s("k"))])
            .unwrap();
        let delta = src.export_updates_since(&before).unwrap();

        assert_eq!(
            write_set_of(&dst, &delta),
            vec![
                pair("orders", "o1"),
                pair("orders", "u1"),
                pair("users", "u2")
            ]
        );
    }

    // (11) A local write is not part of an import's write set.
    #[test]
    fn local_write_is_not_in_the_write_set() {
        let src = CrdtState::new(2).unwrap();
        src.upsert("users", "u1", &[("x", n(1))]).unwrap();
        let dst = CrdtState::new(1).unwrap();
        dst.upsert("users", "local", &[("x", n(9))]).unwrap();

        assert_eq!(
            write_set_of(&dst, &src.export_snapshot().unwrap()),
            vec![pair("users", "u1")]
        );
    }

    fn row_map(state: &CrdtState, collection: &str, row: &str) -> loro::LoroMap {
        match state.doc.get_map(collection).get(row) {
            Some(loro::ValueOrContainer::Container(loro::Container::Map(row))) => row,
            other => panic!("row {collection}/{row} is not a map: {other:?}"),
        }
    }

    /// A source and a target that share one row, and the source's version.
    fn synced_users() -> (CrdtState, CrdtState, loro::VersionVector) {
        let src = CrdtState::new(2).unwrap();
        src.upsert("users", "u1", &[("name", s("a"))]).unwrap();
        let dst = CrdtState::new(1).unwrap();
        dst.import(&src.export_snapshot().unwrap()).unwrap();
        let before = src.oplog_version_vector();
        (src, dst, before)
    }

    fn write_set_error(dst: &CrdtState, data: &[u8]) -> CrdtError {
        let imported = dst.import_with_write_set(data);
        imported.outcome.as_ref().expect("import");
        imported.write_set.expect_err("shape fault")
    }

    // (12) An operation on a root text container is refused.
    #[test]
    fn root_text_delta_is_refused() {
        let (src, dst, before) = synced_users();
        src.doc.get_text("notes").insert(0, "hi").unwrap();
        let delta = src.export_updates_since(&before).unwrap();

        match write_set_error(&dst, &delta) {
            CrdtError::NonMapRootContainer {
                container,
                container_type,
            } => {
                assert_eq!(container, "notes");
                assert_eq!(container_type, "Text");
            }
            other => panic!("expected NonMapRootContainer, got {other:?}"),
        }
    }

    // (13) A root text in a first snapshot import is refused too.
    #[test]
    fn root_text_snapshot_is_refused() {
        let src = CrdtState::new(2).unwrap();
        src.upsert("users", "u1", &[("name", s("a"))]).unwrap();
        src.doc.get_text("notes").insert(0, "hi").unwrap();
        let dst = CrdtState::new(1).unwrap();

        match write_set_error(&dst, &src.export_snapshot().unwrap()) {
            CrdtError::NonMapRootContainer { container, .. } => assert_eq!(container, "notes"),
            other => panic!("expected NonMapRootContainer, got {other:?}"),
        }
    }

    // (14) A row set to a scalar is refused, naming the collection and row.
    #[test]
    fn scalar_row_is_refused() {
        let (src, dst, before) = synced_users();
        src.doc.get_map("users").insert("u2", 5).unwrap();
        let delta = src.export_updates_since(&before).unwrap();

        match write_set_error(&dst, &delta) {
            CrdtError::NonMapRowValue {
                collection, row_id, ..
            } => {
                assert_eq!(collection, "users");
                assert_eq!(row_id, "u2");
            }
            other => panic!("expected NonMapRowValue, got {other:?}"),
        }
    }

    // (15) A scalar row in a first snapshot import is refused too.
    #[test]
    fn scalar_row_snapshot_is_refused() {
        let src = CrdtState::new(2).unwrap();
        src.doc.get_map("users").insert("u1", 5).unwrap();
        let dst = CrdtState::new(1).unwrap();

        let err = write_set_error(&dst, &src.export_snapshot().unwrap());
        assert!(
            matches!(err, CrdtError::NonMapRowValue { ref row_id, .. } if row_id == "u1"),
            "{err:?}"
        );
    }

    // (16) A row set to a non-map container is refused, and so is a later
    //      edit inside that container.
    #[test]
    fn non_map_row_container_is_refused() {
        let (src, dst, before) = synced_users();
        let text = src
            .doc
            .get_map("users")
            .insert_container("u3", loro::LoroText::new())
            .unwrap();
        text.insert(0, "x").unwrap();
        let delta = src.export_updates_since(&before).unwrap();

        let err = write_set_error(&dst, &delta);
        assert!(
            matches!(err, CrdtError::NonMapRowValue { ref row_id, .. } if row_id == "u3"),
            "{err:?}"
        );
    }

    // (17) A nested change under a map row stays accepted.
    #[test]
    fn nested_field_change_is_accepted() {
        let (src, dst, before) = synced_users();
        row_map(&src, "users", "u1")
            .insert_container("tags", loro::LoroList::new())
            .unwrap()
            .push("t")
            .unwrap();
        let delta = src.export_updates_since(&before).unwrap();

        assert_eq!(write_set_of(&dst, &delta), vec![pair("users", "u1")]);
    }

    // (18) A write that loses a concurrent conflict still names its row.
    #[test]
    fn concurrent_conflict_loser_is_reported() {
        // Peer 2 wins a same-lamport conflict against peer 1.
        let loser = CrdtState::new(1).unwrap();
        loser.upsert("users", "u1", &[("name", s("base"))]).unwrap();
        let winner = CrdtState::new(2).unwrap();
        winner.import(&loser.export_snapshot().unwrap()).unwrap();

        let loser_before = loser.oplog_version_vector();
        row_map(&loser, "users", "u1")
            .insert("name", s("lose"))
            .unwrap();
        row_map(&winner, "users", "u1")
            .insert("name", s("win"))
            .unwrap();
        winner.doc.commit();
        let losing_delta = loser.export_updates_since(&loser_before).unwrap();

        assert_eq!(
            write_set_of(&winner, &losing_delta),
            vec![pair("users", "u1")]
        );
        let change = winner
            .build_change_from_row("users", "u1", Surrogate::ZERO)
            .unwrap();
        assert!(change.fields.contains(&("name".to_string(), s("win"))));
    }

    // (19) A nested edit under a row the same import deletes names that row
    //      once, through the root-map delete.
    #[test]
    fn nested_op_under_a_row_deleted_in_the_same_import() {
        let src = CrdtState::new(2).unwrap();
        src.upsert("pages", "p1", &[("title", s("t"))]).unwrap();
        src.list_insert_fields("pages", "p1", "blocks", 0, &[("text".to_owned(), s("one"))])
            .unwrap();
        let dst = CrdtState::new(1).unwrap();
        dst.import(&src.export_snapshot().unwrap()).unwrap();

        let before = src.oplog_version_vector();
        row_map(&src, "pages", "p1")
            .insert("title", s("u"))
            .unwrap();
        src.delete("pages", "p1").unwrap();
        let delta = src.export_updates_since(&before).unwrap();

        assert_eq!(write_set_of(&dst, &delta), vec![pair("pages", "p1")]);
        assert!(
            dst.build_change_from_row("pages", "p1", Surrogate::ZERO)
                .is_none()
        );
    }
}
