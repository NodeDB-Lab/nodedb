// SPDX-License-Identifier: BUSL-1.1

//! CRDT block-list handlers: insert / delete / move a block within a
//! document's Loro list container.
//!
//! Each op is predicted first (`TenantCrdtEngine::predict_row`): the deep
//! row it leaves, block lists resolved, computed from the row alone with no
//! change to the document. The handler writes that row into the sparse
//! document store, and only then runs the op on the CRDT document. SQL and
//! point reads see the blocks at once. A projection that fails leaves the
//! document unchanged, nested containers and version vector included. The
//! cost is the row's size, never the collection's.

use nodedb_types::Surrogate;
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_crdt::state::RowWrite;

/// The block list a list op targets, and the document it lives in.
pub(in crate::data::executor) struct CrdtListTarget<'a> {
    pub collection: &'a str,
    pub document_id: &'a str,
    pub list_path: &'a str,
    /// The parent document's surrogate. Keys its sparse projection.
    pub surrogate: Surrogate,
}

impl CoreLoop {
    /// Insert a block (LoroMap) into a document's block list.
    pub(in crate::data::executor) fn execute_crdt_list_insert(
        &mut self,
        task: &ExecutionTask,
        target: CrdtListTarget<'_>,
        index: usize,
        fields: &[u8],
    ) -> Response {
        let CrdtListTarget {
            collection,
            document_id,
            list_path,
            ..
        } = target;
        debug!(core = self.core_id, %collection, %document_id, list_path, index, "crdt list insert");
        // A body that does not decode is refused. An empty block in its
        // place would drop the caller's fields.
        let fields = match nodedb_physical::physical_plan::decode_fields_body(fields) {
            Ok(fields) => fields,
            Err(detail) => {
                return self.response_error(
                    task,
                    ErrorCode::DataException {
                        detail: format!(
                            "list insert into '{collection}'/'{document_id}' at '{list_path}': \
                             {detail}"
                        ),
                    },
                );
            }
        };
        let fields = match nodedb_crdt::loro_value::fields_to_loro_map(&fields) {
            Ok(fields) => fields.into_iter().collect::<Vec<_>>(),
            Err(error) => return self.response_error(task, ErrorCode::from(&error)),
        };
        let write = RowWrite::ListInsert {
            list_path,
            index,
            fields: &fields,
        };
        self.land_list_write(task, &target, &write)
    }

    /// Delete a block from a document's block list.
    pub(in crate::data::executor) fn execute_crdt_list_delete(
        &mut self,
        task: &ExecutionTask,
        target: CrdtListTarget<'_>,
        index: usize,
    ) -> Response {
        let CrdtListTarget {
            collection,
            document_id,
            list_path,
            ..
        } = target;
        debug!(core = self.core_id, %collection, %document_id, list_path, index, "crdt list delete");
        self.land_list_write(task, &target, &RowWrite::ListDelete { list_path, index })
    }

    /// Move a block within a document's block list.
    pub(in crate::data::executor) fn execute_crdt_list_move(
        &mut self,
        task: &ExecutionTask,
        target: CrdtListTarget<'_>,
        from_index: usize,
        to_index: usize,
    ) -> Response {
        let CrdtListTarget {
            collection,
            document_id,
            list_path,
            ..
        } = target;
        debug!(core = self.core_id, %collection, %document_id, list_path, from_index, to_index, "crdt list move");
        let write = RowWrite::ListMove {
            list_path,
            from: from_index,
            to: to_index,
        };
        self.land_list_write(task, &target, &write)
    }

    /// Predict `write`, project the predicted row, then run `write`.
    ///
    /// A write that changes nothing (a move onto its own index) is checked
    /// against the row and answers Ok. It projects nothing, records no
    /// operation, and emits no change event.
    fn land_list_write(
        &mut self,
        task: &ExecutionTask,
        target: &CrdtListTarget<'_>,
        write: &RowWrite<'_>,
    ) -> Response {
        if write.changes_nothing() {
            let checked = self
                .get_crdt_engine(task.request.database_id, task.request.tenant_id)
                .and_then(|engine| {
                    engine.predict_row(target.collection, target.document_id, write)
                });
            return match checked {
                Ok(_) => self.response_ok(task),
                Err(error) => self.response_error(task, ErrorCode::from(error)),
            };
        }
        let row = super::crdt_materialize::ProjectedRow {
            collection: target.collection,
            document_id: target.document_id,
            surrogate: target.surrogate,
            index_text: true,
        };
        let landed = self
            .predict_row_body(task, &row, write)
            .and_then(|(predicted, body)| {
                self.land_row_write(task, &row, write, &predicted, &body)
            });
        match landed {
            Ok(()) => {
                self.checkpoint_coordinator.mark_dirty("crdt", 1);
                self.response_ok(task)
            }
            Err(error) => self.response_error(task, ErrorCode::from(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::types::{DatabaseId, TenantId};

    const COLL: &str = "pages";
    const DOC: &str = "p1";

    /// Seed `pages`/`p1` with a block list whose one block holds a nested
    /// list container, and a row field that is a binary no reader decodes,
    /// so every projection of the row fails.
    fn seed(core: &mut CoreLoop) {
        let doc = loro::LoroDoc::new();
        let row = doc
            .get_map(COLL)
            .insert_container(DOC, loro::LoroMap::new())
            .expect("row");
        row.insert("raw", loro::LoroValue::Binary(vec![0xc1].into()))
            .expect("raw field");
        let blocks = row
            .insert_container("blocks", loro::LoroMovableList::new())
            .expect("blocks");
        let block = blocks
            .insert_container(0, loro::LoroMap::new())
            .expect("block");
        block.insert("id", "b1").expect("block id");
        let children = block
            .insert_container("children", loro::LoroList::new())
            .expect("nested container");
        children.insert(0, "child").expect("child");
        doc.commit();
        let snapshot = doc.export(loro::ExportMode::Snapshot).expect("snapshot");
        core.get_crdt_engine(DatabaseId::DEFAULT, TenantId::new(1))
            .expect("engine")
            .apply_committed_delta(COLL, &snapshot)
            .expect("seed");
    }

    /// A move onto its own index answers Ok on an existing row and changes
    /// nothing: no projection, no new version, no change event. On an absent
    /// row it is refused like any list write.
    #[test]
    fn a_move_onto_its_own_index_changes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        seed(&mut core);
        let engine = core
            .get_crdt_engine(DatabaseId::DEFAULT, TenantId::new(1))
            .expect("engine");
        let version_before = engine.version_vector_json(COLL).expect("version");
        let snapshot_before = engine.read_snapshot(COLL, DOC).expect("snapshot");
        let target = |document_id: &'static str| CrdtListTarget {
            collection: COLL,
            document_id,
            list_path: "blocks",
            surrogate: Surrogate::new(9),
        };

        // The seeded row holds a field no projection decodes, so a projected
        // write fails. A same-index move projects nothing and succeeds.
        let response = core.execute_crdt_list_move(&task, target(DOC), 0, 0);
        assert_eq!(response.status, Status::Ok, "{response:?}");
        let engine = core
            .get_crdt_engine(DatabaseId::DEFAULT, TenantId::new(1))
            .expect("engine");
        assert_eq!(
            engine.version_vector_json(COLL).expect("version"),
            version_before,
            "no operation reached the document"
        );
        assert_eq!(
            engine.read_snapshot(COLL, DOC).expect("snapshot"),
            snapshot_before
        );
        let key = crate::engine::document::store::StorageKey::for_surrogate(Surrogate::new(9));
        assert!(
            core.sparse
                .get(DatabaseId::DEFAULT.as_u64(), 1, COLL, &key)
                .expect("read sparse")
                .is_none(),
            "nothing projected, so no change event either"
        );

        let response = core.execute_crdt_list_move(&task, target("absent"), 0, 0);
        assert_eq!(response.status, Status::Error, "an absent row is refused");
    }

    /// A delete whose projection fails leaves the row, its nested
    /// container included, and the document's history exactly as they were.
    #[test]
    fn a_failed_projection_leaves_the_row_unchanged() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        seed(&mut core);
        let engine = core
            .get_crdt_engine(DatabaseId::DEFAULT, TenantId::new(1))
            .expect("engine");
        let row_before = engine.read_row_deep(COLL, DOC);
        let version_before = engine.version_vector_json(COLL).expect("version");
        let snapshot_before = engine.read_snapshot(COLL, DOC).expect("snapshot");

        let response = core.execute_crdt_list_delete(
            &task,
            CrdtListTarget {
                collection: COLL,
                document_id: DOC,
                list_path: "blocks",
                surrogate: Surrogate::new(9),
            },
            0,
        );
        assert_eq!(response.status, Status::Error, "the projection fails");

        let engine = core
            .get_crdt_engine(DatabaseId::DEFAULT, TenantId::new(1))
            .expect("engine");
        assert_eq!(engine.read_row_deep(COLL, DOC), row_before, "row unchanged");
        assert_eq!(
            engine.version_vector_json(COLL).expect("version"),
            version_before,
            "no operation reached the document"
        );
        assert_eq!(
            engine.read_snapshot(COLL, DOC).expect("snapshot"),
            snapshot_before,
            "the document is byte-identical"
        );
    }
}
