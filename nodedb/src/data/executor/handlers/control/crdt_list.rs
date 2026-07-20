// SPDX-License-Identifier: BUSL-1.1

//! CRDT block-list handlers: insert / delete / move a block within a
//! document's Loro list container.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Insert a block (LoroMap) into a document's block list.
    pub(in crate::data::executor) fn execute_crdt_list_insert(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
        fields_json: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, list_path, index, "crdt list insert");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        let doc = match engine.collection_doc(collection) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Parse fields and insert as LoroMap container.
        let map = match nodedb_crdt::list_ops::list_insert_container(
            doc,
            collection,
            document_id,
            list_path,
            index,
        ) {
            Ok(m) => m,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Populate fields from JSON.
        if let Ok(fields) =
            sonic_rs::from_str::<serde_json::Map<String, serde_json::Value>>(fields_json)
        {
            for (key, val) in &fields {
                let loro_val = super::convert::json_to_loro_value(val);
                if let Err(e) = map.insert(key, loro_val) {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            }
        }

        self.response_ok(task)
    }

    /// Delete a block from a document's block list.
    pub(in crate::data::executor) fn execute_crdt_list_delete(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, list_path, index, "crdt list delete");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        let doc = match engine.collection_doc(collection) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match nodedb_crdt::list_ops::list_delete(doc, collection, document_id, list_path, index) {
            Ok(()) => self.response_ok(task),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Move a block within a document's block list.
    pub(in crate::data::executor) fn execute_crdt_list_move(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        list_path: &str,
        from_index: usize,
        to_index: usize,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, list_path, from_index, to_index, "crdt list move");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        let doc = match engine.collection_doc(collection) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match nodedb_crdt::list_ops::list_move(
            doc,
            collection,
            document_id,
            list_path,
            from_index,
            to_index,
        ) {
            Ok(()) => self.response_ok(task),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
