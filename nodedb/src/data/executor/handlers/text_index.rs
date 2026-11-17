// SPDX-License-Identifier: BUSL-1.1

//! Resolution of the inverted index a full-text read of one field runs
//! against. Shared by text search, phrase search, score columns, the text
//! leg of hybrid search, and the BM25 leg of graph RAG.

use nodedb_fts::IndexScope;
use nodedb_types::text_search::{TextColumnFault, WHOLE_DOCUMENT};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// The index a read of `field` runs against. `None` field reads the
    /// whole-document index.
    ///
    /// The collection's full-text field set decides the answer. Every text
    /// read reaches this, so it refuses a read no ready index serves. A
    /// named field resolves only when a ready index covers it. A field that
    /// only a building index covers is `TextColumn { IndexBuilding }`. The
    /// whole document resolves when any ready index exists, and its scope
    /// holds every indexed field. Every other read is
    /// `TextColumn { NotIndexed }`.
    pub(in crate::data::executor) fn text_index<'a>(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &'a str,
        field: Option<&'a str>,
    ) -> crate::Result<IndexScope<'a>> {
        let column_error = |column: &str, fault| crate::Error::TextColumn {
            collection: collection.to_owned(),
            column: column.to_owned(),
            fault,
        };
        let index = match field {
            None => IndexScope::document(collection),
            Some(name) => IndexScope::field(collection, name)
                .ok_or_else(|| column_error(name, TextColumnFault::NotAColumn))?,
        };
        let fault = match self.doc_config(task.request.database_id.as_u64(), tid, collection) {
            Some(config) => config.text_fields.read_fault(field),
            None => Some(TextColumnFault::NotIndexed),
        };
        match fault {
            None => Ok(index),
            Some(fault) => Err(column_error(field.unwrap_or(WHOLE_DOCUMENT), fault)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};

    const TID: u64 = 1;

    fn names(fields: &[&str]) -> Vec<String> {
        fields.iter().map(|f| f.to_string()).collect()
    }

    /// Register `notes` with ready full-text indexes on `text_fields`.
    fn declare_notes(core: &mut CoreLoop, text_fields: &[&str]) {
        declare_notes_building(core, &[], text_fields);
    }

    /// Register `notes` with building full-text indexes on `building` and
    /// ready ones on `ready`.
    fn declare_notes_building(core: &mut CoreLoop, building: &[&str], ready: &[&str]) {
        let mut config = crate::engine::document::store::CollectionConfig::new("notes");
        config.text_fields =
            nodedb_physical::physical_plan::TextFieldSet::new(names(building), names(ready));
        core.doc_configs.insert(
            (
                nodedb_types::DatabaseId::DEFAULT,
                nodedb_types::TenantId::new(TID),
                "notes".to_string(),
            ),
            config,
        );
    }

    fn not_indexed(column: &str) -> crate::Error {
        crate::Error::TextColumn {
            collection: "notes".into(),
            column: column.into(),
            fault: TextColumnFault::NotIndexed,
        }
    }

    #[test]
    fn a_declared_field_resolves_to_its_field_scope() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        declare_notes(&mut core, &["body", "title"]);
        let task = make_default_task();
        let index = core
            .text_index(&task, TID, "notes", Some("title"))
            .expect("declared field");
        assert_eq!(Some(index), IndexScope::field("notes", "title"));
        let whole = core
            .text_index(&task, TID, "notes", None)
            .expect("whole document");
        assert_eq!(whole, IndexScope::document("notes"));
    }

    #[test]
    fn an_undeclared_field_is_not_indexed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        declare_notes(&mut core, &["body"]);
        let task = make_default_task();
        let err = core
            .text_index(&task, TID, "notes", Some("title"))
            .expect_err("undeclared field");
        assert_eq!(err.to_string(), not_indexed("title").to_string());
    }

    #[test]
    fn the_whole_document_needs_a_declared_index() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        declare_notes(&mut core, &[]);
        let task = make_default_task();
        let err = core
            .text_index(&task, TID, "notes", None)
            .expect_err("no declared index");
        assert_eq!(err.to_string(), not_indexed(WHOLE_DOCUMENT).to_string());
    }

    /// The fault of a refused read, or a panic naming the result.
    fn fault_of(result: crate::Result<IndexScope<'_>>) -> (String, TextColumnFault) {
        match result {
            Err(crate::Error::TextColumn { column, fault, .. }) => (column, fault),
            other => panic!("expected a TextColumn error, got {other:?}"),
        }
    }

    #[test]
    fn a_building_field_read_is_refused_as_building() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        declare_notes_building(&mut core, &["title"], &["body"]);
        let task = make_default_task();
        assert_eq!(
            fault_of(core.text_index(&task, TID, "notes", Some("title"))),
            ("title".to_string(), TextColumnFault::IndexBuilding)
        );
        assert_eq!(
            fault_of(core.text_index(&task, TID, "notes", Some("abstract"))),
            ("abstract".to_string(), TextColumnFault::NotIndexed)
        );
        let ready = core
            .text_index(&task, TID, "notes", Some("body"))
            .expect("ready field");
        assert_eq!(Some(ready), IndexScope::field("notes", "body"));
    }

    #[test]
    fn the_whole_document_needs_a_ready_index() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        declare_notes_building(&mut core, &["title"], &[]);
        let task = make_default_task();
        assert_eq!(
            fault_of(core.text_index(&task, TID, "notes", None)),
            (WHOLE_DOCUMENT.to_string(), TextColumnFault::IndexBuilding)
        );
    }

    #[test]
    fn an_unregistered_collection_is_not_indexed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        assert_eq!(
            fault_of(core.text_index(&task, TID, "notes", Some("title"))),
            ("title".to_string(), TextColumnFault::NotIndexed)
        );
    }
}
