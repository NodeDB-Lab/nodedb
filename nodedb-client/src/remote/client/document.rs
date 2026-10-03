// SPDX-License-Identifier: Apache-2.0

//! Document operation implementations for `NodeDbRemote`.
//!
//! A document's fields are the row's top-level columns, the shape the native
//! client and Lite store. The `id` column is the document id.

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::document_identity::is_identity_cell;
use crate::sql_escape::{quote_identifier, quote_string_literal};

use super::core::NodeDbRemote;

impl NodeDbRemote {
    /// Read every top-level field of the row whose `id` is `id`.
    ///
    /// The read runs on the simple-query protocol. A schemaless `SELECT *`
    /// has no catalog columns, so an extended-query Describe announces none
    /// and the driver drops every cell. The server renders each schemaless
    /// cell as text, so a non-string field reads back as its text form.
    pub(super) async fn document_get_impl(
        &self,
        collection: &str,
        id: &str,
    ) -> NodeDbResult<Option<Document>> {
        let sql = format!(
            "SELECT * FROM {} WHERE id = {}",
            quote_identifier(collection),
            quote_string_literal(id)
        );
        let (columns, rows) = self.simple_query_raw(&sql).await?;

        let mut rows = rows.into_iter();
        let Some(row) = rows.next() else {
            return Ok(None);
        };
        let extra = rows.count();
        if extra > 0 {
            return Err(NodeDbError::serialization(
                "pgwire",
                format!(
                    "document_get '{collection}'/'{id}': expected at most one row, got {}",
                    extra + 1
                ),
            ));
        }

        let mut doc = Document::new(id);
        for (name, value) in columns.into_iter().zip(row) {
            if !is_identity_cell(&name, &value, id) {
                doc.set(name, value);
            }
        }
        Ok(Some(doc))
    }

    pub(super) async fn document_delete_impl(
        &self,
        collection: &str,
        id: &str,
    ) -> NodeDbResult<()> {
        let collection = quote_identifier(collection);
        let sql = format!("DELETE FROM {collection} WHERE id = $1");
        self.execute_raw(&sql, &[&id]).await?;
        Ok(())
    }
}
