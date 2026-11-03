// SPDX-License-Identifier: BUSL-1.1

//! Narrow rows for a sorted indexed fetch over a strict collection.
//!
//! An ordered fetch gathers every row that passes the residual and keeps
//! `offset + limit` of them. Converting each candidate to a full msgpack row
//! costs one value per column, the widest of which can be an embedding. The
//! sort reads only the columns its keys name, so candidates sort as rows that
//! hold just those columns, and only the rows the sort keeps convert in full.

use std::collections::HashMap;

use nodedb_physical::physical_plan::SortKeySpec;
use nodedb_types::StorageKey;
use nodedb_types::columnar::StrictSchema;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::row_shape::inject_row_identity;
use crate::data::executor::strict_format::{
    binary_tuple_columns_to_msgpack, binary_tuple_to_msgpack,
};

/// The stored bodies of a sorted fetch's candidates, and the columns their
/// narrow rows carry.
pub(super) struct NarrowSort<'s> {
    schema: &'s StrictSchema,
    collection: &'s str,
    columns: Vec<String>,
    bodies: HashMap<String, (StorageKey, Vec<u8>)>,
}

impl<'s> NarrowSort<'s> {
    pub(super) fn new(
        schema: &'s StrictSchema,
        collection: &'s str,
        sort_keys: &[SortKeySpec],
    ) -> Self {
        let mut columns: Vec<String> = Vec::new();
        for key in sort_keys {
            for column in nodedb_query::expr_parse::referenced_columns(&key.expr) {
                if !columns.contains(&column) {
                    columns.push(column);
                }
            }
        }
        Self {
            schema,
            collection,
            columns,
            bodies: HashMap::new(),
        }
    }

    /// The narrow row of a candidate. Keeps the stored `body` for
    /// [`Self::full_rows`].
    pub(super) fn narrow_row(
        &mut self,
        doc_id: &StorageKey,
        body: Vec<u8>,
        identity_column: &str,
    ) -> Result<Vec<u8>, ErrorCode> {
        let narrow = binary_tuple_columns_to_msgpack(&body, self.schema, &self.columns)
            .ok_or_else(|| self.undecodable(doc_id))?;
        let narrow = inject_row_identity(&narrow, doc_id, identity_column).into_owned();
        self.bodies.insert(doc_id.to_string(), (*doc_id, body));
        Ok(narrow)
    }

    /// The full rows of the candidates the sort kept, in the order it left
    /// them.
    pub(super) fn full_rows(
        &self,
        kept: Vec<(String, Vec<u8>)>,
        identity_column: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, ErrorCode> {
        kept.into_iter()
            .map(|(id, _)| {
                let (key, body) = self.bodies.get(&id).ok_or_else(|| ErrorCode::Internal {
                    detail: format!(
                        "indexed fetch on '{}': sorted row '{id}' has no stored body",
                        self.collection
                    ),
                })?;
                let full = binary_tuple_to_msgpack(body, self.schema)
                    .ok_or_else(|| self.undecodable(&id))?;
                let full = inject_row_identity(&full, key, identity_column).into_owned();
                Ok((id, full))
            })
            .collect()
    }

    fn undecodable(&self, doc_id: impl std::fmt::Display) -> ErrorCode {
        ErrorCode::Internal {
            detail: format!(
                "indexed fetch on '{}': stored body of '{doc_id}' does not decode under the \
                 strict schema",
                self.collection
            ),
        }
    }
}
