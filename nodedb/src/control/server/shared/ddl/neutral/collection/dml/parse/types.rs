// SPDX-License-Identifier: BUSL-1.1

use std::collections::HashMap;

use crate::control::server::shared::ddl::result::DdlError;

/// Parsed INSERT/UPSERT statement fields.
pub(in crate::control::server::shared::ddl::neutral::collection) struct ParsedInsert {
    pub coll_name: String,
    pub doc_id: String,
    pub fields: HashMap<String, nodedb_types::Value>,
    pub has_returning: bool,
    /// Collection type looked up from the catalog. Drives the write plan.
    pub collection_type: Option<nodedb_types::CollectionType>,
}

pub(in crate::control::server::shared::ddl::neutral::collection) fn extract_vector_fields(
    fields: &HashMap<String, nodedb_types::Value>,
) -> Vec<(String, Vec<f32>)> {
    fields
        .iter()
        .filter_map(|(field_name, value)| match value {
            nodedb_types::Value::Array(items) => {
                let vector: Vec<f32> = items
                    .iter()
                    .map(|item| match item {
                        nodedb_types::Value::Float(v) => Some(*v as f32),
                        nodedb_types::Value::Integer(v) => Some(*v as f32),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some((field_name.clone(), vector))
            }
            _ => None,
        })
        .collect()
}

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
pub(super) fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}
