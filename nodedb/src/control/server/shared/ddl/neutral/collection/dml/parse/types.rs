// SPDX-License-Identifier: BUSL-1.1

use std::collections::HashMap;

use crate::control::server::shared::ddl::result::DdlError;

/// One row of a parsed INSERT/UPSERT statement.
pub(in crate::control::server::shared::ddl::neutral::collection) struct ParsedRow {
    pub doc_id: String,
    pub fields: HashMap<String, nodedb_types::Value>,
}

/// Parsed INSERT/UPSERT statement fields.
pub(in crate::control::server::shared::ddl::neutral::collection) struct ParsedInsert {
    pub coll_name: String,
    /// Every row of the statement, in statement order. Never empty.
    pub rows: Vec<ParsedRow>,
    /// The raw column list written after `RETURNING`, when the statement has
    /// one. Carried (rather than reduced to a flag) because the statement is
    /// REBUILT from `fields` before planning: a flag would say the clause
    /// existed while the rebuilt SQL dropped it, which is how the clause used
    /// to be answered from this parse instead of from the stored row.
    pub returning_clause: Option<String>,
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
                        // A fractional literal reads as an exact decimal.
                        nodedb_types::Value::Decimal(d) => {
                            rust_decimal::prelude::ToPrimitive::to_f32(d)
                        }
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
    DdlError::new(sqlstate, message)
}
