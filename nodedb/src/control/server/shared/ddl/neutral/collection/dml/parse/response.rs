// SPDX-License-Identifier: BUSL-1.1

use crate::control::server::shared::ddl::result::{DdlError, DdlResult};

/// Format a RETURNING response from parsed fields.
pub(in crate::control::server::shared::ddl::neutral::collection) fn returning_response(
    doc_id: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> Result<Vec<DdlResult>, DdlError> {
    use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
    use serde_json::{Map, Value as JsonValue};

    let mut result_doc = fields.clone();
    result_doc.insert(
        "id".to_string(),
        nodedb_types::Value::String(doc_id.to_string()),
    );
    let json_str =
        sonic_rs::to_string(&nodedb_types::Value::Object(result_doc)).unwrap_or_default();

    let mut row = Map::new();
    row.insert("result".to_string(), JsonValue::String(json_str));

    Ok(vec![DdlResult::Rows(ShapedRows {
        columns: vec!["result".to_string()],
        column_types: vec![DdlColType::Text],
        rows: vec![row],
        notice: None,
    })])
}
