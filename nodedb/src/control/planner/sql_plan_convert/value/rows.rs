// SPDX-License-Identifier: BUSL-1.1

//! Batch-of-rows msgpack encoding for columnar INSERT paths.

use nodedb_sql::types::SqlValue;

use super::convert::sql_value_to_nodedb_value;

/// Encode already-expanded rows as one msgpack array of maps.
///
/// Callers materialize DEFAULTs through `expand_row_defaults` before routing,
/// so every column the declaration promises is already present in `rows`.
pub(crate) fn rows_to_msgpack_array(rows: &[&Vec<(String, SqlValue)>]) -> crate::Result<Vec<u8>> {
    let mut arr: Vec<nodedb_types::Value> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut map = std::collections::HashMap::new();
        for (key, val) in row.iter() {
            map.insert(key.clone(), sql_value_to_nodedb_value(val));
        }
        arr.push(nodedb_types::Value::Object(map));
    }
    let val = nodedb_types::Value::Array(arr);
    nodedb_types::value_to_msgpack(&val).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("columnar row batch: {e}"),
    })
}
