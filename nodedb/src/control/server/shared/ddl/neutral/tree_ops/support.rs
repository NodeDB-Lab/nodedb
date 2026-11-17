// SPDX-License-Identifier: BUSL-1.1

//! Shared error constructor and BFS reader for the protocol-neutral tree-ops
//! handlers.

use nodedb_types::Value;

use super::super::super::result::DdlError;

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
///
/// The SQLSTATE and message reach the client unchanged.
pub(super) fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// The node ids a BFS traversal answers: a msgpack array of node id strings.
///
/// A graph traversal answers node ids, never document bodies, so the
/// payload decodes as plain typed values. An empty payload holds no nodes. A
/// payload that is not a node id array is an error.
pub(super) fn bfs_node_ids(payload: &[u8]) -> Result<Vec<String>, DdlError> {
    let decoded = crate::data::executor::response_codec::decode_payload_value(payload)
        .map_err(|e| DdlError::from_error_in_context("BFS result does not decode", &e))?;
    match decoded {
        Value::Null => Ok(Vec::new()),
        Value::Array(items) => Ok(items
            .into_iter()
            .filter_map(|item| match item {
                Value::String(id) => Some(id),
                _ => None,
            })
            .collect()),
        other => Err(DdlError::internal(format!(
            "BFS answered a {} instead of a node id array",
            other.type_name()
        ))),
    }
}
