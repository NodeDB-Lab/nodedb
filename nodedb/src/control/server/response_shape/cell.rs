// SPDX-License-Identifier: BUSL-1.1

//! Row-level wrapper around the shared wire-JSON cell conversion.
//!
//! The scalar conversion itself, [`value_to_wire_json`], lives in
//! [`crate::util::wire_json`] — a neutral home so `control::security`
//! (which `response_shape` depends on for redaction) never has to depend
//! back on `control::server`. This module re-exports it and adds the
//! row-level helper, which depends on [`ShapedRow`].

use super::types::ShapedRow;

pub use crate::util::wire_json::value_to_wire_json;

/// Render one shaped row as a JSON object, cell by cell.
pub fn row_to_wire_json(row: &ShapedRow) -> serde_json::Map<String, serde_json::Value> {
    row.iter()
        .map(|(k, v)| (k.clone(), value_to_wire_json(v)))
        .collect()
}
