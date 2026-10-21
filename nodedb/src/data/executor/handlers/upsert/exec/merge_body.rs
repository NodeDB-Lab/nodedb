// SPDX-License-Identifier: BUSL-1.1

//! The body an upsert stores over an existing row, shared by the point
//! overwrite branch and the page handler.

use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::Value;
use nodedb_types::columnar::StrictSchema;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::handlers::upsert::merge::{apply_on_conflict_updates, merge_values};

/// The merged row an upsert stores, as the MessagePack body
/// `apply_point_put` takes for both storage modes, and the fields the merge
/// wrote.
pub(in crate::data::executor) struct MergedUpsert {
    pub body: Vec<u8>,
    /// The `ON CONFLICT DO UPDATE SET` targets, else every field of the
    /// incoming body. Typeguards judge them.
    pub assigned: Vec<String>,
}

/// Decode a stored row: a strict Binary Tuple through `strict_schema`, else
/// MessagePack. A strict row stored as MessagePack decodes too.
pub(in crate::data::executor) fn decode_stored_row(
    current_bytes: &[u8],
    strict_schema: Option<&StrictSchema>,
) -> Result<Value, ErrorCode> {
    if let Some(schema) = strict_schema
        && let Some(value) =
            crate::data::executor::strict_format::binary_tuple_to_value(current_bytes, schema)
    {
        return Ok(value);
    }
    nodedb_types::value_from_msgpack(current_bytes).map_err(|_| ErrorCode::Internal {
        detail: "failed to decode document for upsert".into(),
    })
}

/// Merge the incoming `value` into `existing`: the `ON CONFLICT DO UPDATE
/// SET` assignments evaluated against `existing` when there are any, else
/// the plain field merge `UPSERT INTO` uses.
pub(in crate::data::executor) fn merge_upsert_body(
    existing: Value,
    value: &[u8],
    on_conflict_updates: &[(String, UpdateValue)],
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<MergedUpsert, ErrorCode> {
    let new_val = nodedb_types::value_from_msgpack(value).map_err(|_| ErrorCode::Internal {
        detail: "failed to decode upsert value from msgpack".into(),
    })?;
    let assigned: Vec<String> = if on_conflict_updates.is_empty() {
        match &new_val {
            Value::Object(fields) => fields.keys().cloned().collect(),
            _ => Vec::new(),
        }
    } else {
        crate::data::executor::handlers::write_body::assigned_names(on_conflict_updates)
    };
    let merged = if on_conflict_updates.is_empty() {
        merge_values(existing, new_val)
    } else {
        apply_on_conflict_updates(existing, &new_val, on_conflict_updates, crs)?
    };
    // The merged row as a MessagePack body: `apply_point_put` encodes the
    // strict Binary Tuple, and stamps a bitemporal version, itself.
    let body = nodedb_types::value_to_msgpack(&merged).map_err(|_| ErrorCode::Internal {
        detail: "failed to encode merged upsert value".into(),
    })?;
    Ok(MergedUpsert { body, assigned })
}
