// SPDX-License-Identifier: BUSL-1.1

//! Conversion utilities for CRDT value types.

/// The stored body of a CRDT row whose Loro map holds `row`: a native object
/// `[7, {field: tagged value}]`, before the put path canonicalizes it. Each
/// field decodes through `nodedb_crdt::loro_value`, so a typed value in the
/// row (a block-list field among them) keeps its exact type. The live
/// materialization (`encode_crdt_row`) and the transaction staging path
/// (`stage_crdt`) both build a row body here, so a staged row and its COMMIT
/// replay agree byte for byte.
pub(in crate::data::executor) fn crdt_row_body(row: &loro::LoroValue) -> crate::Result<Vec<u8>> {
    let value = nodedb_crdt::loro_value::loro_to_value(row).map_err(crate::Error::Crdt)?;
    zerompk::to_msgpack_vec(&value).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("crdt row body: {e}"),
    })
}
