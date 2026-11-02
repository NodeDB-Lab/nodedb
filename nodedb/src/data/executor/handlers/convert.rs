// SPDX-License-Identifier: BUSL-1.1

//! CONVERT COLLECTION handler: re-encode documents for a new storage mode.
//!
//! Re-encodes every stored version of every row in place, so a read at any
//! earlier cut decodes its version in the new storage mode too.
//! For `TO strict`: injects each row's client-visible `id`, validates the
//! result against the schema, and encodes it as a Binary Tuple.
//! For `TO document` or `TO kv`: a Binary Tuple source re-encodes to
//! MessagePack. A schemaless source needs no re-encoding — the sparse
//! engine already stores it as MessagePack.
//! A row that fails to convert fails the whole statement: the handler
//! returns an error response instead of a success payload, so the caller
//! never flips the catalog's collection type over partially-converted data.

use sonic_rs;

use nodedb_physical::physical_plan::StorageMode;
use nodedb_query::msgpack_scan;
use nodedb_types::columnar::{ColumnDef, StrictSchema};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::sparse_body_format::SparseBodyFormat;
use crate::data::executor::task::ExecutionTask;

/// Map the plan's declared source storage mode to the row-decode format.
///
/// The plan carries this instead of the handler reading `doc_configs`: at
/// dispatch time that cache still describes the mode from BEFORE this
/// conversion, since the catalog flip and Data Plane re-register happen
/// only after this op returns successfully.
fn source_format_of(mode: &StorageMode) -> SparseBodyFormat {
    match mode {
        StorageMode::Strict { schema } => SparseBodyFormat::Strict(schema.clone()),
        StorageMode::Schemaless => SparseBodyFormat::Document,
    }
}

impl CoreLoop {
    /// Execute a collection conversion.
    ///
    /// - `TO document` / `TO kv`: re-encodes a Binary Tuple source to
    ///   MessagePack. A schemaless source is left untouched. Catalog update
    ///   happens on the Control Plane, after this returns successfully.
    /// - `TO strict`: re-encode each document as a Binary Tuple using the
    ///   provided schema. A document that fails to encode fails the
    ///   statement.
    pub(in crate::data::executor) fn execute_convert_collection(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        target_type: &str,
        schema_json: &str,
        source_storage_mode: &StorageMode,
    ) -> Response {
        tracing::debug!(
            core = self.core_id,
            %collection,
            target_type,
            "converting collection"
        );

        let source_format = source_format_of(source_storage_mode);
        // A collection holds its rows as versions. Each version is rewritten
        // in place, so its history survives the conversion.
        let bitemporal = self.is_bitemporal(task.request.database_id.as_u64(), tid, collection);
        self.convert_collection_versions(
            task,
            super::convert_versions::VersionConvert {
                tid,
                collection,
                target_type,
                schema_json,
                source_format,
                bitemporal,
            },
        )
    }
}

/// The target strict columns a CONVERT carries as JSON. An empty or
/// unparsable list is an internal error: the Control Plane built it.
pub(super) fn parse_target_columns(schema_json: &str) -> Result<Vec<ColumnDef>, ErrorCode> {
    let columns: Vec<ColumnDef> =
        sonic_rs::from_str(schema_json).map_err(|e| ErrorCode::Internal {
            detail: format!("invalid schema JSON: {e}"),
        })?;
    if columns.is_empty() {
        return Err(ErrorCode::Internal {
            detail: "schema must have at least one column".into(),
        });
    }
    Ok(columns)
}

/// The target schema's declared primary-key column, if it declares one.
pub(super) fn target_primary_key(schema: &StrictSchema) -> Option<&str> {
    schema
        .columns
        .iter()
        .find(|c| c.primary_key)
        .map(|c| c.name.as_str())
}

/// The column a converted row's identity lands in.
///
/// A system row id keys a schema with no declared key: it is an integer the
/// write path fills from the surrogate. Every other key column holds the
/// client-visible identity text.
pub(super) fn target_identity_column(declared_primary_key: Option<&str>) -> &str {
    match declared_primary_key {
        Some(key) if key == nodedb_types::ROWID_COLUMN => nodedb_types::ROWID_COLUMN,
        other => nodedb_types::declared_key(other).unwrap_or(nodedb_types::DEFAULT_IDENTITY_COLUMN),
    }
}

/// `normalized` with its identity under `identity_column`.
///
/// A row that lacks the target's key column has no client-visible identity
/// yet: the surrogate's decimal string fills it, so the target schema's NOT
/// NULL key column has something to validate. A row that carries its system
/// row id keeps it. One that lacks it takes the surrogate, as a write fills it.
pub(super) fn with_target_identity(
    normalized: &[u8],
    identity_column: &str,
    doc_id: &nodedb_types::StorageKey,
) -> Vec<u8> {
    if identity_column != nodedb_types::ROWID_COLUMN {
        let synth_id = doc_id.to_identity();
        return msgpack_scan::inject_str_field(normalized, identity_column, synth_id.as_str());
    }
    if msgpack_scan::extract_field(normalized, 0, nodedb_types::ROWID_COLUMN).is_some() {
        return normalized.to_vec();
    }
    let mut cell = Vec::with_capacity(9);
    msgpack_scan::writer::write_i64(&mut cell, i64::from(doc_id.surrogate().as_u32()));
    msgpack_scan::merge_fields(normalized, &[(nodedb_types::ROWID_COLUMN, cell.as_slice())])
}
