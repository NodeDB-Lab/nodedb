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
//!
//! Every replica runs this at one position of the data-group log. No input
//! comes from a clock or from this node, so every replica stores the same bytes.

use sonic_rs;

use nodedb_physical::physical_plan::StorageMode;
use nodedb_query::msgpack_scan;
use nodedb_types::columnar::{ColumnDef, StrictSchema};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::sparse_body_format::SparseBodyFormat;
use crate::data::executor::task::ExecutionTask;
use crate::engine::sparse::btree::storage_conversion::StorageConversionRecord;

/// Map the plan's declared source storage mode to the row-decode format.
///
/// The plan carries this instead of the handler reading `doc_configs`, so every
/// replica decodes with the mode its proposer read from the catalog.
fn source_format_of(mode: &StorageMode) -> SparseBodyFormat {
    match mode {
        StorageMode::Strict { schema } => SparseBodyFormat::Strict(schema.clone()),
        StorageMode::Schemaless => SparseBodyFormat::Document,
    }
}

/// Borrowed arguments for [`CoreLoop::execute_convert_collection`].
pub(in crate::data::executor) struct ConvertParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub target_type: &'a str,
    pub schema_json: &'a str,
    pub source_storage_mode: &'a StorageMode,
    pub target_storage_mode: &'a StorageMode,
    pub generation: u64,
    pub incarnation: nodedb_types::Hlc,
}

impl CoreLoop {
    /// Execute a collection conversion at its data-group log position.
    ///
    /// - `TO document` / `TO kv`: re-encodes a Binary Tuple source to
    ///   MessagePack. A schemaless source is left untouched. The catalog update
    ///   follows on the metadata log.
    /// - `TO strict`: re-encode each document as a Binary Tuple using the
    ///   provided schema. A document that fails to encode fails the
    ///   conversion on every replica.
    ///
    /// The rewrite and the conversion record commit together. A core that holds
    /// this generation's record answers without a rewrite, so a re-delivery and
    /// a retried statement find the work done. A record of this generation with
    /// another target is a second conversion that raced this one, and it refuses.
    pub(in crate::data::executor) fn execute_convert_collection(
        &mut self,
        task: &ExecutionTask,
        params: ConvertParams<'_>,
    ) -> Response {
        let ConvertParams {
            tid,
            collection,
            target_type,
            schema_json,
            source_storage_mode,
            target_storage_mode,
            generation,
            incarnation,
        } = params;
        tracing::debug!(
            core = self.core_id,
            %collection,
            target_type,
            generation,
            "converting collection"
        );

        let database_id = task.request.database_id.as_u64();
        let applied = match self
            .sparse
            .get_storage_conversion(database_id, tid, collection)
        {
            Ok(record) => record.filter(|record| record.incarnation == incarnation),
            Err(e) => return self.response_error(task, ErrorCode::from(e)),
        };
        if let Some(applied) = applied.filter(|record| record.generation >= generation) {
            if applied.target != *target_storage_mode {
                return self.response_error(
                    task,
                    ErrorCode::Unsupported {
                        detail: format!(
                            "CONVERT of collection '{collection}' is refused because another \
                             CONVERT of it committed with a different target, so run that \
                             statement again to finish it and then convert from its target"
                        ),
                    },
                );
            }
            self.take_converted_mode(database_id, tid, collection, target_storage_mode);
            return self.converted_response(task, 0, target_type, collection);
        }

        let source_format = source_format_of(source_storage_mode);
        // A collection holds its rows as versions. Each version is rewritten
        // in place, so its history survives the conversion.
        let bitemporal = self.is_bitemporal(database_id, tid, collection);
        self.convert_collection_versions(
            task,
            super::convert_versions::VersionConvert {
                tid,
                collection,
                target_type,
                schema_json,
                source_format,
                bitemporal,
                record: StorageConversionRecord {
                    generation,
                    incarnation,
                    target: target_storage_mode.clone(),
                },
            },
        )
    }

    /// Give `collection`'s config on this core the converted mode, so a later
    /// entry of the log encodes in it.
    pub(super) fn take_converted_mode(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        target: &StorageMode,
    ) {
        let key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        if let Some(config) = self.doc_configs.get_mut(&key) {
            config.storage_mode = target.clone();
        }
    }

    /// The conversion's answer: `{"converted": N}`.
    pub(super) fn converted_response(
        &mut self,
        task: &ExecutionTask,
        converted: u64,
        target_type: &str,
        collection: &str,
    ) -> Response {
        let result = serde_json::json!({
            "converted": converted,
            "target_type": target_type,
            "collection": collection,
        });
        match crate::data::executor::response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
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
