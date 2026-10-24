// SPDX-License-Identifier: BUSL-1.1

//! CONVERT COLLECTION on a bitemporal collection.
//!
//! A bitemporal collection holds its rows only as versions, and its history
//! is append-only. The conversion rewrites every live version in place, in
//! the target encoding, under its own system time and valid interval.
//! Tombstone versions stay as they are. The current read and every
//! `AS OF SYSTEM TIME` read then return the same rows as before.
//!
//! A schemaless or strict document target holds versions. Any other target
//! is refused before a version moves.

use std::collections::{HashMap, HashSet};

use nodedb_types::StorageKey;
use nodedb_types::columnar::StrictSchema;

use super::convert::{
    parse_target_columns, target_identity_column, target_primary_key, with_target_identity,
};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::scan_normalize::sparse_body_to_msgpack;
use crate::data::executor::sparse_body_format::SparseBodyFormat;
use crate::data::executor::strict_format;
use crate::data::executor::task::ExecutionTask;
use crate::engine::sparse::btree_versioned::{VersionedPut, VersionedScanParams};

/// Inputs to [`CoreLoop::convert_bitemporal_collection`].
pub(in crate::data::executor) struct BitemporalConvert<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub target_type: &'a str,
    /// The target strict columns as JSON. Empty for a schemaless target.
    pub schema_json: &'a str,
    /// How the source versions are encoded.
    pub source_format: SparseBodyFormat,
}

/// The encoding a version takes in the target.
enum VersionTarget {
    /// Schemaless MessagePack.
    Schemaless,
    /// A bitemporal strict schema, with the column a row's identity lands in.
    Strict {
        schema: StrictSchema,
        identity_column: String,
    },
}

impl CoreLoop {
    /// Convert a bitemporal collection: rewrite every live version in the
    /// target encoding. Returns `{"converted": N}`, where N counts the live
    /// rows.
    pub(in crate::data::executor) fn convert_bitemporal_collection(
        &mut self,
        task: &ExecutionTask,
        c: BitemporalConvert<'_>,
    ) -> Response {
        let converted = version_target(c.target_type, c.schema_json)
            .and_then(|target| self.rewrite_versions(task, &c, &target));
        let converted = match converted {
            Ok(converted) => converted,
            Err(code) => return self.response_error(task, code),
        };
        tracing::info!(collection = %c.collection, converted, "bitemporal collection converted");
        let result = serde_json::json!({
            "converted": converted,
            "target_type": c.target_type,
            "collection": c.collection,
        });
        match response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Rewrite every live version of the collection in `target`'s encoding,
    /// in one transaction. The current version of each live row also
    /// re-indexes its text and refreshes the document cache.
    ///
    /// Returns the number of live rows.
    fn rewrite_versions(
        &mut self,
        task: &ExecutionTask,
        c: &BitemporalConvert<'_>,
        target: &VersionTarget,
    ) -> Result<u64, ErrorCode> {
        let database_id = task.request.database_id.as_u64();
        let (tid, collection) = (c.tid, c.collection);
        let versions = self.sparse.versioned_scan_all(
            VersionedScanParams {
                database_id,
                tenant: tid,
                coll: collection,
                sys_cutoff: None,
                valid_at_ms: None,
                limit: usize::MAX,
            },
            &|_: &StorageKey, _: &[u8]| true,
            &crate::engine::sparse::scan_stop::never_stop,
        )?;
        // A delete version has no body to rewrite: its tombstone stays.
        let versions: Vec<_> = versions.into_iter().filter(|v| !v.deleted).collect();
        let live: HashSet<StorageKey> = self
            .current_rows(database_id, tid, collection, usize::MAX)?
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        // A live row reads its newest live version.
        let mut current_version: HashMap<StorageKey, i64> = HashMap::new();
        for version in versions.iter().filter(|v| live.contains(&v.doc_id)) {
            let newest = current_version
                .entry(version.doc_id)
                .or_insert(version.system_from);
            *newest = (*newest).max(version.system_from);
        }
        // A schemaless source already stores what a schemaless target holds.
        if matches!(target, VersionTarget::Schemaless)
            && !matches!(c.source_format, SparseBodyFormat::Strict(_))
        {
            return Ok(live.len() as u64);
        }

        let txn = self.sparse.begin_write()?;
        let mut current_bodies: Vec<(StorageKey, Vec<u8>)> = Vec::with_capacity(live.len());
        for version in &versions {
            let msgpack = version_msgpack(&version.body, &c.source_format).ok_or_else(|| {
                strict_format::undecodable_strict_row(
                    collection,
                    version.doc_id.to_identity().as_str(),
                )
            })?;
            // `visible` is the row as a read sees it: the text index follows it.
            let (stored, visible) = match target {
                VersionTarget::Schemaless => (msgpack.clone(), msgpack),
                VersionTarget::Strict {
                    schema,
                    identity_column,
                } => {
                    let with_id = with_target_identity(&msgpack, identity_column, &version.doc_id);
                    let tuple = strict_format::bytes_to_binary_tuple_bitemporal(
                        &with_id,
                        schema,
                        version.system_from,
                        version.valid_from_ms,
                        version.valid_until_ms,
                        collection,
                    )?;
                    let visible = strict_format::binary_tuple_to_row_value(&tuple, schema)
                        .and_then(|row| nodedb_types::value_to_msgpack(&row).ok())
                        .ok_or_else(|| {
                            strict_format::undecodable_strict_row(
                                collection,
                                version.doc_id.to_identity().as_str(),
                            )
                        })?;
                    (tuple, visible)
                }
            };
            self.sparse.versioned_put_in_txn(
                &txn,
                VersionedPut {
                    database_id,
                    tenant: tid,
                    coll: collection,
                    doc_id: &version.doc_id,
                    sys_from: version.system_from,
                    valid_from_ms: version.valid_from_ms,
                    valid_until_ms: version.valid_until_ms,
                    body: &stored,
                },
            )?;
            if current_version.get(&version.doc_id) == Some(&version.system_from) {
                let new_doc = crate::data::executor::doc_format::decode_document(&visible)?;
                self.update_reindex_text(
                    &txn,
                    super::point::update_reindex_text::UpdateTextReindex {
                        database_id,
                        tid,
                        collection,
                        surrogate: version.doc_id.surrogate(),
                        new_doc: &new_doc,
                    },
                )?;
                current_bodies.push((version.doc_id, stored));
            }
        }
        txn.commit().map_err(|e| ErrorCode::Internal {
            detail: format!("bitemporal convert commit: {e}"),
        })?;
        // Write-through: a point-get after this statement sees the converted
        // current version, never a cache entry from before the conversion.
        for (doc_id, body) in &current_bodies {
            self.doc_cache
                .put(database_id, tid, collection, doc_id, body);
        }
        Ok(live.len() as u64)
    }
}

/// The encoding a CONVERT target gives a bitemporal collection's versions.
///
/// A key-value target holds no row versions, so it is refused with nothing
/// moved: converting would erase the collection's history.
fn version_target(target_type: &str, schema_json: &str) -> Result<VersionTarget, ErrorCode> {
    match target_type {
        "document_schemaless" => Ok(VersionTarget::Schemaless),
        "document_strict" => {
            let columns = parse_target_columns(schema_json)?;
            let schema =
                StrictSchema::new_bitemporal(columns).map_err(|e| ErrorCode::DataException {
                    detail: format!("bitemporal strict target schema: {e}"),
                })?;
            let identity_column = target_identity_column(target_primary_key(&schema)).to_string();
            Ok(VersionTarget::Strict {
                schema,
                identity_column,
            })
        }
        other => Err(ErrorCode::Unsupported {
            detail: format!(
                "CONVERT of a bitemporal collection to '{other}': the target holds no row \
                 versions, so the collection's history would be lost"
            ),
        }),
    }
}

/// A source version as MessagePack, without the reserved bitemporal columns
/// a strict tuple carries. `None` when a strict tuple does not decode.
fn version_msgpack(body: &[u8], source: &SparseBodyFormat) -> Option<Vec<u8>> {
    match source {
        SparseBodyFormat::Strict(schema) => {
            let row = strict_format::binary_tuple_to_row_value(body, schema)?;
            nodedb_types::value_to_msgpack(&row).ok()
        }
        SparseBodyFormat::Document | SparseBodyFormat::VectorSidecar => {
            Some(sparse_body_to_msgpack(body, source.as_format_ref()).into_owned())
        }
    }
}
