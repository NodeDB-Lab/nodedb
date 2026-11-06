// SPDX-License-Identifier: BUSL-1.1

//! CONVERT COLLECTION: rewrite every stored version in the target encoding.
//!
//! Every document collection holds its rows as versions. The conversion
//! rewrites every live version in place, in the target encoding, under its
//! own system time and valid interval. Tombstone versions stay as they are.
//! The current read and every `AS OF SYSTEM TIME` read then return the same
//! rows as before, decoded in the new storage mode.
//!
//! A `bitemporal=true` collection converts to a schemaless or a bitemporal
//! strict document target. A key-value target is refused for it before a
//! version moves: the target keeps no valid time, and it purges its versions
//! at the cleanup horizon with no audit retention. Any other collection also
//! converts to a key-value target, whose rows stay MessagePack.

use std::collections::{HashMap, HashSet};

use nodedb_types::StorageKey;
use nodedb_types::columnar::StrictSchema;

use super::convert::{
    parse_target_columns, target_identity_column, target_primary_key, with_target_identity,
};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::scan_normalize::sparse_body_to_msgpack;
use crate::data::executor::sparse_body_format::SparseBodyFormat;
use crate::data::executor::strict_format;
use crate::data::executor::task::ExecutionTask;
use crate::engine::sparse::btree::storage_conversion::StorageConversionRecord;
use crate::engine::sparse::btree_versioned::{VersionedPut, VersionedScanParams};

/// Inputs to [`CoreLoop::convert_collection_versions`].
pub(in crate::data::executor) struct VersionConvert<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub target_type: &'a str,
    /// The target strict columns as JSON. Empty for a schemaless target.
    pub schema_json: &'a str,
    /// How the source versions are encoded.
    pub source_format: SparseBodyFormat,
    /// The collection is `bitemporal=true`: a strict target carries the
    /// temporal columns.
    pub bitemporal: bool,
    /// The conversion record, committed with the rewrite.
    pub record: StorageConversionRecord,
}

/// The encoding a version takes in the target.
enum VersionTarget {
    /// Schemaless MessagePack.
    Schemaless,
    /// A strict schema, with the column a row's identity lands in. A
    /// bitemporal schema stamps each version's temporal columns.
    Strict {
        schema: StrictSchema,
        identity_column: String,
    },
}

impl CoreLoop {
    /// Convert a collection: rewrite every live version in the target
    /// encoding. Returns `{"converted": N}`, where N counts the live rows.
    pub(in crate::data::executor) fn convert_collection_versions(
        &mut self,
        task: &ExecutionTask,
        c: VersionConvert<'_>,
    ) -> Response {
        let converted = version_target(c.collection, c.target_type, c.schema_json, c.bitemporal)
            .and_then(|target| self.rewrite_versions(task, &c, &target));
        let converted = match converted {
            Ok(converted) => converted,
            Err(code) => return self.response_error(task, code),
        };
        tracing::info!(collection = %c.collection, converted, "collection converted");
        self.take_converted_mode(
            task.request.database_id.as_u64(),
            c.tid,
            c.collection,
            &c.record.target,
        );
        self.converted_response(task, converted, c.target_type, c.collection)
    }

    /// Rewrite every live version of the collection in `target`'s encoding,
    /// in one transaction. The current version of each live row also
    /// re-indexes its text and refreshes the document cache.
    ///
    /// Returns the number of live rows.
    fn rewrite_versions(
        &mut self,
        task: &ExecutionTask,
        c: &VersionConvert<'_>,
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
            let txn = self.sparse.begin_write()?;
            self.sparse.put_storage_conversion_in_txn(
                &txn,
                database_id,
                tid,
                collection,
                &c.record,
            )?;
            txn.commit().map_err(|e| ErrorCode::Internal {
                detail: format!("convert commit: {e}"),
            })?;
            return Ok(live.len() as u64);
        }

        let txn = self.sparse.begin_write()?;
        self.sparse
            .put_storage_conversion_in_txn(&txn, database_id, tid, collection, &c.record)?;
        let mut current_bodies: Vec<(StorageKey, i64, Vec<u8>)> = Vec::with_capacity(live.len());
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
                    let encoded = if schema.bitemporal {
                        strict_format::bytes_to_binary_tuple_bitemporal(
                            &with_id,
                            schema,
                            version.system_from,
                            version.valid_from_ms,
                            version.valid_until_ms,
                            collection,
                        )
                    } else {
                        strict_format::bytes_to_binary_tuple(&with_id, schema, collection)
                    };
                    let tuple = encoded.map_err(|e| {
                        strict_encode_error(
                            e,
                            &msgpack,
                            target_primary_key(schema),
                            &version.doc_id,
                        )
                    })?;
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
                        ordinal: version.system_from,
                    },
                )?;
                current_bodies.push((version.doc_id, version.system_from, stored));
            }
        }
        txn.commit().map_err(|e| ErrorCode::Internal {
            detail: format!("convert commit: {e}"),
        })?;
        // Write-through: a point-get after this statement sees the converted
        // current version, never a cache entry from before the conversion.
        for (doc_id, sys_from, body) in &current_bodies {
            self.doc_cache
                .put(database_id, tid, collection, doc_id, *sys_from, body);
        }
        Ok(live.len() as u64)
    }
}

/// The encoding a CONVERT target gives a collection's versions.
///
/// A key-value target keeps row versions without valid time, and purges them
/// at the cleanup horizon with no `audit_retain_ms` retention. A
/// `bitemporal=true` collection's conversion to it is therefore refused with
/// nothing moved: the conversion drops the valid time and the audit history
/// the collection promises to keep.
fn version_target(
    collection: &str,
    target_type: &str,
    schema_json: &str,
    bitemporal: bool,
) -> Result<VersionTarget, ErrorCode> {
    match target_type {
        "document_schemaless" => Ok(VersionTarget::Schemaless),
        "kv" if !bitemporal => Ok(VersionTarget::Schemaless),
        "document_strict" => {
            let columns = parse_target_columns(schema_json)?;
            let schema = if bitemporal {
                StrictSchema::new_bitemporal(columns).map_err(|e| ErrorCode::DataException {
                    detail: format!("bitemporal strict target schema: {e}"),
                })?
            } else {
                StrictSchema {
                    columns,
                    version: 1,
                    dropped_columns: Vec::new(),
                    bitemporal: false,
                }
            };
            let identity_column = target_identity_column(target_primary_key(&schema)).to_string();
            Ok(VersionTarget::Strict {
                schema,
                identity_column,
            })
        }
        "kv" => Err(ErrorCode::Unsupported {
            detail: format!(
                "CONVERT of bitemporal collection '{collection}' to 'kv' is refused because a \
                 key-value collection keeps no valid time and purges its row versions at the \
                 cleanup horizon, so convert it to 'document_schemaless' or 'document_strict' \
                 to keep its audit history"
            ),
        }),
        other => Err(ErrorCode::Internal {
            detail: format!("unsupported conversion target: {other}"),
        }),
    }
}

/// The error a strict target answers a version it cannot encode with.
///
/// A version carrying a field the target schema does not declare is a schema
/// mismatch the client caused, not an internal fault: it names the offending
/// row and column as SQLSTATE 22000 (data_exception). The row is named by the
/// identity a user recognizes, read from the version itself.
fn strict_encode_error(
    e: crate::Error,
    msgpack: &[u8],
    declared_primary_key: Option<&str>,
    doc_id: &StorageKey,
) -> ErrorCode {
    match e {
        crate::Error::UnknownStrictField {
            collection, column, ..
        } => {
            let identity =
                nodedb_types::RowIdentity::of_stored_row(msgpack, declared_primary_key, *doc_id);
            ErrorCode::DataException {
                detail: format!(
                    "column \"{column}\" of collection \"{collection}\" does not exist \
                     (row \"{identity}\")"
                ),
            }
        }
        other => ErrorCode::from(other),
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
