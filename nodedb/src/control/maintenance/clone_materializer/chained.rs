// SPDX-License-Identifier: BUSL-1.1

//! Clone materialization of a `HASH_CHAIN` document collection.
//!
//! Each row's link covers its stored contents and its position, so the clone
//! copies the rows as stored, in source position order, as relink requests:
//! redo records of origin `Restore`, committed through the target vShard's
//! apply log. Every replica links each row from the target head. The target is
//! empty, so the links it derives equal the source's.
//!
//! Each row installs at its source version's own stamps: the clone holds the
//! same logical version the source holds, so a snapshot that read the row
//! through the source reads the same row from the clone.
//!
//! A re-run after a crash sends the same rows in the same order. A row already
//! in the target is kept as installed, and the rest link after it.

use std::collections::HashSet;

use nodedb_types::{CollectionKey, DatabaseId, Surrogate, TenantId};

use crate::control::clone::copy::{CopyTarget, DocCopy, commit_doc_copies};
use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::state::SharedState;
use crate::types::DocVersionStamp;
use crate::types::hash_chain::CHAIN_SEQ_FIELD;

use super::document::{SourcePage, SourceRow, scan_page};

/// One scanned source row, before its target surrogate is bound.
struct ScannedRow {
    seq: u64,
    document_id: String,
    pk_bytes: Vec<u8>,
    body: Vec<u8>,
    stamp: DocVersionStamp,
}

fn clone_error(detail: String) -> crate::Error {
    crate::Error::Storage {
        engine: "clone_materializer".into(),
        detail,
    }
}

/// Copy every live source row of `coll` into the target in source position
/// order. Returns the rows copied.
pub(super) async fn materialize_chained_collection(
    state: &SharedState,
    catalog: &SystemCatalog,
    db_id: DatabaseId,
    coll: &StoredCollection,
    tombstoned: &HashSet<u32>,
    system_as_of_ms: Option<i64>,
) -> crate::Result<u64> {
    let Some(ref origin) = coll.cloned_from else {
        return Ok(0);
    };
    let tenant_id = TenantId::new(coll.tenant_id);
    let source_qualified = db_qualified(origin.source_database, &origin.source_collection);
    let target_qualified = db_qualified(db_id, &coll.name);
    let source_key = CollectionKey::from_bare(origin.source_database, &origin.source_collection);
    let target_key = CollectionKey::from_bare(db_id, &coll.name);

    let mut scanned: Vec<ScannedRow> = Vec::new();
    let mut cursor: Vec<u8> = Vec::new();
    loop {
        let page = SourcePage {
            tenant_id,
            source_db_id: origin.source_database,
            source_qualified: &source_qualified,
            cursor: &cursor,
            system_time: super::document::clone_scope(system_as_of_ms),
            raw_bodies: true,
        };
        let (entries, next_cursor) = scan_page(state, page, None).await?;
        for row in entries {
            let SourceRow {
                doc_id_hex,
                source_surrogate,
                body,
                stamp,
            } = row;
            // The catalog is read per row, so a tombstone recorded while the
            // copy runs counts.
            if tombstoned.contains(&source_surrogate)
                || catalog.is_clone_tombstoned(&target_qualified, source_surrogate)?
                || catalog
                    .get_clone_copyup(&target_qualified, source_surrogate)?
                    .is_some()
            {
                continue;
            }
            // An autocommit scan reads committed versions only, each stamped.
            let stamp = stamp.ok_or_else(|| {
                clone_error(format!(
                    "row {doc_id_hex} of hash-chained '{source_qualified}' carries no version \
                     stamp"
                ))
            })?;
            let doc = nodedb_types::json_from_msgpack(&body).map_err(|e| {
                clone_error(format!(
                    "row {doc_id_hex} of hash-chained '{source_qualified}' does not decode: {e}"
                ))
            })?;
            let seq = doc
                .get(CHAIN_SEQ_FIELD)
                .and_then(|seq| seq.as_u64())
                .ok_or_else(|| {
                    clone_error(format!(
                        "row {doc_id_hex} of hash-chained '{source_qualified}' has no unsigned \
                         integer '{CHAIN_SEQ_FIELD}' field"
                    ))
                })?;
            let pk_bytes = catalog
                .get_pk_for_surrogate(source_key, tenant_id, Surrogate::new(source_surrogate))
                .map_err(|e| {
                    clone_error(format!(
                        "get_pk_for_surrogate failed for surrogate {source_surrogate} in \
                         '{source_qualified}': {e}"
                    ))
                })?
                .unwrap_or_else(|| doc_id_hex.as_bytes().to_vec());
            // A chained schemaless row already carries `id` from its first
            // write, which the source link covers, so this leaves its bytes
            // unchanged and the relinked target link equals the source link.
            let body = match nodedb_types::StorageKey::parse(&doc_id_hex) {
                Some(storage_key) => {
                    let identity = nodedb_types::RowIdentity::of_stored_row(
                        &body,
                        coll.declared_primary_key.as_deref(),
                        storage_key,
                    );
                    crate::control::clone::identity::carry_identity(coll, body, identity.as_str())
                }
                None => body,
            };
            scanned.push(ScannedRow {
                seq,
                document_id: String::from_utf8_lossy(&pk_bytes).into_owned(),
                pk_bytes,
                body,
                stamp,
            });
        }
        if next_cursor.is_empty() {
            break;
        }
        cursor = next_cursor;
    }
    // Every row's target surrogate in one batch at the target collection's
    // home, under the same (collection, pk_bytes) keys the INSERT path uses.
    let pks: Vec<&[u8]> = scanned.iter().map(|row| row.pk_bytes.as_slice()).collect();
    let bound = crate::control::server::surrogate_exchange::assign_surrogates_routed(
        state,
        target_key,
        tenant_id,
        &pks,
        crate::types::TraceId::ZERO,
    )
    .await
    .map_err(|e| {
        clone_error(format!(
            "surrogate assign failed for the rows of '{target_qualified}': {e}"
        ))
    })?;
    super::status::check_bound_surrogates(&target_qualified, bound.len(), scanned.len())?;
    let mut rows: Vec<(u64, DocCopy)> = scanned
        .into_iter()
        .zip(bound)
        .map(|(row, target_surrogate)| {
            (
                row.seq,
                DocCopy {
                    document_id: row.document_id,
                    pk_bytes: row.pk_bytes,
                    target_surrogate,
                    body: row.body,
                    stamp: row.stamp,
                },
            )
        })
        .collect();
    rows.sort_by_key(|(seq, _)| *seq);
    let copied = rows.len() as u64;
    commit_doc_copies(
        state,
        CopyTarget {
            tenant_id,
            database_id: db_id,
            collection: &coll.name,
        },
        rows.into_iter().map(|(_, copy)| copy).collect(),
    )
    .await?;
    Ok(copied)
}
