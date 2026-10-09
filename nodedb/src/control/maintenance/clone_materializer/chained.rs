// SPDX-License-Identifier: BUSL-1.1

//! Clone materialization of a `HASH_CHAIN` document collection.
//!
//! Each row's link covers its stored contents and its position, so the clone
//! copies the rows as stored, in source position order, as relink requests:
//! redo records of origin `Restore`, committed through the target vShard's
//! apply log. Every replica links each row from the target head. The target is
//! empty, so the links it derives equal the source's.
//!
//! A re-run after a crash sends the same rows in the same order. A row already
//! in the target is kept as installed, and the rest link after it.

use std::collections::HashSet;

use nodedb_physical::physical_plan::RedoOrigin;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_types::{CollectionKey, DatabaseId, Surrogate, TenantId};
use nodedb_wal::record::RecordType;

use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::state::SharedState;
use crate::control::surrogate::CarriedIdentity;
use crate::control::wal_replication::encode::transaction_redo_entry;
use crate::control::wal_replication::propose_replicated_entry;
use crate::control::wal_replication::transaction_redo::{RedoTarget, TransactionRedoPayload};
use crate::event::EventSource;
use crate::types::hash_chain::CHAIN_SEQ_FIELD;
use crate::types::{HomedRecord, RecordHomes};
use crate::wal::{RedoRecord, RedoRowChange, RedoRowKind, RedoSubRecord};

use super::document::{SourcePage, scan_page};

/// Most rows one relink record carries.
const ROWS_PER_RECORD: usize = 512;

/// One scanned source row, before its target surrogate is bound.
struct ScannedRow {
    seq: u64,
    document_id: String,
    pk_bytes: Vec<u8>,
    body: Vec<u8>,
}

/// One source row, ready to copy under its bound target surrogate.
struct ChainedRow {
    seq: u64,
    document_id: String,
    pk_bytes: Vec<u8>,
    target_surrogate: Surrogate,
    body: Vec<u8>,
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
            system_as_of_ms,
            raw_bodies: true,
        };
        let (entries, next_cursor) = scan_page(state, page, None).await?;
        for (doc_id_hex, source_surrogate, body) in entries {
            if tombstoned.contains(&source_surrogate)
                || catalog
                    .get_clone_copyup(&target_qualified, source_surrogate)?
                    .is_some()
            {
                continue;
            }
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
    let mut rows: Vec<ChainedRow> = scanned
        .into_iter()
        .zip(bound)
        .map(|(row, target_surrogate)| ChainedRow {
            seq: row.seq,
            document_id: row.document_id,
            pk_bytes: row.pk_bytes,
            target_surrogate,
            body: row.body,
        })
        .collect();
    rows.sort_by_key(|row| row.seq);

    let target = RedoTarget {
        tenant_id,
        database_id: db_id,
        vshard_id: RecordHomes::of(HomedRecord::Row(target_key)).owner(),
    };
    let stored = nodedb_types::QualifiedCollection::new(db_id, &coll.name);
    let copied = rows.len() as u64;
    let mut pending = rows.into_iter().peekable();
    while pending.peek().is_some() {
        let batch: Vec<ChainedRow> = pending.by_ref().take(ROWS_PER_RECORD).collect();
        let payload = relink_payload(stored.as_str(), &coll.name, batch)?;
        commit_relink(state, target, &payload).await?;
    }
    Ok(copied)
}

/// One batch as a relink record: each row a document put carrying its
/// source link, in position order.
fn relink_payload(
    stored: &str,
    bare: &str,
    batch: Vec<ChainedRow>,
) -> crate::Result<TransactionRedoPayload> {
    let mut ops = Vec::with_capacity(batch.len());
    let mut identities = Vec::with_capacity(batch.len());
    // Each relinked row installs in the clone as a new row.
    let mut row_changes = Vec::with_capacity(batch.len());
    for row in batch {
        row_changes.push(RedoRowChange {
            collection: stored.to_string(),
            row: row.document_id.as_str().to_owned(),
            kind: RedoRowKind::Insert,
        });
        let prov: Option<SyncProvenance> = None;
        let payload = zerompk::to_msgpack_vec(&(
            stored,
            row.document_id.as_str(),
            row.body,
            prov,
            row.target_surrogate.as_u32(),
        ))
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("clone relink: encode document put: {e}"),
        })?;
        ops.push(RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload,
        });
        identities.push(CarriedIdentity {
            collection: bare.to_string(),
            pk_bytes: row.pk_bytes,
            surrogate: row.target_surrogate,
        });
    }
    Ok(TransactionRedoPayload {
        redo: RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes,
        },
        collections: vec![stored.to_string()],
        sum_targets: Vec::new(),
        identities,
        // The rows passed their rules when the source first wrote them, and
        // a copy fires no AFTER trigger.
        event_source: EventSource::Restore,
        origin: RedoOrigin::Restore,
    })
}

/// Commit one relink record and wait until it is durable and installed here.
async fn commit_relink(
    state: &SharedState,
    target: RedoTarget,
    payload: &TransactionRedoPayload,
) -> crate::Result<()> {
    let proposer = state.async_raft_proposer()?;
    let entry = transaction_redo_entry(
        target.tenant_id,
        target.database_id,
        target.vshard_id,
        payload,
    );
    let deadline = crate::control::wal_replication::statement_propose_deadline(state);
    propose_replicated_entry(state, proposer, entry, deadline).await?;
    Ok(())
}
