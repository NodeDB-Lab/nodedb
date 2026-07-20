// SPDX-License-Identifier: BUSL-1.1

//! WAL replay that rebuilds secondary vector indexes over document
//! collections after a restart.
//!
//! A document write into a collection carrying a `CREATE VECTOR INDEX`
//! indexes the embedding as an in-memory HNSW side-effect; the write itself
//! is journalled only as a `RecordType::Put` document record (there is no
//! separate `VectorPut` record on this path). Vector-primary and sync inserts
//! are already restart-durable because they journal a `VectorPut` carrying the
//! surrogate, which [`CoreLoop::replay_vector_wal`] rebinds. This pass gives
//! the document + secondary-index path the same guarantee: it re-reads the
//! journalled document body, recovers the row's global surrogate from the
//! record, and replays the exact live indexing routine
//! ([`CoreLoop::apply_point_put_vector_indexes`]) so a rebuilt vector node
//! carries its real surrogate and vector search projects the user PK rather
//! than a headless local id.
//!
//! ## Surrogate recovery, plane separation
//!
//! The surrogate is carried in the document `Put` record itself (a trailing
//! element appended by the Control Plane WAL dispatch). Recovery therefore
//! needs no Control Plane catalog handle on the Data Plane core — the record
//! is self-describing. Records written by an older binary predate the trailing
//! surrogate; their vector nodes are not rebuilt here (a pre-surrogate
//! deployment recovered such indexes from vector checkpoints), and they are
//! skipped rather than bound to a placeholder identity.
//!
//! Must run **after** [`CoreLoop::replay_vector_wal`] so the `VectorParams`
//! records emitted by `CREATE VECTOR INDEX` have already registered the
//! per-collection index parameters this pass relies on.

use nodedb_types::Surrogate;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_wal::record::RecordType;

use super::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_index::KeyRepr;
use crate::engine::document::store::surrogate_to_doc_id;

impl CoreLoop {
    /// Replay document `Put` records to rebuild secondary vector indexes,
    /// binding each rebuilt vector node to the document row's real surrogate.
    ///
    /// Only records routed to this core's vShard are processed. `Put` records
    /// belonging to other engines (KV puts, graph edge puts) share the record
    /// type but decode to different shapes and are skipped. Collections without
    /// a registered vector index incur only a cheap map lookup inside
    /// `apply_point_put_vector_indexes`.
    pub fn replay_document_vector_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        let mut rebuilt = 0usize;

        for record in records {
            let rt = RecordType::from_raw(record.logical_record_type());
            let is_put = rt == Some(RecordType::Put);
            let is_delete = rt == Some(RecordType::Delete);
            if !is_put && !is_delete {
                continue;
            }

            let vshard_id = record.header.vshard_id as usize;
            let target_core = if num_cores > 0 {
                vshard_id % num_cores
            } else {
                0
            };
            if target_core != self.core_id {
                continue;
            }

            let payload = &record.payload;

            // Delete: a document delete journals a surrogate-carrying
            // `RecordType::Delete` (4-tuple `(collection, document_id, prov,
            // surrogate)`). Because this pass rebuilds a node from every `Put`,
            // the deleted row's original insert `Put` would otherwise resurrect
            // its vector; re-derive the row key from the surrogate and
            // soft-delete the node so the delete survives a WAL-only restart.
            // `Delete` records are journalled after the `Put` they cancel (a
            // higher LSN), so processing in WAL order removes exactly what an
            // earlier `Put` rebuilt. KV / other-engine deletes decode to a
            // different shape and are skipped by the strict tuple decode.
            if is_delete {
                let Ok((collection, _document_id, _prov, surrogate_u32)) =
                    zerompk::from_msgpack::<(String, String, Option<SyncProvenance>, u32)>(payload)
                else {
                    continue;
                };
                let tenant_id = record.header.tenant_id;
                if tombstones.is_tombstoned(
                    record.header.database_id,
                    tenant_id,
                    &collection,
                    record.header.lsn,
                ) {
                    continue;
                }
                let database_id = record.header.database_id;
                let row_key = surrogate_to_doc_id(Surrogate::new(surrogate_u32));
                self.remove_document_vector_indexes(database_id, tenant_id, &collection, &row_key);
                let record_lsn = record.header.lsn;
                self.note_replay_write_lsn(
                    database_id,
                    tenant_id,
                    &collection,
                    Some(KeyRepr::Surrogate(surrogate_u32)),
                    record_lsn,
                );
                continue;
            }

            // KV puts share `RecordType::Put` but carry a leading discriminator
            // string; skip them so their value bytes are never misread as a
            // document body. (A document record's leading element is the
            // collection name, never one of these discriminators, and its arity
            // differs, so this never skips a genuine document put.)
            if is_kv_put_record(payload) {
                continue;
            }

            // Recover the document body + surrogate. Current records carry the
            // surrogate as a trailing element; legacy records (no surrogate)
            // cannot rebind the real identity on the Data Plane without a
            // catalog round-trip, so their vector nodes are left to checkpoint
            // recovery and skipped here.
            let Some((collection, value, surrogate)) = decode_document_put(payload) else {
                continue;
            };

            let tenant_id = record.header.tenant_id;
            let record_lsn = record.header.lsn;
            if tombstones.is_tombstoned(
                record.header.database_id,
                tenant_id,
                &collection,
                record_lsn,
            ) {
                continue;
            }

            let database_id = record.header.database_id;
            // Live inserts key the vector reverse-map on the hex surrogate row
            // key (`surrogate_to_doc_id`), not the user PK; reproduce that here
            // so a later delete can still find and soft-delete the node.
            let row_key = surrogate_to_doc_id(surrogate);
            let deltas = self.apply_point_put_vector_indexes(
                crate::data::executor::handlers::point::apply_put::VectorIndexPutParams {
                    database_id,
                    tid: tenant_id,
                    collection: &collection,
                    document_id: &row_key,
                    surrogate,
                    value: &value,
                    wal_lsn: record_lsn,
                },
            );
            if !deltas.is_empty() {
                rebuilt += deltas.len();
            }
            self.note_replay_write_lsn(
                database_id,
                tenant_id,
                &collection,
                Some(KeyRepr::Surrogate(surrogate.as_u32())),
                record_lsn,
            );
        }

        if rebuilt > 0 {
            tracing::info!(
                core = self.core_id,
                rebuilt,
                "WAL document vector-index replay complete"
            );
        }
    }
}

/// True when `payload` is a KV put or KV batch-put record (both share
/// `RecordType::Put` with document writes but lead with a discriminator).
fn is_kv_put_record(payload: &[u8]) -> bool {
    if let Ok((disc, _c, _k, _v, _ttl)) =
        zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64)>(payload)
        && disc == "kv_put"
    {
        return true;
    }
    if let Ok((disc, _c, _e, _ttl)) =
        zerompk::from_msgpack::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64)>(payload)
        && disc == "kv_batch_put"
    {
        return true;
    }
    false
}

/// Decode a document `Put` payload, returning `(collection, value, surrogate)`.
///
/// Only the current surrogate-carrying arity yields a value; the legacy
/// arities (no surrogate) return `None` so the caller skips the vector rebuild
/// rather than bind a placeholder identity. Graph edge puts (which put a
/// `String` where the document value's `Vec<u8>` is) fail both decodes and
/// return `None`.
fn decode_document_put(payload: &[u8]) -> Option<(String, Vec<u8>, Surrogate)> {
    if let Ok((collection, _document_id, value, _prov, surrogate_u32)) =
        zerompk::from_msgpack::<(String, String, Vec<u8>, Option<SyncProvenance>, u32)>(payload)
    {
        return Some((collection, value, Surrogate::new(surrogate_u32)));
    }
    None
}
