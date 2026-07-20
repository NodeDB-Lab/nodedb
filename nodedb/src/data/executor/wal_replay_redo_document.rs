// SPDX-License-Identifier: BUSL-1.1

//! WAL redo replay arm for the Document engine.
//!
//! Document point writes have no standalone WAL replay: they survive a crash
//! today because redb commits synchronously at apply time. Under the
//! write-ahead-then-install protocol a crash between appending the redo record
//! and installing its effects loses them, so a transaction's document
//! sub-records must replay too.
//!
//! ## Sub-record payload shape (chosen here)
//!
//! Reuses the autocommit `RecordType::Put` / `Delete` document shapes
//! (`wal_append_if_write`) so the producer and this decoder share one encoding:
//!
//! * PUT — `(collection, document_id, value, Option<SyncProvenance>, surrogate)`.
//!   Byte-identical to the autocommit `PointPut` / `PointInsert` shape; the
//!   trailing `surrogate` is the stable identity `apply_point_put` keys on. A
//!   `bitemporal=true` collection's put instead carries an 8-tuple that appends
//!   `(sys_from_ms, valid_from_ms, valid_until_ms)`; the decoder tries the
//!   8-tuple first and falls back to the 5-tuple, and a decoded stamp forces the
//!   put onto the versioned store at that exact version key (needed because
//!   `doc_configs` is empty during replay, so `is_bitemporal` would say false).
//! * DELETE — `(collection, document_id, Option<SyncProvenance>, surrogate)`.
//!   The autocommit delete shape `(collection, document_id, prov)` omits the
//!   surrogate; replay needs it (the redb storage key is
//!   `surrogate_to_doc_id(surrogate)`, and the delete cascade keys on it), so
//!   the redo shape appends it as a fourth element.
//!
//! ## Idempotency
//!
//! Both ops are absolute: a PUT is an overwrite of the surrogate-keyed row, a
//! DELETE removes it. Re-applying either converges — no checkpoint gate needed.
//! Applied through the same shared core write path the transaction batch uses
//! (`apply_point_put` / `apply_point_delete`), never a reimplementation.

use nodedb_types::Surrogate;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use super::core_loop::CoreLoop;
use super::handlers::point::apply_delete::PointDeleteParams;
use super::handlers::point::apply_put::PointPutParams;
use super::handlers::transaction::overlay::BitemporalStamp;
use crate::data::executor::core_loop::write_index::KeyRepr;
use crate::engine::document::store::surrogate_to_doc_id;

impl CoreLoop {
    /// Replay reconstituted document `Put` / `Delete` redo sub-records.
    ///
    /// Only records whose payload decodes as a document tuple are applied; KV
    /// (leading `"kv_*"` discriminator) and graph (distinct tuple arity/types)
    /// `Put`/`Delete` records fail the strict decode and are skipped for the
    /// KV / graph arms to handle.
    pub(crate) fn replay_document_redo(
        &mut self,
        records: &[WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        let mut puts = 0usize;
        let mut deletes = 0usize;

        for record in records {
            let record_type = RecordType::from_raw(record.logical_record_type());
            let is_put = record_type == Some(RecordType::Put);
            let is_delete = record_type == Some(RecordType::Delete);
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

            let tenant_id = record.header.tenant_id;
            let database_id = record.header.database_id;
            let record_lsn = record.header.lsn;

            if is_put {
                // Try the bitemporal 8-tuple first, then fall back to the plain
                // 5-tuple (mirrors the KV base-vs-extended tuple discrimination).
                // A `bitemporal=true` collection's put carries its resolve-time
                // stamp; `doc_configs` is empty during replay, so the stamp is
                // the ONLY signal that this row belongs on the versioned store.
                type BitemporalPut = (
                    String,
                    String,
                    Vec<u8>,
                    Option<SyncProvenance>,
                    u32,
                    i64,
                    i64,
                    i64,
                );
                type PlainPut = (String, String, Vec<u8>, Option<SyncProvenance>, u32);
                let decoded = zerompk::from_msgpack::<BitemporalPut>(&record.payload)
                    .map(
                        |(collection, _doc_id, value, _prov, surrogate, sys, vf, vu)| {
                            (
                                collection,
                                value,
                                surrogate,
                                Some(BitemporalStamp {
                                    sys_from_ms: sys,
                                    valid_from_ms: vf,
                                    valid_until_ms: vu,
                                }),
                            )
                        },
                    )
                    .or_else(|_| {
                        zerompk::from_msgpack::<PlainPut>(&record.payload).map(
                            |(collection, _doc_id, value, _prov, surrogate)| {
                                (collection, value, surrogate, None)
                            },
                        )
                    });
                let Ok((collection, value, surrogate_u32, stamp)) = decoded else {
                    continue;
                };
                if tombstones.is_tombstoned(database_id, tenant_id, &collection, record_lsn) {
                    continue;
                }
                // Carry the stamp into apply scratch (forcing the versioned
                // branch at the exact stamp the commit-time install used) and
                // advance the per-core HLC so post-restart writes stay monotonic.
                if let Some(s) = stamp {
                    self.observe_bitemporal_stamp(s.sys_from_ms);
                    self.active_bitemporal_stamps.insert(surrogate_u32, s);
                }
                let applied = self.apply_document_put(
                    database_id,
                    tenant_id,
                    &collection,
                    surrogate_u32,
                    &value,
                    record_lsn,
                );
                if stamp.is_some() {
                    self.active_bitemporal_stamps.remove(&surrogate_u32);
                }
                if applied {
                    puts += 1;
                    self.note_replay_write_lsn(
                        database_id,
                        tenant_id,
                        &collection,
                        Some(KeyRepr::Surrogate(surrogate_u32)),
                        record_lsn,
                    );
                }
            } else {
                let Ok((collection, _document_id, _prov, surrogate_u32)) =
                    zerompk::from_msgpack::<(String, String, Option<SyncProvenance>, u32)>(
                        &record.payload,
                    )
                else {
                    continue;
                };
                if tombstones.is_tombstoned(database_id, tenant_id, &collection, record_lsn) {
                    continue;
                }
                if self.apply_document_delete(database_id, tenant_id, &collection, surrogate_u32) {
                    deletes += 1;
                    self.note_replay_write_lsn(
                        database_id,
                        tenant_id,
                        &collection,
                        Some(KeyRepr::Surrogate(surrogate_u32)),
                        record_lsn,
                    );
                }
            }
        }

        if puts > 0 || deletes > 0 {
            tracing::info!(
                core = self.core_id,
                puts,
                deletes,
                "WAL document redo replay complete"
            );
        }
    }

    /// Apply one document PUT through the shared `apply_point_put` core write
    /// path in its own redb write transaction. `enforce = false`: replayed
    /// writes were admission-checked when first committed, so re-running
    /// stateless enforcement here would double-check already-accepted writes
    /// (matching the CRDT-sync materialization contract). Returns whether the
    /// write was applied and committed.
    fn apply_document_put(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        surrogate_u32: u32,
        value: &[u8],
        record_lsn: u64,
    ) -> bool {
        let surrogate = Surrogate::new(surrogate_u32);
        let row_key = surrogate_to_doc_id(surrogate);
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    core = self.core_id,
                    %collection,
                    error = %e,
                    "WAL document redo: begin_write failed; skipping put"
                );
                return false;
            }
        };
        match self.apply_point_put(
            &txn,
            PointPutParams {
                database_id,
                tid: tenant_id,
                collection,
                document_id: row_key.as_str(),
                surrogate,
                value,
                index_text: true,
                user_roles: &[],
                enforce: false,
                wal_lsn: (record_lsn != 0).then(|| crate::types::Lsn::new(record_lsn)),
            },
        ) {
            Ok(_) => match txn.commit() {
                Ok(()) => {
                    self.checkpoint_coordinator.mark_dirty("sparse", 1);
                    true
                }
                Err(e) => {
                    tracing::warn!(
                        core = self.core_id,
                        %collection,
                        error = %e,
                        "WAL document redo: commit failed; skipping put"
                    );
                    false
                }
            },
            Err(e) => {
                // The write txn is dropped un-committed (rolled back) on the
                // early return.
                tracing::warn!(
                    core = self.core_id,
                    %collection,
                    error = %e,
                    "WAL document redo: apply_point_put failed; skipping put"
                );
                false
            }
        }
    }

    /// Apply one document DELETE through the shared `apply_point_delete` core
    /// path (which owns its redb transaction internally). `enforce = false` for
    /// the same reason as the put path. Returns whether a row was removed.
    fn apply_document_delete(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        surrogate_u32: u32,
    ) -> bool {
        let surrogate = Surrogate::new(surrogate_u32);
        let row_key = surrogate_to_doc_id(surrogate);
        match self.apply_point_delete(PointDeleteParams {
            database_id,
            tid: tenant_id,
            collection,
            document_id: row_key.as_str(),
            surrogate,
            user_roles: &[],
            enforce: false,
        }) {
            Ok(outcome) => {
                self.checkpoint_coordinator.mark_dirty("sparse", 1);
                outcome.prior_value.is_some()
            }
            Err(e) => {
                tracing::warn!(
                    core = self.core_id,
                    %collection,
                    error = %e,
                    "WAL document redo: apply_point_delete failed; skipping delete"
                );
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, Lsn, TenantId};
    use crate::wal::{RedoRecord, RedoSubRecord};
    use nodedb_wal::WalRecord;
    use nodedb_wal::record::WalRecordArgs;
    use std::sync::Arc;

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime. The
    /// tests drive replay directly and never tick the event loop, so the far
    /// ends are unused — they just must not be dropped.
    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        use nodedb_bridge::buffer::RingBuffer;

        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    fn doc_value(name: &str) -> Vec<u8> {
        let mut m = std::collections::HashMap::new();
        m.insert(
            "name".to_string(),
            nodedb_types::Value::String(name.to_string()),
        );
        zerompk::to_msgpack_vec(&nodedb_types::Value::Object(m)).expect("encode doc")
    }

    fn doc_put_sub(collection: &str, surrogate: u32, name: &str) -> RedoSubRecord {
        let prov: Option<SyncProvenance> = None;
        let payload =
            zerompk::to_msgpack_vec(&(collection, "userpk", doc_value(name), prov, surrogate))
                .expect("encode document put sub-record");
        RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload,
        }
    }

    fn doc_delete_sub(collection: &str, surrogate: u32) -> RedoSubRecord {
        let prov: Option<SyncProvenance> = None;
        let payload = zerompk::to_msgpack_vec(&(collection, "userpk", prov, surrogate))
            .expect("encode document delete sub-record");
        RedoSubRecord {
            record_type: RecordType::Delete as u32,
            payload,
        }
    }

    fn kv_put_sub_with_expiry(collection: &str, key: &[u8], expire_at_ms: u64) -> RedoSubRecord {
        // Six-element extended tuple carrying the absolute expiry instant.
        let payload = zerompk::to_msgpack_vec(&(
            "kv_put",
            collection,
            key,
            b"v".as_slice(),
            5_000u64,
            expire_at_ms,
        ))
        .expect("encode kv put sub-record");
        RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload,
        }
    }

    fn redo_record(tenant_id: u64, vshard_id: u32, ops: Vec<RedoSubRecord>) -> WalRecord {
        let redo = RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
        };
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn: 1,
            tenant_id,
            vshard_id,
            database_id: 0,
            payload: redo.to_bytes().expect("encode redo record"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    #[test]
    fn redo_document_put_restores_row() {
        let mut h = make_core();
        let surrogate = 42u32;
        let record = redo_record(7, 0, vec![doc_put_sub("notes", surrogate, "alice")]);

        h.core.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        let row_key = surrogate_to_doc_id(Surrogate::new(surrogate));
        let stored = h
            .core
            .sparse
            .get(0, 7, "notes", row_key.as_str())
            .expect("get");
        assert!(
            stored.is_some(),
            "document row must be restored from redo replay"
        );
    }

    #[test]
    fn redo_document_delete_removes_row() {
        let mut h = make_core();
        let surrogate = 42u32;
        // First materialize the row, then a redo delete removes it.
        let put = redo_record(7, 0, vec![doc_put_sub("notes", surrogate, "alice")]);
        h.core.replay_transaction_redo_wal(
            std::slice::from_ref(&put),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        let del = redo_record(7, 0, vec![doc_delete_sub("notes", surrogate)]);
        h.core.replay_transaction_redo_wal(
            std::slice::from_ref(&del),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        let row_key = surrogate_to_doc_id(Surrogate::new(surrogate));
        let stored = h
            .core
            .sparse
            .get(0, 7, "notes", row_key.as_str())
            .expect("get");
        assert!(stored.is_none(), "redo delete must remove the document row");
    }

    #[test]
    fn redo_document_put_idempotent_double_replay() {
        let mut h = make_core();
        let surrogate = 42u32;
        let record = redo_record(7, 0, vec![doc_put_sub("notes", surrogate, "alice")]);
        let tomb = nodedb_wal::TombstoneSet::new();

        // Absolute overwrite: replaying twice converges to the same single row.
        h.core
            .replay_transaction_redo_wal(std::slice::from_ref(&record), 1, &tomb);
        let first = h
            .core
            .sparse
            .get(
                0,
                7,
                "notes",
                surrogate_to_doc_id(Surrogate::new(surrogate)).as_str(),
            )
            .expect("get");
        h.core
            .replay_transaction_redo_wal(std::slice::from_ref(&record), 1, &tomb);
        let second = h
            .core
            .sparse
            .get(
                0,
                7,
                "notes",
                surrogate_to_doc_id(Surrogate::new(surrogate)).as_str(),
            )
            .expect("get");
        assert_eq!(
            first, second,
            "document put must converge under double replay"
        );
        assert!(second.is_some());
    }

    #[test]
    fn redo_kv_put_preserves_absolute_expiry() {
        let mut h = make_core();
        // An expiry far in the future; the exact instant must survive replay
        // rather than being recomputed as now_ms + ttl_ms.
        let expire_at_ms = crate::engine::kv::current_ms() + 3_600_000;
        let record = redo_record(
            7,
            0,
            vec![kv_put_sub_with_expiry("sessions", b"s1", expire_at_ms)],
        );

        h.core.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        let now = crate::engine::kv::current_ms();
        let value = h.core.kv_engine.get(0, 7, "sessions", b"s1", now);
        assert_eq!(
            value.as_deref(),
            Some(b"v".as_slice()),
            "kv row must be restored"
        );
        let ttl = h
            .core
            .kv_engine
            .get_ttl_ms(0, 7, "sessions", b"s1", now)
            .expect("ttl present");
        // Remaining ≈ expire_at - now, i.e. close to the full hour — proving the
        // absolute instant was installed, not the 5-second relative ttl_ms.
        assert!(
            ttl > 3_000_000,
            "absolute expiry must be preserved (remaining {ttl}ms), not recomputed from ttl_ms"
        );
    }

    fn vector_put_sub(collection: &str, vector: Vec<f32>) -> RedoSubRecord {
        let dim = vector.len();
        let payload = zerompk::to_msgpack_vec(&(collection, vector, dim))
            .expect("encode vector put sub-record");
        RedoSubRecord {
            record_type: RecordType::VectorPut as u32,
            payload,
        }
    }

    #[test]
    fn redo_vector_insert_queryable_after_rebuild() {
        let mut h = make_core();
        let record = redo_record(7, 0, vec![vector_put_sub("emb", vec![1.0, 2.0, 3.0])]);

        h.core.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        let key = CoreLoop::vector_index_key(0, 7, "emb", "");
        let len = h.core.vector_collections.get(&key).map(|c| c.len());
        assert_eq!(
            len,
            Some(1),
            "vector must be present in the rebuilt HNSW index"
        );
    }

    /// The raw engine op `VectorCollection::insert` is still append-only (it
    /// never dedups), but cross-boot (checkpoint) idempotency holds: the replay
    /// gate (`checkpoint_wal_lsn`) is frozen during a replay pass —
    /// `note_checkpoint_lsn` only advances the running `applied_wal_lsn` max,
    /// so sibling sub-records sharing one `TransactionRedo` LSN all apply
    /// instead of the first one gating the rest. The gate only moves at a
    /// checkpoint save (folding `applied_wal_lsn` in) and load (exposing it),
    /// which is what a real reboot does. This test reproduces that: replay
    /// once, round-trip the collection through a checkpoint to install the
    /// persisted watermark as the gate, then replay again and assert the
    /// record is skipped, leaving ONE copy, not two.
    #[test]
    fn redo_vector_insert_idempotent_on_double_replay() {
        let mut h = make_core();
        let record = redo_record(7, 0, vec![vector_put_sub("emb", vec![1.0, 2.0, 3.0])]);
        let tomb = nodedb_wal::TombstoneSet::new();

        h.core
            .replay_transaction_redo_wal(std::slice::from_ref(&record), 1, &tomb);

        // Simulate a checkpoint capture + reboot: saving folds the applied
        // watermark into the persisted gate, and restoring exposes it — so the
        // straddling record is now gated on the second replay.
        let key = CoreLoop::vector_index_key(0, 7, "emb", "");
        let bytes = h
            .core
            .vector_collections
            .get(&key)
            .expect("collection present after first replay")
            .checkpoint_to_bytes(None)
            .unwrap();
        let restored =
            crate::engine::vector::collection::VectorCollection::from_checkpoint(&bytes, None)
                .expect("decode checkpoint");
        h.core.vector_collections.insert(key.clone(), restored);

        h.core
            .replay_transaction_redo_wal(std::slice::from_ref(&record), 1, &tomb);

        let len = h.core.vector_collections.get(&key).map(|c| c.len());
        assert_eq!(
            len,
            Some(1),
            "checkpoint-LSN gate makes replay idempotent: a record at or below the \
             collection's recorded watermark is skipped on re-replay"
        );
    }

    #[test]
    fn redo_replay_populates_write_version_index() {
        use crate::data::executor::core_loop::write_index::{CollKey, WriteKey};

        let mut h = make_core();
        let surrogate = 99u32;
        let expire_at_ms = crate::engine::kv::current_ms() + 3_600_000;
        let put_record = redo_record(
            7,
            0,
            vec![
                doc_put_sub("notes", surrogate, "carol"),
                kv_put_sub_with_expiry("sessions", b"s1", expire_at_ms),
            ],
        );

        h.core.replay_transaction_redo_wal(
            std::slice::from_ref(&put_record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        let db = DatabaseId::new(0);
        let tenant = TenantId::new(7);

        let doc_key = WriteKey {
            db,
            tenant,
            collection: Box::from("notes"),
            key: KeyRepr::Surrogate(surrogate),
        };
        assert_eq!(
            h.core.write_index.key_write_lsn(&doc_key),
            Some(Lsn::new(1)),
            "document redo put must populate the per-key write-version index"
        );

        let doc_coll_key = CollKey {
            db,
            tenant,
            collection: Box::from("notes"),
        };
        assert_eq!(
            h.core.write_index.collection_write_lsn(&doc_coll_key),
            Some(Lsn::new(1)),
            "document redo put must advance the collection write-version floor"
        );

        let kv_key = WriteKey {
            db,
            tenant,
            collection: Box::from("sessions"),
            key: KeyRepr::KvKey(Box::from(b"s1".as_slice())),
        };
        assert_eq!(
            h.core.write_index.key_write_lsn(&kv_key),
            Some(Lsn::new(1)),
            "kv redo put must populate the per-key write-version index"
        );

        let kv_coll_key = CollKey {
            db,
            tenant,
            collection: Box::from("sessions"),
        };
        assert_eq!(
            h.core.write_index.collection_write_lsn(&kv_coll_key),
            Some(Lsn::new(1)),
            "kv redo put must advance the collection write-version floor"
        );

        // A later delete at a higher LSN must bump the document key's write
        // version above the prior put, proving the OCC read-then-delete
        // conflict window is visible after replay, not just after a live
        // delete.
        let delete_record = WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn: 2,
            tenant_id: 7,
            vshard_id: 0,
            database_id: 0,
            payload: RedoRecord {
                version: 1,
                ops: vec![doc_delete_sub("notes", surrogate)],
                calvin_stamp: None,
            }
            .to_bytes()
            .expect("encode redo record"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");

        h.core.replay_transaction_redo_wal(
            std::slice::from_ref(&delete_record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        assert_eq!(
            h.core.write_index.key_write_lsn(&doc_key),
            Some(Lsn::new(2)),
            "redo delete must bump the key's write-version above the prior put"
        );
    }

    #[test]
    fn redo_mixed_kv_and_document_replays_both() {
        let mut h = make_core();
        let doc_surrogate = 5u32;
        let expire_at_ms = crate::engine::kv::current_ms() + 3_600_000;
        let record = redo_record(
            7,
            0,
            vec![
                doc_put_sub("notes", doc_surrogate, "bob"),
                kv_put_sub_with_expiry("sessions", b"s1", expire_at_ms),
            ],
        );

        h.core.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        let row_key = surrogate_to_doc_id(Surrogate::new(doc_surrogate));
        assert!(
            h.core
                .sparse
                .get(0, 7, "notes", row_key.as_str())
                .expect("get")
                .is_some(),
            "document sub-record must be replayed"
        );
        let now = crate::engine::kv::current_ms();
        assert_eq!(
            h.core
                .kv_engine
                .get(0, 7, "sessions", b"s1", now)
                .as_deref(),
            Some(b"v".as_slice()),
            "kv sub-record must be replayed"
        );
    }
}
