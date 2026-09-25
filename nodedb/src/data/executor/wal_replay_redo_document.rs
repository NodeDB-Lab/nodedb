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
//!   `StorageKey::for_surrogate(surrogate)`, and the delete cascade keys on
//!   it), so the redo shape appends it as a fourth element. A
//!   `bitemporal=true` collection's delete appends its resolve-time
//!   `sys_from_ms` as a fifth element, and the decoded stamp forces the
//!   versioned tombstone at that exact version key.
//!
//! ## Idempotency
//!
//! Both ops are absolute: a PUT is an overwrite of the surrogate-keyed row, a
//! DELETE removes it. Re-applying either converges — no checkpoint gate needed.
//! Applied through the same shared core write path the transaction batch uses
//! (`apply_point_put` / `apply_point_delete`), never a reimplementation.
//!
//! ## Write-path enforcement is deliberately NOT run on redo
//!
//! Replay calls `apply_point_put` / `apply_point_delete` directly, one level
//! BELOW the enforcement funnel, so no materialized-sum delta is folded here —
//! and that is the correct behaviour, not an omission. A delta is a RELATIVE
//! change to a target row's stored total, so re-applying one over a target row
//! that is already durable double-counts it. Document rows are
//! redb-synchronous-durable: by the time this replay runs, the target balance
//! the original write produced is already on disk, and the derived target write
//! carries its own redo record naming the target collection. Folding again on
//! replay would add the same amount a second time. A Calvin record is the
//! exception: see "Calvin records in restart replay" below.
//!
//! ### Why Raft replication does the opposite
//!
//! Replication re-EXECUTES rather than re-APPLIES, and the two answers follow
//! from that difference alone — not from a policy about who may fold.
//!
//! A redo record is a POST-IMAGE: the row as it ended up, plus a sibling redo
//! record for the target row as IT ended up. Replaying both restores the pair
//! exactly, and a delta folded on top would be a third, uncounted contribution.
//!
//! A replicated record is the SOURCE row only — no post-image of the target
//! exists on the wire, and no node but the proposer ever computed one. A replica
//! that skipped enforcement would install the source row and leave its own copy
//! of the target's balance untouched, serving a total short by every row it ever
//! replicated while looking perfectly healthy. So it must fold, which is why the
//! replicated record carries the resolution the fold needs (see
//! `control::wal_replication::decode::document`).
//!
//! Both paths run exactly once over their own node's state; they differ only in
//! whether that state already contains the effect.
//!
//! ### Committed-redo apply
//!
//! A replica applying a committed `TransactionRedo` Raft entry drives this arm
//! with a redo-apply scope open. The source rows it writes then fold into
//! their targets and link their hash chain, exactly as replication does (see
//! `handlers::transaction::redo_apply`). With no scope open this arm is plain
//! restart replay.
//!
//! ### Calvin records in restart replay
//!
//! A Calvin redo record's stamp carries the sum targets its slice folds, and
//! no later record carries the target rows. Restart replay runs its document
//! rows through the committed path, which folds them at the record's LSN.
//! The fold subtracts the row's prior image, so a row that already holds its
//! post-image folds nothing.

use nodedb_types::Surrogate;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use super::core_loop::CoreLoop;
use super::handlers::point::apply_delete::PointDeleteParams;
use super::handlers::point::apply_put::PointPutParams;
use super::handlers::transaction::overlay::BitemporalStamp;
use super::handlers::transaction::redo_apply::CommittedDocWrite;
use crate::data::executor::core_loop::write_index::KeyRepr;
use crate::engine::document::store::StorageKey;

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
                // Replay keys the row by its surrogate; the record's text
                // `document_id` is the client key, read only by a committed
                // redo apply for the row's event identity.
                let decoded = zerompk::from_msgpack::<BitemporalPut>(&record.payload)
                    .map(
                        |(collection, document_id, value, _prov, surrogate, sys, vf, vu)| {
                            (
                                collection,
                                document_id,
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
                            |(collection, document_id, value, _prov, surrogate)| {
                                (collection, document_id, value, surrogate, None)
                            },
                        )
                    });
                let Ok((collection, document_id, value, surrogate_u32, stamp)) = decoded else {
                    continue;
                };
                if tombstones.is_tombstoned(database_id, tenant_id, &collection, record_lsn) {
                    continue;
                }
                if self.claim_for_validation() {
                    continue;
                }
                // Carry the stamp into apply scratch (forcing the versioned
                // branch at the exact stamp the commit-time install used) and
                // advance the per-core HLC so post-restart writes stay monotonic.
                if let Some(s) = stamp {
                    self.observe_bitemporal_stamp(s.sys_from_ms);
                    self.active_bitemporal_stamps.insert(surrogate_u32, s);
                }
                let folds = self.redo_folds_at(record_lsn, &collection);
                let applied = if folds {
                    self.apply_committed_document_put(
                        CommittedDocWrite {
                            database_id,
                            tenant_id,
                            collection: &collection,
                            document_id: &document_id,
                            surrogate: surrogate_u32,
                            record_lsn,
                        },
                        &value,
                    )
                } else {
                    self.apply_document_put(
                        database_id,
                        tenant_id,
                        &collection,
                        surrogate_u32,
                        &value,
                        record_lsn,
                    )
                };
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
                // Replay keys the row by its surrogate. The record's text
                // `document_id` is the client key. The graph cascade keys
                // nodes by it, and a committed redo apply names the row's
                // event by it.
                // A `bitemporal=true` collection's delete carries its
                // resolve-time system time as a fifth element; the plain form
                // has four. The stamp forces the versioned tombstone at that
                // exact version key.
                type BitemporalDelete = (String, String, Option<SyncProvenance>, u32, i64);
                type PlainDelete = (String, String, Option<SyncProvenance>, u32);
                let decoded = zerompk::from_msgpack::<BitemporalDelete>(&record.payload)
                    .map(|(collection, document_id, _prov, surrogate, sys)| {
                        (collection, document_id, surrogate, Some(sys))
                    })
                    .or_else(|_| {
                        zerompk::from_msgpack::<PlainDelete>(&record.payload).map(
                            |(collection, document_id, _prov, surrogate)| {
                                (collection, document_id, surrogate, None)
                            },
                        )
                    });
                let Ok((collection, document_id, surrogate_u32, sys_from_ms)) = decoded else {
                    continue;
                };
                if tombstones.is_tombstoned(database_id, tenant_id, &collection, record_lsn) {
                    continue;
                }
                if self.claim_for_validation() {
                    continue;
                }
                if let Some(sys) = sys_from_ms {
                    self.observe_bitemporal_stamp(sys);
                    self.active_bitemporal_stamps.insert(
                        surrogate_u32,
                        BitemporalStamp {
                            sys_from_ms: sys,
                            valid_from_ms: i64::MIN,
                            valid_until_ms: i64::MAX,
                        },
                    );
                }
                let folds = self.redo_folds_at(record_lsn, &collection);
                let removed = if folds {
                    self.apply_committed_document_delete(CommittedDocWrite {
                        database_id,
                        tenant_id,
                        collection: &collection,
                        document_id: &document_id,
                        surrogate: surrogate_u32,
                        record_lsn,
                    })
                } else {
                    self.apply_document_delete(
                        database_id,
                        tenant_id,
                        &collection,
                        &document_id,
                        surrogate_u32,
                    )
                };
                if sys_from_ms.is_some() {
                    self.active_bitemporal_stamps.remove(&surrogate_u32);
                }
                if removed {
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

    /// Whether a document write at `record_lsn` to `collection` runs the
    /// committed path, which folds materialized sums: always under a
    /// committed-redo apply, and in restart replay when the record's Calvin
    /// stamp names sum targets for `collection`. Folding reads the row's
    /// prior image, so a replay over a row that already holds the post-image
    /// folds nothing.
    fn redo_folds_at(&self, record_lsn: u64, collection: &str) -> bool {
        self.redo_apply.scope.is_some()
            || self
                .redo_apply
                .replay_folds_for(record_lsn, collection)
                .is_some()
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
        let storage_key = StorageKey::for_surrogate(surrogate);
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
                storage_key,
                surrogate,
                value,
                index_text: true,
                user_roles: &[],
                enforce: false,
                resolved_targets: &[],
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
    /// path in its own redb write transaction. `enforce = false` for the same
    /// reason as the put path. Returns whether a row was removed.
    fn apply_document_delete(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &str,
        surrogate_u32: u32,
    ) -> bool {
        let surrogate = Surrogate::new(surrogate_u32);
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    core = self.core_id,
                    %collection,
                    error = %e,
                    "WAL document redo: begin_write failed; skipping delete"
                );
                return false;
            }
        };
        match self.apply_point_delete(
            &txn,
            PointDeleteParams {
                database_id,
                tid: tenant_id,
                collection,
                // The graph cascade keys nodes by the client key.
                document_id,
                surrogate,
                user_roles: &[],
                enforce: false,
                resolved_targets: &[],
            },
        ) {
            Ok(outcome) => match txn.commit() {
                Ok(()) => {
                    self.checkpoint_coordinator.mark_dirty("sparse", 1);
                    outcome.prior_value.is_some()
                }
                Err(e) => {
                    tracing::warn!(
                        core = self.core_id,
                        %collection,
                        error = %e,
                        "WAL document redo: commit failed; skipping delete"
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
            crate::data::executor::core_loop::test_governor(),
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

        h.core
            .replay_transaction_redo_wal(
                std::slice::from_ref(&record),
                1,
                &nodedb_wal::TombstoneSet::new(),
            )
            .expect("redo replay must succeed");

        let row_key = nodedb_types::StorageKey::for_surrogate(Surrogate::new(surrogate));
        let stored = h.core.sparse.get(0, 7, "notes", &row_key).expect("get");
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
        h.core
            .replay_transaction_redo_wal(
                std::slice::from_ref(&put),
                1,
                &nodedb_wal::TombstoneSet::new(),
            )
            .expect("redo replay must succeed");

        let del = redo_record(7, 0, vec![doc_delete_sub("notes", surrogate)]);
        h.core
            .replay_transaction_redo_wal(
                std::slice::from_ref(&del),
                1,
                &nodedb_wal::TombstoneSet::new(),
            )
            .expect("redo replay must succeed");

        let row_key = nodedb_types::StorageKey::for_surrogate(Surrogate::new(surrogate));
        let stored = h.core.sparse.get(0, 7, "notes", &row_key).expect("get");
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
            .replay_transaction_redo_wal(std::slice::from_ref(&record), 1, &tomb)
            .expect("redo replay must succeed");
        let first = h
            .core
            .sparse
            .get(
                0,
                7,
                "notes",
                &nodedb_types::StorageKey::for_surrogate(Surrogate::new(surrogate)),
            )
            .expect("get");
        h.core
            .replay_transaction_redo_wal(std::slice::from_ref(&record), 1, &tomb)
            .expect("redo replay must succeed");
        let second = h
            .core
            .sparse
            .get(
                0,
                7,
                "notes",
                &nodedb_types::StorageKey::for_surrogate(Surrogate::new(surrogate)),
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

        h.core
            .replay_transaction_redo_wal(
                std::slice::from_ref(&record),
                1,
                &nodedb_wal::TombstoneSet::new(),
            )
            .expect("redo replay must succeed");

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

        h.core
            .replay_transaction_redo_wal(
                std::slice::from_ref(&record),
                1,
                &nodedb_wal::TombstoneSet::new(),
            )
            .expect("redo replay must succeed");

        let key = CoreLoop::vector_index_key(0, 7, "emb", "");
        let len = h.core.vector_collections.get(&key).map(|c| c.len());
        assert_eq!(
            len,
            Some(1),
            "vector must be present in the rebuilt HNSW index"
        );
    }

    /// The raw engine op `VectorCollection::insert` is append-only (it never
    /// dedups), so cross-boot idempotency rests on the restored checkpoint's
    /// stamp: a record it names is skipped. The stamp is set only at load, so
    /// sibling sub-records sharing one `TransactionRedo` LSN all apply within
    /// one replay pass. This test replays once, installs a stamp naming the
    /// record as a restored checkpoint does, then replays again and asserts
    /// the record is skipped, leaving one copy, not two.
    #[test]
    fn redo_vector_insert_idempotent_on_double_replay() {
        let mut h = make_core();
        let record = redo_record(7, 0, vec![vector_put_sub("emb", vec![1.0, 2.0, 3.0])]);
        let tomb = nodedb_wal::TombstoneSet::new();

        h.core
            .replay_transaction_redo_wal(std::slice::from_ref(&record), 1, &tomb)
            .expect("redo replay must succeed");

        // A checkpoint written after that replay names the record.
        let key = CoreLoop::vector_index_key(0, 7, "emb", "");
        h.core.floors.replay_floors.vector.set(
            crate::data::executor::applied_prefix::ReplayStamp::through(record.header.lsn),
        );

        h.core
            .replay_transaction_redo_wal(std::slice::from_ref(&record), 1, &tomb)
            .expect("redo replay must succeed");

        let len = h.core.vector_collections.get(&key).map(|c| c.len());
        assert_eq!(
            len,
            Some(1),
            "a record the restored checkpoint's stamp names is skipped on re-replay"
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

        h.core
            .replay_transaction_redo_wal(
                std::slice::from_ref(&put_record),
                1,
                &nodedb_wal::TombstoneSet::new(),
            )
            .expect("redo replay must succeed");

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

        h.core
            .replay_transaction_redo_wal(
                std::slice::from_ref(&delete_record),
                1,
                &nodedb_wal::TombstoneSet::new(),
            )
            .expect("redo replay must succeed");

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

        h.core
            .replay_transaction_redo_wal(
                std::slice::from_ref(&record),
                1,
                &nodedb_wal::TombstoneSet::new(),
            )
            .expect("redo replay must succeed");

        let row_key = nodedb_types::StorageKey::for_surrogate(Surrogate::new(doc_surrogate));
        assert!(
            h.core
                .sparse
                .get(0, 7, "notes", &row_key)
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

    fn bitemporal_put_sub(collection: &str, surrogate: u32, sys_from_ms: i64) -> RedoSubRecord {
        let prov: Option<SyncProvenance> = None;
        let payload = zerompk::to_msgpack_vec(&(
            collection,
            "userpk",
            doc_value("alice"),
            prov,
            surrogate,
            sys_from_ms,
            i64::MIN,
            i64::MAX,
        ))
        .expect("encode bitemporal put sub-record");
        RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload,
        }
    }

    fn bitemporal_delete_sub(collection: &str, surrogate: u32, sys_from_ms: i64) -> RedoSubRecord {
        let prov: Option<SyncProvenance> = None;
        let payload =
            zerompk::to_msgpack_vec(&(collection, "userpk", prov, surrogate, sys_from_ms))
                .expect("encode bitemporal delete sub-record");
        RedoSubRecord {
            record_type: RecordType::Delete as u32,
            payload,
        }
    }

    /// A bitemporal redo delete carries its resolve-time system time, so two
    /// independent applies of the same record (two replicas, or a replica and
    /// a restart) write the tombstone at the same version key.
    #[test]
    fn a_bitemporal_redo_delete_tombstones_at_its_carried_system_time_on_every_apply() {
        const PUT_MS: i64 = 1_000;
        const DELETE_MS: i64 = 2_000;
        let surrogate = 42u32;
        let record = redo_record(
            7,
            0,
            vec![
                bitemporal_put_sub("hist", surrogate, PUT_MS),
                bitemporal_delete_sub("hist", surrogate, DELETE_MS),
            ],
        );
        let row_key = nodedb_types::StorageKey::for_surrogate(Surrogate::new(surrogate));

        for _replica in 0..2 {
            let mut h = make_core();
            h.core
                .replay_transaction_redo_wal(
                    std::slice::from_ref(&record),
                    1,
                    &nodedb_wal::TombstoneSet::new(),
                )
                .expect("redo replay must succeed");
            let before = h
                .core
                .sparse
                .versioned_get_as_of(0, 7, "hist", &row_key, Some(DELETE_MS - 1), None)
                .expect("read before the tombstone");
            assert!(
                before.is_some(),
                "the row is live before the carried tombstone"
            );
            let at = h
                .core
                .sparse
                .versioned_get_as_of(0, 7, "hist", &row_key, Some(DELETE_MS), None)
                .expect("read at the tombstone");
            assert!(
                at.is_none(),
                "the tombstone sits at the carried system time, not a locally minted one"
            );
            assert!(
                h.core.active_bitemporal_stamps.is_empty(),
                "the carried stamp is scoped to its own apply"
            );
        }
    }
}
