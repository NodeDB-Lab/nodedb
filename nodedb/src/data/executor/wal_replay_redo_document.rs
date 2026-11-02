// SPDX-License-Identifier: BUSL-1.1

//! WAL replay arm for the Document engine.
//!
//! Every stored document row has a WAL record carrying its post-image, and
//! this arm applies them all in LSN order: the standalone records of
//! autocommit writes, and the sub-records of committed `TransactionRedo`
//! records and of `WriteGroup` records alike. A point write's pre-dispatch
//! record carries the row its plan names. The parts of a write's
//! `WriteGroup`, journalled from the write's `Response::write_set`, carry
//! every row the apply decided. A restart replays the tail above the durable
//! floor, and a point-in-time restore replays every archived record above
//! its base, so both rebuild exactly the rows the WAL names.
//!
//! A delete tombstones no edge here. The edges of its node carry their own
//! records, which the graph arm writes at the ordinal the live write
//! stamped.
//!
//! ## Record payload shape
//!
//! One encoding serves every producer (`wal_dispatch::document`) and this
//! decoder:
//!
//! * PUT — `(collection, document_id, value, Option<SyncProvenance>, surrogate)`.
//!   The trailing `surrogate` is the stable identity `apply_point_put` keys on,
//!   and `value` is the MessagePack body `apply_point_put` encodes into storage.
//!   A stamped put carries an 8-tuple that appends `(sys_from,
//!   valid_from_ms, valid_until_ms)`. `crate::wal::DocumentPutRedo` decodes
//!   both forms, and a decoded stamp forces the put onto the versioned store
//!   at that exact version key. A clone copy appends a trailing `true` to
//!   the stamped tuple, and installs only where its row holds no version.
//! * DELETE — `(collection, document_id, Option<SyncProvenance>, surrogate)`.
//!   The redb storage key is `StorageKey::for_surrogate(surrogate)`, and the
//!   delete cascade keys on it. A stamped delete appends its `sys_from` as a
//!   fifth element. `crate::wal::DocumentDeleteRedo` decodes both forms, and
//!   the decoded stamp forces the versioned tombstone at that exact version
//!   key.
//!
//! Every document collection keeps versions, so every row a write stores
//! reaches the WAL stamped: a point write journals its stamped image after
//! apply (`Response::write_set`), a transaction's redo carries the stamp its
//! resolve pinned, and every other producer stamps the rows it writes. A
//! record without a stamp is a point write's pre-dispatch record. The apply
//! decided that row's version key, and the stamped image carries the row, or
//! the apply wrote nothing (a delete of an absent row). Restart replay skips
//! every unstamped record, so each row lands once, at the key the live write
//! used. A committed-redo install refuses an unstamped record: no producer
//! writes one into a redo, and installing it would stamp a key no replay
//! reproduces.
//!
//! ## Idempotency
//!
//! Every document write lands as a version. A stamped op writes its exact
//! version key, so replaying it again rewrites the same version. A point
//! write journals its stamped image and cancels its unstamped pre-dispatch
//! record. Replaying a suffix of records that already applied re-applies
//! each post-image in the order the core applied them, and converges on the
//! state the last one left, so no checkpoint gate is needed. No replay
//! stamps a version of its own, so no replay adds a version a live write did
//! not write. A secondary vector index is gated by
//! the vector checkpoint's stamp inside `apply_point_put`, so a vector node the
//! restored index holds is never inserted twice. A PUT to a hash-chained
//! collection stores the submitted body with the row's link, which replay
//! reuses from the durable row or derives from the durable head (see
//! `wal_replay_redo_document_apply`).
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

use nodedb_types::Surrogate;
use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use super::core_loop::CoreLoop;
use super::handlers::transaction::overlay::BitemporalStamp;
use super::handlers::transaction::redo_apply::CommittedDocWrite;
use super::wal_replay_redo_document_apply::{ReplayDocDelete, ReplayDocPut};
use crate::data::executor::core_loop::write_note::KeyRepr;
use crate::wal::{DocumentDeleteRedo, DocumentPutRedo};

impl CoreLoop {
    /// Replay every document `Put` / `Delete` record in `records`, standalone
    /// and reconstituted alike, in slice order.
    ///
    /// Only records whose payload decodes as a document tuple are applied; KV
    /// (leading `"kv_*"` discriminator) and graph (map-encoded edge payload)
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
            if self.replay_halted() {
                break;
            }
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
                // A KV value must never be read as a document body.
                if is_kv_put_record(&record.payload) {
                    continue;
                }
                // A decoded stamp places the row on the versioned store at
                // exactly the version key the live write used. Replay keys
                // the row by its surrogate. The record's text `document_id`
                // is the client key, read only by a committed redo apply for
                // the row's event identity.
                let Some(DocumentPutRedo {
                    collection,
                    document_id,
                    value,
                    provenance: _,
                    surrogate: surrogate_u32,
                    stamp,
                    clone_copy,
                }) = DocumentPutRedo::decode(&record.payload)
                else {
                    continue;
                };
                let stamp = stamp.map(|s| BitemporalStamp {
                    sys_from: s.sys_from,
                    valid_from_ms: s.valid_from_ms,
                    valid_until_ms: s.valid_until_ms,
                });
                if tombstones.is_tombstoned(database_id, tenant_id, &collection, record_lsn) {
                    continue;
                }
                // Every document writer binds the row's surrogate before the
                // append. A record without it is refused, never replayed
                // under `ZERO`.
                if surrogate_u32 == Surrogate::ZERO.as_u32() {
                    self.replay_record_unapplied(
                        "document",
                        "put_identity",
                        record_lsn,
                        &format!(
                            "document put of '{document_id}' in '{collection}' carries no surrogate"
                        ),
                    );
                    continue;
                }
                // The stamped image journalled after apply carries the row.
                if stamp.is_none() {
                    self.unstamped_document_record("put", record_lsn, &collection, &document_id);
                    continue;
                }
                if self.claim_for_validation() {
                    continue;
                }
                // A clone copy installs only where its row holds no version:
                // a write into the clone wins over a copy.
                if clone_copy {
                    match self.doc_row_holds_a_version(
                        database_id,
                        tenant_id,
                        &collection,
                        surrogate_u32,
                    ) {
                        Ok(false) => {}
                        Ok(true) => continue,
                        Err(error) => {
                            self.replay_record_unapplied(
                                "document",
                                "clone_copy_probe",
                                record_lsn,
                                &format!(
                                    "clone copy of '{document_id}' in '{collection}' could not \
                                     read its row: {error}"
                                ),
                            );
                            continue;
                        }
                    }
                }
                // Carry the stamp into apply scratch (forcing the versioned
                // branch at the exact stamp the commit-time install used) and
                // fold its ordinal into the shared clock so post-restart writes
                // stamp above it.
                if let Some(s) = stamp {
                    self.observe_bitemporal_ordinal(s.sys_from);
                    self.apply_scope.bitemporal_stamps.insert(surrogate_u32, s);
                }
                let folds = self.redo_folds();
                let applied = if folds {
                    self.apply_committed_document_put(
                        CommittedDocWrite {
                            database_id,
                            tenant_id,
                            collection: &collection,
                            document_id: &document_id,
                            surrogate: surrogate_u32,
                            record_lsn,
                            clone_copy,
                        },
                        &value,
                    )
                } else {
                    self.apply_document_put(
                        ReplayDocPut {
                            database_id,
                            tenant_id,
                            collection: &collection,
                            surrogate: surrogate_u32,
                            record_lsn,
                        },
                        &value,
                    )
                };
                if stamp.is_some() {
                    self.apply_scope.bitemporal_stamps.remove(&surrogate_u32);
                }
                if applied {
                    puts += 1;
                    self.note_replay_write(
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
                // A stamped delete carries its tombstone's system time. The
                // stamp forces the versioned tombstone at that exact version
                // key.
                let Some(DocumentDeleteRedo {
                    collection,
                    document_id,
                    provenance: _,
                    surrogate: surrogate_u32,
                    sys_from,
                }) = DocumentDeleteRedo::decode(&record.payload)
                else {
                    continue;
                };
                if tombstones.is_tombstoned(database_id, tenant_id, &collection, record_lsn) {
                    continue;
                }
                // A delete record names a bound row. One under `ZERO` is
                // refused, never replayed.
                if surrogate_u32 == Surrogate::ZERO.as_u32() {
                    self.replay_record_unapplied(
                        "document",
                        "delete_identity",
                        record_lsn,
                        &format!(
                            "document delete of '{document_id}' in '{collection}' carries no \
                             surrogate"
                        ),
                    );
                    continue;
                }
                // The stamped image journalled after apply carries the
                // removal, or the delete removed nothing.
                if sys_from.is_none() {
                    self.unstamped_document_record("delete", record_lsn, &collection, &document_id);
                    continue;
                }
                if self.claim_for_validation() {
                    continue;
                }
                if let Some(sys) = sys_from {
                    self.observe_bitemporal_ordinal(sys);
                    self.apply_scope.bitemporal_stamps.insert(
                        surrogate_u32,
                        BitemporalStamp {
                            sys_from: sys,
                            valid_from_ms: i64::MIN,
                            valid_until_ms: i64::MAX,
                        },
                    );
                }
                let folds = self.redo_folds();
                let removed = if folds {
                    self.apply_committed_document_delete(CommittedDocWrite {
                        database_id,
                        tenant_id,
                        collection: &collection,
                        document_id: &document_id,
                        surrogate: surrogate_u32,
                        record_lsn,
                        clone_copy: false,
                    })
                } else {
                    self.apply_document_delete(ReplayDocDelete {
                        database_id,
                        tenant_id,
                        collection: &collection,
                        document_id: &document_id,
                        surrogate: surrogate_u32,
                    })
                };
                if sys_from.is_some() {
                    self.apply_scope.bitemporal_stamps.remove(&surrogate_u32);
                }
                if removed {
                    deletes += 1;
                    self.note_replay_write(
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

    /// Settle an unstamped document `kind` record (`put` or `delete`).
    /// Restart replay skips it: it is a point write's pre-dispatch record,
    /// whose stamped image carries the row. A committed-redo install refuses
    /// it: no producer writes an unstamped row into a redo.
    fn unstamped_document_record(
        &mut self,
        kind: &str,
        record_lsn: u64,
        collection: &str,
        document_id: &str,
    ) {
        if self.applying_committed_redo() {
            self.replay_record_unapplied(
                "document",
                "unstamped_version",
                record_lsn,
                &format!(
                    "document {kind} of '{document_id}' in '{collection}' carries no version \
                     stamp"
                ),
            );
        }
    }

    /// Whether a document write runs the committed path, which folds
    /// materialized sums: only under a committed-redo apply. Restart replay
    /// applies the fold target rows from the parts the apply journalled.
    fn redo_folds(&self) -> bool {
        self.redo_apply.scope.is_some()
    }
}

/// True when `payload` is a KV put or KV batch-put record: both share
/// `RecordType::Put` with document writes but lead with a discriminator no
/// document record carries.
fn is_kv_put_record(payload: &[u8]) -> bool {
    let discriminator = |disc: &str| disc == "kv_put" || disc == "kv_batch_put";
    // Only the leading element decides: every KV put arity opens with its
    // discriminator string, and a document record opens with its collection.
    match zerompk::from_msgpack::<(String, String, Vec<u8>, Vec<u8>, u64, Option<u64>, u32)>(
        payload,
    ) {
        Ok((disc, ..)) => discriminator(&disc),
        Err(_) => matches!(
            zerompk::from_msgpack::<(String, String, Vec<(Vec<u8>, Vec<u8>)>, u64, Option<u64>, Vec<u32>)>(payload),
            Ok((disc, ..)) if discriminator(&disc)
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{RedoRecord, RedoSubRecord};
    use nodedb_types::Surrogate;
    use nodedb_types::sync::wire::SyncProvenance;
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
        nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(m)).expect("encode doc")
    }

    /// A document put sub-record, stamped as every producer stamps one.
    fn doc_put_sub(collection: &str, surrogate: u32, name: &str) -> RedoSubRecord {
        crate::data::executor::handlers::transaction::redo_apply::test_commit::doc_put_sub_record(
            collection,
            "userpk",
            &doc_value(name),
            surrogate,
        )
    }

    /// A document delete sub-record, stamped as every producer stamps one.
    fn doc_delete_sub(collection: &str, surrogate: u32) -> RedoSubRecord {
        crate::data::executor::handlers::transaction::redo_apply::test_commit::doc_delete_sub_record(
            collection, "userpk", surrogate,
        )
    }

    /// An unstamped document put sub-record: the shape of a point write's
    /// pre-dispatch record.
    fn unstamped_put_sub(collection: &str, surrogate: u32, name: &str) -> RedoSubRecord {
        let prov: Option<SyncProvenance> = None;
        let payload =
            zerompk::to_msgpack_vec(&(collection, "userpk", doc_value(name), prov, surrogate))
                .expect("encode document put sub-record");
        RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload,
        }
    }

    fn kv_put_sub_with_expiry(collection: &str, key: &[u8], expire_at_ms: u64) -> RedoSubRecord {
        // The `kv_put` shape, carrying the absolute expiry instant and a bound
        // surrogate.
        let payload = zerompk::to_msgpack_vec(&(
            "kv_put",
            collection,
            key,
            b"v".as_slice(),
            5_000u64,
            Some(expire_at_ms),
            1u32,
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
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
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

    /// A stamped document put at `sys_from`, marked a clone copy or not.
    fn stamped_put_sub(
        surrogate: u32,
        name: &str,
        sys_from: i64,
        clone_copy: bool,
    ) -> RedoSubRecord {
        let prov: Option<SyncProvenance> = None;
        let payload = if clone_copy {
            zerompk::to_msgpack_vec(&(
                "notes",
                "userpk",
                doc_value(name),
                prov,
                surrogate,
                sys_from,
                i64::MIN,
                i64::MAX,
                true,
            ))
        } else {
            zerompk::to_msgpack_vec(&(
                "notes",
                "userpk",
                doc_value(name),
                prov,
                surrogate,
                sys_from,
                i64::MIN,
                i64::MAX,
            ))
        }
        .expect("encode document put sub-record");
        RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload,
        }
    }

    /// A clone copy of a row the target holds is skipped, and the target keeps
    /// its newer user version. A copy of a row the target lacks installs at
    /// the source stamp.
    #[test]
    fn a_clone_copy_installs_only_where_the_row_holds_no_version() {
        let mut h = make_core();
        let tomb = nodedb_wal::TombstoneSet::new();
        let user = redo_record(7, 0, vec![stamped_put_sub(42, "user", 900, false)]);
        let copies = redo_record(
            7,
            0,
            vec![
                stamped_put_sub(42, "source", 100, true),
                stamped_put_sub(43, "source", 100, true),
            ],
        );
        h.core
            .replay_transaction_redo_wal(std::slice::from_ref(&user), 1, &tomb)
            .expect("replay the user write");
        h.core
            .replay_transaction_redo_wal(std::slice::from_ref(&copies), 1, &tomb)
            .expect("replay the copies");

        let version = |surrogate: u32, cut: Option<i64>| {
            h.core
                .sparse
                .versioned_get_stamped(
                    0,
                    7,
                    "notes",
                    &nodedb_types::StorageKey::for_surrogate(Surrogate::new(surrogate)),
                    cut,
                )
                .expect("read")
        };
        assert_eq!(version(42, None).map(|v| v.sys_from), Some(900));
        assert_eq!(
            version(42, Some(500)),
            None,
            "the skipped copy installs no history below the user version"
        );
        let fresh = version(43, None).expect("the copied row");
        assert_eq!(fresh.sys_from, 100);
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
        let payload = crate::control::server::wal_dispatch::encode_vector_put_payload(
            collection,
            &vector,
            dim,
            "",
            nodedb_types::Surrogate::new(1),
            None,
        )
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
    fn redo_replay_notes_each_record_as_applied() {
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

        assert_eq!(
            h.core.watermark,
            crate::types::Lsn::new(1),
            "redo put replay must note its record as applied"
        );

        // A later delete at a higher LSN advances the watermark past the put.
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
                cross_shard_applied: None,
                row_sources: Vec::new(),
                publishes: Vec::new(),
                row_changes: Vec::new(),
                user_roles: Vec::new(),
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
            h.core.watermark,
            crate::types::Lsn::new(2),
            "redo delete replay must note its record as applied"
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

    fn bitemporal_put_sub(collection: &str, surrogate: u32, sys_from: i64) -> RedoSubRecord {
        let prov: Option<SyncProvenance> = None;
        let payload = zerompk::to_msgpack_vec(&(
            collection,
            "userpk",
            doc_value("alice"),
            prov,
            surrogate,
            sys_from,
            i64::MIN,
            i64::MAX,
        ))
        .expect("encode bitemporal put sub-record");
        RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload,
        }
    }

    fn bitemporal_delete_sub(collection: &str, surrogate: u32, sys_from: i64) -> RedoSubRecord {
        let prov: Option<SyncProvenance> = None;
        let payload = zerompk::to_msgpack_vec(&(collection, "userpk", prov, surrogate, sys_from))
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
                h.core.apply_scope.bitemporal_stamps.is_empty(),
                "the carried stamp is scoped to its own apply"
            );
        }
    }

    /// A replayed version stamped ahead of the local clock folds into it, so
    /// the next local write stamps above the replayed version.
    #[test]
    fn a_replayed_ordinal_ahead_of_the_clock_keeps_later_stamps_above_it() {
        let mut h = make_core();
        let ahead = h.core.hlc.next_ordinal().saturating_add(1_000_000_000_000);
        let record = redo_record(7, 0, vec![bitemporal_put_sub("hist", 42, ahead)]);
        h.core
            .replay_transaction_redo_wal(
                std::slice::from_ref(&record),
                1,
                &nodedb_wal::TombstoneSet::new(),
            )
            .expect("redo replay must succeed");
        assert!(h.core.bitemporal_ordinal() > ahead);
    }

    /// A document put or delete record under `Surrogate::ZERO` names no row:
    /// the committed apply refuses it through `replay_record_unapplied` and
    /// stores nothing under the `ZERO` key.
    #[test]
    fn a_document_record_under_zero_is_refused_and_writes_nothing() {
        use crate::data::executor::handlers::transaction::redo_apply::test_commit::{
            doc_delete_sub_record, doc_put_sub_record,
        };
        use nodedb_physical::physical_plan::RedoOrigin;

        let mut h = make_core();
        let put = doc_put_sub_record("notes", "unbound", &doc_value("alice"), 0);
        let (_, error) = h
            .core
            .install_from_for_test(7, 10, vec![put], RedoOrigin::Commit);
        assert!(error.is_some(), "a put under ZERO is refused");
        let zero_key = nodedb_types::StorageKey::for_surrogate(Surrogate::ZERO);
        assert!(
            h.core
                .sparse
                .get(0, 7, "notes", &zero_key)
                .expect("get")
                .is_none(),
            "nothing is stored under the ZERO key"
        );

        let delete = doc_delete_sub_record("notes", "unbound", 0);
        let (_, error) = h
            .core
            .install_from_for_test(7, 11, vec![delete], RedoOrigin::Commit);
        assert!(error.is_some(), "a delete under ZERO is refused");
    }

    /// Restart replay skips an unstamped document put on every collection:
    /// it is a point write's pre-dispatch record, whose stamped image carries
    /// the row. The stamped image lands the row once, at its own key, so the
    /// replay adds no version the live write did not write.
    #[test]
    fn an_unstamped_put_is_skipped_and_its_stamped_image_lands_once() {
        let mut h = make_core();
        let row_key = nodedb_types::StorageKey::for_surrogate(Surrogate::new(42));
        let alone = redo_record(7, 0, vec![unstamped_put_sub("plain", 42, "alice")]);
        h.core
            .replay_transaction_redo_wal(
                std::slice::from_ref(&alone),
                1,
                &nodedb_wal::TombstoneSet::new(),
            )
            .expect("redo replay must succeed");
        assert!(
            h.core
                .sparse
                .get(0, 7, "plain", &row_key)
                .expect("get")
                .is_none(),
            "an unstamped record writes nothing"
        );

        let stamp =
            crate::data::executor::handlers::transaction::redo_apply::test_commit::test_stamp();
        let both = redo_record(
            7,
            0,
            vec![
                unstamped_put_sub("plain", 42, "alice"),
                bitemporal_put_sub("plain", 42, stamp),
            ],
        );
        for _ in 0..2 {
            h.core
                .replay_transaction_redo_wal(
                    std::slice::from_ref(&both),
                    1,
                    &nodedb_wal::TombstoneSet::new(),
                )
                .expect("redo replay must succeed");
            assert_eq!(
                h.core
                    .sparse
                    .versioned_latest_sys_from(0, 7, "plain", &row_key)
                    .expect("read version"),
                Some(stamp),
                "the row's newest version is the stamped image's, on every replay"
            );
        }
    }

    /// A committed-redo install refuses an unstamped document put: no
    /// producer writes one into a redo.
    #[test]
    fn a_committed_redo_install_refuses_an_unstamped_put() {
        use nodedb_physical::physical_plan::RedoOrigin;

        let mut h = make_core();
        let (_, error) = h.core.install_from_for_test(
            7,
            10,
            vec![unstamped_put_sub("plain", 42, "alice")],
            RedoOrigin::Commit,
        );
        assert!(error.is_some(), "an unstamped put is refused");
        let row_key = nodedb_types::StorageKey::for_surrogate(Surrogate::new(42));
        assert!(
            h.core
                .sparse
                .get(0, 7, "plain", &row_key)
                .expect("get")
                .is_none()
        );
    }
}
