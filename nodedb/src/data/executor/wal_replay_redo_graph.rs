// SPDX-License-Identifier: BUSL-1.1

//! WAL redo replay arm for the Graph engine (edges).
//!
//! Like document point writes, graph edge writes have no standalone WAL replay
//! — they survive a crash today via redb's synchronous commit at apply time.
//! Under write-ahead-then-install, a transaction's edge sub-records must replay.
//!
//! ## Sub-record payload shape (chosen here)
//!
//! Extends the autocommit `RecordType::Put` / `Delete` edge shapes
//! (`wal_append_if_write`):
//!
//! * PUT — `(collection, src_id, label, dst_id, properties, src_surrogate,
//!   dst_surrogate, system_from)`. The autocommit shape stops at `properties`;
//!   the redo shape appends endpoint surrogates and the original temporal
//!   ordinal. Legacy seven-element transaction redo remains decodable.
//! * DELETE — `(collection, src_id, label, dst_id)`. Byte-identical to the
//!   autocommit shape; `execute_edge_delete` needs no surrogate.
//!
//! ## Idempotency
//!
//! Both ops are absolute: a PUT overwrites the versioned edge and its CSR entry,
//! a DELETE soft-deletes it. Re-applying either converges — no checkpoint gate.
//! Applied through the same `execute_edge_put` / `execute_edge_delete` handlers
//! the transaction batch replays through, never a reimplementation.

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use nodedb_physical::physical_plan::GraphOp;

use super::core_loop::CoreLoop;
use super::handlers::graph::EdgePutParams;
use super::task::{ExecutionTask, TaskState};
use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::types::{DatabaseId, Lsn, ReadConsistency};
use nodedb_types::RlsWriteCheck;

impl CoreLoop {
    /// Replay reconstituted graph edge `Put` / `Delete` redo sub-records.
    ///
    /// Only records whose payload decodes as an edge tuple are applied; KV and
    /// document `Put`/`Delete` records fail the strict decode (distinct
    /// discriminator / arity / element types) and are left to their own arms.
    pub(crate) fn replay_graph_redo(
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
            let database_id = DatabaseId::new(record.header.database_id);
            let record_lsn = record.header.lsn;

            if is_put {
                // One struct decodes both the current record and any legacy
                // record predating `system_from` (tolerant array decode →
                // `None`), so there is no hand-maintained fallback tuple to
                // drift out of sync with the encoder.
                let Ok(crate::wal::EdgePutRedo {
                    collection,
                    src_id,
                    label,
                    dst_id,
                    properties,
                    src_surrogate: src_sur,
                    dst_surrogate: dst_sur,
                    system_from,
                }) = zerompk::from_msgpack::<crate::wal::EdgePutRedo>(&record.payload)
                else {
                    continue;
                };
                if tombstones.is_tombstoned(
                    database_id.as_u64(),
                    tenant_id,
                    &collection,
                    record_lsn,
                ) {
                    continue;
                }
                if self.claim_for_validation() {
                    continue;
                }
                let task = Self::replay_graph_task(
                    tenant_id,
                    database_id,
                    crate::types::VShardId::new(record.header.vshard_id),
                    record_lsn,
                    PhysicalPlan::Graph(GraphOp::EdgePut {
                        collection: nodedb_types::QualifiedCollection::from_stored(
                            collection.clone(),
                        ),
                        src_id: src_id.clone(),
                        label: label.clone(),
                        dst_id: dst_id.clone(),
                        properties: properties.clone(),
                        src_surrogate: nodedb_types::Surrogate::new(src_sur),
                        dst_surrogate: nodedb_types::Surrogate::new(dst_sur),
                    }),
                );
                self.active_graph_system_from = system_from;
                let mut undo = Vec::new();
                let recording = self.recording_redo_undo();
                let response = self.execute_edge_put_with_undo(
                    &task,
                    EdgePutParams {
                        tid: tenant_id,
                        collection: &collection,
                        src_id: &src_id,
                        label: &label,
                        dst_id: &dst_id,
                        properties: &properties,
                        src_surrogate: nodedb_types::Surrogate::new(src_sur),
                        dst_surrogate: nodedb_types::Surrogate::new(dst_sur),
                    },
                    recording.then_some(&mut undo),
                );
                self.active_graph_system_from = None;
                self.record_redo_undo(undo);
                if response.status == crate::bridge::envelope::Status::Ok {
                    puts += 1;
                } else {
                    self.replay_record_rejected(
                        "graph",
                        record_lsn,
                        response.error_code,
                        &format!("graph edge put into '{collection}' failed"),
                    );
                }
            } else {
                // One struct decodes both the current record and any legacy
                // record predating `system_from` (tolerant array decode →
                // `None`), so there is no hand-maintained fallback tuple.
                let Ok(crate::wal::EdgeDeleteRedo {
                    collection,
                    src_id,
                    label,
                    dst_id,
                    system_from,
                }) = zerompk::from_msgpack::<crate::wal::EdgeDeleteRedo>(&record.payload)
                else {
                    continue;
                };
                if tombstones.is_tombstoned(
                    database_id.as_u64(),
                    tenant_id,
                    &collection,
                    record_lsn,
                ) {
                    continue;
                }
                if self.claim_for_validation() {
                    continue;
                }
                let task = Self::replay_graph_task(
                    tenant_id,
                    database_id,
                    crate::types::VShardId::new(record.header.vshard_id),
                    record_lsn,
                    PhysicalPlan::Graph(GraphOp::EdgeDelete {
                        collection: nodedb_types::QualifiedCollection::from_stored(
                            collection.clone(),
                        ),
                        src_id: src_id.clone(),
                        label: label.clone(),
                        dst_id: dst_id.clone(),
                        src_surrogate: nodedb_types::Surrogate::ZERO,
                        dst_surrogate: nodedb_types::Surrogate::ZERO,
                        // No predicate here: this is crash-recovery replay of
                        // an already-committed WAL record. The identity that
                        // wrote it is not present at boot.
                        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
                    }),
                );
                self.active_graph_system_from = system_from;
                let mut undo = Vec::new();
                let recording = self.recording_redo_undo();
                let response = self.execute_edge_delete_with_undo(
                    &task,
                    crate::data::executor::handlers::graph::EdgeDeleteParams {
                        tid: tenant_id,
                        collection: &collection,
                        src_id: &src_id,
                        label: &label,
                        dst_id: &dst_id,
                        // Replay carries no predicate: the policy decided this
                        // edge when the record was written, and the writing
                        // identity is not present at boot.
                        rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
                    },
                    recording.then_some(&mut undo),
                );
                self.active_graph_system_from = None;
                self.record_redo_undo(undo);
                if response.status == crate::bridge::envelope::Status::Ok {
                    deletes += 1;
                } else {
                    self.replay_record_rejected(
                        "graph",
                        record_lsn,
                        response.error_code,
                        &format!("graph edge delete in '{collection}' failed"),
                    );
                }
            }
        }

        if puts > 0 || deletes > 0 {
            tracing::info!(
                core = self.core_id,
                puts,
                deletes,
                "WAL graph redo replay complete"
            );
        }
    }

    /// Replay reconstituted `GraphNodeLabelSet` / `GraphNodeLabelRemove` redo
    /// sub-records — the transaction-resolve counterpart of
    /// `replay_graph_node_label_wal` (autocommit, `wal_replay_graph_labels.rs`).
    ///
    /// A transaction's staged node-label deltas resolve to the SAME
    /// `(node_id, labels)` payload shape the autocommit path produces (see
    /// `resolve/graph.rs`'s `serialize_node_label_deltas`), so this routes
    /// each reconstituted record through the SAME `try_replay_graph_node_label`
    /// decoder rather than reimplementing it — producer and both replay paths
    /// never drift on shape.
    pub(crate) fn replay_graph_node_labels_redo(
        &mut self,
        records: &[WalRecord],
        num_cores: usize,
    ) {
        let mut replayed = 0usize;

        for record in records {
            let record_type = RecordType::from_raw(record.logical_record_type());
            let is_set = record_type == Some(RecordType::GraphNodeLabelSet);
            let is_remove = record_type == Some(RecordType::GraphNodeLabelRemove);
            if !is_set && !is_remove {
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

            let database_id = DatabaseId::new(record.header.database_id);
            if let Some(applied) = self.try_replay_graph_node_label(record, database_id) {
                replayed += applied;
            }
        }

        if replayed > 0 {
            tracing::info!(
                core = self.core_id,
                replayed,
                "WAL graph node-label redo replay complete"
            );
        }
    }

    /// Build a synthetic `ExecutionTask` for graph edge redo replay. Carries the
    /// enclosing record's `database_id` (which the edge handlers read for
    /// keying) and its LSN as `wal_lsn` so the committed-edge write-version index
    /// is repopulated exactly as on the live path.
    fn replay_graph_task(
        tenant_id: u64,
        database_id: DatabaseId,
        vshard_id: crate::types::VShardId,
        record_lsn: u64,
        plan: PhysicalPlan,
    ) -> ExecutionTask {
        let wal_lsn = Some(Lsn::new(record_lsn));
        ExecutionTask {
            request: Request {
                request_id: crate::types::RequestId::new(0),
                tenant_id: crate::types::TenantId::new(tenant_id),
                database_id,
                vshard_id,
                plan,
                deadline: std::time::Instant::now()
                    + crate::data::executor::deadline::REPLAY_DEADLINE,
                priority: Priority::Normal,
                trace_id: crate::types::TraceId::ZERO,
                consistency: ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
                user_id: None,
                statement_digest: None,
                txn_id: None,
                wal_lsn,
                resolved_now_ms: None,
                admission: crate::bridge::envelope::Admission::Exempt(
                    crate::bridge::envelope::ExemptReason::AlreadyOrdered,
                ),
            },
            state: TaskState::Running,
            wal_lsn,
            resolved_now_ms: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{RedoRecord, RedoSubRecord};
    use nodedb_wal::record::WalRecordArgs;
    use std::sync::Arc;

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

    fn edge_put_sub(collection: &str, src: &str, label: &str, dst: &str) -> RedoSubRecord {
        let payload = zerompk::to_msgpack_vec(&crate::wal::EdgePutRedo {
            collection: collection.to_string(),
            src_id: src.to_string(),
            label: label.to_string(),
            dst_id: dst.to_string(),
            properties: Vec::new(),
            src_surrogate: 10,
            dst_surrogate: 20,
            system_from: Some(nodedb_types::ms_to_ordinal_upper(100)),
        })
        .expect("encode edge put sub-record");
        RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload,
        }
    }

    fn redo_record(tenant_id: u64, ops: Vec<RedoSubRecord>) -> WalRecord {
        let redo = RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
        };
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn: 1,
            tenant_id,
            vshard_id: crate::types::VShardId::from_key(b"a").as_u32(),
            database_id: 0,
            payload: redo.to_bytes().expect("encode redo record"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    fn edge_count(h: &CoreHarness, collection: &str, src: &str) -> usize {
        h.core
            .edge_store
            .neighbors_out(0, crate::types::TenantId::new(7), collection, src, None)
            .expect("neighbors_out")
            .len()
    }

    #[test]
    fn redo_graph_edge_put_restores_edge() {
        let mut h = make_core();
        let record = redo_record(7, vec![edge_put_sub("knows", "a", "KNOWS", "b")]);

        h.core
            .replay_transaction_redo_wal(
                std::slice::from_ref(&record),
                1,
                &nodedb_wal::TombstoneSet::new(),
            )
            .expect("redo replay must succeed");

        assert_eq!(
            edge_count(&h, "knows", "a"),
            1,
            "graph edge must be restored from redo replay"
        );
        let stats = h
            .core
            .edge_store
            .collection_stats(0, crate::types::TenantId::new(7), "knows", None)
            .expect("graph stats after redo");
        assert_eq!(stats.edge_count, 1, "source-home redo must restore stats");
        let historical = h
            .core
            .edge_store
            .collection_stats(
                0,
                crate::types::TenantId::new(7),
                "knows",
                Some(nodedb_types::ms_to_ordinal_upper(100)),
            )
            .expect("historical graph stats after redo");
        assert_eq!(
            historical.edge_count, 1,
            "redo must restore the committed temporal version at its original time"
        );
    }

    #[test]
    fn redo_graph_edge_put_records_write_version_floor() {
        use crate::data::executor::core_loop::write_index::{KeyRepr, WriteKey};

        let mut h = make_core();
        let record = redo_record(7, vec![edge_put_sub("knows", "a", "KNOWS", "b")]);

        h.core
            .replay_transaction_redo_wal(
                std::slice::from_ref(&record),
                1,
                &nodedb_wal::TombstoneSet::new(),
            )
            .expect("redo replay must succeed");

        let write_key = WriteKey {
            db: DatabaseId::new(0),
            tenant: crate::types::TenantId::new(7),
            collection: Box::from("knows"),
            key: KeyRepr::Edge {
                src: Box::from("a"),
                label: Box::from("KNOWS"),
                dst: Box::from("b"),
            },
        };
        assert_eq!(
            h.core.write_index.key_write_lsn(&write_key),
            Some(Lsn::new(1)),
            "graph edge redo replay must record the write-version floor at the record's LSN"
        );
    }

    #[test]
    fn redo_graph_edge_put_idempotent_double_replay() {
        let mut h = make_core();
        let record = redo_record(7, vec![edge_put_sub("knows", "a", "KNOWS", "b")]);
        let tomb = nodedb_wal::TombstoneSet::new();

        // The edge is keyed by (src, label, dst); re-applying overwrites the
        // same versioned edge and CSR entry, so double replay converges to one.
        h.core
            .replay_transaction_redo_wal(std::slice::from_ref(&record), 1, &tomb)
            .expect("redo replay must succeed");
        h.core
            .replay_transaction_redo_wal(std::slice::from_ref(&record), 1, &tomb)
            .expect("redo replay must succeed");

        assert_eq!(
            edge_count(&h, "knows", "a"),
            1,
            "graph edge put must converge under double replay"
        );
    }
}
