// SPDX-License-Identifier: BUSL-1.1

//! WAL replay arm for the Graph engine (edges).
//!
//! Every edge version the edge store holds has a WAL record carrying it at
//! its ordinal, and this arm applies them all in LSN order: the version
//! sub-records an autocommit edge write journals in its `WriteGroup` after
//! apply, and the sub-records of committed `TransactionRedo` records alike.
//! A restart re-applies versions the store already holds, and a
//! point-in-time restore applies the versions its base lacks.
//!
//! ## Record payload shape
//!
//! The map-encoded payloads every edge record carries:
//!
//! * PUT — [`crate::wal::EdgePutRedo`]: the edge, its properties, both endpoint
//!   surrogates, and the version's ordinal.
//! * DELETE — [`crate::wal::EdgeDeleteRedo`]: the edge, both endpoint
//!   surrogates, and the tombstone's ordinal.
//! * `GraphNodeCascade` — [`crate::wal::NodeCascadeRedo`]: every edge one
//!   document delete's node cascade tombstoned, each at its own ordinal, in
//!   a WAL an older build wrote. Replay writes exactly those tombstones,
//!   skips a key that already holds one, and drops the node's identity
//!   binding. No live write produces this record.
//! * `GraphEdgeCut` — [`crate::wal::EdgeCutRedo`]: one TRUNCATE share's cut
//!   of an edge collection, installed by `wal_replay_redo_graph_cut`.
//!
//! A PUT or DELETE that carries an applied ordinal is a restored version: it
//! keeps its historical system time and is applied at the restore's ordinal.
//!
//! An autocommit write's pre-dispatch record carries no ordinal: the apply
//! decides it. That record is skipped here, and the version record that
//! follows it installs the version. An edge record that decodes but carries
//! an unbound endpoint surrogate is refused as unapplied. It is never
//! installed under `Surrogate::ZERO`.
//!
//! ## Idempotency
//!
//! Both ops write one version key, `(edge, ordinal)`: a PUT overwrites that
//! version and the edge's CSR entry, a DELETE writes that tombstone.
//! Re-applying either converges — no checkpoint gate. Applied through the
//! same `execute_edge_put` / `execute_edge_delete` handlers the transaction
//! batch replays through, never a reimplementation.

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
        let mut cuts = 0usize;

        for record in records {
            if self.replay_halted() {
                break;
            }
            let record_type = RecordType::from_raw(record.logical_record_type());
            let is_put = record_type == Some(RecordType::Put);
            let is_delete = record_type == Some(RecordType::Delete);
            let is_cascade = record_type == Some(RecordType::GraphNodeCascade);
            let is_cut = record_type == Some(RecordType::GraphEdgeCut);
            if !is_put && !is_delete && !is_cascade && !is_cut {
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

            if is_cascade {
                if self.replay_node_cascade(record) {
                    deletes += 1;
                }
                continue;
            }
            if is_cut {
                if self.replay_edge_cut(record, tombstones) {
                    cuts += 1;
                }
                continue;
            }

            if is_put {
                // A payload that is not an edge put belongs to another arm.
                let Ok(edge) = zerompk::from_msgpack::<crate::wal::EdgePutRedo>(&record.payload)
                else {
                    continue;
                };
                // An autocommit write's pre-dispatch record names the write,
                // not the version: the apply decided the ordinal, and the
                // version record journalled after apply carries it.
                if edge.system_from.is_none() {
                    continue;
                }
                let Some((src_surrogate, dst_surrogate)) = edge.endpoints() else {
                    self.replay_record_unapplied(
                        "graph",
                        "edge_put_identity",
                        record_lsn,
                        &format!(
                            "edge put in '{}' carries an unbound endpoint",
                            edge.collection
                        ),
                    );
                    continue;
                };
                let crate::wal::EdgePutRedo {
                    collection,
                    src_id,
                    label,
                    dst_id,
                    properties,
                    system_from,
                    applied,
                    ..
                } = edge;
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
                        src_surrogate,
                        dst_surrogate,
                    }),
                );
                self.apply_scope.graph_system_from = system_from;
                self.apply_scope.graph_applied = applied;
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
                        src_surrogate,
                        dst_surrogate,
                    },
                    recording.then_some(&mut undo),
                );
                self.apply_scope.graph_system_from = None;
                self.apply_scope.graph_applied = None;
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
                // A payload that is not an edge delete belongs to another arm.
                let Ok(edge) = zerompk::from_msgpack::<crate::wal::EdgeDeleteRedo>(&record.payload)
                else {
                    continue;
                };
                // See the put arm: the tombstone record journalled after
                // apply carries the ordinal.
                if edge.system_from.is_none() {
                    continue;
                }
                let Some((src_surrogate, dst_surrogate)) = edge.endpoints() else {
                    self.replay_record_unapplied(
                        "graph",
                        "edge_delete_identity",
                        record_lsn,
                        &format!(
                            "edge delete in '{}' carries an unbound endpoint",
                            edge.collection
                        ),
                    );
                    continue;
                };
                let crate::wal::EdgeDeleteRedo {
                    collection,
                    src_id,
                    label,
                    dst_id,
                    system_from,
                    applied,
                    ..
                } = edge;
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
                        src_surrogate,
                        dst_surrogate,
                        // No predicate here: this is crash-recovery replay of
                        // an already-committed WAL record. The identity that
                        // wrote it is not present at boot.
                        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
                    }),
                );
                self.apply_scope.graph_system_from = system_from;
                self.apply_scope.graph_applied = applied;
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
                        src_surrogate,
                        dst_surrogate,
                        // Replay carries no predicate: the policy decided this
                        // edge when the record was written, and the writing
                        // identity is not present at boot.
                        rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
                    },
                    recording.then_some(&mut undo),
                );
                self.apply_scope.graph_system_from = None;
                self.apply_scope.graph_applied = None;
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

        if puts > 0 || deletes > 0 || cuts > 0 {
            tracing::info!(
                core = self.core_id,
                puts,
                deletes,
                cuts,
                "WAL graph redo replay complete"
            );
        }
    }

    /// Write one journalled node cascade: each tombstone at its ordinal, the
    /// node's identity binding dropped, and the node's edges gone from the
    /// CSR. Returns whether it applied. A cascade that does not decode is a
    /// committed effect replay cannot apply, and halts replay.
    fn replay_node_cascade(&mut self, record: &WalRecord) -> bool {
        let record_lsn = record.header.lsn;
        let cascade = match zerompk::from_msgpack::<crate::wal::NodeCascadeRedo>(&record.payload) {
            Ok(cascade) => cascade,
            Err(e) => {
                self.replay_record_unapplied(
                    "graph",
                    "node_cascade_decode",
                    record_lsn,
                    &format!("node cascade does not decode: {e}"),
                );
                return false;
            }
        };
        if self.claim_for_validation() {
            return false;
        }
        let database_id = record.header.database_id;
        let tenant_id = record.header.tenant_id;
        if let Err(e) = self.edge_store.apply_node_cascade(
            database_id,
            crate::types::TenantId::new(tenant_id),
            &cascade.node,
            &cascade.edges,
        ) {
            self.replay_record_unapplied(
                "graph",
                "node_cascade_apply",
                record_lsn,
                &format!("node cascade of '{}' failed: {e}", cascade.node),
            );
            return false;
        }
        self.csr_partition_mut(database_id, tenant_id)
            .remove_node_edges(&cascade.node);
        true
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
            if self.replay_halted() {
                break;
            }
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
                commit_hlc: None,
                entry_version: None,
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
            applied: None,
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
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
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
    fn redo_graph_edge_put_records_its_write_version() {
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

        // The edge applies on the vShard its record names, at the record's
        // LSN: no registered stamp names an entry for it.
        let write_key = WriteKey {
            vshard: crate::types::VShardId::new(record.header.vshard_id),
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
            h.core.write_index.key_version(&write_key),
            Some(crate::data::executor::core_loop::write_index::tests::local(
                1
            )),
            "graph edge redo replay must record the write version at the record's LSN"
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
