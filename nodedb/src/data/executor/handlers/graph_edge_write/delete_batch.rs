// SPDX-License-Identifier: BUSL-1.1

//! `EdgeDeleteBatch`: batched edge tombstone in a single SPSC round-trip.

use tracing::debug;

use crate::bridge::envelope::{EdgeImage, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::partial_refusal::refusal_after_rows;
use crate::data::executor::handlers::transaction::undo::edge_write::EdgeTarget;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

use super::shared::owns_logical_edge_stats;

impl CoreLoop {
    /// Apply a batched edge delete in a single SPSC round-trip.
    ///
    /// The bitemporal edge store always appends a tombstone version on
    /// success, whether or not a live edge existed, so each edge's pre-image
    /// is read before its tombstone is written and the affected count is the
    /// number that were actually live — never the batch length.
    pub(in crate::data::executor) fn execute_edge_delete_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        edges: &[nodedb_physical::physical_plan::BatchEdge],
    ) -> Response {
        debug!(
            core = self.core_id,
            count = edges.len(),
            "edge delete batch"
        );
        // Every endpoint carries the surrogate its coordinator bound. One
        // unbound endpoint refuses the batch before any tombstone is written.
        if let Some(refusal) = edges.iter().find_map(|edge| {
            [edge.src_surrogate, edge.dst_surrogate]
                .into_iter()
                .find_map(|surrogate| {
                    crate::data::executor::handlers::unbound_surrogate::refuse_unbound(
                        "graph",
                        edge.collection.as_str(),
                        surrogate,
                    )
                })
        }) {
            return self.response_error(task, refusal);
        }
        // Each edge's write gate is decided against its stored image before
        // any tombstone is written: one denied edge refuses the whole batch
        // and nothing is deleted.
        if let Err(error) = self.admit_edge_deletes(task, tid, edges) {
            return self.response_error(task, error);
        }
        let database_id = task.request.database_id.as_u64();
        let mut removed: u64 = 0;
        // Each edge's tombstone, at the ordinal decided here, journalled after
        // apply. A tombstone that failed after earlier ones landed refuses the
        // batch with the landed tombstones, which stay.
        let mut write_set: Vec<WriteSetEntry> = Vec::with_capacity(edges.len());
        for edge in edges {
            // A pre-image read error refuses the batch: read as absent, the
            // count would leave out an edge the tombstone removes.
            let existed = match self.edge_store.get_edge(
                database_id,
                TenantId::new(tid),
                edge.collection.as_str(),
                &edge.src_id,
                &edge.label,
                &edge.dst_id,
            ) {
                Ok(properties) => properties.is_some(),
                Err(e) => {
                    let code = refusal_after_rows(write_set.len() as u64, e);
                    return self.refusal_with_landed_rows(task, code, write_set);
                }
            };
            let target = EdgeTarget {
                database_id,
                tid,
                collection: edge.collection.as_str(),
                src_id: &edge.src_id,
                label: &edge.label,
                dst_id: &edge.dst_id,
            };
            let csr_prior = self.capture_edge_csr(&target);
            let stamp = match self.graph_write_stamp() {
                Ok(stamp) => stamp,
                Err(e) => {
                    let code = refusal_after_rows(write_set.len() as u64, e);
                    return self.refusal_with_landed_rows(task, code, write_set);
                }
            };
            let ord = stamp.system_from;
            use crate::engine::graph::edge_store::EdgeRef;
            // A tombstone that fails to persist fails the statement, same as
            // the single-edge delete: a count that ignored it would report an
            // edge removed that is still live.
            let tombstone = match self.edge_store.soft_delete_edge_recorded(
                EdgeRef::new(
                    task.request.database_id,
                    TenantId::new(tid),
                    edge.collection.as_str(),
                    &edge.src_id,
                    &edge.label,
                    &edge.dst_id,
                ),
                stamp,
                owns_logical_edge_stats(task, &edge.src_id),
            ) {
                Ok(tombstone) => tombstone,
                Err(e) => {
                    let code = refusal_after_rows(write_set.len() as u64, e);
                    return self.refusal_with_landed_rows(task, code, write_set);
                }
            };
            let current = tombstone.current.clone();
            // The CSR follows what the edge resolves to after the tombstone.
            // A CSR refusal takes this tombstone back out; the edges before it
            // stand and are journalled.
            if let Err(e) = self.mirror_edge_csr(
                database_id,
                tid,
                (&edge.src_id, &edge.label, &edge.dst_id),
                edge.collection.as_str(),
                current.as_deref(),
            ) {
                let reversed = self.reverse_edge_write(target.undo(tombstone, csr_prior), e);
                let code = refusal_after_rows(write_set.len() as u64, reversed);
                return self.refusal_with_landed_rows(task, code, write_set);
            }
            if existed {
                removed += 1;
            }
            write_set.push(WriteSetEntry::edge(EdgeImage::Delete(
                crate::wal::EdgeDeleteRedo {
                    collection: edge.collection.to_string(),
                    src_id: edge.src_id.clone(),
                    label: edge.label.clone(),
                    dst_id: edge.dst_id.clone(),
                    src_surrogate: edge.src_surrogate.as_u32(),
                    dst_surrogate: edge.dst_surrogate.as_u32(),
                    system_from: Some(ord),
                    applied: (stamp.applied != ord).then_some(stamp.applied),
                },
            )));
        }
        if !edges.is_empty() {
            self.checkpoint_coordinator
                .mark_dirty("sparse", edges.len());
        }
        for edge in edges {
            self.note_edge_write(
                task,
                tid,
                edge.collection.as_str(),
                &edge.src_id,
                &edge.label,
                &edge.dst_id,
            );
            // CDC: one Delete event per edge on the edge's own collection.
            self.emit_graph_edge_event(
                task,
                crate::data::executor::core_loop::event_emit::GraphEdgeEvent {
                    collection: edge.collection.as_str(),
                    src_id: &edge.src_id,
                    label: &edge.label,
                    dst_id: &edge.dst_id,
                    src_surrogate: edge.src_surrogate,
                    dst_surrogate: edge.dst_surrogate,
                    op: crate::event::WriteOp::Delete,
                    properties: None,
                },
            );
        }
        let mut response = self.response_affected(task, removed);
        response.write_set = write_set;
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use nodedb_physical::physical_plan::BatchEdge;
    use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate};

    use super::super::shared::EdgePutParams;
    use super::super::shared::test_support::{affected_count, make_core, make_task_with_lsn};

    /// Compiled filters equivalent to a `FOR WRITE` policy on `owner`.
    fn owner_write_check(owner: &str) -> RlsWriteCheck {
        let filter = crate::bridge::scan_filter::ScanFilter {
            field: "owner".into(),
            op: crate::bridge::scan_filter::FilterOp::Eq,
            value: nodedb_types::Value::String(owner.into()),
            clauses: Vec::new(),
            expr: None,
        };
        RlsWriteCheck::Predicate(
            zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy filter"),
        )
    }

    fn owner_properties(owner: &str) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(&serde_json::json!({ "owner": owner }))
            .expect("encode properties")
    }

    fn batch_edge(dst: &str, n: u32, gate: RlsWriteCheck) -> BatchEdge {
        BatchEdge {
            collection: QualifiedCollection::new(crate::types::DatabaseId::DEFAULT, "knows"),
            src_id: "a".into(),
            label: "KNOWS".into(),
            dst_id: dst.into(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(n),
            write_gate: gate,
        }
    }

    /// Seed `a -KNOWS-> b|c|d`, owned by alice, alice, mallory.
    fn seeded() -> super::super::shared::test_support::CoreHarness {
        let mut h = make_core();
        for (lsn, dst, n, owner) in [
            (10, "b", 2, "alice"),
            (11, "c", 3, "alice"),
            (12, "d", 4, "mallory"),
        ] {
            let task = make_task_with_lsn(lsn);
            assert_eq!(
                h.core
                    .execute_edge_put(
                        &task,
                        EdgePutParams {
                            tid: 1,
                            collection: "knows",
                            src_id: "a",
                            label: "KNOWS",
                            dst_id: dst,
                            properties: &owner_properties(owner),
                            src_surrogate: Surrogate::new(1),
                            dst_surrogate: Surrogate::new(n),
                        },
                    )
                    .status,
                Status::Ok
            );
        }
        h
    }

    fn live(h: &super::super::shared::test_support::CoreHarness, dst: &str) -> bool {
        h.core
            .edge_store
            .get_edge(
                crate::types::DatabaseId::DEFAULT.as_u64(),
                TenantId::new(1),
                "knows",
                "a",
                "KNOWS",
                dst,
            )
            .expect("read edge back")
            .is_some()
    }

    /// One denied edge refuses the whole batch: the allowed edges before and
    /// after it stay too.
    #[test]
    fn a_denied_edge_refuses_the_batch_and_deletes_nothing() {
        let mut h = seeded();
        let alice = owner_write_check("alice");
        let edges = [
            batch_edge("b", 2, alice.clone()),
            batch_edge("d", 4, alice.clone()),
            batch_edge("c", 3, alice),
        ];
        let resp = h
            .core
            .execute_edge_delete_batch(&make_task_with_lsn(20), 1, &edges);
        assert_eq!(resp.status, Status::Error);
        assert!(
            resp.write_set.is_empty(),
            "a refused batch journals nothing"
        );
        for dst in ["b", "c", "d"] {
            assert!(live(&h, dst), "edge to {dst} must survive a refused batch");
        }
    }

    /// Edges the policy admits all go in one batch, and the count is theirs.
    #[test]
    fn an_all_admitted_batch_deletes_every_edge() {
        let mut h = seeded();
        let alice = owner_write_check("alice");
        let edges = [batch_edge("b", 2, alice.clone()), batch_edge("c", 3, alice)];
        let resp = h
            .core
            .execute_edge_delete_batch(&make_task_with_lsn(21), 1, &edges);
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(affected_count(&resp), 2);
        assert!(!live(&h, "b") && !live(&h, "c"));
        assert!(live(&h, "d"), "an edge outside the batch must survive");
    }

    /// A gate still at `PendingInjection` denies the batch rather than
    /// admitting it unchecked.
    #[test]
    fn an_uninjected_gate_refuses_the_batch() {
        let mut h = seeded();
        let edges = [batch_edge("b", 2, RlsWriteCheck::pending_injection())];
        let resp = h
            .core
            .execute_edge_delete_batch(&make_task_with_lsn(22), 1, &edges);
        assert_eq!(resp.status, Status::Error);
        assert!(live(&h, "b"));
    }
}
