// SPDX-License-Identifier: BUSL-1.1

//! `EdgePutBatch`: batched edge insert in a single SPSC round-trip.

use tracing::debug;

use crate::bridge::envelope::{EdgeImage, ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::edge_write::{EdgeTarget, EdgeWriteUndo};
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

use super::shared::{check_batch_collections, owns_logical_edge_stats};

impl CoreLoop {
    /// Apply a batched edge insert in a single SPSC round-trip. The batch
    /// writes every edge or none: a refusal after some edges landed takes
    /// them back first.
    ///
    /// Each edge unconditionally writes a new edge-store version and CSR
    /// entry (same no-op-free semantics as [`CoreLoop::execute_edge_put`]),
    /// so a successful batch always reports exactly `edges.len()` affected.
    pub(in crate::data::executor) fn execute_edge_put_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        edges: &[nodedb_physical::physical_plan::BatchEdge],
    ) -> Response {
        debug!(core = self.core_id, count = edges.len(), "edge put batch");
        let database_id = task.request.database_id.as_u64();
        // Every endpoint carries the surrogate its coordinator bound. One
        // unbound endpoint refuses the batch before any edge is written.
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
        if let Err(error) = check_batch_collections(task, edges) {
            return self.response_error(task, error);
        }
        // Each edge's version journals in one write-set part. One edge over
        // the row image limit refuses the batch before any edge is written.
        if let Some(refusal) = edges.iter().find_map(|edge| {
            crate::data::executor::row_image_limit::check_edge(
                edge.collection.as_str(),
                &edge.src_id,
                &edge.label,
                &edge.dst_id,
                &edge.properties,
            )
            .err()
        }) {
            return self.response_error(task, refusal);
        }
        // Every endpoint is checked before any edge is written, so a dangling
        // refusal applies nothing. A decided ordinal marks committed versions
        // written again, which the dangling rule never refuses.
        if self.apply_scope.graph_system_from.is_none()
            && let Some(missing_node) = edges.iter().find_map(|edge| {
                [&edge.src_id, &edge.dst_id]
                    .into_iter()
                    .find(|node| {
                        self.is_node_deleted(database_id, tid, edge.collection.as_str(), node)
                    })
                    .cloned()
            })
        {
            return self.response_error(task, ErrorCode::RejectedDanglingEdge { missing_node });
        }
        // Each edge's version, at the ordinal decided here, journalled after
        // apply. An edge that fails after earlier ones landed takes every
        // landed edge back, so the batch writes all of its edges or none.
        let mut write_set: Vec<WriteSetEntry> = Vec::with_capacity(edges.len());
        let mut landed: Vec<EdgeWriteUndo> = Vec::with_capacity(edges.len());
        for edge in edges {
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
                Err(e) => return self.refuse_edge_batch(task, landed, e.into()),
            };
            let ord = stamp.system_from;
            let valid_from_ms = edge
                .valid_from_ms
                .unwrap_or_else(|| match self.epoch_system_ms {
                    Some(ms) => ms,
                    None => nodedb_types::ordinal_to_ms(ord),
                });
            let valid_until_ms = edge.valid_until_ms.unwrap_or(i64::MAX);
            if valid_until_ms <= valid_from_ms {
                let code = crate::Error::BadRequest {
                    detail: format!(
                        "edge ({})-[{}]->({}) valid time [{valid_from_ms}, \
                         {valid_until_ms}) is empty",
                        edge.src_id, edge.label, edge.dst_id
                    ),
                };
                return self.refuse_edge_batch(task, landed, code.into());
            }
            use crate::engine::graph::edge_store::EdgeRef;
            match self.edge_store.put_edge_version_recorded(
                EdgeRef::new(
                    task.request.database_id,
                    TenantId::new(tid),
                    edge.collection.as_str(),
                    &edge.src_id,
                    &edge.label,
                    &edge.dst_id,
                )
                .with_surrogates(edge.src_surrogate, edge.dst_surrogate),
                &edge.properties,
                stamp,
                valid_from_ms,
                valid_until_ms,
                owns_logical_edge_stats(task, &edge.src_id),
            ) {
                Ok(version) => {
                    self.historical_csr.note_version(
                        task.request.database_id,
                        TenantId::new(tid),
                        edge.collection.as_str(),
                        ord,
                    );
                    let current = version.current.clone();
                    // The CSR follows what the edge resolves to: a version a
                    // TRUNCATE hides leaves the edge as it was. A CSR refusal
                    // takes this version back out, then every edge before it.
                    if let Err(e) = self.mirror_edge_csr(
                        database_id,
                        tid,
                        (&edge.src_id, &edge.label, &edge.dst_id),
                        edge.collection.as_str(),
                        current.as_deref(),
                    ) {
                        let reversed = self.reverse_edge_write(target.undo(version, csr_prior), e);
                        return self.refuse_edge_batch(task, landed, reversed);
                    }
                    write_set.push(WriteSetEntry::edge(EdgeImage::Put(
                        crate::wal::EdgePutRedo {
                            collection: edge.collection.to_string(),
                            src_id: edge.src_id.clone(),
                            label: edge.label.clone(),
                            dst_id: edge.dst_id.clone(),
                            properties: edge.properties.clone(),
                            src_surrogate: edge.src_surrogate.as_u32(),
                            dst_surrogate: edge.dst_surrogate.as_u32(),
                            system_from: Some(ord),
                            applied: (stamp.applied != ord).then_some(stamp.applied),
                            valid_from_ms: Some(valid_from_ms),
                            valid_until_ms: Some(valid_until_ms),
                        },
                    )));
                    let partition = self.csr_partition_mut(database_id, tid);
                    partition.set_node_surrogate(&edge.src_id, edge.src_surrogate);
                    partition.set_node_surrogate(&edge.dst_id, edge.dst_surrogate);
                    landed.push(target.undo(version, csr_prior));
                }
                Err(e) => return self.refuse_edge_batch(task, landed, e.into()),
            }
        }
        for edge in edges {
            if let Err(e) = self.note_edge_write(
                task,
                tid,
                edge.collection.as_str(),
                &edge.src_id,
                &edge.label,
                &edge.dst_id,
            ) {
                return self.refuse_edge_batch(task, landed, e.into());
            }
        }
        // Every edge stands: the batch counts, marks and publishes them.
        if !edges.is_empty() {
            self.checkpoint_coordinator
                .mark_dirty("sparse", edges.len());
        }
        for edge in edges {
            self.count_edge_written(task, &edge.src_id);
            // CDC: `new_value` is the properties image the edge was applied with.
            self.emit_graph_edge_event(
                task,
                crate::data::executor::core_loop::event_emit::GraphEdgeEvent {
                    collection: edge.collection.as_str(),
                    src_id: &edge.src_id,
                    label: &edge.label,
                    dst_id: &edge.dst_id,
                    src_surrogate: edge.src_surrogate,
                    dst_surrogate: edge.dst_surrogate,
                    op: crate::event::WriteOp::Insert,
                    properties: Some(&edge.properties),
                },
            );
        }
        let mut response = self.response_affected(task, edges.len() as u64);
        response.write_set = write_set;
        response
    }
}

#[cfg(test)]
mod tests {
    use crate::bridge::envelope::{ErrorCode, Status};
    use crate::types::TenantId;
    use nodedb_physical::physical_plan::BatchEdge;
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    use super::super::shared::test_support::{make_core, make_task_with_lsn};

    fn edge(src: &str, dst: &str) -> BatchEdge {
        BatchEdge {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "knows"),
            src_id: src.to_string(),
            label: "KNOWS".to_string(),
            dst_id: dst.to_string(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
            valid_from_ms: None,
            valid_until_ms: None,
            properties: Vec::new(),
        }
    }

    /// A node deleted from another collection is not dangling here: a row
    /// delete scopes the tracker to its own collection.
    #[test]
    fn a_node_deleted_in_another_collection_is_not_dangling() {
        let mut h = make_core();
        h.core
            .mark_node_deleted(DatabaseId::DEFAULT.as_u64(), 1, "users", "gone");
        let task = make_task_with_lsn(9);
        let resp = h
            .core
            .execute_edge_put_batch(&task, 1, &[edge("c", "gone")]);
        assert_eq!(resp.status, Status::Ok, "{resp:?}");
    }

    /// The funnel cancels the batch's record on a dangling refusal, so the
    /// refusal must leave no edge of the batch behind.
    #[test]
    fn a_dangling_edge_late_in_the_batch_writes_no_edge() {
        let mut h = make_core();
        h.core
            .mark_node_deleted(DatabaseId::DEFAULT.as_u64(), 1, "knows", "gone");
        let task = make_task_with_lsn(9);
        let edges = vec![edge("a", "b"), edge("c", "gone")];

        let resp = h.core.execute_edge_put_batch(&task, 1, &edges);

        assert_eq!(resp.status, Status::Error);
        assert!(matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::RejectedDanglingEdge { missing_node }) if missing_node == "gone"
        ));
        let first = h
            .core
            .edge_store
            .get_edge(
                DatabaseId::DEFAULT.as_u64(),
                TenantId::new(1),
                "knows",
                "a",
                "KNOWS",
                "b",
            )
            .expect("read edge");
        assert!(
            first.is_none(),
            "the edge before the dangling one is not written"
        );
    }

    /// One edge with properties over the row image limit refuses the batch
    /// with SQLSTATE 54000, and no edge of the batch is written.
    #[test]
    fn an_edge_over_the_row_limit_late_in_the_batch_writes_no_edge() {
        use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;

        let mut h = make_core();
        let task = make_task_with_lsn(10);
        let mut large = edge("c", "d");
        large.properties = vec![0x80; nodedb_types::MAX_ROW_IMAGE_BYTES + 1];
        let edges = vec![edge("a", "b"), large];

        let resp = h.core.execute_edge_put_batch(&task, 1, &edges);

        assert_eq!(resp.status, Status::Error);
        let code = resp.error_code.as_deref().expect("error code");
        assert!(
            matches!(code, ErrorCode::ProgramLimitExceeded { .. }),
            "{code:?}"
        );
        assert_eq!(error_code_to_sqlstate(code).1, "54000");
        assert!(resp.write_set.is_empty(), "a refused batch reports no edge");
        let first = h
            .core
            .edge_store
            .get_edge(
                DatabaseId::DEFAULT.as_u64(),
                TenantId::new(1),
                "knows",
                "a",
                "KNOWS",
                "b",
            )
            .expect("read edge");
        assert!(first.is_none(), "no edge of the refused batch is written");
    }

    /// An edge refused after an earlier edge landed takes the earlier edge
    /// back: the refusal is definite, carries no write set, and leaves the
    /// edge store and the CSR as the batch found them.
    #[test]
    fn an_edge_refused_after_one_landed_takes_the_landed_edge_back() {
        let mut h = make_core();
        let task = make_task_with_lsn(11);
        // An end of valid time before any write's start: the second edge is
        // refused only once its version stamp is decided, after the first
        // edge is written.
        let mut empty = edge("c", "d");
        empty.valid_until_ms = Some(i64::MIN);
        let edges = vec![edge("a", "b"), empty];

        let resp = h.core.execute_edge_put_batch(&task, 1, &edges);

        assert_eq!(resp.status, Status::Error);
        assert!(
            matches!(
                resp.error_code.as_deref(),
                Some(ErrorCode::BadRequest { .. })
            ),
            "a definite refusal: {:?}",
            resp.error_code
        );
        assert!(resp.write_set.is_empty(), "a refused batch reports no edge");
        let first = h
            .core
            .edge_store
            .get_edge(
                DatabaseId::DEFAULT.as_u64(),
                TenantId::new(1),
                "knows",
                "a",
                "KNOWS",
                "b",
            )
            .expect("read edge");
        assert!(first.is_none(), "the landed edge is taken back");
        let partition = h.core.csr_partition(DatabaseId::DEFAULT.as_u64(), 1);
        assert!(
            partition.is_none_or(|csr| !csr.contains_node("a") && !csr.contains_node("b")),
            "the CSR holds no node the batch created"
        );
    }
}
