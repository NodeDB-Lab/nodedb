// SPDX-License-Identifier: BUSL-1.1

use tracing::warn;

use crate::bridge::dispatch::BridgeResponse;
use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::io::io_metrics::{TIER_CRITICAL, TIER_HIGH, TIER_LOW};

use super::super::task::{ExecutionTask, TaskState};
use super::CoreLoop;

impl CoreLoop {
    /// Drain incoming requests from the SPSC bridge into the priority queues.
    ///
    /// The number of requests drained per call is bounded by `spsc_read_depth`,
    /// which is reduced under Critical memory pressure and set to zero under
    /// Emergency. Each request is routed to the Critical, High, or Low tier
    /// based on `Request.priority`.
    pub fn drain_requests(&mut self) {
        if self.throttle.suspends_reads() {
            // Intake suspended. In-flight tasks still answer, so the
            // response ring drains and the condition clears itself.
            return;
        }
        let depth = self.throttle.read_depth();
        let mut batch = Vec::new();
        // A disconnected producer here means the Control Plane is gone, which
        // only happens as the process itself terminates: a core thread has no
        // shutdown signal of its own and relies on process exit. Whatever is
        // already buffered is still drained and executed; there is no separate
        // action for the core to take, so the flag is not acted on.
        let (_drained, _control_plane_gone) = self.request_rx.drain_into(&mut batch, depth);
        for br in batch {
            self.task_queue.push(ExecutionTask::new(br.inner));
        }
    }

    /// Process the next pending task using the 8:4:2 priority drain ratio.
    ///
    /// Advances `self.drain_cycle` by one slot and returns `true` if a task
    /// was processed.
    pub fn poll_one(&mut self) -> bool {
        let Some(qt) = self.task_queue.pop_next(&mut self.drain_cycle) else {
            return false;
        };

        // Record IO wait from enqueue to execution start.
        let wait_ns = qt.enqueued_at.elapsed().as_nanos() as u64;
        use crate::bridge::envelope::Priority;
        let tier = match qt.task.request.priority {
            Priority::Background | Priority::Normal => TIER_LOW,
            Priority::High => TIER_HIGH,
            Priority::Critical => TIER_CRITICAL,
        };
        self.io_metrics.record_wait(tier, wait_ns);

        let mut task = qt.task;

        if let Some(key) = task.request.idempotency_key
            && let Some(&succeeded) = self.idempotency_cache.get(&key)
        {
            let response = if succeeded {
                self.response_ok(&task)
            } else {
                self.response_error(&task, ErrorCode::DuplicateWrite)
            };
            if let Err(e) = self
                .response_tx
                .try_push(BridgeResponse { inner: response })
            {
                warn!(core = self.core_id, error = %e, "failed to send idempotent response");
            }
            return true;
        }

        let response = if task.is_expired() {
            task.state = TaskState::Failed;
            Response {
                request_id: task.request_id(),
                status: Status::Error,
                attempt: 1,
                partial: false,
                payload: Payload::empty(),
                watermark_lsn: self.watermark,
                error_code: Some(Box::new(ErrorCode::DeadlineExceeded)),
                read_set_valid: None,
                read_version_lsn: crate::types::Lsn::ZERO,
                write_set: Vec::new(),
            }
        } else {
            task.state = TaskState::Running;
            let resp = self.execute(&task);
            task.state = TaskState::Completed;
            resp
        };

        if let Some(key) = task.request.idempotency_key {
            let succeeded = response.status == Status::Ok;
            if self.idempotency_cache.len() >= 16_384
                && let Some(oldest_key) = self.idempotency_order.pop_front()
            {
                self.idempotency_cache.remove(&oldest_key);
            }
            self.idempotency_cache.insert(key, succeeded);
            self.idempotency_order.push_back(key);
        }

        // Bound the dangling-edge tracker across all tenants. Count
        // all entries; when the aggregate exceeds the cap, drop the
        // whole map — callers are tolerant of false negatives (an
        // `EdgePut` to a recently-deleted node races the tracker, so
        // the semantics are advisory regardless).
        let total: usize = self.deleted_nodes.values().map(|s| s.len()).sum();
        if total > 100_000 {
            self.deleted_nodes.clear();
        }

        if let Err(e) = self
            .response_tx
            .try_push(BridgeResponse { inner: response })
        {
            warn!(core = self.core_id, error = %e, "failed to send response — response queue full");
        }

        true
    }

    /// Run one iteration of the event loop: drain requests, process tasks.
    ///
    /// After processing, update the per-priority queue-depth gauges so the
    /// Prometheus endpoint reflects the post-tick state.
    pub fn tick(&mut self) -> usize {
        self.poll_build_completions();
        self.poll_pending_reindex();
        // Adjust SPSC read depth based on current memory pressure.
        self.apply_spsc_pressure();
        self.drain_requests();
        let mut processed = 0;
        while !self.task_queue.is_empty() {
            let batched = self.poll_write_batch();
            if batched > 0 {
                processed += batched;
                continue;
            }
            if self.poll_one() {
                processed += 1;
            } else {
                break;
            }
        }

        // Update queue-depth gauges after draining.
        self.io_metrics
            .record_queue_depth(TIER_CRITICAL, self.task_queue.critical_len() as u64);
        self.io_metrics
            .record_queue_depth(TIER_HIGH, self.task_queue.high_len() as u64);
        self.io_metrics
            .record_queue_depth(TIER_LOW, self.task_queue.low_len() as u64);

        processed
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
    use nodedb_physical::physical_plan::{DocumentOp, MetaOp};
    use nodedb_types::{QualifiedCollection, Surrogate, SurrogateBitmap};

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Priority, Request, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

    fn make_core() -> (
        CoreLoop,
        Producer<BridgeRequest>,
        Consumer<BridgeResponse>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .unwrap();
        (core, req_tx, resp_rx, dir)
    }

    fn make_request(plan: PhysicalPlan) -> Request {
        Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Admitted,
        }
    }

    #[test]
    fn empty_tick_processes_nothing() {
        let (mut core, _, _, _dir) = make_core();
        assert_eq!(core.tick(), 0);
    }

    #[test]
    fn expired_task_returns_deadline_exceeded() {
        let (mut core, mut req_tx, mut resp_rx, _dir) = make_core();
        req_tx
            .try_push(BridgeRequest {
                inner: Request {
                    deadline: Instant::now() - Duration::from_secs(1),
                    ..make_request(PhysicalPlan::Document(DocumentOp::PointGet {
                        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "x"),
                        document_id: "y".into(),
                        surrogate: nodedb_types::Surrogate::ZERO,
                        pk_bytes: Vec::new(),
                        rls_filters: Vec::new(),
                        system_time: nodedb_types::SystemTimeScope::Current,
                        valid_at_ms: None,
                    }))
                },
            })
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Error);
        assert_eq!(
            resp.inner.error_code.as_deref(),
            Some(&ErrorCode::DeadlineExceeded)
        );
    }

    #[test]
    fn watermark_in_response() {
        let (mut core, mut req_tx, mut resp_rx, _dir) = make_core();
        core.advance_watermark(Lsn::new(99));
        core.sparse
            .put(
                0,
                1,
                "x",
                &nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::new(999)),
                b"data",
            )
            .unwrap();
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::Document(DocumentOp::PointGet {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "x"),
                    document_id: "y".into(),
                    surrogate: nodedb_types::Surrogate::ZERO,
                    pk_bytes: Vec::new(),
                    rls_filters: Vec::new(),
                    system_time: nodedb_types::SystemTimeScope::Current,
                    valid_at_ms: None,
                })),
            })
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.watermark_lsn, Lsn::new(99));
    }

    #[test]
    fn cancel_removes_pending_task() {
        let (mut core, mut req_tx, _resp_rx, _dir) = make_core();
        req_tx
            .try_push(BridgeRequest {
                inner: Request {
                    request_id: RequestId::new(10),
                    deadline: Instant::now() + Duration::from_secs(60),
                    ..make_request(PhysicalPlan::Document(DocumentOp::PointGet {
                        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "x"),
                        document_id: "y".into(),
                        surrogate: nodedb_types::Surrogate::ZERO,
                        pk_bytes: Vec::new(),
                        rls_filters: Vec::new(),
                        system_time: nodedb_types::SystemTimeScope::Current,
                        valid_at_ms: None,
                    }))
                },
            })
            .unwrap();
        core.drain_requests();
        assert_eq!(core.pending_count(), 1);

        req_tx
            .try_push(BridgeRequest {
                inner: Request {
                    request_id: RequestId::new(99),
                    priority: Priority::Critical,
                    consistency: ReadConsistency::Eventual,
                    ..make_request(PhysicalPlan::Meta(MetaOp::Cancel {
                        target_request_id: RequestId::new(10),
                    }))
                },
            })
            .unwrap();
        // Cancel runs at Critical priority and is drained before the Normal-priority
        // target. The cancel removes id=10 from the queue, so only the Cancel itself
        // is processed in this tick (no response is emitted for the cancelled task).
        assert_eq!(core.tick(), 1);
        assert_eq!(core.pending_count(), 0);
    }

    #[test]
    fn point_put_stores_schemaless_docs_as_canonical_msgpack_maps() {
        let (mut core, mut req_tx, mut resp_rx, _dir) = make_core();

        let mut obj = std::collections::HashMap::new();
        obj.insert(
            "user_id".to_string(),
            nodedb_types::Value::String("u1".into()),
        );
        obj.insert(
            "item".to_string(),
            nodedb_types::Value::String("book".into()),
        );
        let tagged = zerompk::to_msgpack_vec(&nodedb_types::Value::Object(obj)).unwrap();

        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
                    document_id: "o1".into(),
                    value: tagged,
                    surrogate: nodedb_types::Surrogate::ZERO,
                    pk_bytes: Vec::new(),
                    returning: None,
                    rls_filters: Vec::new(),
                    resolved_sum_targets: Vec::new(),
                })),
            })
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok);

        // The handler hex-encodes the surrogate to compute the substrate
        // row key; this fixture used `Surrogate::ZERO`, which renders to
        // "00000000".
        let stored = core
            .sparse
            .get(
                0,
                1,
                "orders",
                &nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::ZERO),
            )
            .unwrap()
            .unwrap();
        assert!(nodedb_query::msgpack_scan::map_header(&stored, 0).is_some());
        assert!(nodedb_query::msgpack_scan::extract_field(&stored, 0, "user_id").is_some());
        assert!(nodedb_query::msgpack_scan::extract_field(&stored, 0, "item").is_some());
    }

    #[test]
    fn scan_with_prefilter_returns_only_bitmap_members() {
        let (mut core, mut req_tx, mut resp_rx, _dir) = make_core();

        // Insert three documents with surrogates 1, 2, and 3.
        let surrogates: &[(u32, &str)] = &[(1, "alpha"), (2, "beta"), (3, "gamma")];
        for (sur_val, name) in surrogates {
            let mut obj = std::collections::HashMap::new();
            obj.insert(
                "name".to_string(),
                nodedb_types::Value::String((*name).into()),
            );
            let bytes = zerompk::to_msgpack_vec(&nodedb_types::Value::Object(obj)).unwrap();
            req_tx
                .try_push(BridgeRequest {
                    inner: make_request(PhysicalPlan::Document(DocumentOp::PointPut {
                        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "things"),
                        document_id: format!("doc_{sur_val}"),
                        value: bytes,
                        surrogate: Surrogate::new(*sur_val),
                        pk_bytes: Vec::new(),
                        returning: None,
                        rls_filters: Vec::new(),
                        resolved_sum_targets: Vec::new(),
                    })),
                })
                .unwrap();
            core.tick();
            let _ = resp_rx.try_pop().unwrap();
        }

        // Build a prefilter containing only surrogates 1 and 3 (not 2).
        let prefilter = SurrogateBitmap::from_iter([Surrogate::new(1), Surrogate::new(3)]);

        // Issue a scan with the prefilter.
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::Document(DocumentOp::Scan {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "things"),
                    limit: 100,
                    offset: 0,
                    sort_keys: Vec::new(),
                    filters: Vec::new(),
                    distinct: false,
                    projection: Vec::new(),
                    computed_columns: Vec::new(),
                    window_functions: Vec::new(),
                    system_time: nodedb_types::SystemTimeScope::Current,
                    valid_at_ms: None,
                    prefilter: Some(prefilter),
                })),
            })
            .unwrap();
        core.tick();

        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok, "scan should succeed");

        // Decode the response payload: array of {id, data} maps.
        // Use msgpack_scan to iterate the outer array and extract each row's "id" field.
        let payload = resp.inner.payload.to_vec();
        let (count, mut pos) = nodedb_query::msgpack_scan::array_header(&payload, 0)
            .expect("payload should be a msgpack array");

        assert_eq!(count, 2, "expected exactly 2 rows after prefilter");

        let mut returned_ids = std::collections::HashSet::new();
        for _ in 0..count {
            // Each element is a 2-entry fixmap {"id": "...", "data": ...}.
            if let Some((id_start, _)) =
                nodedb_query::msgpack_scan::extract_field(&payload, pos, "id")
                && let Some(id_str) = nodedb_query::msgpack_scan::read_str(&payload, id_start)
            {
                returned_ids.insert(id_str.to_string());
            }
            pos = nodedb_query::msgpack_scan::skip_value(&payload, pos)
                .expect("should be able to skip map entry");
        }

        assert!(
            returned_ids.contains("00000001"),
            "surrogate 1 should be in results"
        );
        assert!(
            returned_ids.contains("00000003"),
            "surrogate 3 should be in results"
        );
        assert!(
            !returned_ids.contains("00000002"),
            "surrogate 2 (not in prefilter) must not appear"
        );
    }
}
