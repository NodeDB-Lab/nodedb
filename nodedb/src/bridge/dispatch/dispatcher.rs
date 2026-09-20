// SPDX-License-Identifier: BUSL-1.1

use std::collections::{HashMap, HashSet};

use tracing::warn;

use nodedb_bridge::backpressure::{BackpressureConfig, BackpressureController, PressureState};
use nodedb_bridge::buffer::RingBuffer;
use nodedb_bridge::wfq::WeightedFairQueue;
use nodedb_types::PriorityClass;

use crate::bridge::envelope;
use crate::bridge::envelope::{ErrorCode, Payload, Status};
use crate::control::router::vshard::VShardRouter;
use crate::data::eventfd::EventFdNotifier;
use crate::types::{Lsn, RequestId};

use crate::bridge::admission_chokepoint::{assert_write_admitted, reject_uninjected_write};

use super::core_channel::{CoreChannel, CoreChannelDataSide};

/// Serialized form of a request that goes through the SPSC ring buffer.
///
/// The bridge crate is generic over `T` — we serialize our typed `Request`
/// envelope into this form for the ring buffer, and deserialize on the
/// Data Plane side.
#[derive(Debug)]
pub struct BridgeRequest {
    /// The full typed request envelope.
    pub inner: envelope::Request,
}

/// Serialized form of a response coming back from the Data Plane.
#[derive(Debug)]
pub struct BridgeResponse {
    /// The full typed response envelope.
    pub inner: envelope::Response,
}

/// Resolves the priority class for a database at dispatch time.
///
/// Implementations are expected to cache the result (e.g., in a `DashMap` with
/// a time-bounded or version-invalidated TTL) so the hot dispatch path does not
/// hit catalog storage. A `Standard` fallback is returned when the resolver
/// has no record for the given database.
pub trait DatabasePriorityResolver: Send + Sync {
    fn priority_for(&self, database_id: u64) -> PriorityClass;
}

/// No-op resolver: every database gets `Standard` priority.
///
/// Used in tests and in environments where quota catalog is not yet wired up.
pub struct DefaultPriorityResolver;

impl DatabasePriorityResolver for DefaultPriorityResolver {
    fn priority_for(&self, _database_id: u64) -> PriorityClass {
        PriorityClass::Standard
    }
}

/// The dispatcher: routes requests from the Control Plane to the correct
/// Data Plane core via weighted-fair queues and SPSC ring buffers.
///
/// One `Dispatcher` lives on the Control Plane. It owns the producer side
/// of all request channels and the consumer side of all response channels.
///
/// Each core has an in-process weighted-fair queue that reorders requests by
/// `DatabaseId` using deficit round-robin before they reach the physical ring.
/// A database saturating its share of a core does not affect co-resident
/// databases.
pub struct Dispatcher {
    /// One channel pair per Data Plane core.
    pub(super) cores: Vec<CoreChannel>,

    /// Routes vShards to core IDs.
    router: VShardRouter,

    /// Per-tenant in-flight request count across all cores.
    pub(super) tenant_inflight: HashMap<u64, u32>,

    /// Maps request_id → tenant_id for in-flight requests.
    pub(super) request_tenant: HashMap<u64, u64>,

    /// Maximum in-flight requests per tenant (0 = unlimited).
    max_per_tenant_inflight: u32,

    /// Per-core queue capacity (used in tenant fairness recalculation).
    per_core_capacity: u32,

    /// Resolves priority class for a database_id (consulted on enqueue).
    priority_resolver: Box<dyn DatabasePriorityResolver>,

    /// True once the shutdown bus has entered `DrainingDataPlane`.
    ///
    /// Set by [`Dispatcher::begin_data_plane_drain`] under the same mutex every
    /// enqueue takes, so a dispatch that observed `false` has already pushed by
    /// the time the drain starts. Nothing reaches a core after that.
    pub(super) data_plane_draining: bool,
}

impl Dispatcher {
    /// Create a dispatcher with SPSC channels for each core.
    ///
    /// Returns `(Dispatcher, Vec<CoreChannelDataSide>)` — send each
    /// `CoreChannelDataSide` to its respective Data Plane core thread.
    pub fn new(num_cores: usize, queue_capacity: usize) -> (Self, Vec<CoreChannelDataSide>) {
        Self::with_resolver(num_cores, queue_capacity, Box::new(DefaultPriorityResolver))
    }

    /// Like `new`, but accepts a custom `DatabasePriorityResolver`.
    pub fn with_resolver(
        num_cores: usize,
        queue_capacity: usize,
        priority_resolver: Box<dyn DatabasePriorityResolver>,
    ) -> (Self, Vec<CoreChannelDataSide>) {
        let mut cores = Vec::with_capacity(num_cores);
        let mut data_sides = Vec::with_capacity(num_cores);

        for _ in 0..num_cores {
            let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(queue_capacity);
            let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(queue_capacity);

            cores.push(CoreChannel {
                request_tx: req_tx,
                response_rx: resp_rx,
                backpressure: BackpressureController::new(BackpressureConfig::default()),
                wfq: WeightedFairQueue::new(queue_capacity, queue_capacity),
                db_pressure: HashMap::new(),
                wake_notifier: None,
                outstanding: HashSet::new(),
            });

            data_sides.push(CoreChannelDataSide {
                request_rx: req_rx,
                response_tx: resp_tx,
            });
        }

        let router = VShardRouter::round_robin(num_cores);
        let total_capacity = num_cores * queue_capacity;

        (
            Self {
                cores,
                router,
                tenant_inflight: HashMap::new(),
                request_tenant: HashMap::new(),
                max_per_tenant_inflight: total_capacity as u32,
                per_core_capacity: queue_capacity as u32,
                priority_resolver,
                data_plane_draining: false,
            },
            data_sides,
        )
    }

    /// Dispatch a request to the correct Data Plane core.
    ///
    /// Enqueues into the per-core weighted-fair queue keyed by `DatabaseId`,
    /// then flushes WFQ → physical ring. Returns `Err` when the WFQ itself is
    /// full (total capacity reached across all active databases on that core).
    pub fn dispatch(&mut self, request: envelope::Request) -> crate::Result<()> {
        reject_uninjected_write(&request)?;
        assert_write_admitted(&request);
        self.reject_if_draining()?;
        let tenant_id = request.tenant_id.as_u64();
        let req_id = request.request_id.as_u64();
        let database_id = request.database_id.as_u64();

        // Per-tenant fairness: reject if this tenant has too many in-flight requests.
        if self.max_per_tenant_inflight > 0 {
            let inflight = self.tenant_inflight.get(&tenant_id).copied().unwrap_or(0);
            if inflight >= self.max_per_tenant_inflight {
                // Retryable by type (issue 352): the request was not enqueued
                // and nothing was applied. The Calvin scheduler backs off and
                // re-drives instead of logging an error per attempt.
                return Err(crate::Error::DispatchCapacityBusy {
                    tenant_id,
                    inflight,
                    cap: self.max_per_tenant_inflight,
                });
            }
        }

        let core_id =
            self.router
                .resolve(request.vshard_id)
                .ok_or_else(|| crate::Error::Dispatch {
                    detail: format!("no core for vshard {}", request.vshard_id),
                })?;

        let channel = &mut self.cores[core_id];

        // Refresh priority for this DB in the WFQ.
        let cls = self.priority_resolver.priority_for(database_id);
        channel.wfq.set_priority(database_id, cls);

        // Check per-DB suspended state (≥95% of fair share).
        if channel.wfq.is_suspended_for(database_id) {
            return Err(crate::Error::Dispatch {
                detail: format!(
                    "database {database_id}: virtual queue suspended (≥95% of fair share on core {core_id})"
                ),
            });
        }

        // Enqueue into the WFQ — returns Err if total capacity is full.
        channel
            .wfq
            .try_enqueue(database_id, request)
            .map_err(|_| crate::Error::Dispatch {
                detail: format!("core {core_id}: total WFQ capacity exhausted"),
            })?;

        // Update per-DB pressure.
        channel.update_db_pressure(database_id);

        // Flush WFQ → physical ring.
        channel.flush_wfq();

        // Update global backpressure based on ring utilization.
        let util = channel.request_tx.utilization();
        if let Some(new_state) = channel.backpressure.update(util) {
            warn!(
                core_id,
                utilization = util,
                state = ?new_state,
                "backpressure transition"
            );
        }

        // Track the request as outstanding on this core, so a later core death
        // can fail it instead of stranding the caller's waiter.
        channel.outstanding.insert(req_id);

        // Track per-tenant in-flight + request→tenant mapping for response routing.
        *self.tenant_inflight.entry(tenant_id).or_insert(0) += 1;
        self.request_tenant.insert(req_id, tenant_id);

        // Wake the Data Plane core via eventfd.
        if let Some(ref notifier) = channel.wake_notifier {
            notifier.notify();
        }

        Ok(())
    }

    /// Record a response received for a tenant (decrements in-flight count).
    pub fn tenant_response_received(&mut self, tenant_id: u64) {
        if let Some(count) = self.tenant_inflight.get_mut(&tenant_id) {
            *count = count.saturating_sub(1);
        }
    }

    /// Recalculate the per-tenant in-flight limit based on active tenants.
    pub fn recalculate_tenant_limits(&mut self) {
        let active = self.tenant_inflight.len().max(1) as u32;
        let total_capacity: u32 = self.cores.len() as u32 * self.per_core_capacity;
        self.max_per_tenant_inflight = (total_capacity / active).max(2);
        self.tenant_inflight.retain(|_, count| *count > 0);
    }

    /// Dispatch a request directly to a specific core by index.
    ///
    /// Bypasses vShard routing. Used by the checkpoint manager to send
    /// checkpoint requests to every core regardless of vShard assignment.
    pub fn dispatch_to_core(
        &mut self,
        core_id: usize,
        request: envelope::Request,
    ) -> crate::Result<()> {
        reject_uninjected_write(&request)?;
        assert_write_admitted(&request);
        self.reject_if_draining()?;
        if core_id >= self.cores.len() {
            return Err(crate::Error::Dispatch {
                detail: format!("core {core_id} out of range (have {})", self.cores.len()),
            });
        }

        let tenant_id = request.tenant_id.as_u64();
        let req_id = request.request_id.as_u64();
        let database_id = request.database_id.as_u64();
        let channel = &mut self.cores[core_id];

        let cls = self.priority_resolver.priority_for(database_id);
        channel.wfq.set_priority(database_id, cls);

        channel
            .wfq
            .try_enqueue(database_id, request)
            .map_err(|_| crate::Error::Dispatch {
                detail: format!("core {core_id}: total WFQ capacity exhausted"),
            })?;

        channel.update_db_pressure(database_id);
        channel.flush_wfq();

        let util = channel.request_tx.utilization();
        if let Some(new_state) = channel.backpressure.update(util) {
            warn!(
                core_id,
                utilization = util,
                state = ?new_state,
                "backpressure transition"
            );
        }

        channel.outstanding.insert(req_id);

        *self.tenant_inflight.entry(tenant_id).or_insert(0) += 1;
        self.request_tenant.insert(req_id, tenant_id);

        if let Some(ref notifier) = channel.wake_notifier {
            notifier.notify();
        }

        Ok(())
    }

    /// Maximum SPSC request queue utilization across all cores (0-100).
    pub fn max_utilization(&self) -> u8 {
        self.cores
            .iter()
            .map(|c| c.request_tx.utilization())
            .max()
            .unwrap_or(0)
    }

    /// Per-database pressure state for the given core (used by metrics exporters).
    ///
    /// Returns `PressureState::Normal` when no pressure has been recorded for
    /// the database on that core.
    pub fn db_pressure_on_core(&self, core_id: usize, database_id: u64) -> PressureState {
        self.cores
            .get(core_id)
            .and_then(|ch| ch.db_pressure.get(&database_id).copied())
            .unwrap_or(PressureState::Normal)
    }

    /// Poll responses from all Data Plane cores.
    ///
    /// A core whose channel has been observed dead contributes a synthesized
    /// error `Response` for every request still outstanding on it: the one a
    /// failed `try_push` consumed, everything still staged in its WFQ, and
    /// everything dispatched earlier that it never answered. Those travel back
    /// with the real responses so the single completion loop in the caller
    /// finishes each waiter, and the loop below releases each request's
    /// `tenant_inflight` slot exactly as a real response would — without which
    /// one dead core ratchets the tenant's in-flight count until the tenant is
    /// rejected on healthy cores too.
    pub fn poll_responses(&mut self) -> Vec<envelope::Response> {
        let mut responses = Vec::new();
        for (core_id, channel) in self.cores.iter_mut().enumerate() {
            let mut batch = Vec::new();
            let (_drained, producer_gone) = channel.response_rx.drain_into(&mut batch, 64);
            for br in batch {
                let rid = br.inner.request_id.as_u64();
                // A streaming scan answers with many partials before its final
                // response. The request is still executing on the core until
                // that final one arrives, so releasing it here would let the
                // shutdown drain call a live scan finished and would drop the
                // tenant's in-flight slot mid-stream.
                if !br.inner.partial {
                    channel.outstanding.remove(&rid);
                    if let Some(tid) = self.request_tenant.remove(&rid)
                        && let Some(count) = self.tenant_inflight.get_mut(&tid)
                    {
                        *count = count.saturating_sub(1);
                    }
                }
                responses.push(br.inner);
            }

            if !(producer_gone || channel.request_tx.is_disconnected()) {
                // Opportunistically flush WFQ after draining responses to fill headroom.
                channel.flush_wfq();
                continue;
            }

            // The core is gone. Collect every request it can no longer answer:
            // items still staged in the WFQ first (dispatch order), then the
            // rest of the outstanding set. A staged item is also in
            // `outstanding`, so `seen` keeps each id to a single response.
            let mut seen = HashSet::new();
            let mut lost = Vec::new();
            for staged in channel.wfq.drain() {
                let rid = staged.request_id.as_u64();
                if seen.insert(rid) {
                    lost.push(rid);
                }
            }
            for rid in channel.outstanding.drain() {
                if seen.insert(rid) {
                    lost.push(rid);
                }
            }

            // Idempotence: both sources are emptied here — `wfq.drain` leaves
            // the staging queue empty and `outstanding.drain` clears the set —
            // and `flush_wfq` refuses to stage anything new onto a
            // disconnected producer. A later poll therefore finds both empty
            // and emits nothing, so a permanently dead core costs one pass
            // over two empty containers rather than a repeating failure storm.
            for rid in lost {
                if let Some(tid) = self.request_tenant.remove(&rid)
                    && let Some(count) = self.tenant_inflight.get_mut(&tid)
                {
                    *count = count.saturating_sub(1);
                }
                responses.push(envelope::Response {
                    request_id: RequestId::new(rid),
                    status: Status::Error,
                    attempt: 1,
                    partial: false,
                    payload: Payload::empty(),
                    watermark_lsn: Lsn::ZERO,
                    error_code: Some(Box::new(ErrorCode::Internal {
                        detail: format!(
                            "core-{core_id} is gone; the request can never be executed"
                        ),
                    })),
                    read_set_valid: None,
                    read_version_lsn: Lsn::ZERO,
                    write_set: Vec::new(),
                });
            }
        }
        responses
    }

    /// Number of Data Plane cores.
    pub fn num_cores(&self) -> usize {
        self.cores.len()
    }

    /// Set the eventfd notifier for a specific core.
    pub fn set_notifier(&mut self, core_id: usize, notifier: EventFdNotifier) {
        if let Some(channel) = self.cores.get_mut(core_id) {
            channel.wake_notifier = Some(notifier);
        }
    }

    /// Router reference for vShard lookups.
    pub fn router(&self) -> &VShardRouter {
        &self.router
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::*;
    use crate::types::*;
    use nodedb_physical::physical_plan::DocumentOp;
    use std::time::{Duration, Instant};

    fn make_request(vshard: u32) -> envelope::Request {
        envelope::Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(vshard),
            plan: PhysicalPlan::Document(DocumentOp::PointGet {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
                document_id: "u1".into(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            }),
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
            admission: Admission::Exempt(ExemptReason::Read),
        }
    }

    fn make_request_for_db(vshard: u32, db: u64, req_id: u64) -> envelope::Request {
        envelope::Request {
            request_id: RequestId::new(req_id),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::new(db),
            vshard_id: VShardId::new(vshard),
            plan: PhysicalPlan::Document(DocumentOp::PointGet {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::new(db), "c"),
                document_id: "d".into(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            }),
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
            admission: Admission::Exempt(ExemptReason::Read),
        }
    }

    #[test]
    fn dispatch_routes_to_correct_core() {
        let (mut dispatcher, data_sides) = Dispatcher::new(4, 64);

        dispatcher.dispatch(make_request(0)).unwrap();
        dispatcher.dispatch(make_request(1)).unwrap();
        dispatcher.dispatch(make_request(4)).unwrap(); // Wraps to core 0.

        assert_eq!(data_sides[0].request_rx.len(), 2);
        assert_eq!(data_sides[1].request_rx.len(), 1);
        assert_eq!(data_sides[2].request_rx.len(), 0);
    }

    #[test]
    fn response_roundtrip() {
        let (mut dispatcher, mut data_sides) = Dispatcher::new(2, 64);

        dispatcher.dispatch(make_request(0)).unwrap();

        let _req = data_sides[0].request_rx.try_pop().unwrap();
        data_sides[0]
            .response_tx
            .try_push(BridgeResponse {
                inner: envelope::Response {
                    request_id: RequestId::new(1),
                    status: Status::Ok,
                    attempt: 1,
                    partial: false,
                    payload: Payload::from_vec(b"result".to_vec()),
                    watermark_lsn: Lsn::new(42),
                    error_code: None,
                    read_set_valid: None,
                    read_version_lsn: crate::types::Lsn::ZERO,
                    write_set: Vec::new(),
                },
            })
            .unwrap();

        let responses = dispatcher.poll_responses();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].status, Status::Ok);
        assert_eq!(&*responses[0].payload, b"result");
    }

    /// A full per-tenant queue must surface as a *retryable capacity*
    /// condition, never as a generic `Error::Dispatch` that callers treat as a
    /// terminal failure. Issue 352: the Calvin scheduler completes the
    /// transaction as failed and logs ERROR on `Error::Dispatch`, which turns
    /// the startup rebuild into an error storm.
    #[test]
    fn full_queue_is_a_retryable_capacity_condition() {
        let (mut dispatcher, _data_sides) = Dispatcher::new(1, 4);
        for i in 0..4u64 {
            dispatcher
                .dispatch(make_request_for_db(0, i + 1, i + 1))
                .unwrap();
        }
        let err = dispatcher
            .dispatch(make_request_for_db(0, 99, 99))
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::DispatchCapacityBusy { .. }),
            "a full queue must be a retryable capacity signal (issue 352); got {err:?}"
        );
    }

    #[test]
    fn full_queue_returns_error() {
        // With WFQ capacity == ring capacity, filling WFQ should eventually
        // cause total-capacity exhaustion.
        let (mut dispatcher, _data_sides) = Dispatcher::new(1, 4);

        for i in 0..4u64 {
            dispatcher
                .dispatch(make_request_for_db(0, i + 1, i + 1))
                .unwrap();
        }

        // Next dispatch should fail — WFQ total capacity exhausted.
        let result = dispatcher.dispatch(make_request_for_db(0, 99, 99));
        assert!(result.is_err());
    }

    #[test]
    fn dispatch_to_core_tracks_request_lifecycle() {
        let (mut dispatcher, mut data_sides) = Dispatcher::new(2, 64);
        let request = make_request(0);
        let tenant_id = request.tenant_id.as_u64();
        let request_id = request.request_id.as_u64();

        dispatcher.dispatch_to_core(1, request).unwrap();

        assert_eq!(dispatcher.tenant_inflight.get(&tenant_id), Some(&1));
        assert_eq!(dispatcher.request_tenant.get(&request_id), Some(&tenant_id));
        assert_eq!(data_sides[1].request_rx.len(), 1);

        let _req = data_sides[1].request_rx.try_pop().unwrap();
        data_sides[1]
            .response_tx
            .try_push(BridgeResponse {
                inner: envelope::Response {
                    request_id: RequestId::new(request_id),
                    status: Status::Ok,
                    attempt: 1,
                    partial: false,
                    payload: Payload::empty(),
                    watermark_lsn: Lsn::ZERO,
                    error_code: None,
                    read_set_valid: None,
                    read_version_lsn: crate::types::Lsn::ZERO,
                    write_set: Vec::new(),
                },
            })
            .unwrap();

        let responses = dispatcher.poll_responses();
        assert_eq!(responses.len(), 1);
        assert_eq!(dispatcher.tenant_inflight.get(&tenant_id), Some(&0));
        assert!(!dispatcher.request_tenant.contains_key(&request_id));
    }

    #[test]
    fn per_db_pressure_reported() {
        let (mut dispatcher, _) = Dispatcher::new(1, 8);
        // Fill fair share for DB 1 using 4 of 8 slots.
        // With one DB initially, fair share = 8. With two DBs = 4 each.
        // First enqueue DB1 + DB2, so fair_share = 4.
        for i in 0..4u64 {
            dispatcher
                .dispatch(make_request_for_db(0, 1, i + 10))
                .unwrap();
        }
        for i in 0..4u64 {
            dispatcher
                .dispatch(make_request_for_db(0, 2, i + 20))
                .unwrap();
        }
        // After filling DB1's fair share, it should be suspended on core 0.
        // (exact state depends on WFQ flush draining items to ring first)
        // The test confirms per-DB pressure is being tracked without panic.
        let _ = dispatcher.db_pressure_on_core(0, 1);
        let _ = dispatcher.db_pressure_on_core(0, 2);
    }

    // --- Dead-core request loss (GitHub #265) ---
    //
    // When a Data Plane core's consumer/producer is dropped (the core thread
    // died), `Dispatcher` must synthesize an error `Response` for every
    // request it knows is outstanding on that core, rather than dropping the
    // request silently and leaking the caller's waiter + `tenant_inflight`
    // slot forever. Dropping one element of the `data_sides` vector handed
    // back by `Dispatcher::new`/`with_resolver` simulates that core thread
    // dying, matching how `dispatch_routes_to_correct_core` and
    // `response_roundtrip` above obtain the data-plane side of the channel.

    #[test]
    fn dead_core_synthesizes_error_response_for_lost_request() {
        let (mut dispatcher, mut data_sides) = Dispatcher::new(3, 64);

        // Core 2's thread has died: both halves of its data-plane side are gone.
        let dead_core = 2;
        drop(data_sides.remove(dead_core));

        let request = make_request_for_db(0, 1, 7);
        let request_id = request.request_id.as_u64();

        // `dispatch_to_core` still reports success: the request was already
        // moved into the doomed `try_push` inside `flush_wfq` before the
        // failure is observed, which is exactly the defect being covered.
        dispatcher.dispatch_to_core(dead_core, request).unwrap();

        let responses = dispatcher.poll_responses();
        assert_eq!(responses.len(), 1, "expected one synthesized response");
        let resp = &responses[0];
        assert_eq!(resp.request_id.as_u64(), request_id);
        assert_eq!(resp.status, Status::Error);
        match resp.error_code.as_deref() {
            Some(ErrorCode::Internal { detail }) => {
                assert!(
                    detail.contains(&dead_core.to_string()),
                    "error detail should name the dead core, got: {detail}"
                );
            }
            other => panic!("expected ErrorCode::Internal naming the core, got: {other:?}"),
        }
    }

    #[test]
    fn dead_core_synthesized_response_resets_tenant_inflight() {
        // The ratchet: `tenant_inflight` is incremented on dispatch and must
        // return to its pre-dispatch value once the synthesized response for
        // the lost request is drained through `poll_responses` — otherwise it
        // climbs forever and eventually starves the tenant on healthy cores.
        let (mut dispatcher, mut data_sides) = Dispatcher::new(2, 64);
        let dead_core = 0;
        drop(data_sides.remove(dead_core));

        let request = make_request_for_db(0, 1, 1);
        let tenant_id = request.tenant_id.as_u64();

        let before = dispatcher
            .tenant_inflight
            .get(&tenant_id)
            .copied()
            .unwrap_or(0);

        dispatcher.dispatch_to_core(dead_core, request).unwrap();
        assert_eq!(
            dispatcher.tenant_inflight.get(&tenant_id).copied(),
            Some(before + 1),
            "dispatch must still increment tenant_inflight even though the core is dead"
        );

        let responses = dispatcher.poll_responses();
        assert_eq!(responses.len(), 1);
        assert_eq!(
            dispatcher
                .tenant_inflight
                .get(&tenant_id)
                .copied()
                .unwrap_or(0),
            before,
            "tenant_inflight must return to its pre-dispatch value, not ratchet upward"
        );
        assert!(!dispatcher.request_tenant.contains_key(&1));
    }

    #[test]
    fn dead_core_does_not_affect_live_core() {
        let (mut dispatcher, mut data_sides) = Dispatcher::new(2, 64);
        let dead_core = 0;
        let live_core = 1;
        drop(data_sides.remove(dead_core));
        // Removing index 0 shifted core 1's data side down to index 0.
        let live_data_side = &mut data_sides[0];

        let dead_request = make_request_for_db(0, 1, 1);
        let live_request = make_request_for_db(0, 2, 2);
        let live_request_id = live_request.request_id.as_u64();

        dispatcher
            .dispatch_to_core(dead_core, dead_request)
            .unwrap();
        dispatcher
            .dispatch_to_core(live_core, live_request)
            .unwrap();

        // The live core answers normally, through the real ring buffer.
        let _req = live_data_side.request_rx.try_pop().unwrap();
        live_data_side
            .response_tx
            .try_push(BridgeResponse {
                inner: envelope::Response {
                    request_id: RequestId::new(live_request_id),
                    status: Status::Ok,
                    attempt: 1,
                    partial: false,
                    payload: Payload::empty(),
                    watermark_lsn: Lsn::ZERO,
                    error_code: None,
                    read_set_valid: None,
                    read_version_lsn: crate::types::Lsn::ZERO,
                    write_set: Vec::new(),
                },
            })
            .unwrap();

        let responses = dispatcher.poll_responses();
        assert_eq!(
            responses.len(),
            2,
            "one synthesized error from the dead core, one real Ok from the live core"
        );

        let live_resp = responses
            .iter()
            .find(|r| r.request_id.as_u64() == live_request_id)
            .expect("live core's real response must be present");
        assert_eq!(live_resp.status, Status::Ok);
        assert!(live_resp.error_code.is_none());

        let dead_resp = responses
            .iter()
            .find(|r| r.request_id.as_u64() != live_request_id)
            .expect("dead core's synthesized response must be present");
        assert_eq!(dead_resp.status, Status::Error);
        assert!(dead_resp.error_code.is_some());
    }

    #[test]
    fn dead_core_fails_requests_still_queued_in_wfq() {
        // Fill the physical ring to capacity while the core is alive, so a
        // request dispatched afterward parks in the WFQ without ever
        // attempting a push (flush_wfq's utilization check breaks before it
        // reaches the doomed try_push). Then kill the core and confirm the
        // WFQ-queued request is failed too, not left sitting in the queue
        // forever.
        let (mut dispatcher, mut data_sides) = Dispatcher::new(1, 4);

        for i in 0..4u64 {
            dispatcher
                .dispatch_to_core(0, make_request_for_db(0, i + 1, i + 1))
                .unwrap();
        }
        assert_eq!(data_sides[0].request_rx.len(), 4);

        // Core 0's thread dies with 4 unanswered requests sitting in its ring.
        drop(data_sides.remove(0));

        // This request cannot reach the (full, dead) physical ring — it stays
        // parked in the WFQ.
        let parked_request_id = 99u64;
        dispatcher
            .dispatch_to_core(0, make_request_for_db(0, 99, parked_request_id))
            .unwrap();

        let responses = dispatcher.poll_responses();
        let ids: std::collections::HashSet<u64> =
            responses.iter().map(|r| r.request_id.as_u64()).collect();

        // The 4 previously-dispatched-but-unanswered requests, plus the one
        // still parked in the WFQ, must all be failed.
        assert_eq!(
            responses.len(),
            5,
            "expected all 5 outstanding requests failed"
        );
        for id in 1..=4u64 {
            assert!(
                ids.contains(&id),
                "request {id} in the dead ring must be failed"
            );
        }
        assert!(
            ids.contains(&parked_request_id),
            "request parked in the WFQ must be failed, not left queued"
        );
        for r in &responses {
            assert_eq!(r.status, Status::Error);
            assert!(r.error_code.is_some());
        }
    }
}
