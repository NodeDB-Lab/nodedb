// SPDX-License-Identifier: BUSL-1.1

//! The bridge [`Dispatcher`]: its per-core channels, construction, and
//! read-only accessors.
//!
//! Admission and enqueue live in `enqueue`. Response polling and dead-core
//! synthesis live in `response_poll`. The shutdown drain lives in `drain`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use nodedb_bridge::backpressure::{BackpressureConfig, BackpressureController, PressureState};
use nodedb_bridge::buffer::RingBuffer;
use nodedb_bridge::wfq::WeightedFairQueue;
use nodedb_types::PriorityClass;
use tokio::sync::Notify;

use crate::bridge::envelope;
use crate::control::router::vshard::VShardRouter;
use crate::data::eventfd::EventFdNotifier;
use crate::types::Lsn;

use super::core_channel::{CoreChannel, CoreChannelDataSide};
use super::dispatched_lsns::DispatchedLsns;
use super::outcome_floor::OutcomeFloor;

/// Per-core request queue capacity of the server's bridge dispatcher.
///
/// Each core's weighted-fair queue and SPSC rings hold at most this many
/// requests. Every request for one vShard routes to the same core.
pub const DATA_PLANE_QUEUE_CAPACITY: usize = 1024;

/// Serialized form of a request that goes through the SPSC ring buffer.
///
/// The bridge crate is generic over `T` — we serialize our typed `Request`
/// envelope into this form for the ring buffer, and deserialize on the
/// Data Plane side.
#[derive(Debug)]
pub struct BridgeRequest {
    /// The full typed request envelope.
    pub inner: envelope::Request,
    /// The outcome floor when this request entered the ring: every record at
    /// or below it that any core receives has a final outcome.
    pub outcome_floor: Lsn,
}

impl BridgeRequest {
    /// A request that carries no outcome floor. A core that reads it learns
    /// nothing about the floor.
    pub fn unfloored(inner: envelope::Request) -> Self {
        Self {
            inner,
            outcome_floor: Lsn::ZERO,
        }
    }
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
    pub(super) router: VShardRouter,

    /// Per-tenant in-flight request count across all cores.
    pub(super) tenant_inflight: HashMap<u64, u32>,

    /// Maps request_id → tenant_id for in-flight requests.
    pub(super) request_tenant: HashMap<u64, u64>,

    /// Maximum in-flight requests per tenant (0 = unlimited).
    pub(super) max_per_tenant_inflight: u32,

    /// Per-core queue capacity (used in tenant fairness recalculation).
    pub(super) per_core_capacity: u32,

    /// Resolves priority class for a database_id (consulted on enqueue).
    pub(super) priority_resolver: Box<dyn DatabasePriorityResolver>,

    /// True once the shutdown bus has entered `DrainingDataPlane`.
    ///
    /// Set by [`Dispatcher::begin_data_plane_drain`] under the same mutex every
    /// enqueue takes, so a dispatch that observed `false` has already pushed by
    /// the time the drain starts. Nothing reaches a core after that.
    pub(super) data_plane_draining: bool,

    /// Capacity freed on the bridge dispatcher. Every path that releases an
    /// in-flight slot wakes all waiters once per call.
    pub(super) capacity_freed: Arc<Notify>,

    /// The node's outcome floor. Every ring push carries its current value.
    pub(super) outcome_floor: Arc<OutcomeFloor>,

    /// The floor windows of accepted requests that carry a WAL LSN.
    pub(super) dispatched_lsns: DispatchedLsns,
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
                capacity_freed: Arc::new(Notify::new()),
                outcome_floor: OutcomeFloor::new(),
                dispatched_lsns: DispatchedLsns::default(),
            },
            data_sides,
        )
    }

    /// The signal fired whenever the dispatcher frees at least one in-flight
    /// slot.
    ///
    /// A caller refused with [`crate::Error::DispatchCapacity`] waits on it
    /// before it retries. It wakes only registered waiters, so a caller
    /// enables its `notified()` future before it checks for refused work.
    pub fn capacity_freed(&self) -> Arc<Notify> {
        Arc::clone(&self.capacity_freed)
    }

    /// The node's outcome floor, shared with every write that opens a window.
    pub fn outcome_floor(&self) -> Arc<OutcomeFloor> {
        Arc::clone(&self.outcome_floor)
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
