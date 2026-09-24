// SPDX-License-Identifier: BUSL-1.1

use std::collections::{HashMap, HashSet};

use tracing::error;

use nodedb_bridge::BridgeError;
use nodedb_bridge::backpressure::{BackpressureController, PressureState};
use nodedb_bridge::buffer::{Consumer, Producer};
use nodedb_bridge::wfq::WeightedFairQueue;

use crate::bridge::envelope;
use crate::data::eventfd::EventFdNotifier;
use crate::types::Lsn;

use super::dispatcher::{BridgeRequest, BridgeResponse};

/// A pair of SPSC channels for one Data Plane core, augmented with a
/// weighted-fair staging queue that enforces per-database fairness before
/// requests reach the physical ring buffer.
pub struct CoreChannel {
    /// Control Plane pushes requests to the Data Plane core.
    pub request_tx: Producer<BridgeRequest>,

    /// Control Plane pops responses from the Data Plane core.
    pub response_rx: Consumer<BridgeResponse>,

    /// Backpressure controller for the request queue (global, across all DBs).
    pub backpressure: BackpressureController,

    /// Per-database weighted-fair staging queue. Items are popped from here in
    /// DRR order and forwarded to `request_tx`.
    pub wfq: WeightedFairQueue<envelope::Request>,

    /// Per-virtual-queue backpressure states, keyed by `database_id`.
    ///
    /// **Writer**: `dispatch` and `flush_wfq` call `update_db_pressure` after
    /// each enqueue/pop, snapshotting the WFQ throttle/suspend predicates for
    /// that database into this map.
    ///
    /// **Reader**: `Dispatcher::db_pressure_on_core` for the metrics exporter.
    ///
    /// **Lifetime**: entries are written in place and never reach a "remove"
    /// path on their own. Stale databases that no longer enqueue requests
    /// retain a `Normal` (or last-observed) entry until the surrounding
    /// dispatcher is dropped or `recalculate_tenant_limits` rotates state.
    /// The map is bounded by the universe of `database_id`s that have ever
    /// been dispatched against this core, so unbounded growth is not a
    /// concern in practice.
    ///
    /// **Threading**: this field is accessed only from the Control Plane
    /// thread that owns the `Dispatcher`. `HashMap` is intentional —
    /// the field is never shared across threads.
    pub db_pressure: HashMap<u64, PressureState>,

    /// Eventfd notifier to wake the Data Plane core after pushing a request.
    /// `None` until `set_notifier` is called (after core thread startup).
    pub wake_notifier: Option<EventFdNotifier>,

    /// Request ids dispatched to this core and not yet answered.
    ///
    /// Populated by `dispatch` / `dispatch_to_core` and cleared in
    /// `poll_responses` — either by the core's own response, or by the
    /// synthesized failure emitted when the core is found dead. A core
    /// answers only on its own channel, so the core a request belongs to is
    /// the core whose set holds its id; no separate request→core map is
    /// needed.
    pub outstanding: HashSet<u64>,
}

impl CoreChannel {
    /// Flush as many items from the WFQ into the physical ring as will fit.
    /// Updates per-DB pressure states and returns the number of items flushed.
    ///
    /// `try_push` consumes the request by value, so a failure on push would
    /// drop the request. The two failure modes are handled explicitly so
    /// nothing is lost silently:
    ///
    /// - `BridgeError::Full` is unreachable: the SPSC ring has a single
    ///   producer (this dispatcher), and we re-check `utilization() < 100`
    ///   on every iteration before popping from the WFQ. If it ever fires,
    ///   the SPSC invariant is violated and we trip an `unreachable!` so
    ///   the bug surfaces loudly rather than as silent request loss.
    /// - `BridgeError::Disconnected` means the Data Plane core has gone
    ///   away. Continuing to drain the WFQ into a dead consumer would lose
    ///   every queued request, so we stop flushing and leave the rest
    ///   staged. `Dispatcher::poll_responses` then fails everything
    ///   outstanding on the core, including the request this push consumed.
    ///
    /// The disconnect is also checked before the first pop, so a core known
    /// to be dead never has another request moved into a doomed `try_push`.
    ///
    /// Every pushed request carries `outcome_floor`, the floor the caller read
    /// just before this flush.
    pub(super) fn flush_wfq(&mut self, outcome_floor: Lsn) -> usize {
        let mut flushed = 0;
        if self.request_tx.is_disconnected() {
            return 0;
        }
        while self.request_tx.utilization() < 100 {
            let Some(req) = self.wfq.pop_next() else {
                break;
            };
            let db_id = req.database_id.as_u64();
            let req_id = req.request_id.as_u64();
            match self.request_tx.try_push(BridgeRequest {
                inner: req,
                outcome_floor,
            }) {
                Ok(()) => {
                    flushed += 1;
                    self.update_db_pressure(db_id);
                }
                Err(BridgeError::Full { capacity, pending }) => {
                    unreachable!(
                        "SPSC ring reported Full (capacity={capacity}, pending={pending}) \
                         despite utilization < 100 immediately before push — \
                         single-producer invariant violated"
                    );
                }
                Err(e @ BridgeError::Disconnected { .. }) => {
                    error!(
                        request_id = req_id,
                        database_id = db_id,
                        "data plane core disconnected during WFQ flush — stopping; the request is failed by the next poll_responses: {e}"
                    );
                    break;
                }
                Err(
                    e @ (BridgeError::Empty
                    | BridgeError::Backpressure { .. }
                    | BridgeError::DeadlineExceeded { .. }),
                ) => {
                    // `Producer::try_push` only ever produces `Full` or
                    // `Disconnected`; these other variants are returned by
                    // consumer/backpressure paths and cannot reach here.
                    unreachable!("Producer::try_push returned non-producer BridgeError: {e}");
                }
            }
        }
        flushed
    }

    /// Recompute and store the pressure state for a single database.
    pub(super) fn update_db_pressure(&mut self, database_id: u64) {
        let state = if self.wfq.is_suspended_for(database_id) {
            PressureState::Suspended
        } else if self.wfq.is_throttled_for(database_id) {
            PressureState::Throttled
        } else {
            PressureState::Normal
        };
        self.db_pressure.insert(database_id, state);
    }
}

/// Data Plane side of a core's channel pair.
pub struct CoreChannelDataSide {
    /// Data Plane pops requests from the Control Plane.
    pub request_rx: Consumer<BridgeRequest>,

    /// Data Plane pushes responses back to the Control Plane.
    pub response_tx: Producer<BridgeResponse>,
}
