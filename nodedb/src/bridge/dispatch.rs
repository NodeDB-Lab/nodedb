use tracing::warn;

use nodedb_bridge::backpressure::{BackpressureConfig, BackpressureController};
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};

use crate::bridge::envelope;
use crate::control::router::vshard::VShardRouter;
use crate::data::eventfd::EventFdNotifier;

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

/// A pair of SPSC channels for one Data Plane core.
pub struct CoreChannel {
    /// Control Plane pushes requests to the Data Plane core.
    pub request_tx: Producer<BridgeRequest>,

    /// Control Plane pops responses from the Data Plane core.
    pub response_rx: Consumer<BridgeResponse>,

    /// Backpressure controller for the request queue.
    pub backpressure: BackpressureController,

    /// Eventfd notifier to wake the Data Plane core after pushing a request.
    /// `None` until `set_notifier` is called (after core thread startup).
    pub wake_notifier: Option<EventFdNotifier>,
}

/// Data Plane side of a core's channel pair.
pub struct CoreChannelDataSide {
    /// Data Plane pops requests from the Control Plane.
    pub request_rx: Consumer<BridgeRequest>,

    /// Data Plane pushes responses back to the Control Plane.
    pub response_tx: Producer<BridgeResponse>,
}

/// The dispatcher: routes requests from the Control Plane to the correct
/// Data Plane core via SPSC ring buffers.
///
/// One `Dispatcher` lives on the Control Plane. It owns the producer side
/// of all request channels and the consumer side of all response channels.
pub struct Dispatcher {
    /// One channel pair per Data Plane core.
    cores: Vec<CoreChannel>,

    /// Routes vShards to core IDs.
    router: VShardRouter,
}

impl Dispatcher {
    /// Create a dispatcher with SPSC channels for each core.
    ///
    /// Returns `(Dispatcher, Vec<CoreChannelDataSide>)` — send each
    /// `CoreChannelDataSide` to its respective Data Plane core thread.
    pub fn new(num_cores: usize, queue_capacity: usize) -> (Self, Vec<CoreChannelDataSide>) {
        let mut cores = Vec::with_capacity(num_cores);
        let mut data_sides = Vec::with_capacity(num_cores);

        for _ in 0..num_cores {
            let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(queue_capacity);
            let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(queue_capacity);

            cores.push(CoreChannel {
                request_tx: req_tx,
                response_rx: resp_rx,
                backpressure: BackpressureController::new(BackpressureConfig::default()),
                wake_notifier: None,
            });

            data_sides.push(CoreChannelDataSide {
                request_rx: req_rx,
                response_tx: resp_tx,
            });
        }

        let router = VShardRouter::round_robin(num_cores);

        (Self { cores, router }, data_sides)
    }

    /// Dispatch a request to the correct Data Plane core.
    ///
    /// Uses the vShard router to determine which core handles this request,
    /// then pushes it into that core's SPSC request queue.
    pub fn dispatch(&mut self, request: envelope::Request) -> crate::Result<()> {
        let core_id =
            self.router
                .resolve(request.vshard_id)
                .ok_or_else(|| crate::Error::Dispatch {
                    detail: format!("no core for vshard {}", request.vshard_id),
                })?;

        let channel = &mut self.cores[core_id];

        // Update backpressure state.
        let util = channel.request_tx.utilization();
        if let Some(new_state) = channel.backpressure.update(util) {
            warn!(
                core_id,
                utilization = util,
                state = ?new_state,
                "backpressure transition"
            );
        }

        channel
            .request_tx
            .try_push(BridgeRequest { inner: request })
            .map_err(|e| crate::Error::Dispatch {
                detail: format!("core {core_id}: {e}"),
            })?;

        // Wake the Data Plane core via eventfd.
        if let Some(ref notifier) = channel.wake_notifier {
            notifier.notify();
        }

        Ok(())
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
        if core_id >= self.cores.len() {
            return Err(crate::Error::Dispatch {
                detail: format!("core {core_id} out of range (have {})", self.cores.len()),
            });
        }

        let channel = &mut self.cores[core_id];
        channel
            .request_tx
            .try_push(BridgeRequest { inner: request })
            .map_err(|e| crate::Error::Dispatch {
                detail: format!("core {core_id}: {e}"),
            })?;

        if let Some(ref notifier) = channel.wake_notifier {
            notifier.notify();
        }

        Ok(())
    }

    /// Poll responses from all Data Plane cores.
    ///
    /// Returns responses that have been produced since the last poll.
    pub fn poll_responses(&mut self) -> Vec<envelope::Response> {
        let mut responses = Vec::new();
        for channel in &mut self.cores {
            let mut batch = Vec::new();
            channel.response_rx.drain_into(&mut batch, 64);
            for br in batch {
                responses.push(br.inner);
            }
        }
        responses
    }

    /// Number of Data Plane cores.
    pub fn num_cores(&self) -> usize {
        self.cores.len()
    }

    /// Set the eventfd notifier for a specific core.
    ///
    /// Called after `spawn_core` returns the `EventFdNotifier`.
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
    use crate::bridge::physical_plan::DocumentOp;
    use crate::types::*;
    use std::time::{Duration, Instant};

    fn make_request(vshard: u16) -> envelope::Request {
        envelope::Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(vshard),
            plan: PhysicalPlan::Document(DocumentOp::PointGet {
                collection: "users".into(),
                document_id: "u1".into(),
            }),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: 0,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
        }
    }

    #[test]
    fn dispatch_routes_to_correct_core() {
        let (mut dispatcher, data_sides) = Dispatcher::new(4, 64);

        // vShard 0 → core 0, vShard 1 → core 1, etc.
        dispatcher.dispatch(make_request(0)).unwrap();
        dispatcher.dispatch(make_request(1)).unwrap();
        dispatcher.dispatch(make_request(4)).unwrap(); // Wraps to core 0.

        // Core 0 should have 2 requests, core 1 should have 1.
        assert_eq!(data_sides[0].request_rx.len(), 2);
        assert_eq!(data_sides[1].request_rx.len(), 1);
        assert_eq!(data_sides[2].request_rx.len(), 0);
    }

    #[test]
    fn response_roundtrip() {
        let (mut dispatcher, mut data_sides) = Dispatcher::new(2, 64);

        // Dispatch a request.
        dispatcher.dispatch(make_request(0)).unwrap();

        // Data Plane side processes it and sends response.
        let _req = data_sides[0].request_rx.try_pop().unwrap();
        data_sides[0]
            .response_tx
            .try_push(BridgeResponse {
                inner: envelope::Response {
                    request_id: RequestId::new(1),
                    status: Status::Ok,
                    attempt: 1,
                    partial: false,
                    payload: Payload::from_arc(std::sync::Arc::from(b"result".as_slice())),
                    watermark_lsn: Lsn::new(42),
                    error_code: None,
                },
            })
            .unwrap();

        // Control Plane polls responses.
        let responses = dispatcher.poll_responses();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].status, Status::Ok);
        assert_eq!(&*responses[0].payload, b"result");
    }

    #[test]
    fn full_queue_returns_error() {
        let (mut dispatcher, _data_sides) = Dispatcher::new(1, 4);

        // Fill the queue (capacity rounds to 4).
        for _ in 0..4 {
            dispatcher.dispatch(make_request(0)).unwrap();
        }

        // Next dispatch should fail — queue is full.
        let result = dispatcher.dispatch(make_request(0));
        assert!(result.is_err());
    }
}
