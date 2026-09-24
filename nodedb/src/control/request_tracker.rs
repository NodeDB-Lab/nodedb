// SPDX-License-Identifier: BUSL-1.1

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use tokio::sync::{mpsc, oneshot};

use crate::bridge::envelope::Response;
use crate::types::RequestId;

/// Per-request partial-response capacity. A streaming scan produces at most
/// `ceil(rows / STREAM_CHUNK_SIZE)` partials — a few hundred for the
/// largest realistic queries. Capacity here bounds how many chunks can
/// sit in RAM while the Control-Plane session's TCP write buffer is
/// stalled; once full, `complete` returns false so the Data Plane can
/// observe backpressure instead of silently growing RSS.
pub const REQUEST_CHANNEL_CAPACITY: usize = 256;

/// The sending ends of one tracked request.
struct PendingRequest {
    partials: mpsc::Sender<Response>,
    final_tx: oneshot::Sender<Response>,
}

/// The receiving end of one tracked request.
///
/// Partial responses arrive through a bounded channel. The final response
/// has a slot of its own, so a full partial channel never drops it.
pub struct ResponseReceiver {
    partials: mpsc::Receiver<Response>,
    final_rx: Option<oneshot::Receiver<Response>>,
}

impl ResponseReceiver {
    /// The next response: every buffered partial in order, then the final
    /// one. `None` once the final response was taken, or once the request
    /// ended without one.
    ///
    /// Cancel-safe: a dropped `recv` future loses no response.
    pub async fn recv(&mut self) -> Option<Response> {
        if let Some(response) = self.partials.recv().await {
            return Some(response);
        }
        let final_rx = self.final_rx.as_mut()?;
        let answer = final_rx.await;
        self.final_rx = None;
        answer.ok()
    }

    /// The next response when one is ready, without waiting. `None` when
    /// nothing is ready, or once the request ended.
    pub fn try_recv(&mut self) -> Option<Response> {
        match self.partials.try_recv() {
            Ok(response) => return Some(response),
            Err(mpsc::error::TryRecvError::Empty) => return None,
            Err(mpsc::error::TryRecvError::Disconnected) => {}
        }
        let final_rx = self.final_rx.as_mut()?;
        match final_rx.try_recv() {
            Ok(response) => {
                self.final_rx = None;
                Some(response)
            }
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => {
                self.final_rx = None;
                None
            }
        }
    }

    /// A receiver fed by `partials` alone: every response, final included,
    /// arrives on it in order.
    #[cfg(test)]
    pub(crate) fn from_channel(partials: mpsc::Receiver<Response>) -> Self {
        Self {
            partials,
            final_rx: None,
        }
    }
}

/// Routes Data Plane responses back to the waiting Control Plane session.
///
/// Each dispatched request registers its senders here. The background
/// response poller forwards responses as they arrive. For streaming queries,
/// multiple partial responses arrive before the final one.
///
/// - Partial responses (`response.partial == true`): forwarded but request
///   stays in the map for more chunks.
/// - Final response (`response.partial == false`): forwarded into the
///   request's final slot and request removed from the map.
#[derive(Default)]
pub struct RequestTracker {
    pending: Mutex<HashMap<RequestId, PendingRequest>>,
}

impl RequestTracker {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    fn lock_pending(&self) -> MutexGuard<'_, HashMap<RequestId, PendingRequest>> {
        match self.pending.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Register a pending request. Returns the receiver the session awaits.
    ///
    /// For non-streaming requests, exactly one response arrives.
    /// For streaming requests, multiple partial responses arrive before the final one.
    /// Partial capacity applies backpressure when the session is slow.
    pub fn register(&self, id: RequestId) -> ResponseReceiver {
        let (partials_tx, partials_rx) = mpsc::channel(REQUEST_CHANNEL_CAPACITY);
        let (final_tx, final_rx) = oneshot::channel();
        self.lock_pending().insert(
            id,
            PendingRequest {
                partials: partials_tx,
                final_tx,
            },
        );
        ResponseReceiver {
            partials: partials_rx,
            final_rx: Some(final_rx),
        }
    }

    /// Forward a response from the Data Plane to the waiting session.
    ///
    /// - If `response.partial` is true: sends the chunk but keeps the
    ///   request in the map for subsequent chunks.
    /// - If `response.partial` is false: puts the response in the final
    ///   slot and removes the request from the map. A full partial channel
    ///   never refuses it.
    ///
    /// Returns `false` if the request was cancelled or its receiver dropped,
    /// or if a partial found the session buffer full (backpressure signal —
    /// the Data Plane must stop producing further chunks for this request).
    pub fn complete(&self, response: Response) -> bool {
        let is_final = !response.partial;
        let mut pending = self.lock_pending();

        if is_final {
            match pending.remove(&response.request_id) {
                Some(request) => request.final_tx.send(response).is_ok(),
                None => false,
            }
        } else {
            let request_id = response.request_id;
            if let Some(request) = pending.get(&request_id) {
                match request.partials.try_send(response) {
                    Ok(()) => true,
                    Err(_) => {
                        // Full channel (session stalled) or closed (cancelled):
                        // evict the pending entry so the Data Plane stops
                        // producing further chunks for this request.
                        pending.remove(&request_id);
                        false
                    }
                }
            } else {
                false
            }
        }
    }

    /// Remove a pending request (e.g., on session disconnect).
    pub fn cancel(&self, id: &RequestId) {
        self.lock_pending().remove(id);
    }

    /// Number of in-flight requests.
    pub fn in_flight(&self) -> usize {
        self.lock_pending().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{Payload, Status};
    use crate::types::Lsn;

    fn make_response(id: u64) -> Response {
        Response {
            request_id: RequestId::new(id),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    fn make_partial(id: u64, data: &str) -> Response {
        Response {
            request_id: RequestId::new(id),
            status: Status::Partial,
            attempt: 1,
            partial: true,
            payload: Payload::from_vec(data.as_bytes().to_vec()),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    #[tokio::test]
    async fn register_and_complete() {
        let tracker = RequestTracker::new();
        let mut rx = tracker.register(RequestId::new(1));
        assert_eq!(tracker.in_flight(), 1);

        assert!(tracker.complete(make_response(1)));
        assert_eq!(tracker.in_flight(), 0);

        let resp = rx.recv().await.unwrap();
        assert_eq!(resp.request_id, RequestId::new(1));
    }

    #[test]
    fn complete_unknown_returns_false() {
        let tracker = RequestTracker::new();
        assert!(!tracker.complete(make_response(999)));
    }

    #[test]
    fn cancel_removes_pending() {
        let tracker = RequestTracker::new();
        let _rx = tracker.register(RequestId::new(5));
        assert_eq!(tracker.in_flight(), 1);
        tracker.cancel(&RequestId::new(5));
        assert_eq!(tracker.in_flight(), 0);
    }

    #[tokio::test]
    async fn streaming_partial_then_final() {
        let tracker = RequestTracker::new();
        let mut rx = tracker.register(RequestId::new(10));

        assert!(tracker.complete(make_partial(10, "chunk1")));
        assert_eq!(tracker.in_flight(), 1);
        assert!(tracker.complete(make_partial(10, "chunk2")));
        assert_eq!(tracker.in_flight(), 1);

        assert!(tracker.complete(make_response(10)));
        assert_eq!(tracker.in_flight(), 0);

        let r1 = rx.recv().await.unwrap();
        assert!(r1.partial);
        let r2 = rx.recv().await.unwrap();
        assert!(r2.partial);
        let r3 = rx.recv().await.unwrap();
        assert!(!r3.partial);
    }

    #[test]
    fn full_channel_signals_backpressure() {
        let tracker = RequestTracker::new();
        let _rx = tracker.register(RequestId::new(7));
        let mut rejected = 0usize;
        for i in 0u32..(REQUEST_CHANNEL_CAPACITY as u32 * 2) {
            if !tracker.complete(make_partial(7, &format!("chunk-{i}"))) {
                rejected += 1;
            }
        }
        assert!(rejected > 0);
        // Entry was evicted on first full-channel hit.
        assert_eq!(tracker.in_flight(), 0);
    }

    /// A session that stalls with its partial buffer full still receives
    /// the final response, after every buffered partial.
    #[tokio::test]
    async fn a_full_partial_buffer_never_drops_the_final_response() {
        let tracker = RequestTracker::new();
        let mut rx = tracker.register(RequestId::new(11));
        for i in 0..REQUEST_CHANNEL_CAPACITY {
            assert!(tracker.complete(make_partial(11, &format!("chunk-{i}"))));
        }

        assert!(tracker.complete(make_response(11)));

        for _ in 0..REQUEST_CHANNEL_CAPACITY {
            let partial = rx.recv().await.expect("buffered partial");
            assert!(partial.partial);
        }
        let last = rx.recv().await.expect("final response");
        assert!(!last.partial);
        assert!(rx.recv().await.is_none());
    }

    /// A `recv` dropped while it waits keeps the final response for the
    /// next `recv`.
    #[tokio::test]
    async fn a_cancelled_recv_keeps_the_final_response() {
        let tracker = RequestTracker::new();
        let mut rx = tracker.register(RequestId::new(12));
        let waited = tokio::time::timeout(std::time::Duration::from_millis(10), rx.recv()).await;
        assert!(waited.is_err(), "nothing has arrived yet");

        assert!(tracker.complete(make_response(12)));

        let last = rx.recv().await.expect("final response");
        assert_eq!(last.request_id, RequestId::new(12));
    }
}
