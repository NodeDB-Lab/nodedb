// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage: RequestTracker must apply bounded backpressure to
//! streaming responses.
//!
//! `register` must not return `mpsc::UnboundedReceiver` — a slow Control-Plane
//! session (TLS back-pressure, slow TCP consumer) would let streaming `partial`
//! chunks accumulate in RAM without bound, with RSS growing as
//! `(producer_rate - consumer_rate) * duration`.
//!
//! `register` must return a bounded receiver; `complete` must signal
//! backpressure (returns false, drops-with-sentinel, or similar) once the
//! buffer is full rather than silently expanding forever.

use nodedb::bridge::envelope::{Payload, Response, Status};
use nodedb::control::request_tracker::{RequestTracker, ResponseReceiver};
use nodedb::types::{Lsn, RequestId};

fn partial(id: u64, data: &[u8]) -> Response {
    Response {
        request_id: RequestId::new(id),
        status: Status::Partial,
        attempt: 1,
        partial: true,
        payload: Payload::from_vec(data.to_vec()),
        watermark_lsn: Lsn::ZERO,
        read_version_lsn: Lsn::ZERO,
        error_code: None,
        read_set_valid: None,
        write_set: Vec::new(),
    }
}

#[test]
fn register_returns_a_response_receiver() {
    // Compile-gate: partials reach the session through the receiver's
    // bounded channel. The next test shows the bound.
    let tracker = RequestTracker::new();
    let _rx: ResponseReceiver = tracker.register(RequestId::new(1));
}

#[test]
fn complete_signals_backpressure_when_session_buffer_fills() {
    // Register a request but never poll the receiver — simulates a slow
    // session whose TCP write buffer is full.
    let tracker = RequestTracker::new();
    let _rx = tracker.register(RequestId::new(42));

    // Flood partials. With the bounded channel, one of two observable
    // behaviors is acceptable:
    //   (a) `complete` returns false once the session buffer fills, OR
    //   (b) the oldest partial is dropped with a sentinel error code.
    //
    // The current (buggy) behavior accepts 10k+ partials without any
    // signal — that is the class of bug being captured.
    let mut rejected = 0usize;
    for i in 0u32..10_000 {
        if !tracker.complete(partial(42, &i.to_le_bytes())) {
            rejected += 1;
        }
    }

    assert!(
        rejected > 0,
        "RequestTracker must signal backpressure for never-polled receivers; \
         unbounded buffering grows RSS without bound under slow consumers"
    );
}
