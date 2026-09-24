// SPDX-License-Identifier: BUSL-1.1

//! Response polling for the bridge [`Dispatcher`], including the error
//! responses it synthesizes for a core that died.

use std::collections::HashSet;

use crate::bridge::envelope;
use crate::bridge::envelope::{ErrorCode, Payload, Status};
use crate::types::{Lsn, RequestId};

use super::dispatcher::Dispatcher;
use super::enqueue::release_inflight_slot;

impl Dispatcher {
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
    ///
    /// Fires [`Dispatcher::capacity_freed`] once when the poll released at
    /// least one in-flight slot.
    pub fn poll_responses(&mut self) -> Vec<envelope::Response> {
        let mut responses = Vec::new();
        let mut freed = false;
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
                    self.dispatched_lsns.settle(rid);
                    freed |= release_inflight_slot(
                        &mut self.request_tenant,
                        &mut self.tenant_inflight,
                        rid,
                    );
                }
                responses.push(br.inner);
            }

            if !(producer_gone || channel.request_tx.is_disconnected()) {
                // Opportunistically flush WFQ after draining responses to fill headroom.
                channel.flush_wfq(self.outcome_floor.floor());
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
                // A dead core never publishes a watermark again.
                self.dispatched_lsns.settle(rid);
                freed |=
                    release_inflight_slot(&mut self.request_tenant, &mut self.tenant_inflight, rid);
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
        if freed {
            self.capacity_freed.notify_waiters();
        }
        responses
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::bridge::dispatch::BridgeResponse;
    use crate::bridge::dispatch::test_requests::{make_request, make_request_for_db};
    use crate::types::*;

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

    /// A routed final response frees its tenant's slot and fires the
    /// capacity-freed signal.
    #[tokio::test]
    async fn final_response_fires_capacity_freed() {
        let (mut dispatcher, mut data_sides) = Dispatcher::new(1, 64);
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
                    payload: Payload::empty(),
                    watermark_lsn: Lsn::ZERO,
                    error_code: None,
                    read_set_valid: None,
                    read_version_lsn: Lsn::ZERO,
                    write_set: Vec::new(),
                },
            })
            .unwrap();

        let signal = dispatcher.capacity_freed();
        let notified = signal.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        assert_eq!(dispatcher.poll_responses().len(), 1);

        assert!(
            tokio::time::timeout(Duration::from_millis(100), notified)
                .await
                .is_ok(),
            "a freed in-flight slot must fire the capacity-freed signal"
        );
    }

    // --- Dead-core request loss ---
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

    // --- Outcome floor ---

    fn ok_response(request_id: u64) -> BridgeResponse {
        BridgeResponse {
            inner: envelope::Response {
                request_id: RequestId::new(request_id),
                status: Status::Ok,
                attempt: 1,
                partial: false,
                payload: Payload::empty(),
                watermark_lsn: Lsn::ZERO,
                error_code: None,
                read_set_valid: None,
                read_version_lsn: Lsn::ZERO,
                write_set: Vec::new(),
            },
        }
    }

    #[test]
    fn a_dispatched_lsn_holds_the_outcome_floor_until_its_response() {
        let (mut dispatcher, mut data_sides) = Dispatcher::new(1, 64);
        let floor = dispatcher.outcome_floor();
        let mut request = make_request_for_db(0, 0, 5);
        request.wal_lsn = Some(Lsn::new(40));
        dispatcher.dispatch(request).unwrap();
        assert_eq!(dispatcher.dispatched_lsns.len(), 1);
        assert_eq!(floor.floor(), Lsn::new(39));

        let pushed = data_sides[0].request_rx.try_pop().unwrap();
        assert!(
            pushed.outcome_floor < Lsn::new(40),
            "the request's own push carries a floor below its lsn"
        );

        data_sides[0].response_tx.try_push(ok_response(5)).unwrap();
        assert_eq!(dispatcher.poll_responses().len(), 1);
        assert_eq!(dispatcher.dispatched_lsns.len(), 0);
        assert_eq!(floor.floor(), Lsn::new(40));

        dispatcher.dispatch(make_request_for_db(0, 0, 6)).unwrap();
        let next = data_sides[0].request_rx.try_pop().unwrap();
        assert_eq!(
            next.outcome_floor,
            Lsn::new(40),
            "a push after the response carries the advanced floor"
        );
    }

    #[test]
    fn a_partial_response_keeps_the_outcome_floor_held() {
        let (mut dispatcher, mut data_sides) = Dispatcher::new(1, 64);
        let floor = dispatcher.outcome_floor();
        let mut request = make_request_for_db(0, 0, 8);
        request.wal_lsn = Some(Lsn::new(12));
        dispatcher.dispatch(request).unwrap();
        let _req = data_sides[0].request_rx.try_pop().unwrap();

        let mut partial = ok_response(8);
        partial.inner.partial = true;
        data_sides[0].response_tx.try_push(partial).unwrap();
        dispatcher.poll_responses();
        assert_eq!(floor.floor(), Lsn::new(11));

        data_sides[0].response_tx.try_push(ok_response(8)).unwrap();
        dispatcher.poll_responses();
        assert_eq!(floor.floor(), Lsn::new(12));
    }

    #[test]
    fn a_dead_core_releases_the_outcome_floor_it_held() {
        let (mut dispatcher, mut data_sides) = Dispatcher::new(1, 64);
        let floor = dispatcher.outcome_floor();
        let mut request = make_request_for_db(0, 0, 3);
        request.wal_lsn = Some(Lsn::new(25));
        dispatcher.dispatch(request).unwrap();
        assert_eq!(floor.floor(), Lsn::new(24));

        drop(data_sides.remove(0));
        let responses = dispatcher.poll_responses();
        assert_eq!(responses.len(), 1);
        assert_eq!(dispatcher.dispatched_lsns.len(), 0);
        assert_eq!(floor.floor(), Lsn::new(25));
    }

    #[test]
    fn a_request_without_an_lsn_holds_nothing() {
        let (mut dispatcher, _data_sides) = Dispatcher::new(1, 64);
        dispatcher.dispatch(make_request_for_db(0, 0, 4)).unwrap();
        assert_eq!(dispatcher.dispatched_lsns.len(), 0);
        assert_eq!(dispatcher.outcome_floor().floor(), Lsn::ZERO);
    }
}
