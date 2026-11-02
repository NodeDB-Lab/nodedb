// SPDX-License-Identifier: BUSL-1.1

//! Outbound RPC to a known peer: one attempt, its circuit-breaker record,
//! and the retry loop around it.
//!
//! Only a link failure counts against the peer's circuit breaker. That is a
//! failed connect, handshake, stream open or write, a read timeout, or a lost
//! or reset connection. Every answer the peer sends counts as a success, a
//! typed refusal included. A refusal goes back to the caller as its typed
//! error and is not resent.
//!
//! The peer can have run a request written before a link failure. The
//! transport resends such a request only when [`RaftRpc::resend_safe`]
//! holds. Any other request ends as `ClusterError::Unanswered`, an unknown
//! outcome.
//!
//! A link failure drops the pooled connection only when the connection
//! itself failed. Other sends share that connection, so one stream's
//! failure must not cut them off. The connection fails when:
//! - it is closed, lost or reset;
//! - it reached a node other than the target;
//! - a read timed out and the peer sent nothing on it during the wait.
//!
//! A live peer acknowledges the request within its ACK delay. A read that
//! times out on a connection that received datagrams meanwhile is a slow
//! handler, and the connection stays pooled.
//!
//! A peer silent for the RPC timeout is dead or cut off: its connection hit
//! the QUIC idle timeout, or a dial got no answer. The call ends at once with
//! no resend to that peer, so the caller can try another node. A written
//! request then ends as `ClusterError::Unanswered`.

use std::time::Duration;

use tracing::debug;

use crate::circuit_breaker::{Admission, RetryPolicy};
use crate::error::{ClusterError, Result};
use crate::rpc_codec::{self, RaftRpc};
use crate::transport::frame_io::read_envelope_or_finish;

use super::transport::NexarTransport;

/// How one attempt ended.
#[derive(Debug)]
enum Attempt {
    /// The peer answered. The result is final: a response, or the typed
    /// error the peer refused the request with.
    Answered(Result<RaftRpc>),
    /// The peer's replay window refused the frame. The peer is up, and a
    /// retry goes out under a fresh sequence number.
    FrameRefused(ClusterError),
    /// The link to the peer failed.
    LinkFailed(LinkFailure),
}

/// A link failure, and the pooled connection it condemns.
#[derive(Debug)]
struct LinkFailure {
    error: Box<ClusterError>,
    /// Stable id of the connection to drop from the pool. `None` when no
    /// connection was obtained or the connection still works.
    condemned: Option<usize>,
    /// Whether the whole request was written before the failure. The peer
    /// can have run a written request.
    written: bool,
    /// The peer sent nothing for the RPC timeout. No resend goes to it.
    silent: bool,
}

impl LinkFailure {
    /// A dial the peer never answered.
    fn from_dial(failed: super::pool::DialError) -> Self {
        Self {
            error: Box::new(failed.error),
            condemned: None,
            written: false,
            silent: failed.silent,
        }
    }

    /// The error of a written request that got no answer from `target`.
    fn into_unanswered(self, target: u64) -> ClusterError {
        ClusterError::Unanswered {
            node_id: target,
            detail: self.error.to_string(),
        }
    }
}

/// Whether a failed attempt is sent again.
///
/// `Ok` holds the error kept while the next attempt goes out. `Err` ends the
/// call. A written request that is not [`RaftRpc::resend_safe`] can have run
/// on the peer. It is never resent and ends as `ClusterError::Unanswered`.
///
/// A silent peer gets no resend: a written request ends as `Unanswered`, an
/// unsent one with its own error.
fn resend_verdict(failure: LinkFailure, target: u64, resend_safe: bool) -> Result<ClusterError> {
    if failure.written && (failure.silent || !resend_safe) {
        return Err(failure.into_unanswered(target));
    }
    if failure.silent {
        return Err(*failure.error);
    }
    if RetryPolicy::is_retryable(&failure.error) {
        Ok(*failure.error)
    } else {
        Err(*failure.error)
    }
}

/// Sort the outcome of one send into an [`Attempt`].
///
/// The outer error is a link failure. The inner result is what the peer
/// sent back, or why its reply is unreadable.
fn classify(target: u64, sent: std::result::Result<Result<RaftRpc>, LinkFailure>) -> Attempt {
    match sent {
        Err(link) => Attempt::LinkFailed(link),
        Ok(Ok(RaftRpc::FrameRefused(refusal))) => Attempt::FrameRefused(ClusterError::Transport {
            detail: format!("node {target} refused the frame: {}", refusal.detail),
        }),
        Ok(Ok(RaftRpc::RequestRefused(refusal))) => Attempt::Answered(Err(refusal.into_error())),
        Ok(answer) => Attempt::Answered(answer),
    }
}

impl NexarTransport {
    /// Send an RPC to a peer with retry and circuit breaker.
    ///
    /// The response read is bounded by the transport's default `rpc_timeout`.
    /// For RPCs whose handler legitimately blocks far longer than a normal
    /// request/response round-trip (e.g. a routed Calvin submit-and-await, which
    /// the leader-side handler holds open until the transaction is sequenced AND
    /// completion-acked), use [`send_rpc_with_read_timeout`](Self::send_rpc_with_read_timeout)
    /// so the generic short timeout does not abort the call while the remote
    /// handler is still legitimately working.
    pub async fn send_rpc(&self, target: u64, rpc: RaftRpc) -> Result<RaftRpc> {
        self.send_rpc_with_read_timeout(target, rpc, self.rpc_timeout)
            .await
    }

    /// [`send_rpc`](Self::send_rpc) with an explicit response-read timeout.
    ///
    /// `read_timeout` bounds the wait for the response envelope on each attempt
    /// (the connect / handshake / write phases still use the transport's pooled
    /// connection). Callers pass a value derived from the remote handler's own
    /// deadline (plus a margin) so a long-running handler is not aborted early.
    pub async fn send_rpc_with_read_timeout(
        &self,
        target: u64,
        rpc: RaftRpc,
        read_timeout: Duration,
    ) -> Result<RaftRpc> {
        self.check_not_severed(target)?;
        // Encode the inner RPC once (codec errors are not retryable).
        // Each retry wraps it in a fresh envelope so the seq advances
        // per attempt — a retry is a new frame, not a replayed frame.
        let inner = rpc_codec::encode(&rpc, &self.auth.epoch)?;
        let resend_safe = rpc.resend_safe();

        let mut last_err = None;
        for attempt in 0..self.retry_policy.max_attempts {
            if attempt > 0 {
                let delay = self.retry_policy.delay_for_attempt(attempt - 1);
                debug!(target, attempt, ?delay, "retrying RPC");
                tokio::time::sleep(delay).await;
            }
            // The breaker admits each attempt right before it goes out.
            let admission = self.circuit_breaker.check(target)?;

            match self.attempt(target, &inner, read_timeout, admission).await {
                Attempt::Answered(answer) => return answer,
                Attempt::FrameRefused(e) => last_err = Some(e),
                Attempt::LinkFailed(failure) => {
                    last_err = Some(resend_verdict(failure, target, resend_safe)?)
                }
            }
        }

        Err(last_err.unwrap_or_else(|| ClusterError::Transport {
            detail: format!("send_rpc to node {target}: all attempts exhausted"),
        }))
    }

    /// Send a recovery probe: one attempt that an open circuit never refuses.
    ///
    /// The probe's outcome decides the circuit. An answer closes it, and a
    /// link failure reopens it. The health monitor and the reachability
    /// driver probe peers this way, so a peer whose circuit is open is still
    /// probed and can recover.
    pub async fn send_probe_rpc(&self, target: u64, rpc: RaftRpc) -> Result<RaftRpc> {
        self.check_not_severed(target)?;
        let inner = rpc_codec::encode(&rpc, &self.auth.epoch)?;
        let admission = self.circuit_breaker.admit_probe(target);
        match self
            .attempt(target, &inner, self.rpc_timeout, admission)
            .await
        {
            Attempt::Answered(answer) => answer,
            Attempt::FrameRefused(e) => Err(e),
            Attempt::LinkFailed(failure) => Err(*failure.error),
        }
    }

    /// One attempt, recorded on the circuit breaker under `admission`.
    ///
    /// A link failure that condemns the pooled connection also evicts it, so
    /// the next attempt dials a fresh one.
    async fn attempt(
        &self,
        target: u64,
        inner: &[u8],
        read_timeout: Duration,
        admission: Admission,
    ) -> Attempt {
        let outcome = classify(
            target,
            self.try_send_once(target, inner, read_timeout).await,
        );
        match &outcome {
            Attempt::LinkFailed(failure) => {
                self.circuit_breaker.record_failure(target, admission);
                if let Some(stable_id) = failure.condemned {
                    self.evict_connection(target, stable_id);
                }
            }
            Attempt::Answered(_) | Attempt::FrameRefused(_) => {
                self.circuit_breaker.record_success(target, admission);
            }
        }
        outcome
    }

    /// Single-attempt RPC send (no retry, no circuit breaker). `inner` is the
    /// encoded RPC. It is wrapped in a fresh envelope once the stream is
    /// open.
    ///
    /// The outer error is a link failure. The inner result is the peer's
    /// reply. A stream the peer finished without a reply is an inner error:
    /// the peer refused the request before its handler, and a resend gets
    /// the same answer. The peer resets a stream whose handler started and
    /// sent no answer, and that reset is a link failure of a written request.
    async fn try_send_once(
        &self,
        target: u64,
        inner: &[u8],
        read_timeout: Duration,
    ) -> std::result::Result<Result<RaftRpc>, LinkFailure> {
        let conn = self
            .connect_peer(target)
            .await
            .map_err(LinkFailure::from_dial)?;
        let stable_id = conn.stable_id();
        if let Err(error) = self.verify_connection_target(&conn, target) {
            return Err(LinkFailure {
                error: Box::new(error),
                condemned: Some(stable_id),
                written: false,
                silent: false,
            });
        }
        let received_before = received_datagrams(&conn);
        self.exchange(&conn, target, inner, read_timeout)
            .await
            .map_err(|failure| {
                let (error, timed_out, written) = match failure {
                    StreamFailure::Unsent(error) => (error, false, false),
                    StreamFailure::ReadTimeout(error) => (error, true, true),
                    StreamFailure::Unanswered(error) => (error, false, true),
                };
                let heard = received_datagrams(&conn) > received_before;
                let close_reason = conn.close_reason();
                let broken = condemns_connection(timed_out, close_reason.is_some(), heard);
                LinkFailure {
                    error: Box::new(error),
                    condemned: broken.then_some(stable_id),
                    written,
                    silent: matches!(close_reason, Some(quinn::ConnectionError::TimedOut)),
                }
            })
    }

    /// Send one request on its own stream of `conn` and read the reply.
    async fn exchange(
        &self,
        conn: &quinn::Connection,
        target: u64,
        inner: &[u8],
        read_timeout: Duration,
    ) -> std::result::Result<Result<RaftRpc>, StreamFailure> {
        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| {
            StreamFailure::Unsent(ClusterError::Transport {
                detail: format!("open_bi to node {target}: {e}"),
            })
        })?;

        let envelope = self.wrap_inner(inner).map_err(StreamFailure::Unsent)?;
        // A failed `write_all` left part of the frame unbuffered, so the peer
        // cannot hold the whole request. Once it returns, the peer can.
        send.write_all(&envelope).await.map_err(|e| {
            StreamFailure::Unsent(ClusterError::Transport {
                detail: format!("write to node {target}: {e}"),
            })
        })?;
        send.finish().map_err(|e| {
            StreamFailure::Unanswered(ClusterError::Transport {
                detail: format!("finish send to node {target}: {e}"),
            })
        })?;

        let response_envelope =
            tokio::time::timeout(read_timeout, read_envelope_or_finish(&mut recv))
                .await
                .map_err(|_| {
                    StreamFailure::ReadTimeout(ClusterError::Transport {
                        detail: format!(
                            "RPC timeout ({}ms) to node {target}",
                            read_timeout.as_millis()
                        ),
                    })
                })?
                .map_err(StreamFailure::Unanswered)?;
        let Some(response_envelope) = response_envelope else {
            return Ok(Err(ClusterError::RemoteUntyped {
                detail: format!("node {target} finished the stream without a reply"),
            }));
        };

        // Envelope / MAC / replay-window / codec errors are not transport
        // errors — return them wrapped in Ok so retry logic doesn't retry
        // a failed MAC as if it were a flaky network.
        Ok(self.parse_inbound(&response_envelope, Some(target)))
    }
}

/// How one stream on a pooled connection failed.
#[derive(Debug)]
enum StreamFailure {
    /// The request did not go out whole: the stream did not open, the
    /// envelope did not build, or the write failed.
    Unsent(ClusterError),
    /// The request was written, and the reply did not arrive within the
    /// read timeout.
    ReadTimeout(ClusterError),
    /// The request was written, and finishing the stream or reading the
    /// reply failed.
    Unanswered(ClusterError),
}

/// Whether a stream failure condemns its connection.
///
/// `closed` holds when the connection is closed, lost or reset. `heard`
/// holds when the connection received a datagram since the stream opened.
/// Any other stream failure leaves the connection to the sends sharing it.
fn condemns_connection(timed_out: bool, closed: bool, heard: bool) -> bool {
    closed || (timed_out && !heard)
}

/// UDP datagrams `conn` received so far.
fn received_datagrams(conn: &quinn::Connection) -> u64 {
    conn.stats().udp_rx.datagrams
}

#[cfg(test)]
mod tests {
    use nodedb_raft::message::RequestVoteRequest;

    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::super::transport::tests::{
        SLOW, SLOW_GROUP, STALL, STALLED_GROUP, serve_echo, serve_echo_with_rpc_timeout,
    };
    use super::*;
    use crate::rpc_codec::{FrameRefusal, PongResponse, RequestRefusal};

    #[test]
    fn only_a_closed_or_silent_connection_is_condemned() {
        // A closed connection is condemned whatever failed on it.
        assert!(condemns_connection(false, true, true));
        assert!(condemns_connection(true, true, true));
        // A read timeout with no datagram during the wait: the peer is gone.
        assert!(condemns_connection(true, false, false));
        // A read timeout while the peer kept acknowledging: a slow handler.
        assert!(!condemns_connection(true, false, true));
        // A stream reset or refused write on an open connection.
        assert!(!condemns_connection(false, false, false));
        assert!(!condemns_connection(false, false, true));
    }

    /// A handler slower than the caller's read timeout counts against the
    /// breaker, but the connection other sends share stays pooled.
    #[tokio::test]
    async fn a_read_timeout_on_a_live_connection_keeps_it_pooled() {
        let (_server, client, _shutdown) = serve_echo().await;
        client.warm_peer(1).await.expect("warm");
        let pooled = client.peer_connection_stable_id(1).expect("pooled");
        let vote = RaftRpc::RequestVoteRequest(RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            group_id: STALLED_GROUP,
            transfer: false,
        });
        let inner = rpc_codec::encode(&vote, &client.auth.epoch).expect("encode");
        let read_timeout = STALL / 4;

        let outcome = client
            .attempt(1, &inner, read_timeout, Admission::Normal)
            .await;

        match outcome {
            Attempt::LinkFailed(LinkFailure {
                condemned: None, ..
            }) => {}
            other => panic!("expected a kept connection, got {other:?}"),
        }
        assert_eq!(client.peer_connection_stable_id(1), Some(pooled));
        assert_eq!(client.circuit_breaker().failure_count(1), 1);
    }

    /// RPC timeout, and so QUIC idle timeout, of the liveness tests.
    const LIVENESS_RPC_TIMEOUT: Duration = Duration::from_millis(300);

    fn vote(group_id: u64) -> RaftRpc {
        RaftRpc::RequestVoteRequest(RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            group_id,
            transfer: false,
        })
    }

    /// A handler slower than the idle timeout on a live peer gets the whole
    /// read timeout: keep-alives hold the connection open.
    #[tokio::test]
    async fn a_stalled_handler_on_a_live_peer_answers_past_the_idle_timeout() {
        let (_server, client, _shutdown) = serve_echo_with_rpc_timeout(LIVENESS_RPC_TIMEOUT).await;
        client.warm_peer(1).await.expect("warm");
        let pooled = client.peer_connection_stable_id(1).expect("pooled");
        assert!(SLOW > LIVENESS_RPC_TIMEOUT * 3);

        let reply = client
            .send_rpc_with_read_timeout(1, vote(SLOW_GROUP), Duration::from_secs(2))
            .await
            .expect("a live peer answers");

        assert!(
            matches!(reply, RaftRpc::RequestVoteResponse(_)),
            "{reply:?}"
        );
        assert_eq!(client.peer_connection_stable_id(1), Some(pooled));
        assert_eq!(client.circuit_breaker().failure_count(1), 0);
    }

    /// A UDP relay between a client and a server that drops every datagram
    /// once `cut` is set.
    async fn relay(server: SocketAddr, cut: Arc<AtomicBool>) -> SocketAddr {
        let socket = Arc::new(
            tokio::net::UdpSocket::bind("127.0.0.1:0")
                .await
                .expect("bind relay"),
        );
        let addr = socket.local_addr().expect("relay address");
        tokio::spawn(async move {
            let mut client: Option<SocketAddr> = None;
            let mut buf = vec![0u8; 65_536];
            while let Ok((len, from)) = socket.recv_from(&mut buf).await {
                if cut.load(Ordering::SeqCst) {
                    continue;
                }
                let to = if from == server {
                    match client {
                        Some(client) => client,
                        None => continue,
                    }
                } else {
                    client = Some(from);
                    server
                };
                let _ = socket.send_to(&buf[..len], to).await;
            }
        });
        addr
    }

    /// A peer cut off mid-read fails a long read at the idle timeout. The
    /// read is not resent to it, and its connection leaves the pool.
    #[tokio::test]
    async fn a_silent_peer_fails_a_long_read_within_the_idle_timeout() {
        let (server, client, _shutdown) = serve_echo_with_rpc_timeout(LIVENESS_RPC_TIMEOUT).await;
        let cut = Arc::new(AtomicBool::new(false));
        client.register_peer(1, relay(server.local_addr(), Arc::clone(&cut)).await);
        client.warm_peer(1).await.expect("warm through the relay");
        assert!(client.peer_connection_stable_id(1).is_some());

        cut.store(true, Ordering::SeqCst);
        let started = tokio::time::Instant::now();
        let err = client
            .send_rpc_with_read_timeout(1, vote(STALLED_GROUP), Duration::from_secs(5))
            .await
            .unwrap_err();
        let elapsed = started.elapsed();

        assert!(
            matches!(err, ClusterError::Unanswered { node_id: 1, .. }),
            "{err:?}"
        );
        let slack = Duration::from_millis(700);
        assert!(
            elapsed < LIVENESS_RPC_TIMEOUT + slack,
            "failed after {elapsed:?}, idle timeout {LIVENESS_RPC_TIMEOUT:?}"
        );
        assert!(
            !client.peers.read().unwrap().contains_key(&1),
            "the dead connection is evicted"
        );
    }

    fn lost(written: bool) -> LinkFailure {
        LinkFailure {
            error: Box::new(ClusterError::Transport {
                detail: "connection lost".into(),
            }),
            condemned: None,
            written,
            silent: false,
        }
    }

    /// A propose that went out can have run on the peer. It is never
    /// resent, and it ends as an unknown outcome.
    #[test]
    fn a_written_request_that_is_not_resend_safe_ends_unanswered() {
        match resend_verdict(lost(true), 2, false) {
            Err(ClusterError::Unanswered { node_id: 2, .. }) => {}
            other => panic!("expected Unanswered, got {other:?}"),
        }
    }

    /// A request that never went out ran nowhere, so it is resent.
    #[test]
    fn an_unsent_request_is_resent() {
        for resend_safe in [false, true] {
            assert!(matches!(
                resend_verdict(lost(false), 2, resend_safe),
                Ok(ClusterError::Transport { .. })
            ));
        }
    }

    /// A request whose second run is harmless is resent after a write.
    #[test]
    fn a_written_resend_safe_request_is_resent() {
        assert!(matches!(
            resend_verdict(lost(true), 2, true),
            Ok(ClusterError::Transport { .. })
        ));
    }

    fn silent(written: bool) -> LinkFailure {
        LinkFailure {
            silent: true,
            ..lost(written)
        }
    }

    /// A silent peer is dead or cut off. A written read is not resent to it
    /// and ends unanswered, so the caller can try another node.
    #[test]
    fn a_written_request_to_a_silent_peer_ends_unanswered() {
        for resend_safe in [false, true] {
            match resend_verdict(silent(true), 2, resend_safe) {
                Err(ClusterError::Unanswered { node_id: 2, .. }) => {}
                other => panic!("expected Unanswered, got {other:?}"),
            }
        }
    }

    /// A request a silent peer never got ends with its own error, unsent.
    #[test]
    fn an_unsent_request_to_a_silent_peer_is_not_resent() {
        assert!(matches!(
            resend_verdict(silent(false), 2, true),
            Err(ClusterError::Transport { .. })
        ));
    }

    #[test]
    fn a_typed_refusal_is_a_final_answer() {
        let refusal = RaftRpc::RequestRefused(RequestRefusal::from(ClusterError::GroupNotFound {
            group_id: 4,
        }));
        match classify(1, Ok(Ok(refusal))) {
            Attempt::Answered(Err(ClusterError::GroupNotFound { group_id: 4 })) => {}
            other => panic!("expected a final GroupNotFound, got {other:?}"),
        }
    }

    #[test]
    fn a_frame_refusal_is_resent() {
        let refusal = RaftRpc::FrameRefused(FrameRefusal {
            detail: "stale sequence".into(),
        });
        assert!(matches!(
            classify(1, Ok(Ok(refusal))),
            Attempt::FrameRefused(_)
        ));
    }

    #[test]
    fn only_the_outer_error_is_a_link_failure() {
        assert!(matches!(
            classify(1, Err(lost(false))),
            Attempt::LinkFailed(_)
        ));

        let unreadable = ClusterError::Codec {
            detail: "bad crc".into(),
        };
        assert!(matches!(
            classify(1, Ok(Err(unreadable))),
            Attempt::Answered(Err(ClusterError::Codec { .. }))
        ));

        let pong = RaftRpc::Pong(PongResponse {
            responder_id: 1,
            topology_version: 2,
            oldest_snapshot: crate::snapshot_board::NO_SNAPSHOT_REPORT,
        });
        assert!(matches!(
            classify(1, Ok(Ok(pong))),
            Attempt::Answered(Ok(RaftRpc::Pong(_)))
        ));
    }
}
