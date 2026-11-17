// SPDX-License-Identifier: BUSL-1.1

//! The one way a sync session writes to its WebSocket.
//!
//! A write to a client that stops reading blocks once the socket buffers
//! fill. Every send therefore races the session's kill and a send timeout.
//! A send that loses either race counts as failed, and the session closes.
//! A stalled client never holds a killed session open.

use std::future::Future;
use std::time::Duration;

use futures::{Sink, SinkExt};
use tokio::sync::watch;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

use crate::control::security::sessions::{KillReason, kill_signal_ended};
use crate::control::server::sync::session::SyncSession;
use crate::control::server::sync::wire::SyncFrame;

type Ws = WebSocketStream<tokio::net::TcpStream>;

/// The longest one WebSocket send waits for the client to take the bytes.
///
/// A client that takes no bytes for this long is not reading. Its pushes
/// pile up behind it, so the session closes and the client reconnects.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// Send `msg` on `sink`, racing `killed` and [`SEND_TIMEOUT`]. Returns
/// whether the send completed. A kill wins over a send that is ready at the
/// same time.
async fn send_racing<S, K>(sink: &mut S, killed: K, msg: Message) -> bool
where
    S: Sink<Message> + Unpin,
    K: Future,
{
    tokio::select! {
        biased;
        _ = killed => false,
        sent = tokio::time::timeout(SEND_TIMEOUT, sink.send(msg)) => matches!(sent, Ok(Ok(()))),
    }
}

/// The kill receiver of `session`. `None` when the session has no binding,
/// and then the kill never fires.
pub(super) fn kill_of(session: &SyncSession) -> Option<watch::Receiver<KillReason>> {
    session
        .binding
        .as_ref()
        .map(|binding| binding.kill_signal())
}

/// Send `msg` to the session's client. Returns whether it went out.
pub(in super::super) async fn send_message(
    ws: &mut Ws,
    session: &SyncSession,
    msg: Message,
) -> bool {
    let mut kill = kill_of(session);
    send_racing(ws, kill_signal_ended(&mut kill), msg).await
}

/// Send encoded frame bytes to the session's client.
pub(in super::super) async fn send_bytes(
    ws: &mut Ws,
    session: &SyncSession,
    bytes: Vec<u8>,
) -> bool {
    send_message(ws, session, Message::Binary(bytes.into())).await
}

/// Send `frame` to the session's client.
pub(in super::super) async fn send_frame(
    ws: &mut Ws,
    session: &SyncSession,
    frame: &SyncFrame,
) -> bool {
    send_bytes(ws, session, frame.to_bytes()).await
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use super::*;

    /// A sink whose client never takes a byte.
    struct StalledSink;

    impl Sink<Message> for StalledSink {
        type Error = std::io::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }
    }

    fn message() -> Message {
        Message::Binary(vec![1, 2, 3].into())
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_send_ends_on_kill() {
        let (kill_tx, kill_rx) = tokio::sync::oneshot::channel::<()>();
        let started = tokio::time::Instant::now();
        let mut sink = StalledSink;
        let send = tokio::spawn(async move { send_racing(&mut sink, kill_rx, message()).await });
        tokio::task::yield_now().await;
        assert!(
            !send.is_finished(),
            "a stalled send waits while the session lives"
        );

        kill_tx.send(()).expect("kill");
        let sent = send.await.expect("send task");
        assert!(!sent, "a killed session's send fails");
        assert!(
            started.elapsed() < SEND_TIMEOUT,
            "the kill ends the send, not the timeout"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_send_ends_at_the_timeout() {
        let mut sink = StalledSink;
        let started = tokio::time::Instant::now();
        let sent = send_racing(&mut sink, std::future::pending::<()>(), message()).await;
        assert!(!sent, "a send the client never takes fails");
        assert!(started.elapsed() >= SEND_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn ready_kill_wins_over_a_ready_send() {
        let (mut tx, _rx) = futures::channel::mpsc::channel::<Message>(4);
        let sent = send_racing(&mut tx, std::future::ready(()), message()).await;
        assert!(!sent, "a session already killed sends nothing");
    }

    #[tokio::test(start_paused = true)]
    async fn ready_sink_sends() {
        let (mut tx, mut rx) = futures::channel::mpsc::channel::<Message>(4);
        let sent = send_racing(&mut tx, std::future::pending::<()>(), message()).await;
        assert!(sent);
        assert_eq!(futures::StreamExt::next(&mut rx).await, Some(message()));
    }
}
