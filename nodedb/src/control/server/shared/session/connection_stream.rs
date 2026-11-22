// SPDX-License-Identifier: BUSL-1.1

//! The request stream of the client connection running on this task.
//!
//! Each client connection future runs inside [`scoped`] with the stream of
//! the id it took from the shared allocator. A plain HTTP request runs inside
//! its own scope. A leg another node forwarded runs inside the scope of the
//! stream it carried. Every Control -> Data request the work fans out into
//! reads [`current`], so each lands on that stream. A trigger or procedure
//! body runs on its connection's task and keeps the stream. Work outside any
//! scope is [`RequestStream::Internal`]: Raft apply, Calvin, replay,
//! maintenance, the Event Plane and background loops.

use std::future::Future;

use futures::Stream;

use crate::bridge::envelope::RequestStream;

tokio::task_local! {
    static STREAM: RequestStream;
}

/// Run `future` as the work of `stream`.
pub fn scoped<F: Future>(stream: RequestStream, future: F) -> impl Future<Output = F::Output> {
    STREAM.scope(stream, future)
}

/// The stream of the work running on this task, or
/// [`RequestStream::Internal`] outside every scope.
pub fn current() -> RequestStream {
    STREAM
        .try_with(|stream| *stream)
        .unwrap_or(RequestStream::Internal)
}

/// Poll `body` as the work of the stream current at this call.
///
/// A response body is polled after its handler returns, outside the
/// handler's scope. Each poll of the returned stream runs inside the
/// captured stream, so the requests the body sends keep the client's stream.
pub fn carry_current<S: Stream>(body: S) -> impl Stream<Item = S::Item> {
    let stream = current();
    let mut body = Box::pin(body);
    futures::stream::poll_fn(move |cx| STREAM.sync_scope(stream, || body.as_mut().poll_next(cx)))
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::control::server::shared::session::ConnectionId;

    fn client(origin_node: u64, raw: u64) -> RequestStream {
        RequestStream::client(origin_node, ConnectionId::new(raw).expect("non-zero id"))
    }

    #[tokio::test]
    async fn a_connection_scope_names_its_stream() {
        let stream = scoped(client(1, 7), async { current() }).await;
        assert_eq!(stream, client(1, 7));
    }

    #[tokio::test]
    async fn work_outside_a_connection_is_internal() {
        assert_eq!(current(), RequestStream::Internal);
    }

    #[tokio::test]
    async fn an_inner_scope_names_the_inner_connection() {
        let stream = scoped(client(1, 1), scoped(client(1, 2), async { current() })).await;
        assert_eq!(stream, client(1, 2));
    }

    #[tokio::test]
    async fn an_internal_scope_hides_the_outer_connection() {
        let stream = scoped(
            client(1, 1),
            scoped(RequestStream::Internal, async { current() }),
        )
        .await;
        assert_eq!(stream, RequestStream::Internal);
    }

    /// A body built inside a scope and polled outside it keeps the stream.
    #[tokio::test]
    async fn a_carried_body_polls_on_the_stream_it_was_built_in() {
        let body = scoped(client(2, 9), async {
            carry_current(futures::stream::poll_fn(|_| {
                std::task::Poll::Ready(Some(current()))
            }))
        })
        .await;
        let mut body = Box::pin(body);
        assert_eq!(current(), RequestStream::Internal);
        assert_eq!(body.next().await, Some(client(2, 9)));
    }
}
