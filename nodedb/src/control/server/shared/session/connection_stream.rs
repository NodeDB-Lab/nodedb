// SPDX-License-Identifier: BUSL-1.1

//! The request stream of the client connection running on this task.
//!
//! Each client connection future runs inside [`scoped`] with the id it took
//! from the shared allocator. A plain HTTP request runs inside its own scope.
//! Every Control -> Data request the connection fans out into reads
//! [`current`], so each lands on the connection's stream. A trigger or
//! procedure body runs on its connection's task and keeps the stream. Work
//! outside any connection scope is [`RequestStream::Internal`]: Raft apply,
//! Calvin, replay, maintenance, the Event Plane and background loops.

use std::future::Future;

use crate::bridge::envelope::RequestStream;

use super::connection::ConnectionId;

tokio::task_local! {
    static CONNECTION: ConnectionId;
}

/// Run `future` as the work of connection `id`.
pub fn scoped<F: Future>(id: ConnectionId, future: F) -> impl Future<Output = F::Output> {
    CONNECTION.scope(id, future)
}

/// The stream of the connection running on this task, or
/// [`RequestStream::Internal`] outside every connection scope.
pub fn current() -> RequestStream {
    CONNECTION
        .try_with(|id| RequestStream::Connection(*id))
        .unwrap_or(RequestStream::Internal)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(raw: u64) -> ConnectionId {
        ConnectionId::new(raw).expect("non-zero id")
    }

    #[tokio::test]
    async fn a_connection_scope_names_its_stream() {
        let stream = scoped(id(7), async { current() }).await;
        assert_eq!(stream, RequestStream::Connection(id(7)));
    }

    #[tokio::test]
    async fn work_outside_a_connection_is_internal() {
        assert_eq!(current(), RequestStream::Internal);
    }

    #[tokio::test]
    async fn an_inner_scope_names_the_inner_connection() {
        let stream = scoped(id(1), scoped(id(2), async { current() })).await;
        assert_eq!(stream, RequestStream::Connection(id(2)));
    }
}
