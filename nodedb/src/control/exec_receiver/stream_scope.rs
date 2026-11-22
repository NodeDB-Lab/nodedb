// SPDX-License-Identifier: BUSL-1.1

//! The request stream a received leg runs on.
//!
//! The sender puts its client's stream on the wire. The leg runs inside that
//! stream's scope, so every request it fans out to this node's cores lands on
//! the client's stream. A leg with no stream is internal work.

use std::future::Future;

use nodedb_cluster::rpc_codec::{ForwardedStream, TypedClusterError};

use crate::control::server::shared::session::{connection_stream, forwarded_stream};

/// Run `leg` on the stream the request's `stream` field names. A zero
/// connection id is refused.
pub(super) async fn on_request_stream<F: Future>(
    wire: Option<ForwardedStream>,
    leg: F,
) -> Result<F::Output, TypedClusterError> {
    let stream = forwarded_stream::received(wire)?;
    Ok(connection_stream::scoped(stream, leg).await)
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::rpc_codec::{ExecuteRequest, PLAN_DECODE_FAILED};

    use super::*;
    use crate::bridge::envelope::RequestStream;
    use crate::control::server::shared::session::ConnectionId;

    fn request_on(stream: Option<ForwardedStream>) -> ExecuteRequest {
        ExecuteRequest {
            plan_bytes: Vec::new(),
            tenant_id: 1,
            database_id: 0,
            deadline_remaining_ms: 1000,
            trace_id: [0u8; 16],
            descriptor_versions: Vec::new(),
            txn_id: None,
            vshard_id: None,
            read_groups: Vec::new(),
            user_roles: Vec::new(),
            stream,
        }
    }

    /// A forwarded client leg runs on the origin node's connection. Every
    /// `Request` the leg builds reads this stream.
    #[tokio::test]
    async fn a_forwarded_leg_runs_on_the_stream_it_carried() {
        let req = request_on(Some(ForwardedStream {
            origin_node: 2,
            connection: 7,
        }));
        let stream = on_request_stream(req.stream, async { connection_stream::current() })
            .await
            .expect("a valid stream");
        let id = ConnectionId::new(7).expect("non-zero id");
        assert_eq!(stream, RequestStream::client(2, id));
    }

    /// A leg with no stream is internal work, even inside a client scope.
    #[tokio::test]
    async fn a_leg_without_a_stream_is_internal() {
        let id = ConnectionId::new(3).expect("non-zero id");
        let stream = connection_stream::scoped(RequestStream::client(1, id), async {
            on_request_stream(request_on(None).stream, async {
                connection_stream::current()
            })
            .await
        })
        .await
        .expect("no stream is valid");
        assert_eq!(stream, RequestStream::Internal);
    }

    #[tokio::test]
    async fn a_zero_connection_on_the_wire_is_refused() {
        let req = request_on(Some(ForwardedStream {
            origin_node: 2,
            connection: 0,
        }));
        let outcome = on_request_stream(req.stream, async {}).await;
        assert!(matches!(
            outcome,
            Err(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                ..
            })
        ));
    }
}
