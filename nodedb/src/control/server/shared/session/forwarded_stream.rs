// SPDX-License-Identifier: BUSL-1.1

//! The request stream a cross-node RPC carries.
//!
//! A sender puts its current stream on the wire. The receiver runs the leg
//! inside that stream's scope, so the leg orders on the receiving node with
//! the client's other tasks there, and against no other client.

use nodedb_cluster::rpc_codec::{ForwardedStream, PLAN_DECODE_FAILED, TypedClusterError};

use crate::bridge::envelope::{ClientStream, RequestStream};

use super::connection::{ConnectionId, ConnectionIdError};
use super::connection_stream;

/// `stream` as the wire field. `None` for internal work.
pub fn to_wire(stream: RequestStream) -> Option<ForwardedStream> {
    stream.connection().map(|client| ForwardedStream {
        origin_node: client.origin_node,
        connection: client.id.get(),
    })
}

/// The stream of the work running on this task, as the wire field.
pub fn current_on_wire() -> Option<ForwardedStream> {
    to_wire(connection_stream::current())
}

/// The stream the wire field names. A zero connection id is an error.
pub fn from_wire(wire: Option<ForwardedStream>) -> Result<RequestStream, ConnectionIdError> {
    match wire {
        Some(forwarded) => Ok(RequestStream::Connection(ClientStream {
            origin_node: forwarded.origin_node,
            id: ConnectionId::new(forwarded.connection)?,
        })),
        None => Ok(RequestStream::Internal),
    }
}

/// The stream the wire field of a received RPC names. A zero connection id
/// is refused with the typed error the sender surfaces.
pub fn received(wire: Option<ForwardedStream>) -> Result<RequestStream, TypedClusterError> {
    from_wire(wire).map_err(|error| TypedClusterError::Internal {
        code: PLAN_DECODE_FAILED,
        message: format!("forwarded request stream: {error}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(origin_node: u64, raw: u64) -> RequestStream {
        RequestStream::client(origin_node, ConnectionId::new(raw).expect("non-zero id"))
    }

    #[test]
    fn a_client_stream_round_trips_through_the_wire_field() {
        let stream = client(3, 7);
        assert_eq!(from_wire(to_wire(stream)), Ok(stream));
    }

    #[test]
    fn internal_work_carries_no_wire_field() {
        assert_eq!(to_wire(RequestStream::Internal), None);
        assert_eq!(from_wire(None), Ok(RequestStream::Internal));
    }

    #[test]
    fn a_zero_connection_on_the_wire_is_refused() {
        let wire = ForwardedStream {
            origin_node: 1,
            connection: 0,
        };
        assert_eq!(from_wire(Some(wire)), Err(ConnectionIdError::Zero));
        assert!(matches!(
            received(Some(wire)),
            Err(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn the_current_stream_goes_on_the_wire() {
        let wire = connection_stream::scoped(client(2, 5), async { current_on_wire() }).await;
        assert_eq!(
            wire,
            Some(ForwardedStream {
                origin_node: 2,
                connection: 5,
            })
        );
    }
}
