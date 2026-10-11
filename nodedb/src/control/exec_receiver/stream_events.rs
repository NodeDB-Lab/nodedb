// SPDX-License-Identifier: BUSL-1.1

//! Receiver side of the event-stream `ClusterEvent` RPC: CDC consume.
//!
//! The consume runs no `confirm_read_leg`, and needs none.
//!
//! - A CDC consume reads this node's change-stream buffer. Every replica
//!   routes each committed entry's change events into its own buffer at the
//!   entry's Raft log position, so a lagging replica holds a prefix of the
//!   same sequence. It returns fewer events, never different ones.
//! - The consumer cursor belongs to the caller. It arrives in
//!   `committed_offsets` and is never read from or written to this node's
//!   `OffsetStore`. Each read starts after the caller's committed position, so
//!   a stale serving node cannot make a consumer re-read an event.

use nodedb_cluster::rpc_codec::{ExecuteResponse, TypedClusterError};
use nodedb_physical::physical_plan::ClusterEventOp;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::support::PLAN_DECODE_FAILED;

/// The decoded fields of a `ClusterEventOp::ConsumeStream` plan.
struct ConsumeStreamRequest<'a> {
    stream_database_id: DatabaseId,
    stream_name: &'a str,
    group_name: &'a str,
    partition: Option<u32>,
    limit: u64,
    committed_offsets: &'a [(u32, u64, u64, u64)],
}

/// Answer `plan` when it is a CDC consume.
///
/// Returns `None` for every other plan.
pub(super) fn answer_stream_event_plan(
    state: &SharedState,
    plan: &PhysicalPlan,
    database_id: DatabaseId,
    tenant_id: u64,
) -> Option<ExecuteResponse> {
    match plan {
        PhysicalPlan::ClusterEvent(ClusterEventOp::ConsumeStream {
            database_id: stream_database_id,
            stream_name,
            group_name,
            partition,
            limit,
            committed_offsets,
        }) => {
            let request = ConsumeStreamRequest {
                stream_database_id: *stream_database_id,
                stream_name,
                group_name,
                partition: *partition,
                limit: *limit,
                committed_offsets,
            };
            Some(answer_consume_stream(
                state,
                database_id,
                tenant_id,
                request,
            ))
        }
        _ => None,
    }
}

/// Read this node's CDC buffer from the caller's committed cursor.
fn answer_consume_stream(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    request: ConsumeStreamRequest<'_>,
) -> ExecuteResponse {
    if let Err(error) = reject_consume_database_mismatch(request.stream_database_id, database_id) {
        return ExecuteResponse::err(error);
    }
    let limit = match usize::try_from(request.limit) {
        Ok(limit) => limit,
        Err(_) => {
            return ExecuteResponse::err(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: "CDC consume limit exceeds platform range".into(),
            });
        }
    };
    let params = crate::event::cdc::consume::ConsumeParams {
        database_id: request.stream_database_id,
        tenant_id,
        stream_name: request.stream_name,
        group_name: request.group_name,
        partition: request.partition,
        limit,
    };
    if let Err(error) = crate::event::cdc::consume::validate_consume_identity(state, &params) {
        return ExecuteResponse::err(TypedClusterError::Internal {
            code: PLAN_DECODE_FAILED,
            message: error.to_string(),
        });
    }
    let committed_offsets = match crate::event::cdc::consume::decode_remote_committed_offsets(
        request.committed_offsets,
    ) {
        Ok(offsets) => offsets,
        Err(error) => {
            return ExecuteResponse::err(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: error.to_string(),
            });
        }
    };
    // Events go to an authenticated peer node, not a subscriber. The
    // requesting node applies its caller's redaction at the delivery surface
    // (SELECT / HTTP poll / SSE), using the same replicated catalog policies
    // on both sides.
    // A cursor below the events this node holds answers a typed reply, so
    // the caller surfaces `OffsetOutOfRange` rather than an opaque error.
    let reply = crate::event::cdc::consume::RemoteConsumeReply::from_result(
        crate::event::cdc::consume::consume_local_with_offsets(
            state,
            &params,
            Some(&committed_offsets),
        ),
    );
    match reply {
        Ok(reply) => match zerompk::to_msgpack_vec(&reply) {
            Ok(payload) => ExecuteResponse::ok(vec![payload], 0, Vec::new()),
            Err(error) => ExecuteResponse::err(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: format!("CDC response encoding failed: {error}"),
            }),
        },
        Err(error) => ExecuteResponse::err(TypedClusterError::Internal {
            code: PLAN_DECODE_FAILED,
            message: error.to_string(),
        }),
    }
}

fn reject_consume_database_mismatch(
    stream_database_id: DatabaseId,
    envelope_database_id: DatabaseId,
) -> Result<(), TypedClusterError> {
    if stream_database_id == envelope_database_id {
        Ok(())
    } else {
        Err(TypedClusterError::Internal {
            code: PLAN_DECODE_FAILED,
            message: "CDC consume database does not match RPC database".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consume_stream_rejects_database_mismatch() {
        assert!(matches!(
            reject_consume_database_mismatch(DatabaseId::new(7), DatabaseId::new(8)),
            Err(TypedClusterError::Internal { .. })
        ));
    }

    #[test]
    fn consume_stream_rejects_duplicate_caller_offsets() {
        assert!(matches!(
            crate::event::cdc::consume::decode_remote_committed_offsets(&[
                (3, 0, 7, 1),
                (3, 0, 8, 1)
            ]),
            Err(crate::event::cdc::consume::ConsumeError::InvalidRemoteOffsets(_))
        ));
    }
}
