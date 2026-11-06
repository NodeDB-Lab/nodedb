// SPDX-License-Identifier: BUSL-1.1

//! The transactional outbox of a trigger body's cross-node writes.
//!
//! A body's cross-node request commits in the body's own redo record, as a
//! committed message to a reserved topic, beside the body's writes and its
//! applied key. So the request is durable exactly when the body is: no queue
//! can refuse it after the body committed. Every replica of the record holds
//! it in the publish ledger, the partition's lease holder delivers it from the
//! replicated cursor, and the cursor passes it only once the receiver
//! answered. A new lease holder, or a restart, resumes from the cursor, and
//! the receiver's key applies a request sent twice once.

use std::collections::HashMap;
use std::sync::Mutex;

use tracing::{error, warn};

use crate::control::state::SharedState;
use crate::event::cross_shard::dlq::DlqEnqueueParams;
use crate::event::cross_shard::types::{CrossShardWriteRequest, CrossShardWriteResponse};
use crate::event::topic::types::PublishOrigin;
use crate::wal::RedoPublish;

/// The reserved topic an outbound request is committed to. No topic takes
/// the name: a `:` in a topic name is reserved.
pub const OUTBOX_TOPIC: &str = "_outbox:cross_shard";

/// Receiver refusals a request survives before it is dead-lettered.
const MAX_REFUSALS: u32 = 5;

/// How many times each held request was refused, by origin. A refusal holds
/// the partition's cursor on the request until it is retried or
/// dead-lettered. The count is this node's: a new lease holder starts over.
#[derive(Debug, Default)]
pub struct OutboxRefusals {
    counts: Mutex<HashMap<PublishOrigin, u32>>,
}

impl OutboxRefusals {
    fn forget(&self, origin: PublishOrigin) {
        self.counts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&origin);
    }

    fn count(&self, origin: PublishOrigin) -> u32 {
        let mut counts = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        let count = counts.entry(origin).or_insert(0);
        *count += 1;
        *count
    }
}

fn outbox_error(detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("cross-shard outbox entry: {detail}"),
    }
}

/// The committed message that carries `request`, owned by `owner`.
pub fn outbox_message(owner: &str, request: &CrossShardWriteRequest) -> crate::Result<RedoPublish> {
    let bytes = zerompk::to_msgpack_vec(request).map_err(outbox_error)?;
    Ok(RedoPublish {
        owner: owner.to_owned(),
        database_id: request.database_id,
        tenant_id: request.tenant_id,
        topic: OUTBOX_TOPIC.to_owned(),
        payload: hex::encode(bytes),
        metadata_floor: 0,
        position: None,
    })
}

/// The request `message` carries.
fn request_of(message: &RedoPublish) -> crate::Result<CrossShardWriteRequest> {
    let bytes = hex::decode(&message.payload).map_err(outbox_error)?;
    zerompk::from_msgpack(&bytes).map_err(outbox_error)
}

/// What delivering one outbound request did.
pub enum OutboxDelivery {
    /// The receiver applied it, or had applied it before: the cursor passes
    /// it.
    Delivered,
    /// It spent its refusals and is dead-lettered: the cursor passes it.
    DeadLettered,
    /// Not delivered yet: the cursor stays on it, and the next pass sends it
    /// again.
    Retry,
}

/// Send the request `message` carries to the node that leads its target
/// vShard now, and learn its receiver's answer.
pub async fn deliver_outbox(
    state: &SharedState,
    refusals: &OutboxRefusals,
    origin: PublishOrigin,
    message: &RedoPublish,
) -> OutboxDelivery {
    let request = match request_of(message) {
        Ok(request) => request,
        Err(error) => {
            error!(owner = %message.owner, error = %error, "an outbox entry does not decode");
            return OutboxDelivery::DeadLettered;
        }
    };
    let answer = match target_of(state, request.target_vshard) {
        Target::Here => crate::event::cross_shard::receiver::apply_here(state, &request).await,
        Target::Node(node) => match state.cluster_transport.as_ref() {
            Some(transport) => {
                crate::event::cross_shard::dispatcher::send_write(
                    transport,
                    state.node_id,
                    node,
                    &request,
                )
                .await
            }
            None => return OutboxDelivery::Retry,
        },
        Target::Unknown => return OutboxDelivery::Retry,
    };
    match answer {
        Ok(CrossShardWriteResponse { success: true, .. })
        | Ok(CrossShardWriteResponse {
            duplicate: true, ..
        }) => {
            refusals.forget(origin);
            OutboxDelivery::Delivered
        }
        Ok(CrossShardWriteResponse { error, .. }) => {
            refused(state, refusals, origin, &request, &error)
        }
        // The request can be missing at the receiver: send it again.
        Err(error) => {
            warn!(owner = %message.owner, error = %error, "outbox request not delivered; retried");
            OutboxDelivery::Retry
        }
    }
}

/// Where a request to `vshard` goes now.
enum Target {
    Here,
    Node(u64),
    Unknown,
}

fn target_of(state: &SharedState, vshard: u32) -> Target {
    match crate::control::gateway::live_leaders::resolve_live_decision(state, vshard) {
        crate::control::gateway::RouteDecision::Local => Target::Here,
        crate::control::gateway::RouteDecision::Remote { node_id, .. } => Target::Node(node_id),
        _ => Target::Unknown,
    }
}

/// Count a receiver's refusal of `request`. Once it spent its refusals, the
/// request goes to the cross-shard DLQ and the cursor passes it.
fn refused(
    state: &SharedState,
    counts: &OutboxRefusals,
    origin: PublishOrigin,
    request: &CrossShardWriteRequest,
    reason: &str,
) -> OutboxDelivery {
    let refusals = counts.count(origin);
    if refusals < MAX_REFUSALS {
        return OutboxDelivery::Retry;
    }
    let Some(dlq) = state.cross_shard.dlq.as_ref() else {
        return OutboxDelivery::Retry;
    };
    let enqueued = dlq
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .enqueue(DlqEnqueueParams {
            tenant_id: request.tenant_id,
            source_collection: request.source_collection.clone(),
            sql: request.sql.clone(),
            source_vshard: request.source_vshard,
            target_vshard: request.target_vshard,
            target_node: 0,
            source_lsn: request.source_lsn,
            source_sequence: request.source_sequence,
            origin: request.origin.clone(),
            error: reason.to_owned(),
            retry_count: refusals,
        });
    match enqueued {
        Ok(_) => {
            counts.forget(origin);
            OutboxDelivery::DeadLettered
        }
        Err(error) => {
            error!(
                origin = %request.origin,
                error = %error,
                "cross-shard DLQ refused an outbox request; it keeps its cursor"
            );
            OutboxDelivery::Retry
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_round_trips_through_its_outbox_message() {
        let request = CrossShardWriteRequest {
            sql: "BEGIN\nINSERT INTO t (id) VALUES ('a');\nEND;".into(),
            tenant_id: 1,
            database_id: 0,
            source_vshard: 3,
            source_lsn: 12,
            source_sequence: 2,
            origin: "trigger/0/t/remote".into(),
            cascade_depth: 1,
            source_collection: "src".into(),
            target_vshard: 9,
        };
        let message = outbox_message("trigger/0/t", &request).expect("encode");
        assert_eq!(message.topic, OUTBOX_TOPIC);
        let back = request_of(&message).expect("decode");
        assert_eq!(back.sql, request.sql);
        assert_eq!(back.origin, request.origin);
        assert_eq!(back.target_vshard, 9);
    }
}
