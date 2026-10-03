// SPDX-License-Identifier: BUSL-1.1

//! The events that carry a committed transaction's `PUBLISH TO` messages.
//!
//! A redo record carries its messages in record order. The install emits one
//! [`WriteOp::Publish`] event per message, and WAL catch-up rebuilds the same
//! events from the same record. Each event holds the message with its ordinal
//! in the record, so every replica names the message alike.

use std::sync::Arc;

use tracing::warn;

use crate::event::types::{RecordPosition, RowId, WriteEvent, WriteOp};
use crate::event::wal_replay_scope::ReplayScope;
use crate::wal::RedoPublish;

/// The event stream a topic's committed publishes name. A collection name
/// holds no `:`, so no row write shares it.
pub fn publish_stream(topic: &str) -> String {
    format!("topic:{topic}")
}

/// One message of a committed record, with its zero-based position among the
/// record's messages.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct CommittedPublish {
    pub ordinal: u32,
    pub publish: RedoPublish,
}

/// The `new_value` of the publish event for the record's `ordinal`-th message.
pub fn encode_publish(ordinal: u32, publish: &RedoPublish) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&CommittedPublish {
        ordinal,
        publish: publish.clone(),
    })
    .map_err(|error| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("committed publish encode: {error}"),
    })
}

pub(super) fn decode_publish(bytes: &[u8]) -> crate::Result<CommittedPublish> {
    zerompk::from_msgpack(bytes).map_err(|error| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("committed publish decode: {error}"),
    })
}

/// The publish events WAL catch-up rebuilds from one redo record, in record
/// order, as the install emitted them. Each takes the next `sequence`.
pub(crate) fn replayed_publish_events(
    publishes: &[RedoPublish],
    scope: &ReplayScope,
    sequence: &mut u64,
) -> Vec<WriteEvent> {
    let mut events = Vec::with_capacity(publishes.len());
    for (ordinal, publish) in (0u32..).zip(publishes) {
        let value = match encode_publish(ordinal, publish) {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    lsn = scope.lsn.as_u64(),
                    topic = %publish.topic,
                    error = %error,
                    "WAL replay: a committed publish did not encode; no event rebuilt"
                );
                continue;
            }
        };
        *sequence += 1;
        events.push(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(publish_stream(&publish.topic)),
            op: WriteOp::Publish,
            row_id: RowId::Batch,
            lsn: scope.lsn,
            record: Some(RecordPosition::first(scope.lsn)),
            database_id: scope.database_id,
            tenant_id: scope.tenant_id,
            vshard_id: scope.vshard_id,
            source: scope.sources.other,
            new_value: Some(Arc::from(value)),
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
            commit_hlc: scope.commit_hlc,
            image_fault: None,
        });
    }
    events
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::event::types::EventSource;
    use crate::event::wal_replay_scope::RowSources;
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

    pub(in crate::event::topic::committed) fn publish(topic: &str, payload: &str) -> RedoPublish {
        RedoPublish {
            owner: "trigger/1/notify".into(),
            database_id: 1,
            tenant_id: 1,
            topic: topic.into(),
            payload: payload.into(),
            metadata_floor: 0,
            position: None,
        }
    }

    pub(in crate::event::topic::committed) fn scope() -> ReplayScope {
        ReplayScope {
            database_id: DatabaseId::new(1),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(4),
            lsn: Lsn::new(90),
            sources: RowSources::committed_redo(EventSource::User),
            commit_hlc: None,
        }
    }

    #[test]
    fn a_replayed_publish_carries_its_message_and_ordinal_on_its_topic_stream() {
        let mut sequence = 10;
        let events = replayed_publish_events(
            &[publish("feed", "a"), publish("feed", "b")],
            &scope(),
            &mut sequence,
        );
        assert_eq!(events.len(), 2);
        assert_eq!(sequence, 12);
        for event in &events {
            assert_eq!(event.op, WriteOp::Publish);
            assert!(!event.op.is_data_event());
            assert_eq!(event.collection.as_ref(), "topic:feed");
            assert_eq!(event.lsn, Lsn::new(90));
        }
        let second = events[1].new_value.as_deref().map(decode_publish);
        assert_eq!(
            second.and_then(Result::ok),
            Some(CommittedPublish {
                ordinal: 1,
                publish: publish("feed", "b"),
            }),
            "the event holds the message and its place in the record"
        );
    }
}
