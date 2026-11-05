// SPDX-License-Identifier: BUSL-1.1

//! Publish committed messages to durable topics.
//!
//! A publication is a Raft entry of the data group that owns the topic's home
//! vShard, on a one-node cluster too. Every replica appends it at apply (see
//! [`super::apply`]), so a home change loses no message and a consumer reads
//! from any replica.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::event::topic::types::PublishOrigin;
use crate::types::DatabaseId;

/// Publish a message to a durable topic.
///
/// Returns the persistent sequence number the topic assigned.
pub async fn publish_to_topic(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    topic_name: &str,
    payload: &str,
) -> Result<u64, PublishError> {
    publish(state, database_id, tenant_id, topic_name, payload, None).await
}

/// Publish a committed transaction's message, named by its `origin`. A topic
/// that already holds a message of that origin appends nothing, so a message
/// delivered again after a lease move reaches the topic once.
pub async fn publish_committed(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    topic_name: &str,
    payload: &str,
    origin: PublishOrigin,
) -> Result<(), PublishError> {
    publish(
        state,
        database_id,
        tenant_id,
        topic_name,
        payload,
        Some(origin),
    )
    .await
    .map(|_| ())
}

/// Publish one message. Returns the sequence the topic assigned, `0` for a
/// message of an `origin` the topic already holds.
async fn publish(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    topic_name: &str,
    payload: &str,
    origin: Option<PublishOrigin>,
) -> Result<u64, PublishError> {
    state
        .ep_topic_registry
        .get(database_id, tenant_id, topic_name)
        .ok_or_else(|| PublishError::TopicNotFound(topic_name.to_string()))?;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let proposer = state
        .async_raft_proposer()
        .map_err(|error| PublishError::Persistence(error.to_string()))?;
    publish_replicated(
        state,
        proposer,
        ReplicatedEntry::new(
            tenant_id,
            database_id.as_u64(),
            topic_vshard(database_id, topic_name),
            ReplicatedWrite::TopicPublish {
                topic: topic_name.to_owned(),
                payload: payload.to_owned(),
                event_time: now_ms,
                origin,
            },
        ),
        topic_name,
    )
    .await
}

/// Propose a publication to the home vShard's data group, and return the
/// sequence this node's apply assigned it. Every replica assigns the same.
async fn publish_replicated(
    state: &SharedState,
    proposer: &Arc<crate::control::wal_replication::AsyncRaftProposer>,
    entry: ReplicatedEntry,
    topic_name: &str,
) -> Result<u64, PublishError> {
    let payload = crate::control::wal_replication::propose_replicated_entry(
        state,
        proposer,
        entry,
        crate::control::server::shared::session::StatementDeadline::of_statement(state)
            .tokio_instant(),
    )
    .await
    .map_err(|error| propose_error(error, topic_name))?;
    zerompk::from_msgpack::<u64>(&payload)
        .map_err(|error| PublishError::Persistence(format!("publish result: {error}")))
}

/// The publish error for a failed propose.
///
/// A leader's write gate answers through the propose path, so a refusal
/// reaches this node as `Error::RemoteTyped` with the public code of its class,
/// never as the gate's own variant. The rule is the one
/// `propose_replicated_entry` applies: only an unclassified failure loses its
/// type. A classified error keeps its code and SQLSTATE.
fn propose_error(error: crate::Error, topic_name: &str) -> PublishError {
    match error {
        crate::Error::UndefinedObject { kind: "topic", .. } => {
            PublishError::TopicNotFound(topic_name.to_string())
        }
        unclassified if crate::error_classify::is_unclassified_failure(&unclassified) => {
            PublishError::Persistence(unclassified.to_string())
        }
        classified => PublishError::Refused(Box::new(classified)),
    }
}

/// The home vShard of a topic.
pub fn topic_vshard(database_id: DatabaseId, topic_name: &str) -> u32 {
    nodedb_cluster::routing::vshard_for_collection(nodedb_types::CollectionKey::from_bare(
        database_id,
        topic_name,
    ))
}

#[derive(Debug)]
pub enum PublishError {
    TopicNotFound(String),
    /// The durable catalog cannot be read or committed, or the replicated
    /// publication did not commit.
    Persistence(String),
    /// The propose answered with a classified error, such as a write gate's
    /// refusal. It keeps its class, so the client gets its SQLSTATE.
    Refused(Box<crate::Error>),
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TopicNotFound(topic) => write!(f, "topic '{topic}' does not exist"),
            Self::Persistence(error) => write!(f, "topic persistence error: {error}"),
            Self::Refused(error) => write!(f, "{error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PublishError, propose_error, publish_to_topic};
    use crate::event::cdc::stream_def::RetentionConfig;
    use crate::event::topic::TopicDef;
    use crate::types::DatabaseId;

    /// A leader gate's refusal arrives by public code, and keeps its class.
    #[test]
    fn a_remote_refusal_keeps_its_class() {
        let error = crate::Error::RemoteTyped {
            code: nodedb_types::error::ErrorCode::MIRROR_READ_ONLY,
            message: "database 'm' is a read-only mirror".into(),
        };
        let PublishError::Refused(refusal) = propose_error(error, "events") else {
            panic!("a classified error stays typed");
        };
        assert_eq!(
            crate::control::server::pgwire::types::error_to_sqlstate(&refusal).1,
            nodedb_types::error::sqlstate::READ_ONLY_SQL_TRANSACTION
        );
    }

    #[test]
    fn an_unclassified_failure_is_a_persistence_error() {
        let error = crate::Error::Internal {
            detail: "raft loop stopped".into(),
        };
        assert!(matches!(
            propose_error(error, "events"),
            PublishError::Persistence(_)
        ));
    }

    /// A topic the registry does not hold refuses the publication before
    /// anything is proposed.
    #[tokio::test]
    async fn an_unregistered_topic_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_, _, state, _, _) = crate::event::test_utils::event_test_deps(&dir);
        let error = publish_to_topic(&state, DatabaseId::new(7), 11, "missing", "{}")
            .await
            .expect_err("an unregistered topic is refused");
        assert!(matches!(error, super::PublishError::TopicNotFound(_)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_commits_align_catalog_buffer_and_live_bus() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = &cluster.state;
        let database_id = DatabaseId::new(7);
        let tenant_id = 11;
        let topic_name = "events";
        let definition = TopicDef {
            database_id,
            tenant_id,
            name: topic_name.into(),
            retention: RetentionConfig::default(),
            owner: "test".into(),
            created_at: 0,
            last_sequence: 0,
            last_lsn: 0,
            last_epoch: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        };
        state
            .credentials
            .catalog()
            .put_ep_topic(&definition)
            .expect("persist topic");
        state.ep_topic_registry.register(definition);
        let mut live = state
            .ep_topic_registry
            .subscribe(database_id, tenant_id, topic_name)
            .expect("live receiver");

        let payloads: Vec<String> = (0..8)
            .map(|number| format!("{{\"number\":{number}}}"))
            .collect();
        let publishes = payloads
            .iter()
            .map(|payload| publish_to_topic(state, database_id, tenant_id, topic_name, payload));
        let sequences = futures::future::join_all(publishes)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("publish");
        assert_eq!(sequences.len(), 8);

        let catalog_messages = state
            .credentials
            .catalog()
            .load_ep_topic_messages(database_id, tenant_id, topic_name)
            .expect("catalog messages");
        let buffer_messages = state
            .cdc_router
            .get_buffer(database_id, tenant_id, "topic:events")
            .expect("topic buffer")
            .read_from(crate::event::cdc::CdcOffset::ZERO, 16);
        let mut live_messages = Vec::new();
        for _ in 0..8 {
            live_messages.push(live.recv().await.expect("live message"));
        }

        let expected: Vec<u64> = (1..=8).collect();
        assert_eq!(
            catalog_messages
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            buffer_messages
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            live_messages
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            live_messages
                .iter()
                .map(|message| &message.payload)
                .collect::<Vec<_>>(),
            catalog_messages
                .iter()
                .map(|message| &message.payload)
                .collect::<Vec<_>>()
        );
        cluster.shutdown().await;
    }
}
