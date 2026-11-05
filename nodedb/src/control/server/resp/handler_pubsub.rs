// SPDX-License-Identifier: BUSL-1.1

//! RESP Pub/Sub handlers: SUBSCRIBE, PSUBSCRIBE, and durable-topic PUBLISH.
//!
//! RESP `PUBLISH` authorizes writes to `topic:<channel>` and appends accepted
//! messages to the Event Plane durable-topic buffer. Its integer response is
//! an acceptance acknowledgement (`1`), not a live subscriber count.

use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::sync::watch;
use tracing::debug;

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::security::sessions::{KillReason, kill_signal_ended};
use crate::control::server::conn_stream::ConnStream;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::codec::RespValue;
use super::command::RespCommand;
use super::session::RespSession;

/// Maximum channels or patterns accepted by a single subscription command.
pub(crate) const MAX_PUBSUB_SUBSCRIPTIONS: usize = 128;
/// Maximum UTF-8 byte length of a channel or pattern name.
pub(crate) const MAX_PUBSUB_NAME_BYTES: usize = 256;
/// Maximum byte length of a PUBLISH payload.
pub(crate) const MAX_PUBSUB_PAYLOAD_BYTES: usize = 1024 * 1024;

/// The kill signal a push loop races. A subscribed connection reads nothing from
/// its client, so it is not idle, and the kill ends it between messages.
fn enter_push_mode(session: &RespSession) -> Option<watch::Receiver<KillReason>> {
    session.binding.as_ref().map(|binding| {
        binding.set_statement("subscribe");
        binding.kill_signal()
    })
}

/// Handle SUBSCRIBE command: enter subscription mode.
///
/// Returns `true` after taking over the connection for a valid subscription,
/// and `false` after writing a rejection that leaves the connection usable.
pub async fn handle_subscribe(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
    stream: &mut ConnStream,
) -> crate::Result<bool> {
    let channels = match subscription_names(cmd, "subscribe") {
        Ok(channels) => channels,
        Err(response) => return write_rejection(stream, response).await,
    };
    if session.identity.is_none() {
        return write_rejection(stream, RespValue::err("NOAUTH Authentication required")).await;
    }
    let topic_resources: Vec<String> = channels
        .iter()
        .map(|channel| format!("topic:{channel}"))
        .collect();
    let Some(identity) = authorize_channels(session, state, &topic_resources, Permission::Read)
    else {
        return write_rejection(
            stream,
            RespValue::err("NOPERM this user has no permissions to access this channel"),
        )
        .await;
    };
    let tenant_id = identity.tenant_id.as_u64();
    if let Some(channel) = channels.iter().find(|channel| {
        state
            .ep_topic_registry
            .get(DatabaseId::DEFAULT, tenant_id, channel)
            .is_none()
    }) {
        return write_rejection(
            stream,
            RespValue::err(format!("ERR no such topic '{channel}'")),
        )
        .await;
    }

    // Exact subscriptions use only their topic buses, so unrelated topic,
    // tenant, and database traffic cannot create lag or be observed here.
    // A topic dropped after the check above has no bus to subscribe to.
    let Some(mut subscriptions) = channels
        .iter()
        .map(|channel| {
            state
                .ep_topic_registry
                .subscribe(DatabaseId::DEFAULT, tenant_id, channel)
        })
        .collect::<Option<Vec<_>>>()
    else {
        return write_rejection(
            stream,
            RespValue::err("ERR a subscribed topic was dropped; retry the SUBSCRIBE"),
        )
        .await;
    };

    for (i, channel) in channels.iter().enumerate() {
        let confirm = RespValue::array(vec![
            RespValue::bulk_str("subscribe"),
            RespValue::bulk_str(channel),
            RespValue::integer((i + 1) as i64),
        ]);
        write_value(stream, confirm).await?;
    }

    debug!(channels = ?channels, "RESP SUBSCRIBE: entering subscription mode");

    let mut kill = enter_push_mode(session);
    loop {
        let receives: Vec<_> = subscriptions
            .iter_mut()
            .map(|subscription| Box::pin(subscription.recv()))
            .collect();
        let received = tokio::select! {
            biased;
            ended = kill_signal_ended(&mut kill) => {
                let _ = write_value(stream, RespValue::from_error(&ended)).await;
                break;
            }
            received = futures::future::select_all(receives) => received.0,
        };
        match received {
            Ok(message) => {
                let msg = RespValue::array(vec![
                    RespValue::bulk_str("message"),
                    RespValue::bulk_str(&message.topic),
                    RespValue::bulk(message.payload.clone().into_bytes()),
                ]);
                if write_value(stream, msg).await.is_err() {
                    break;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                let _ = write_value(
                    stream,
                    RespValue::err(format!(
                        "ERR subscription lagged {n} messages; connection closed"
                    )),
                )
                .await;
                break;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }

    Ok(true)
}

/// Handle PSUBSCRIBE command: pattern-based subscription.
///
/// Pattern matches cannot be authorized collection-by-collection before a
/// subscription is created, so a Read grant on the wildcard collection is
/// required.
pub async fn handle_psubscribe(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
    stream: &mut ConnStream,
) -> crate::Result<bool> {
    let patterns = match subscription_names(cmd, "psubscribe") {
        Ok(patterns) => patterns,
        Err(response) => return write_rejection(stream, response).await,
    };
    let wildcard = ["topic:*".to_string()];
    if session.identity.is_none() {
        return write_rejection(stream, RespValue::err("NOAUTH Authentication required")).await;
    }
    let Some(identity) = authorize_channels(session, state, &wildcard, Permission::Read) else {
        return write_rejection(
            stream,
            RespValue::err("NOPERM this user has no permissions to access this channel"),
        )
        .await;
    };

    // Pattern matching needs a shared bus, but that bus remains isolated to
    // this database and tenant.
    let tenant_id = identity.tenant_id.as_u64();
    let mut subscription = state
        .ep_topic_registry
        .subscribe_scope(DatabaseId::DEFAULT, tenant_id);

    for (i, pattern) in patterns.iter().enumerate() {
        let confirm = RespValue::array(vec![
            RespValue::bulk_str("psubscribe"),
            RespValue::bulk_str(pattern),
            RespValue::integer((i + 1) as i64),
        ]);
        write_value(stream, confirm).await?;
    }

    debug!(patterns = ?patterns, "RESP PSUBSCRIBE: entering pattern subscription mode");

    let mut kill = enter_push_mode(session);
    loop {
        let received = tokio::select! {
            biased;
            ended = kill_signal_ended(&mut kill) => {
                let _ = write_value(stream, RespValue::from_error(&ended)).await;
                break;
            }
            received = subscription.recv() => received,
        };
        match received {
            Ok(message) => {
                let Some(pattern) = matching_topic_pattern(&message, tenant_id, &patterns) else {
                    continue;
                };

                let msg = RespValue::array(vec![
                    RespValue::bulk_str("pmessage"),
                    RespValue::bulk_str(pattern),
                    RespValue::bulk_str(&message.topic),
                    RespValue::bulk(message.payload.clone().into_bytes()),
                ]);
                if write_value(stream, msg).await.is_err() {
                    break;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                let _ = write_value(
                    stream,
                    RespValue::err(format!(
                        "ERR subscription lagged {n} messages; connection closed"
                    )),
                )
                .await;
                break;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }

    Ok(true)
}

/// Handle PUBLISH by appending to an existing Event Plane durable topic.
///
/// RESP's integer result acknowledges an accepted publish (`1`). Durable topic
/// buffers do not expose a live RESP subscriber count, so this deliberately
/// does not claim Redis's receiver-count semantics.
pub async fn handle_publish(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() != 2 {
        return RespValue::err("ERR wrong number of arguments for 'publish' command");
    }

    let (Some(channel_bytes), Some(message_bytes)) = (cmd.arg(0), cmd.arg(1)) else {
        return RespValue::err("ERR wrong number of arguments for 'publish' command");
    };
    let channel = match channel_name(channel_bytes, "publish") {
        Ok(channel) => channel,
        Err(response) => return response,
    };
    let message = match publish_payload(message_bytes) {
        Ok(message) => message,
        Err(response) => return response,
    };
    if session.identity.is_none() {
        return RespValue::err("NOAUTH Authentication required");
    }
    let topic_resource = format!("topic:{channel}");
    let Some(identity) = authorize_channels(
        session,
        state,
        std::slice::from_ref(&topic_resource),
        Permission::Write,
    ) else {
        return RespValue::err("NOPERM this user has no permissions to access this channel");
    };

    use crate::event::topic::publish::PublishError;

    match crate::event::topic::publish::publish_to_topic(
        state,
        DatabaseId::DEFAULT,
        identity.tenant_id.as_u64(),
        &channel,
        message,
    )
    .await
    {
        // Durable topics do not track live RESP subscribers. `1` means the
        // Event Plane accepted the message into the topic's durable buffer.
        Ok(_) => RespValue::integer(1),
        Err(PublishError::TopicNotFound(topic)) => {
            RespValue::err(format!("ERR no such topic '{topic}'"))
        }
        Err(PublishError::Refused(error)) => {
            RespValue::err(format!("ERR publish refused: {error}"))
        }
        Err(PublishError::Persistence(error)) => {
            RespValue::err(format!("ERR publish failed: {error}"))
        }
    }
}

fn subscription_names(cmd: &RespCommand, command: &str) -> Result<Vec<String>, RespValue> {
    if cmd.argc() == 0 {
        return Err(RespValue::err(format!(
            "ERR wrong number of arguments for '{command}' command"
        )));
    }
    if cmd.argc() > MAX_PUBSUB_SUBSCRIPTIONS {
        return Err(RespValue::err(format!(
            "ERR too many channels or patterns; maximum is {MAX_PUBSUB_SUBSCRIPTIONS}"
        )));
    }

    cmd.args
        .iter()
        .map(|name| channel_name(name, command))
        .collect()
}

fn channel_name(name: &[u8], command: &str) -> Result<String, RespValue> {
    if name.is_empty() || name.len() > MAX_PUBSUB_NAME_BYTES {
        return Err(RespValue::err(format!(
            "ERR {command} channel name exceeds maximum size"
        )));
    }
    std::str::from_utf8(name).map(str::to_owned).map_err(|_| {
        RespValue::err(format!(
            "ERR protocol error: {command} channel must be valid UTF-8"
        ))
    })
}

fn matching_topic_pattern<'a>(
    message: &crate::event::topic::TopicMessage,
    tenant_id: u64,
    patterns: &'a [String],
) -> Option<&'a String> {
    (message.database_id == DatabaseId::DEFAULT && message.tenant_id == tenant_id).then_some(())?;
    patterns.iter().find(|pattern| {
        crate::engine::kv::scan::glob_match(pattern.as_bytes(), message.topic.as_bytes())
    })
}

fn publish_payload(bytes: &[u8]) -> Result<&str, RespValue> {
    if bytes.len() > MAX_PUBSUB_PAYLOAD_BYTES {
        return Err(RespValue::err("ERR publish payload exceeds maximum size"));
    }
    std::str::from_utf8(bytes)
        .map_err(|_| RespValue::err("ERR protocol error: publish payload must be valid UTF-8"))
}

fn authorize_channels<'a>(
    session: &'a RespSession,
    state: &SharedState,
    channels: &[String],
    permission: Permission,
) -> Option<&'a AuthenticatedIdentity> {
    let identity = session.identity.as_ref()?;
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    session_permitted(
        Some(identity),
        channels,
        permission,
        &state.permissions,
        &state.roles,
        &emitter,
    )
    .then_some(identity)
}

fn session_permitted(
    identity: Option<&AuthenticatedIdentity>,
    channels: &[String],
    permission: Permission,
    permissions: &crate::control::security::permission::PermissionStore,
    roles: &crate::control::security::role::RoleStore,
    emitter: &dyn crate::control::security::audit::AuditEmitter,
) -> bool {
    let Some(identity) = identity else {
        return false;
    };
    channels.iter().all(|channel| {
        permissions.check(
            identity,
            permission,
            DatabaseId::DEFAULT,
            channel,
            roles,
            emitter,
        )
    })
}

async fn write_rejection(stream: &mut ConnStream, response: RespValue) -> crate::Result<bool> {
    write_value(stream, response).await?;
    Ok(false)
}

async fn write_value(stream: &mut ConnStream, response: RespValue) -> crate::Result<()> {
    let bytes = response.to_bytes();
    stream
        .write_all(&bytes)
        .await
        .map_err(|e| crate::Error::Bridge {
            detail: format!("RESP write: {e}"),
        })
}

#[cfg(test)]
mod tests {
    use crate::control::security::identity::{AuthMethod, DatabaseSet};
    use crate::control::security::permission::{PermissionStore, collection_target};
    use crate::control::security::role::RoleStore;
    use crate::event::cdc::stream_def::RetentionConfig;
    use crate::event::topic::TopicDef;
    use crate::types::TenantId;

    use super::*;

    fn identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            7,
            "alice",
            TenantId::new(1),
            AuthMethod::CleartextPassword,
            Vec::new(),
            None,
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        )
    }

    #[test]
    fn subscription_names_enforce_count_and_name_bounds() {
        let exactly_max = RespCommand {
            name: "SUBSCRIBE".into(),
            args: vec![b"a".to_vec(); MAX_PUBSUB_SUBSCRIPTIONS],
        };
        assert!(subscription_names(&exactly_max, "subscribe").is_ok());

        let too_many = RespCommand {
            name: "SUBSCRIBE".into(),
            args: vec![b"a".to_vec(); MAX_PUBSUB_SUBSCRIPTIONS + 1],
        };
        assert!(subscription_names(&too_many, "subscribe").is_err());
        assert!(channel_name(&vec![b'a'; MAX_PUBSUB_NAME_BYTES], "subscribe").is_ok());
        assert!(channel_name(&vec![b'a'; MAX_PUBSUB_NAME_BYTES + 1], "subscribe").is_err());
    }

    #[test]
    fn publish_bounds_accept_exact_limit_and_reject_over_limit() {
        assert_eq!(MAX_PUBSUB_PAYLOAD_BYTES, 1024 * 1024);
        assert!(publish_payload(&vec![b'x'; MAX_PUBSUB_PAYLOAD_BYTES]).is_ok());
        assert!(publish_payload(&vec![b'x'; MAX_PUBSUB_PAYLOAD_BYTES + 1]).is_err());
    }

    #[test]
    fn topic_filters_are_tenant_database_scoped_and_preserve_exact_payload() {
        let message = crate::event::topic::TopicMessage {
            database_id: DatabaseId::DEFAULT,
            tenant_id: 1,
            topic: "orders.created".into(),
            sequence: 7,
            event_time: 1,
            lsn: 7,
            epoch: 0,
            payload: "{\"id\":7,\"unchanged\":true}".into(),
        };
        let channels = ["orders.created".to_string()];
        let patterns = ["orders.*".to_string()];
        assert!(channels.contains(&message.topic));
        assert_eq!(
            matching_topic_pattern(&message, 1, &patterns).map(String::as_str),
            Some("orders.*")
        );
        assert_eq!(message.payload.as_bytes(), b"{\"id\":7,\"unchanged\":true}");

        let other_tenant = crate::event::topic::TopicMessage {
            tenant_id: 2,
            ..message.clone()
        };
        let other_database = crate::event::topic::TopicMessage {
            database_id: DatabaseId::new(9),
            ..message
        };
        assert!(matching_topic_pattern(&other_tenant, 1, &patterns).is_none());
        assert!(matching_topic_pattern(&other_database, 1, &patterns).is_none());
    }

    #[test]
    fn unauthenticated_or_ungranted_subscribe_does_not_create_a_topic_receiver() {
        let registry = crate::event::topic::EpTopicRegistry::new();
        let permissions = PermissionStore::new();
        let roles = RoleStore::new();
        let before = registry.receiver_count();
        let channels = ["topic:orders".to_string()];
        assert!(!session_permitted(
            None,
            &channels,
            Permission::Read,
            &permissions,
            &roles,
            &crate::control::security::audit::NoopAuditEmitter,
        ));
        let identity = identity();
        assert!(!session_permitted(
            Some(&identity),
            &channels,
            Permission::Read,
            &permissions,
            &roles,
            &crate::control::security::audit::NoopAuditEmitter,
        ));
        assert_eq!(registry.receiver_count(), before);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publish_authorizes_then_appends_to_durable_topic_buffer() {
        // A publication proposes through the topic's data group.
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = &cluster.state;
        let identity = identity();
        let retention = RetentionConfig {
            max_events: 10,
            max_age_secs: 60,
        };
        let topic = TopicDef {
            database_id: DatabaseId::DEFAULT,
            tenant_id: identity.tenant_id.as_u64(),
            name: "orders".into(),
            retention: retention.clone(),
            owner: identity.username.clone(),
            created_at: 0,
            last_sequence: 0,
            last_lsn: 0,
            last_epoch: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        };
        state
            .credentials
            .catalog()
            .put_ep_topic(&topic)
            .expect("persist topic");
        state.ep_topic_registry.register(topic);
        let buffer = state.cdc_router.ensure_buffer(
            DatabaseId::DEFAULT,
            identity.tenant_id.as_u64(),
            "topic:orders",
            &retention,
        );
        let session = RespSession {
            identity: Some(identity.clone()),
            ..RespSession::default()
        };
        let command = RespCommand {
            name: "PUBLISH".into(),
            args: vec![b"orders".to_vec(), b"accepted".to_vec()],
        };

        assert_eq!(
            handle_publish(&command, &session, state).await,
            RespValue::err("NOPERM this user has no permissions to access this channel")
        );
        assert_eq!(buffer.total_pushed(), 0);

        state
            .permissions
            .grant(
                &collection_target(DatabaseId::DEFAULT, identity.tenant_id, "topic:orders"),
                "user:alice",
                Permission::Write,
                "test",
                None,
            )
            .expect("in-memory grant");
        assert_eq!(
            handle_publish(&command, &session, state).await,
            RespValue::integer(1)
        );
        assert_eq!(buffer.total_pushed(), 1);

        state
            .permissions
            .grant(
                &collection_target(DatabaseId::DEFAULT, identity.tenant_id, "topic:missing"),
                "user:alice",
                Permission::Write,
                "test",
                None,
            )
            .expect("in-memory grant");
        let missing_topic = RespCommand {
            name: "PUBLISH".into(),
            args: vec![b"missing".to_vec(), b"accepted".to_vec()],
        };
        assert_eq!(
            handle_publish(&missing_topic, &session, state).await,
            RespValue::err("ERR no such topic 'missing'")
        );
        cluster.shutdown().await;
    }
}
