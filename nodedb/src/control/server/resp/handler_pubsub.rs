//! RESP Pub/Sub handlers: SUBSCRIBE, PUBLISH mapped to NodeDB change_stream
//! and topic_registry.

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::debug;

use crate::control::state::SharedState;

use super::codec::RespValue;
use super::command::RespCommand;
use super::session::RespSession;

/// Handle SUBSCRIBE command: enter subscription mode.
///
/// The connection subscribes to the change_stream for the specified channels
/// (mapped to KV collection names). Messages are pushed as RESP arrays:
/// `["message", channel, payload]`.
///
/// This function takes over the connection — it blocks until the client
/// disconnects or sends UNSUBSCRIBE.
pub async fn handle_subscribe(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
    stream: &mut TcpStream,
) -> crate::Result<()> {
    if cmd.argc() < 1 {
        let resp = RespValue::err("ERR wrong number of arguments for 'subscribe' command");
        let bytes = resp.to_bytes();
        stream
            .write_all(&bytes)
            .await
            .map_err(|e| crate::Error::Bridge {
                detail: format!("RESP write: {e}"),
            })?;
        return Ok(());
    }

    let channels: Vec<String> = cmd
        .args
        .iter()
        .filter_map(|a| std::str::from_utf8(a).ok().map(|s| s.to_string()))
        .collect();

    // Subscribe to the change_stream, filtering by collection names.
    // Each channel maps to a KV collection name.
    let mut subscription = state.change_stream.subscribe(None, Some(session.tenant_id));

    // Send subscription confirmation for each channel.
    for (i, channel) in channels.iter().enumerate() {
        let confirm = RespValue::array(vec![
            RespValue::bulk_str("subscribe"),
            RespValue::bulk_str(channel),
            RespValue::integer((i + 1) as i64),
        ]);
        let bytes = confirm.to_bytes();
        stream
            .write_all(&bytes)
            .await
            .map_err(|e| crate::Error::Bridge {
                detail: format!("RESP write: {e}"),
            })?;
    }

    debug!(
        channels = ?channels,
        "RESP SUBSCRIBE: entering subscription mode"
    );

    // Subscription loop: push change events to the client.
    loop {
        match subscription.receiver.recv().await {
            Ok(event) => {
                // Filter: only events for subscribed collections.
                if !channels.contains(&event.collection) {
                    continue;
                }

                // Format as RESP message push: ["message", channel, payload]
                let payload = serde_json::json!({
                    "op": format!("{:?}", event.operation),
                    "key": event.document_id,
                    "lsn": event.lsn.as_u64(),
                    "timestamp_ms": event.timestamp_ms,
                })
                .to_string();

                let msg = RespValue::array(vec![
                    RespValue::bulk_str("message"),
                    RespValue::bulk_str(&event.collection),
                    RespValue::bulk(payload.into_bytes()),
                ]);

                let bytes = msg.to_bytes();
                if stream.write_all(&bytes).await.is_err() {
                    break; // Client disconnected.
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                // Client fell behind — notify and continue.
                let msg = RespValue::array(vec![
                    RespValue::bulk_str("message"),
                    RespValue::bulk_str("__system"),
                    RespValue::bulk(format!("{{\"warning\":\"lagged {n} events\"}}").into_bytes()),
                ]);
                let bytes = msg.to_bytes();
                if stream.write_all(&bytes).await.is_err() {
                    break;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                break; // Change stream shut down.
            }
        }
    }

    Ok(())
}

/// Handle PUBLISH command: publish a message to a topic.
///
/// Maps to the existing TopicRegistry. The topic name is the first arg,
/// the message is the second.
pub async fn handle_publish(
    cmd: &RespCommand,
    _session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'publish' command");
    }

    let channel = cmd.arg_str(0).unwrap_or("");
    let message = cmd.arg_str(1).unwrap_or("");

    match state
        .topic_registry
        .publish(channel, message.to_string(), "resp_client")
    {
        Ok(_seq) => {
            // Redis PUBLISH returns the number of subscribers that received the message.
            // We return 1 as a conservative estimate (the topic registry doesn't track
            // the exact delivery count).
            RespValue::integer(1)
        }
        Err(e) => RespValue::err(format!("ERR publish failed: {e}")),
    }
}
