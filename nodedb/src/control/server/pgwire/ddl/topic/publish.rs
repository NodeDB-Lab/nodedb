//! `PUBLISH TO` DDL handler.
//!
//! Syntax: `PUBLISH TO <topic> '<payload>'`

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::topic::publish::publish_to_topic;

use super::super::super::types::sqlstate_error;

/// Handle `PUBLISH TO <topic> '<payload>'`
///
/// Cluster-aware: if the topic's home node is remote, forwards via QUIC.
pub async fn handle_publish(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    // Parse: PUBLISH TO <topic> '<payload>'
    let prefix = "PUBLISH TO ";
    if !upper.starts_with(prefix) {
        return Err(sqlstate_error(
            "42601",
            "expected PUBLISH TO <topic> '<payload>'",
        ));
    }

    let rest = trimmed[prefix.len()..].trim();

    // Extract topic name (first word).
    let (topic_name, payload_part) = rest
        .split_once(char::is_whitespace)
        .ok_or_else(|| sqlstate_error("42601", "expected payload after topic name"))?;
    let topic_name = topic_name.to_lowercase();

    // Extract payload (between single quotes or raw).
    let payload = payload_part.trim();
    let payload = if payload.starts_with('\'') && payload.ends_with('\'') && payload.len() >= 2 {
        &payload[1..payload.len() - 1]
    } else {
        payload
    };

    let tenant_id = identity.tenant_id.as_u32();

    use crate::event::topic::publish::PublishError;

    match publish_to_topic(state, tenant_id, &topic_name, payload) {
        Ok(seq) => {
            tracing::trace!(topic = %topic_name, seq, "message published");
            Ok(vec![Response::Execution(Tag::new("PUBLISH"))])
        }
        Err(PublishError::RemoteHome { leader_node, .. }) => {
            // Forward to remote home node.
            match crate::event::topic::publish::publish_remote(
                state,
                tenant_id,
                &topic_name,
                payload,
                leader_node,
            )
            .await
            {
                Ok(_) => Ok(vec![Response::Execution(Tag::new("PUBLISH"))]),
                Err(e) => Err(sqlstate_error("58000", &e.to_string())),
            }
        }
        Err(e) => Err(sqlstate_error("42704", &e.to_string())),
    }
}
