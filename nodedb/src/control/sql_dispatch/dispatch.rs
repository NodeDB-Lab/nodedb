// SPDX-License-Identifier: BUSL-1.1

//! Unified SQL dispatcher: handles NodeDB SQL extensions that cannot be
//! handled by `plan_sql` (sqlparser-based), without requiring pgwire context.
//!
//! Used by:
//! - The procedural statement executor (trigger bodies, procedures)
//! - The pgwire streaming router (as a thin adapter)
//!
//! For INSERT/UPDATE/DELETE and other plan_sql-bound statements, returns `None`
//! so the caller can use `QueryContext::plan_sql` directly (the procedural
//! executor applies transaction buffering on that path).

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::DispatchOutcome;

/// Attempt to dispatch `sql` as a NodeDB SQL extension.
///
/// Returns `Some(result)` if the statement was handled (e.g. `PUBLISH TO`),
/// or `None` if the caller handles the SQL via `plan_sql`.
///
/// The caller is responsible for handling `None`.
pub async fn dispatch_sql(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Option<crate::Result<DispatchOutcome>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !nodedb_types::starts_with_ascii_case_insensitive(trimmed, "PUBLISH TO ") {
        return None;
    }
    let database_id = match identity.default_database {
        Some(database_id) => database_id,
        None => {
            return Some(Err(crate::Error::BadRequest {
                detail: "PUBLISH requires an active database".into(),
            }));
        }
    };
    Some(handle_publish(state, identity, database_id, trimmed).await)
}

/// Dispatch a SQL extension in the caller-selected database.
///
/// Protocol handlers that carry session database state must call this variant
/// so a `USE DATABASE` selection is not replaced by the identity default.
pub async fn dispatch_sql_in_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    sql: &str,
) -> crate::Result<Option<DispatchOutcome>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if nodedb_types::starts_with_ascii_case_insensitive(trimmed, "PUBLISH TO ") {
        return handle_publish(state, identity, database_id, trimmed)
            .await
            .map(Some);
    }
    Ok(None)
}

/// A `PUBLISH TO` whose syntax, authorization and topic were checked at the
/// statement. A procedural transaction holds it and sends it after COMMIT.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct PreparedPublish {
    pub database_id: u64,
    pub tenant_id: u64,
    pub topic: String,
    pub payload: String,
    /// This node's metadata floor once it found the topic. See
    /// [`crate::wal::RedoPublish::metadata_floor`].
    pub metadata_floor: u64,
}

/// Whether `sql` is a NodeDB SQL extension this module dispatches.
pub fn is_sql_extension(sql: &str) -> bool {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    nodedb_types::starts_with_ascii_case_insensitive(trimmed, "PUBLISH TO ")
}

/// Handle `PUBLISH TO <topic> <payload>` without pgwire coupling.
async fn handle_publish(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    sql: &str,
) -> crate::Result<DispatchOutcome> {
    let publish = prepare_publish(state, identity, database_id, sql)?;
    send_publish(state, &publish).await?;
    Ok(DispatchOutcome {
        rows_affected: 1,
        rows: Vec::new(),
    })
}

/// Check a `PUBLISH TO <topic> <payload>` statement without sending it:
/// syntax, write permission on the topic, and that the topic exists.
pub fn prepare_publish(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    sql: &str,
) -> crate::Result<PreparedPublish> {
    let sql = sql.trim().trim_end_matches(';').trim();
    let prefix = "PUBLISH TO ";
    if !nodedb_types::starts_with_ascii_case_insensitive(sql, prefix) {
        return Err(crate::Error::BadRequest {
            detail: "expected PUBLISH TO <topic> <payload>".into(),
        });
    }

    let rest = sql[prefix.len()..].trim();

    let (topic_name, payload_part) =
        rest.split_once(char::is_whitespace)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: "expected payload after topic name in PUBLISH TO".into(),
            })?;
    let topic_name = nodedb_sql::reserved::check_identifier(topic_name).map_err(|error| {
        crate::Error::BadRequest {
            detail: error.to_string(),
        }
    })?;

    let payload = parse_payload(payload_part.trim())?;

    let tenant_id = identity.tenant_id.as_u64();
    let emitter =
        crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    crate::control::server::shared::authorization::authorize_collection(
        identity,
        database_id,
        &format!("topic:{topic_name}"),
        crate::control::security::identity::Permission::Write,
        &state.permissions,
        &state.roles,
        &emitter,
    )
    .map_err(crate::Error::from)?;

    if state
        .ep_topic_registry
        .get(database_id, tenant_id, &topic_name)
        .is_none()
    {
        return Err(crate::Error::CollectionNotFound {
            tenant_id: identity.tenant_id,
            collection: topic_name.to_string(),
        });
    }

    Ok(PreparedPublish {
        database_id: database_id.as_u64(),
        tenant_id,
        topic: topic_name.to_string(),
        payload,
        // Read after the topic lookup: covers the metadata batch that made
        // the topic visible, even while that batch is still applying.
        metadata_floor: state
            .applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID)
            .floor(),
    })
}

/// Send a checked publish to its topic. In a cluster it commits through the
/// topic's home data group.
pub async fn send_publish(state: &SharedState, publish: &PreparedPublish) -> crate::Result<()> {
    use crate::event::topic::publish::PublishError;

    let database_id = crate::types::DatabaseId::new(publish.database_id);
    let topic_name = publish.topic.as_str();
    match crate::event::topic::publish::publish_to_topic(
        state,
        database_id,
        publish.tenant_id,
        topic_name,
        &publish.payload,
    )
    .await
    {
        Ok(_seq) => Ok(()),
        Err(PublishError::TopicNotFound(t)) => Err(crate::Error::CollectionNotFound {
            tenant_id: crate::types::TenantId::new(publish.tenant_id),
            collection: t,
        }),
        Err(PublishError::Refused(refusal)) => Err(*refusal),
        Err(PublishError::Persistence(e)) => Err(crate::Error::Dispatch {
            detail: format!("publish to '{topic_name}' failed: {e}"),
        }),
    }
}

/// Decode a SQL-literal payload or pass through a bare payload.
///
/// If the input is single-quoted, strip the outer quotes and unescape doubled
/// quotes (`''` → `'`) per SQL string-literal rules. Reject unterminated
/// quotes rather than silently treating them as bare payloads.
fn parse_payload(input: &str) -> crate::Result<String> {
    if input.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "PUBLISH TO payload is empty".into(),
        });
    }
    if !input.starts_with('\'') {
        return Ok(input.to_string());
    }
    let mut chars = input[1..].chars().peekable();
    let mut out = String::with_capacity(input.len());
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            if chars.peek() == Some(&'\'') {
                chars.next();
                out.push('\'');
                continue;
            }
            if chars.next().is_some() {
                return Err(crate::Error::BadRequest {
                    detail: "PUBLISH TO payload has trailing tokens after closing quote".into(),
                });
            }
            return Ok(out);
        }
        out.push(ch);
    }
    Err(crate::Error::BadRequest {
        detail: "PUBLISH TO payload has unterminated string literal".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::parse_payload;

    #[test]
    fn plain_quoted() {
        assert_eq!(parse_payload("'hello'").unwrap(), "hello");
    }

    #[test]
    fn escaped_quote_unescaped() {
        assert_eq!(parse_payload("'it''s'").unwrap(), "it's");
        assert_eq!(parse_payload("''''").unwrap(), "'");
    }

    #[test]
    fn quoted_utf8_and_doubled_quotes_are_exact() {
        assert_eq!(parse_payload("'café 東京'").unwrap(), "café 東京");
        assert_eq!(parse_payload("'東京''駅'").unwrap(), "東京'駅");
    }

    #[test]
    fn empty_quoted_is_empty_string() {
        assert_eq!(parse_payload("''").unwrap(), "");
    }

    #[test]
    fn bare_payload_passes_through() {
        assert_eq!(parse_payload("{\"k\":1}").unwrap(), "{\"k\":1}");
    }

    #[test]
    fn unterminated_rejected() {
        assert!(parse_payload("'oops").is_err());
    }

    #[test]
    fn empty_input_rejected() {
        assert!(parse_payload("").is_err());
    }

    #[test]
    fn trailing_tokens_after_close_rejected() {
        assert!(parse_payload("'a' junk").is_err());
    }
}
