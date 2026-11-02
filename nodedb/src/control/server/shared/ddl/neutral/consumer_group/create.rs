// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE CONSUMER GROUP` DDL handler.
//!
//! The definition is replicated: the row and the `GroupRegistry` install both
//! land on every node, so a group created here resolves cluster wide. The
//! registry is the duplicate authority, and it is rebuilt from the catalog at
//! boot.
//!
//! Syntax: `CREATE CONSUMER GROUP <name> ON <stream>`

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::cdc::consumer_group::ConsumerGroupDef;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};
use super::identity::canonical_stream_name;
use crate::control::server::shared::ddl::sql_parse::{parse_ident_token, parse_stream_ident_token};

/// Handle `CREATE CONSUMER GROUP <name> ON <stream>`.
///
/// `group_name` and `stream_name` come from the typed
/// [`nodedb_sql::ddl_ast::statement::StreamViewStmt::CreateConsumerGroup`] variant.
pub async fn create_consumer_group(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    group_name: &str,
    stream_name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "create consumer groups")?;

    let group_name = parse_ident_token(group_name)?;
    let tenant_id = identity.tenant_id.as_u64();
    let requested_stream_name = parse_stream_ident_token(stream_name)?;

    // Consumer groups can be created on change streams or durable topics.
    let mut stream_name =
        canonical_stream_name(state, database_id, tenant_id, &requested_stream_name);
    let topic_lock = stream_name.strip_prefix("topic:").map(|topic| {
        state
            .ep_topic_registry
            .lifecycle_lock(database_id, tenant_id, topic)
    });
    let _topic_guard = match topic_lock {
        Some(lock) => Some(lock.lock_owned().await),
        None => None,
    };
    // Re-resolve after taking the topic lock so a completed DROP cannot leave
    // this CREATE targeting its removed topic incarnation.
    stream_name = canonical_stream_name(state, database_id, tenant_id, &requested_stream_name);
    let is_stream = state
        .stream_registry
        .get(database_id, tenant_id, &requested_stream_name)
        .is_some();
    let is_topic = stream_name.starts_with("topic:");
    if !is_stream && !is_topic {
        return Err(DdlError::new(
            "42704",
            format!("change stream or topic '{stream_name}' does not exist"),
        ));
    }

    let lifecycle_lock =
        state
            .group_registry
            .lifecycle_lock(database_id, tenant_id, &stream_name, &group_name);
    let _group_guard = lifecycle_lock.lock().await;

    if state
        .group_registry
        .get(database_id, tenant_id, &stream_name, &group_name)
        .is_some()
    {
        return Err(DdlError::new(
            "42710",
            format!("consumer group '{group_name}' already exists on stream '{stream_name}'"),
        ));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| DdlError::internal("system clock error"))?
        .as_secs();

    let def = ConsumerGroupDef {
        database_id,
        tenant_id,
        name: group_name.clone(),
        stream_name: stream_name.clone(),
        owner: identity.username.clone(),
        created_at: now,
        modification_hlc: nodedb_types::Hlc::ZERO,
    };

    super::replicate::propose_create(state, &def).await?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE CONSUMER GROUP {group_name} ON {stream_name}"),
    );

    Ok(status("CREATE CONSUMER GROUP"))
}
