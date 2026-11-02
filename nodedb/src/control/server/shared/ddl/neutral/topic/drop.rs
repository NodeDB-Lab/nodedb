// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP TOPIC` DDL handler.
//!
//! The topic mutation lock serializes durable deletion with publication. Offset
//! cleanup commits first in its separate database. Then one replicated entry
//! removes the definition, messages, and the topic's consumer groups on every
//! node.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::event::topic::validate_topic_name;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

pub async fn drop_topic(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop topics")?;

    // parts: ["DROP", "TOPIC", "<name>"]
    if parts.len() < 3 {
        return Err(DdlError::new("42601", "expected DROP TOPIC <name>"));
    }

    let name = parse_ident_token(parts[2])?;
    validate_topic_name(&name).map_err(|message| DdlError::new("42601", message.to_string()))?;
    let tenant_id = identity.tenant_id.as_u64();
    let lifecycle_lock = state
        .ep_topic_registry
        .lifecycle_lock(database_id, tenant_id, &name);
    let _guard = lifecycle_lock.lock().await;

    if state
        .ep_topic_registry
        .get(database_id, tenant_id, &name)
        .is_none()
    {
        return Err(DdlError::new(
            "42704",
            format!("topic '{name}' does not exist"),
        ));
    }

    let catalog = state.credentials.catalog();
    let buffer_key = format!("topic:{name}");

    // Enumerate every durable and runtime group before changing either store.
    // A catalog-read failure is fatal: reporting success without identifying a
    // group will let it attach to a recreated topic.
    let mut group_names = std::collections::BTreeSet::new();
    for group in state
        .group_registry
        .list_for_stream(database_id, tenant_id, &buffer_key)
    {
        group_names.insert(group.name);
    }
    for group_name in catalog
        .topic_consumer_group_names(database_id, tenant_id, &name)
        .map_err(|error| {
            DdlError::from_error_in_context("catalog enumerate topic consumer groups", &error)
        })?
    {
        group_names.insert(group_name);
    }

    // Topic lifecycle is held above. Acquire every group lock in group-name
    // order before durable cleanup or runtime mutation.
    let mut group_guards = Vec::with_capacity(group_names.len());
    for group_name in &group_names {
        let lock =
            state
                .group_registry
                .lifecycle_lock(database_id, tenant_id, &buffer_key, group_name);
        group_guards.push(lock.lock_owned().await);
    }

    // The offsets live in a separate redb database. Commit their complete
    // cleanup first. Any error leaves the catalog topic and groups intact and
    // returns, so DROP TOPIC never claims success with cursors that can revive
    // on a recreate.
    let offset_groups: Vec<(String, String)> = group_names
        .iter()
        .map(|group| (buffer_key.clone(), group.clone()))
        .collect();
    state
        .offset_store
        .delete_groups(database_id, tenant_id, &offset_groups)
        .map_err(|error| DdlError::from_error_in_context("durable topic offset cleanup", &error))?;

    // Definition, retained messages, and the topic's consumer groups share
    // one catalog transaction, and the registry teardown rides the same entry
    // to every node. There is no best-effort path after this point.
    super::replicate::propose_delete(state, database_id, tenant_id, &name).await?;
    drop(group_guards);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP TOPIC {name}"),
    );

    Ok(status("DROP TOPIC"))
}
