// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for durable-topic catalog entries.
//!
//! `EpTopicRegistry` is an in-memory map rebuilt from redb only at boot.
//! Installing the applied definition here makes a topic publishable and
//! subscribable on every node the moment the entry applies.

use crate::control::state::SharedState;
use crate::event::topic::TopicDef;
use crate::types::DatabaseId;

/// Install an applied topic. An existing registration is kept, so the
/// create-only entry never rewinds a live topic's high-water marks.
pub fn create_if_absent(def: &TopicDef, shared: &SharedState) {
    if shared
        .ep_topic_registry
        .get(def.database_id, def.tenant_id, &def.name)
        .is_some()
    {
        return;
    }
    shared.ep_topic_registry.register(def.clone());
}

/// Tear down an applied topic: its registration, its CDC buffer, and every
/// consumer group attached to its `topic:<name>` stream.
pub fn delete_with_consumer_groups(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    shared: &SharedState,
) {
    let database_id = DatabaseId::new(database_id);
    let buffer_key = format!("topic:{name}");
    shared
        .ep_topic_registry
        .unregister(database_id, tenant_id, name);
    shared
        .cdc_router
        .remove_buffer(database_id, tenant_id, &buffer_key);
    // Every group's cursor is cleared, or a recreated topic inherits it.
    for def in shared
        .group_registry
        .list_for_stream(database_id, tenant_id, &buffer_key)
    {
        shared
            .group_registry
            .unregister(database_id, tenant_id, &buffer_key, &def.name);
        super::consumer_group::forget_offsets(
            database_id,
            tenant_id,
            &buffer_key,
            &def.name,
            shared,
        );
    }
}
