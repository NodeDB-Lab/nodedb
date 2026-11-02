// SPDX-License-Identifier: BUSL-1.1

//! Canonical consumer-group stream identities.

use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// Resolve a stream name to its durable consumer-group identity.
///
/// A name identifies a topic only when a topic definition exists for its bare
/// name. Such groups always use the topic buffer key (`topic:<name>`); ordinary
/// change-stream names are left unchanged.
///
/// `stream_name` arrives in its stored form. A caller holding a raw SQL token
/// decodes it first through
/// `crate::control::server::shared::ddl::sql_parse::parse_stream_ident_token`.
pub fn canonical_stream_name(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    stream_name: &str,
) -> String {
    let bare = stream_name.strip_prefix("topic:").unwrap_or(stream_name);
    if state
        .ep_topic_registry
        .get(database_id, tenant_id, bare)
        .is_some()
    {
        format!("topic:{bare}")
    } else {
        stream_name.to_string()
    }
}
