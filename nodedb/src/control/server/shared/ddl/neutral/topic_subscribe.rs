// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SUBSCRIBE TO` durable-topic metadata handler.
//!
//! ```sql
//! SUBSCRIBE TO <topic> [GROUP <group>] [SINCE <seq>]
//! ```
//!
//! Topic definitions, consumer groups, and retained messages all belong to
//! the Event Plane. This statement validates those durable identities and
//! returns backlog metadata; delivery is consumed through the canonical topic
//! CDC buffer and never creates an ephemeral subscription receiver.

use std::sync::atomic::Ordering;

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use serde_json::{Map, Value as JsonValue};

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// SUBSCRIBE TO <topic> [GROUP <group>] [SINCE <seq>]
pub fn subscribe_to(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let topic_name = parse_ident_token(parts.get(2).copied().unwrap_or(""))?;
    let since_seq: u64 = find_ascii_case_insensitive(sql, " SINCE ")
        .and_then(|pos| sql[pos + 7..].split_whitespace().next())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Check for GROUP clause: SUBSCRIBE TO topic GROUP group_name [SINCE seq]
    let group_name = find_ascii_case_insensitive(sql, " GROUP ")
        .map(|pos| sql[pos + 7..].split_whitespace().next().unwrap_or(""))
        .filter(|g| !g.is_empty())
        .map(parse_ident_token)
        .transpose()?;

    let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    authorize_collection(
        identity,
        database_id,
        &format!("topic:{topic_name}"),
        Permission::Read,
        &state.permissions,
        &state.roles,
        &emitter,
    )
    .map_err(crate::Error::from)
    .map_err(|error| err("42501", error.to_string()))?;

    let tenant_id = identity.tenant_id.as_u64();
    let _definition = state
        .ep_topic_registry
        .get(database_id, tenant_id, &topic_name)
        .ok_or_else(|| err("42P01", format!("topic '{topic_name}' does not exist")))?;
    let stream_name = format!("topic:{topic_name}");

    if let Some(group) = &group_name
        && state
            .group_registry
            .get(database_id, tenant_id, &stream_name, group)
            .is_none()
    {
        return Err(err(
            "42P01",
            format!("consumer group '{group}' does not exist on topic '{topic_name}'"),
        ));
    }

    // The catalog is the durable authority for SINCE. The buffer is retained
    // for delivery and may be rebuilt on startup, so it must not be treated as
    // an independent source or cause a subscription side effect here.
    let backlog = if since_seq == 0 {
        Vec::new()
    } else {
        state
            .credentials
            .catalog()
            .load_ep_topic_messages(database_id, tenant_id, &topic_name)
            .map_err(|error| err("XX000", format!("topic backlog: {error}")))?
            .into_iter()
            .filter(|message| message.sequence >= since_seq)
            .collect()
    };
    // Resolve the canonical buffer before reporting success. A topic with no
    // publications legitimately has no buffer yet; its persisted backlog above
    // remains authoritative.
    let _buffer = state
        .cdc_router
        .get_buffer(database_id, tenant_id, &stream_name);
    let sub_id = state.request_id_counter.fetch_add(1, Ordering::Relaxed);

    let columns = vec![
        "subscription_id".to_string(),
        "topic".to_string(),
        "group".to_string(),
        "backlog".to_string(),
    ];
    let mut row = Map::new();
    row.insert(
        "subscription_id".to_string(),
        JsonValue::String(sub_id.to_string()),
    );
    row.insert("topic".to_string(), JsonValue::String(topic_name));
    row.insert(
        "group".to_string(),
        JsonValue::String(group_name.as_deref().unwrap_or("-").to_string()),
    );
    row.insert(
        "backlog".to_string(),
        JsonValue::String(backlog.len().to_string()),
    );

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        columns,
        vec![row],
    ))])
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::identity::{AuthMethod, DatabaseSet};
    use crate::control::security::permission::collection_target;
    use crate::event::cdc::stream_def::RetentionConfig;
    use crate::event::topic::TopicDef;
    use crate::types::TenantId;
    use crate::wal::WalManager;

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

    #[tokio::test]
    async fn since_uses_durable_topic_backlog_without_creating_a_receiver() {
        let directory = tempfile::tempdir().expect("temporary test directory");
        let wal_directory = directory.path().join("wal");
        std::fs::create_dir_all(&wal_directory).expect("create WAL directory");
        let wal = Arc::new(WalManager::open_for_testing(&wal_directory).expect("test WAL"));
        let (dispatcher, _) = Dispatcher::new(1, 16);
        let state = SharedState::new(dispatcher, wal).expect("test shared state");
        let identity = identity();
        let topic = TopicDef {
            database_id: DatabaseId::DEFAULT,
            tenant_id: identity.tenant_id.as_u64(),
            name: "orders".into(),
            retention: RetentionConfig::default(),
            owner: identity.username.clone(),
            created_at: 0,
            last_sequence: 0,
            last_lsn: 0,
        };
        state
            .credentials
            .catalog()
            .put_ep_topic(&topic)
            .expect("persist topic");
        state.ep_topic_registry.register(topic);
        state
            .credentials
            .catalog()
            .append_ep_topic_message(
                DatabaseId::DEFAULT,
                identity.tenant_id.as_u64(),
                "orders",
                "exact durable payload",
                u64::MAX,
                1,
            )
            .expect("persist message");
        state
            .permissions
            .grant(
                &collection_target(identity.tenant_id, "topic:orders"),
                "user:alice",
                Permission::Read,
                "test",
                None,
            )
            .expect("grant read");

        let result = subscribe_to(
            &state,
            &identity,
            DatabaseId::DEFAULT,
            "SUBSCRIBE TO orders SINCE 1",
            &["SUBSCRIBE", "TO", "orders", "SINCE", "1"],
        )
        .expect("subscribe metadata");
        let DdlResult::Rows(rows) = &result[0] else {
            panic!("expected rows");
        };
        assert_eq!(
            rows.rows[0]["backlog"],
            nodedb_types::Value::String("1".into())
        );
        assert_eq!(state.ep_topic_registry.receiver_count(), 0);
    }
}
