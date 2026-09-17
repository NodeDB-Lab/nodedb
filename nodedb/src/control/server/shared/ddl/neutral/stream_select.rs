// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SELECT * FROM STREAM` handler.
//!
//! Syntax:
//! ```sql
//! SELECT * FROM STREAM <stream> CONSUMER GROUP <group> [PARTITION <p>] [LIMIT <n>]
//! ```
//!
//! Ported from the pgwire `ddl::stream_select::select_from_stream` handler.
//! Reads events from the stream buffer starting after the consumer group's
//! committed offsets and returns a materialized result set — there is no
//! per-connection push stream — so this handler carries no per-connection
//! state. The partition/limit token parsing, the cluster-aware forwarding to
//! the leader node on `ConsumeError::RemotePartition`, the empty-result
//! short-circuit on `ConsumeError::BufferEmpty`, and the column layout are
//! preserved verbatim; only the result construction changed from pgwire
//! `Response` / `PgWireError` to the protocol-neutral [`DdlResult`] /
//! [`DdlError`].

use serde_json::{Map, Value as JsonValue};
use sonic_rs;

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::server::shared::ddl::sql_parse::{parse_ident_token, parse_stream_ident_token};
use crate::control::state::SharedState;
use crate::event::cdc::CdcSubscriberScope;
use crate::event::cdc::consume::{ConsumeError, ConsumeParams, consume_stream};
use crate::types::DatabaseId;

use super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Handle `SELECT * FROM STREAM <stream> CONSUMER GROUP <group> [PARTITION <p>] [LIMIT <n>]`
///
/// Cluster-aware: if the requested partition is on a remote node, forwards
/// the consume request to the leader via the gateway (C-δ.6: `ExecuteRequest`).
pub async fn select_from_stream(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    // Parse: SELECT * FROM STREAM <stream> CONSUMER GROUP <group> [PARTITION <p>] [LIMIT <n>]
    // parts: [SELECT, *, FROM, STREAM, <stream>, CONSUMER, GROUP, <group>, ...]
    //         0       1  2     3       4         5         6      7
    if parts.len() < 8
        || !parts[3].eq_ignore_ascii_case("STREAM")
        || !parts[5].eq_ignore_ascii_case("CONSUMER")
        || !parts[6].eq_ignore_ascii_case("GROUP")
    {
        return Err(err(
            "42601",
            "expected SELECT * FROM STREAM <stream> CONSUMER GROUP <group> [PARTITION <p>] [LIMIT <n>]",
        ));
    }

    let stream_name = parse_stream_ident_token(parts[4])?;
    let group_name = parse_ident_token(parts[7])?;

    let mut partition: Option<u32> = None;
    let mut limit: usize = 100;
    let mut i = 8;

    while i < parts.len() {
        if parts[i].eq_ignore_ascii_case("PARTITION") && i + 1 < parts.len() {
            partition = Some(
                parts[i + 1]
                    .parse()
                    .map_err(|_| err("42601", format!("invalid partition: '{}'", parts[i + 1])))?,
            );
            i += 2;
        } else if parts[i].eq_ignore_ascii_case("LIMIT") && i + 1 < parts.len() {
            limit = parts[i + 1]
                .parse()
                .map_err(|_| err("42601", format!("invalid limit: '{}'", parts[i + 1])))?;
            i += 2;
        } else {
            i += 1;
        }
    }

    // A change stream's events are protected by Read access to its source
    // collection. Resolve it in the selected database and caller tenant before
    // consumption can be forwarded to a remote partition. `topic:` names have
    // no source collection and intentionally retain their existing semantics.
    if let Some(topic_name) = stream_name.strip_prefix("topic:") {
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
    } else if let Some(stream_def) = state
        .stream_registry
        .get(database_id, tenant_id, &stream_name)
    {
        let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
        authorize_collection(
            identity,
            database_id,
            &stream_def.collection,
            Permission::Read,
            &state.permissions,
            &state.roles,
            &emitter,
        )
        .map_err(crate::Error::from)
        .map_err(|error| err("42501", error.to_string()))?;
    }

    let consume_params = ConsumeParams {
        database_id,
        tenant_id,
        stream_name: &stream_name,
        group_name: &group_name,
        partition,
        limit,
    };

    let mut result = match consume_stream(state, &consume_params) {
        Ok(r) => r,
        Err(ConsumeError::RemotePartition { leader_node, .. }) => {
            match crate::event::cdc::consume::consume_remote(state, &consume_params, leader_node)
                .await
            {
                Ok(r) => r,
                Err(e) => return Err(err("58000", e.to_string())),
            }
        }
        Err(ConsumeError::BufferEmpty(_)) => {
            // Return empty result set.
            let columns = result_columns();
            return Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
                columns,
                Vec::new(),
            ))]);
        }
        Err(e) => {
            return Err(err("42704", e.to_string()));
        }
    };

    // A stream event carries the written row, so the caller's column
    // redaction rules apply to it exactly as they do to a SELECT of the
    // source collection. The reader here is the authenticated caller, so the
    // scope is its own resolved roles rather than the subscription's.
    let mut subscriber = CdcSubscriberScope::new(
        identity.tenant_id,
        RequestAuthScope::for_database(identity, state.auth_stores(), database_id)
            .auth()
            .roles
            .clone(),
    );
    subscriber.retain_deliverable(&state.redaction, &mut result.events);

    let columns = result_columns();
    let mut rows = Vec::with_capacity(result.events.len());

    for event in &result.events {
        let mut row = Map::new();
        row.insert(
            "sequence".to_string(),
            JsonValue::String(event.sequence.to_string()),
        );
        row.insert(
            "partition".to_string(),
            JsonValue::String(event.partition.to_string()),
        );
        row.insert(
            "collection".to_string(),
            JsonValue::String(event.collection.clone()),
        );
        row.insert(
            "event_type".to_string(),
            JsonValue::String(event.op.clone()),
        );
        row.insert(
            "row_id".to_string(),
            JsonValue::String(event.row_id.clone()),
        );
        row.insert("lsn".to_string(), JsonValue::String(event.lsn.to_string()));
        row.insert(
            "offset".to_string(),
            JsonValue::String(event.offset_token()),
        );
        row.insert(
            "event_time".to_string(),
            JsonValue::String(event.event_time.to_string()),
        );
        let new_val = event
            .new_value
            .as_ref()
            .map(|v| sonic_rs::to_string(v).unwrap_or_default())
            .unwrap_or_default();
        row.insert("new_value".to_string(), JsonValue::String(new_val));
        let old_val = event
            .old_value
            .as_ref()
            .map(|v| sonic_rs::to_string(v).unwrap_or_default())
            .unwrap_or_default();
        row.insert("old_value".to_string(), JsonValue::String(old_val));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// Column schema for stream SELECT results.
fn result_columns() -> Vec<String> {
    vec![
        "sequence".to_string(),
        "partition".to_string(),
        "collection".to_string(),
        "event_type".to_string(),
        "row_id".to_string(),
        "lsn".to_string(),
        "offset".to_string(),
        "event_time".to_string(),
        "new_value".to_string(),
        "old_value".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::{parse_ident_token, parse_stream_ident_token};

    #[test]
    fn generated_stream_identifiers_decode_without_losing_quoted_case() {
        assert_eq!(
            parse_stream_ident_token("\"orders_stream\"").expect("quoted stream"),
            "orders_stream"
        );
        assert_eq!(
            parse_ident_token("\"Analytics\"").expect("quoted group"),
            "Analytics"
        );
        assert!(parse_stream_ident_token("orders;DROP").is_err());
    }
}
