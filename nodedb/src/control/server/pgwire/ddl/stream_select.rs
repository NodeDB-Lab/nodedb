//! `SELECT * FROM STREAM` handler.
//!
//! Syntax:
//! ```sql
//! SELECT * FROM STREAM <stream> CONSUMER GROUP <group> [PARTITION <p>] [LIMIT <n>]
//! ```
//!
//! Reads events from the stream buffer starting after the consumer group's
//! committed offsets. Returns events as a pgwire query result set.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::cdc::consume::{ConsumeError, ConsumeParams, consume_stream};

use super::super::types::{sqlstate_error, text_field};

/// Handle `SELECT * FROM STREAM <stream> CONSUMER GROUP <group> [PARTITION <p>] [LIMIT <n>]`
///
/// Cluster-aware: if the requested partition is on a remote node, forwards
/// the consume request to the leader via QUIC `ForwardRequest`.
pub async fn select_from_stream(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();

    // Parse: SELECT * FROM STREAM <stream> CONSUMER GROUP <group> [PARTITION <p>] [LIMIT <n>]
    // parts: [SELECT, *, FROM, STREAM, <stream>, CONSUMER, GROUP, <group>, ...]
    //         0       1  2     3       4         5         6      7
    if parts.len() < 8
        || !parts[3].eq_ignore_ascii_case("STREAM")
        || !parts[5].eq_ignore_ascii_case("CONSUMER")
        || !parts[6].eq_ignore_ascii_case("GROUP")
    {
        return Err(sqlstate_error(
            "42601",
            "expected SELECT * FROM STREAM <stream> CONSUMER GROUP <group> [PARTITION <p>] [LIMIT <n>]",
        ));
    }

    let stream_name = parts[4].to_lowercase();
    let group_name = parts[7].to_lowercase();

    let mut partition: Option<u16> = None;
    let mut limit: usize = 100;
    let mut i = 8;

    while i < parts.len() {
        if parts[i].eq_ignore_ascii_case("PARTITION") && i + 1 < parts.len() {
            partition = Some(parts[i + 1].parse().map_err(|_| {
                sqlstate_error("42601", &format!("invalid partition: '{}'", parts[i + 1]))
            })?);
            i += 2;
        } else if parts[i].eq_ignore_ascii_case("LIMIT") && i + 1 < parts.len() {
            limit = parts[i + 1].parse().map_err(|_| {
                sqlstate_error("42601", &format!("invalid limit: '{}'", parts[i + 1]))
            })?;
            i += 2;
        } else {
            i += 1;
        }
    }

    let consume_params = ConsumeParams {
        tenant_id,
        stream_name: &stream_name,
        group_name: &group_name,
        partition,
        limit,
    };

    let result = match consume_stream(state, &consume_params) {
        Ok(r) => r,
        Err(ConsumeError::RemotePartition { leader_node, .. }) => {
            match crate::event::cdc::consume::consume_remote(state, &consume_params, leader_node)
                .await
            {
                Ok(r) => r,
                Err(e) => return Err(sqlstate_error("58000", &e.to_string())),
            }
        }
        Err(ConsumeError::BufferEmpty(_)) => {
            // Return empty result set.
            let schema = Arc::new(result_schema());
            return Ok(vec![Response::Query(QueryResponse::new(
                schema,
                stream::iter(Vec::new()),
            ))]);
        }
        Err(e) => {
            return Err(sqlstate_error("42704", &e.to_string()));
        }
    };

    let schema = Arc::new(result_schema());
    let mut rows = Vec::with_capacity(result.events.len());

    for event in &result.events {
        let mut encoder = DataRowEncoder::new(schema.clone());
        let _ = encoder.encode_field(&event.sequence.to_string());
        let _ = encoder.encode_field(&event.partition.to_string());
        let _ = encoder.encode_field(&event.collection);
        let _ = encoder.encode_field(&event.op);
        let _ = encoder.encode_field(&event.row_id);
        let _ = encoder.encode_field(&event.lsn.to_string());
        let _ = encoder.encode_field(&event.event_time.to_string());
        let new_val = event
            .new_value
            .as_ref()
            .map(|v| serde_json::to_string(v).unwrap_or_default())
            .unwrap_or_default();
        let _ = encoder.encode_field(&new_val);
        let old_val = event
            .old_value
            .as_ref()
            .map(|v| serde_json::to_string(v).unwrap_or_default())
            .unwrap_or_default();
        let _ = encoder.encode_field(&old_val);
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Column schema for stream SELECT results.
fn result_schema() -> Vec<pgwire::api::results::FieldInfo> {
    vec![
        text_field("sequence"),
        text_field("partition"),
        text_field("collection"),
        text_field("event_type"),
        text_field("row_id"),
        text_field("lsn"),
        text_field("event_time"),
        text_field("new_value"),
        text_field("old_value"),
    ]
}
