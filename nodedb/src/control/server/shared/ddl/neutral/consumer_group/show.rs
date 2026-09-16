// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW CONSUMER GROUPS ON <stream>` and
//! `SHOW PARTITIONS ON <stream>` handlers.
//!
//! Ported from the pgwire `ddl::consumer_group::show` handlers. The token-based
//! syntax checks, the tenant scoping, the per-group offset counting, and the
//! per-partition buffer-scan statistics are preserved verbatim; only the result
//! construction changed from a pgwire `QueryResponse` to the protocol-neutral
//! [`DdlResult::Rows`].

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::identity::canonical_stream_name;
use crate::control::server::shared::ddl::sql_parse::parse_stream_ident_token;

/// Handle `SHOW CONSUMER GROUPS ON <stream>`
pub fn show_consumer_groups(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // parts: ["SHOW", "CONSUMER", "GROUPS", "ON", "<stream>"]
    if parts.len() < 5 || !parts[3].eq_ignore_ascii_case("ON") {
        return Err(DdlError::new(
            "42601",
            "expected SHOW CONSUMER GROUPS ON <stream>",
        ));
    }

    let tenant_id = identity.tenant_id.as_u64();
    let requested_stream = parse_stream_ident_token(parts[4])?;
    let stream_name = canonical_stream_name(state, database_id, tenant_id, &requested_stream);

    let columns = vec![
        "group_name".to_string(),
        "stream".to_string(),
        "committed_partitions".to_string(),
        "owner".to_string(),
    ];

    let groups = state
        .group_registry
        .list_for_stream(database_id, tenant_id, &stream_name);

    let mut rows = Vec::with_capacity(groups.len());
    for g in &groups {
        let offsets =
            state
                .offset_store
                .get_all_offsets(database_id, tenant_id, &stream_name, &g.name);
        let committed_count = offsets.len();

        let mut row = Map::new();
        row.insert("group_name".to_string(), JsonValue::String(g.name.clone()));
        row.insert(
            "stream".to_string(),
            JsonValue::String(g.stream_name.clone()),
        );
        row.insert(
            "committed_partitions".to_string(),
            JsonValue::String(committed_count.to_string()),
        );
        row.insert("owner".to_string(), JsonValue::String(g.owner.clone()));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// Handle `SHOW PARTITIONS ON <stream>`
///
/// Lists all vShard partitions that have events in the stream's buffer,
/// with earliest/latest composite offset for each partition.
pub fn show_partitions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // parts: ["SHOW", "PARTITIONS", "ON", "<stream>"]
    if parts.len() < 4 || !parts[2].eq_ignore_ascii_case("ON") {
        return Err(DdlError::new(
            "42601",
            "expected SHOW PARTITIONS ON <stream>",
        ));
    }

    let tenant_id = identity.tenant_id.as_u64();
    let requested_stream = parse_stream_ident_token(parts[3])?;
    let stream_name = canonical_stream_name(state, database_id, tenant_id, &requested_stream);

    // Get the stream's buffer from the CdcRouter.
    let buffer = state
        .cdc_router
        .get_buffer(database_id, tenant_id, &stream_name);

    let columns = vec![
        "partition_id".to_string(),
        "earliest_offset".to_string(),
        "latest_offset".to_string(),
        "event_count".to_string(),
    ];

    let mut rows = Vec::new();
    if let Some(buf) = buffer {
        // Scan the buffer and collect per-partition stats.
        let events = buf.read_from(crate::event::cdc::CdcOffset::ZERO, usize::MAX);
        let mut partition_stats: std::collections::BTreeMap<
            u32,
            (
                crate::event::cdc::CdcOffset,
                crate::event::cdc::CdcOffset,
                usize,
            ),
        > = std::collections::BTreeMap::new();
        for event in &events {
            let entry = partition_stats.entry(event.partition).or_insert((
                event.position(),
                event.position(),
                0,
            ));
            entry.0 = entry.0.min(event.position());
            entry.1 = entry.1.max(event.position());
            entry.2 += 1;
        }
        for (pid, (earliest, latest, count)) in &partition_stats {
            let mut row = Map::new();
            row.insert(
                "partition_id".to_string(),
                JsonValue::String(pid.to_string()),
            );
            row.insert(
                "earliest_offset".to_string(),
                JsonValue::String(earliest.token()),
            );
            row.insert(
                "latest_offset".to_string(),
                JsonValue::String(latest.token()),
            );
            row.insert(
                "event_count".to_string(),
                JsonValue::String(count.to_string()),
            );
            rows.push(row);
        }
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}
