// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `COMMIT OFFSET` DDL handler.
//!
//! The two-form token parsing, the group-existence checks, the
//! per-partition tail-tracker batch-commit path (NOT a full buffer scan),
//! and the `OffsetRegression` error mapping run here. The result is the
//! protocol-neutral [`DdlResult`] / [`DdlError`].
//!
//! Syntax:
//! - `COMMIT OFFSET PARTITION <p> AT <epoch>:<index>:<sequence> ON <stream> CONSUMER GROUP <name>`
//!   (a bare `<index>` acknowledges every event of that write)
//! - `COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>` (batch: commit all at latest)
//!
//! A commit is a replicated catalog entry: every node raises its offset
//! store on apply, so a consumer that moves to another node resumes from it.

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::server::shared::ddl::sql_parse::{parse_ident_token, parse_stream_ident_token};
use crate::control::state::SharedState;
use crate::event::cdc::CdcOffset;
use crate::event::cdc::consumer_group::ConsumerGroupDef;
use crate::event::cdc::consumer_group::types::PartitionOffset;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::status;
use super::identity::canonical_stream_name;
use super::replicate::propose_commit_offsets;

fn authorize_offset_commit(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    stream_name: &str,
) -> Result<(), DdlError> {
    let resource = if let Some(topic_name) = stream_name.strip_prefix("topic:") {
        format!("topic:{topic_name}")
    } else if let Some(stream_def) =
        state
            .stream_registry
            .get(database_id, identity.tenant_id.as_u64(), stream_name)
    {
        stream_def.collection
    } else {
        return Ok(());
    };
    let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    authorize_collection(
        identity,
        database_id,
        &resource,
        Permission::Read,
        &state.permissions,
        &state.roles,
        &emitter,
    )
    .map_err(crate::Error::from)
    .map_err(|error| DdlError::new("42501", error.to_string()))
}

/// Handle `COMMIT OFFSET PARTITION <p> AT <epoch>:<index>:<sequence> ON <stream> CONSUMER GROUP <name>`.
pub async fn commit_offset(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    // Single partition: COMMIT OFFSET PARTITION <p> AT <epoch>:<index>:<sequence> ON <stream> CONSUMER GROUP <name>
    // parts: [COMMIT, OFFSET, PARTITION, <p>, AT, <offset>, ON, <stream>, CONSUMER, GROUP, <name>]
    // indices:  0       1       2         3   4    5     6     7        8         9      10
    if parts.len() >= 11
        && parts[2].eq_ignore_ascii_case("PARTITION")
        && parts[4].eq_ignore_ascii_case("AT")
        && parts[6].eq_ignore_ascii_case("ON")
        && parts[8].eq_ignore_ascii_case("CONSUMER")
        && parts[9].eq_ignore_ascii_case("GROUP")
    {
        let partition_id: u32 = parts[3]
            .parse()
            .map_err(|_| DdlError::new("42601", format!("invalid partition: '{}'", parts[3])))?;
        let offset: CdcOffset =
            parts[5]
                .parse()
                .map_err(|error: crate::event::cdc::offset::ParseCdcOffsetError| {
                    DdlError::new("42601", error.to_string())
                })?;
        let requested_stream = parse_stream_ident_token(parts[7])?;
        let mut stream_name =
            canonical_stream_name(state, database_id, tenant_id, &requested_stream);
        let topic_lock = stream_name.strip_prefix("topic:").map(|topic| {
            state
                .ep_topic_registry
                .lifecycle_lock(database_id, tenant_id, topic)
        });
        let _topic_guard = match topic_lock {
            Some(lock) => Some(lock.lock_owned().await),
            None => None,
        };
        stream_name = canonical_stream_name(state, database_id, tenant_id, &requested_stream);
        let group_name = parse_ident_token(parts[10])?;
        authorize_offset_commit(state, identity, database_id, &stream_name)?;
        let lifecycle_lock =
            state
                .group_registry
                .lifecycle_lock(database_id, tenant_id, &stream_name, &group_name);
        let _group_guard = lifecycle_lock.lock().await;

        let def = registered_group(state, database_id, tenant_id, &stream_name, &group_name)?;
        let current = state.offset_store.get_offset(
            database_id,
            tenant_id,
            &stream_name,
            &group_name,
            partition_id,
        );
        if offset < current {
            let regression = crate::Error::OffsetRegression {
                stream: stream_name,
                group: group_name,
                partition_id,
                offsets: Box::new(crate::error::RegressedOffsets {
                    current,
                    attempted: offset,
                }),
            };
            return Err(DdlError::new("22023", regression.to_string()));
        }
        propose_commit_offsets(
            state,
            &def,
            vec![PartitionOffset::new(partition_id, offset)],
        )
        .await?;

        return Ok(status("COMMIT OFFSET"));
    }

    // Batch: COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>
    // parts: [COMMIT, OFFSETS, ON, <stream>, CONSUMER, GROUP, <name>]
    // indices:  0       1      2     3        4         5      6
    if parts.len() >= 7
        && parts[1].eq_ignore_ascii_case("OFFSETS")
        && parts[2].eq_ignore_ascii_case("ON")
        && parts[4].eq_ignore_ascii_case("CONSUMER")
        && parts[5].eq_ignore_ascii_case("GROUP")
    {
        let requested_stream = parse_stream_ident_token(parts[3])?;
        let mut stream_name =
            canonical_stream_name(state, database_id, tenant_id, &requested_stream);
        let topic_lock = stream_name.strip_prefix("topic:").map(|topic| {
            state
                .ep_topic_registry
                .lifecycle_lock(database_id, tenant_id, topic)
        });
        let _topic_guard = match topic_lock {
            Some(lock) => Some(lock.lock_owned().await),
            None => None,
        };
        stream_name = canonical_stream_name(state, database_id, tenant_id, &requested_stream);
        let group_name = parse_ident_token(parts[6])?;
        authorize_offset_commit(state, identity, database_id, &stream_name)?;
        let lifecycle_lock =
            state
                .group_registry
                .lifecycle_lock(database_id, tenant_id, &stream_name, &group_name);
        let _group_guard = lifecycle_lock.lock().await;

        let def = registered_group(state, database_id, tenant_id, &stream_name, &group_name)?;

        // Use the buffer's per-partition tail tracker — NOT a full
        // buffer scan. A scan is O(N) and silently
        // misses partitions whose events have been evicted by retention.
        // Every replica positions events alike, so this node's tails name
        // the same events on every node.
        let raised: Vec<PartitionOffset> = state
            .cdc_router
            .get_buffer(database_id, tenant_id, &stream_name)
            .map(|buffer| buffer.partition_tails())
            .unwrap_or_default()
            .into_iter()
            .filter(|(partition_id, offset)| {
                *offset
                    > state.offset_store.get_offset(
                        database_id,
                        tenant_id,
                        &stream_name,
                        &group_name,
                        *partition_id,
                    )
            })
            .map(|(partition_id, offset)| PartitionOffset::new(partition_id, offset))
            .collect();
        if !raised.is_empty() {
            propose_commit_offsets(state, &def, raised).await?;
        }

        return Ok(status("COMMIT OFFSETS"));
    }

    Err(DdlError::new(
        "42601",
        "expected COMMIT OFFSET PARTITION <p> AT <epoch>:<index>:<sequence> ON <stream> CONSUMER GROUP <name>, \
         or COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>; a bare <index> acknowledges every event of that write",
    ))
}

/// The registered definition of a consumer group, or `42704` when none is.
fn registered_group(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    stream_name: &str,
    group_name: &str,
) -> Result<ConsumerGroupDef, DdlError> {
    state
        .group_registry
        .get(database_id, tenant_id, stream_name, group_name)
        .ok_or_else(|| {
            DdlError::new(
                "42704",
                format!("consumer group '{group_name}' does not exist on stream '{stream_name}'"),
            )
        })
}

/// Commit `offsets` for a group through the replicated catalog, raising each
/// partition on every node. A partition already at or past its offset keeps
/// its position. Deferred `COMMIT OFFSET` inside a transaction flushes here.
pub async fn commit_group_offsets(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    stream_name: &str,
    group_name: &str,
    offsets: Vec<PartitionOffset>,
) -> Result<(), DdlError> {
    let def = registered_group(state, database_id, tenant_id, stream_name, group_name)?;
    propose_commit_offsets(state, &def, offsets).await
}
