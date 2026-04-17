//! `COMMIT OFFSET` DDL handler.
//!
//! Syntax:
//! - `COMMIT OFFSET PARTITION <p> AT <lsn> ON <stream> CONSUMER GROUP <name>`
//! - `COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>` (batch: commit all at latest)

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;

/// Handle `COMMIT OFFSET PARTITION <p> AT <lsn> ON <stream> CONSUMER GROUP <name>`
pub fn commit_offset(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();

    // Single partition: COMMIT OFFSET PARTITION <p> AT <lsn> ON <stream> CONSUMER GROUP <name>
    // parts: [COMMIT, OFFSET, PARTITION, <p>, AT, <lsn>, ON, <stream>, CONSUMER, GROUP, <name>]
    // indices:  0       1       2         3   4    5     6     7        8         9      10
    if parts.len() >= 11
        && parts[2].eq_ignore_ascii_case("PARTITION")
        && parts[4].eq_ignore_ascii_case("AT")
        && parts[6].eq_ignore_ascii_case("ON")
        && parts[8].eq_ignore_ascii_case("CONSUMER")
        && parts[9].eq_ignore_ascii_case("GROUP")
    {
        let partition_id: u16 = parts[3]
            .parse()
            .map_err(|_| sqlstate_error("42601", &format!("invalid partition: '{}'", parts[3])))?;
        let lsn: u64 = parts[5]
            .parse()
            .map_err(|_| sqlstate_error("42601", &format!("invalid LSN: '{}'", parts[5])))?;
        let stream_name = parts[7].to_lowercase();
        let group_name = parts[10].to_lowercase();

        // Verify group exists.
        if state
            .group_registry
            .get(tenant_id, &stream_name, &group_name)
            .is_none()
        {
            return Err(sqlstate_error(
                "42704",
                &format!("consumer group '{group_name}' does not exist on stream '{stream_name}'"),
            ));
        }

        state
            .offset_store
            .commit_offset(tenant_id, &stream_name, &group_name, partition_id, lsn)
            .map_err(|e| match e {
                crate::Error::OffsetRegression { .. } => sqlstate_error("22023", &e.to_string()),
                _ => sqlstate_error("XX000", &format!("offset commit: {e}")),
            })?;

        return Ok(vec![Response::Execution(Tag::new("COMMIT OFFSET"))]);
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
        let stream_name = parts[3].to_lowercase();
        let group_name = parts[6].to_lowercase();

        if state
            .group_registry
            .get(tenant_id, &stream_name, &group_name)
            .is_none()
        {
            return Err(sqlstate_error(
                "42704",
                &format!("consumer group '{group_name}' does not exist on stream '{stream_name}'"),
            ));
        }

        // Use the buffer's per-partition tail tracker — NOT a full
        // read_from_lsn(0, MAX) scan. The scan is O(N) and silently
        // misses partitions whose events have been evicted by retention.
        if let Some(buffer) = state.cdc_router.get_buffer(tenant_id, &stream_name) {
            for (partition_id, lsn) in buffer.partition_tails() {
                // Skip partitions whose committed offset already meets
                // or exceeds the current tail — commit_offset rejects
                // regressions and we want idempotent auto-commit.
                let current = state.offset_store.get_offset(
                    tenant_id,
                    &stream_name,
                    &group_name,
                    partition_id,
                );
                if lsn <= current {
                    continue;
                }
                state
                    .offset_store
                    .commit_offset(tenant_id, &stream_name, &group_name, partition_id, lsn)
                    .map_err(|e| sqlstate_error("XX000", &format!("offset commit: {e}")))?;
            }
        }

        return Ok(vec![Response::Execution(Tag::new("COMMIT OFFSETS"))]);
    }

    Err(sqlstate_error(
        "42601",
        "expected COMMIT OFFSET PARTITION <p> AT <lsn> ON <stream> CONSUMER GROUP <name>, \
         or COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>",
    ))
}
