// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for integration tests.

pub mod array_sync;
pub mod cluster_harness;
pub mod core_loop_runner;
pub mod ilp_client;
pub mod insert_returning_engines;
pub mod jwks_fixture;
pub mod native_harness;
pub mod occ_shuffle;
pub mod pgwire_auth_helpers;
pub mod pgwire_harness;
pub mod sync_client;
pub mod test_tracing;
pub mod tx_batch_helpers;
pub mod tx_commit;

use nodedb::event::cdc::event::CdcEvent;
use nodedb_types::DatabaseId;

/// Current time in milliseconds since UNIX epoch.
#[allow(dead_code)]
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Create a [`CdcEvent`] with sensible test defaults.
#[allow(dead_code)]
pub fn make_cdc_event(
    database_id: DatabaseId,
    seq: u64,
    partition: u32,
    collection: &str,
    op: &str,
) -> CdcEvent {
    CdcEvent {
        sequence: seq,
        partition,
        collection: collection.into(),
        op: op.into(),
        row_id: format!("r-{seq}"),
        event_time: now_ms(),
        lsn: seq * 10,
        database_id,
        tenant_id: 1,
        new_value: Some(serde_json::json!({"id": seq})),
        old_value: None,
        schema_version: 0,
        field_diffs: None,
        system_time_ms: None,
        valid_time_ms: None,
        source: nodedb::event::EventSource::User,
    }
}
