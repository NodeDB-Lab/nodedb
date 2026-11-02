// SPDX-License-Identifier: BUSL-1.1

//! Apply a committed `ArraySchema` entry on the local node.

use std::sync::Arc;

use tracing::warn;

use super::common::AppliedPosition;
use crate::control::distributed_applier::{AppliedWrite, ProposeTracker};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// Payload extracted from a `ReplicatedWrite::ArraySchema` entry.
pub(crate) struct ArraySchemaPayload<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub array: &'a str,
    pub snapshot_payload: &'a [u8],
    pub schema_hlc_bytes: [u8; 18],
}

/// Apply a committed `ArraySchema` entry on the local node: import the Loro
/// snapshot into the local `OriginSchemaRegistry`.
///
/// The array's catalog row is not written here. The receiving node proposes
/// it as a `PutArray` on the metadata group, so every node holds it, not only
/// the replicas of this data group.
///
/// This is the one apply path that mints no WAL redo record and needs none:
/// the import lands in a fsync-committed redb transaction before it returns,
/// which is the same fact the durable applied floor asserts for every other
/// branch.
///
/// Returns `true` when the import committed durably, `false` otherwise. The caller
/// uses this to gate the durable applied floor and Raft log compaction.
pub(crate) fn apply_array_schema(
    state: &Arc<SharedState>,
    tracker: &Arc<ProposeTracker>,
    pos: AppliedPosition,
    payload: ArraySchemaPayload<'_>,
) -> bool {
    let AppliedPosition {
        group_id,
        log_index,
        applied_key,
        ..
    } = pos;
    use nodedb_array::sync::hlc::Hlc;

    let ArraySchemaPayload {
        tenant_id,
        database_id,
        array,
        snapshot_payload,
        schema_hlc_bytes,
    } = payload;
    let remote_hlc = Hlc::from_bytes(&schema_hlc_bytes);

    // Use the replicated import path so every replica converges to the same
    // schema_hlc (the one committed in the Raft log entry) rather than each
    // bumping independently via their local HLC generator.
    if let Err(e) = state
        .array_sync_schemas
        .import_snapshot_replicated_in_database(
            database_id,
            tenant_id.as_u64(),
            array,
            snapshot_payload,
            remote_hlc,
        )
    {
        warn!(
            group_id, index = log_index, array = %array, error = %e,
            "apply_array_schema: import_snapshot_replicated failed"
        );
        tracker.complete(
            group_id,
            log_index,
            applied_key,
            Err(crate::Error::Internal {
                detail: format!("schema import: {e}"),
            }),
        );
        return false;
    }

    // A schema import touches the registries, not a Data-Plane collection, so it
    // answers an empty payload.
    tracker.complete(
        group_id,
        log_index,
        applied_key,
        Ok(AppliedWrite::new(Vec::new())),
    );
    true
}
