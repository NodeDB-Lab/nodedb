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

/// Apply a committed `ArraySchema` entry on the local node.
///
/// 1. Imports the Loro snapshot into the local `OriginSchemaRegistry`.
/// 2. Decodes the `ArraySchema` and registers + persists an `ArrayCatalogEntry`
///    so the Data Plane can open the array when a subsequent `ArrayOp` arrives.
///    This is the canonical DDL propagation path for followers: the Raft
///    `ArraySchema` entry is the single source of truth — no out-of-band
///    catalog registration is needed.
///
/// This is the one apply path that mints no WAL redo record and needs none: both
/// steps land in fsync-committed redb transactions before it returns, which is
/// the same fact the durable applied floor asserts for every other branch.
///
/// Returns `true` when BOTH committed durably, `false` otherwise. The caller
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

    // Decode the ArraySchema from the just-imported Loro document, register it
    // in the array catalog, and persist it, so the Data Plane can open the array
    // on this node now and after a restart. Shared with the single-node
    // direct-import path in `inbound.rs` via
    // `catalog_register::register_array_catalog_entry` so both codepaths
    // converge on the same catalog-visibility guarantee.
    //
    // A failure here fails the whole apply. The caller advances this group's
    // DURABLE applied floor on our `true`, and the next boot resumes Raft
    // delivery above that floor — so reporting success without the catalog entry
    // would leave the array permanently unopenable, with the one entry that
    // could repair it excluded from redelivery. Returning `false` leaves the
    // floor behind and keeps the entry replayable; both steps are idempotent
    // (the import re-imports the same committed HLC, the register no-ops on an
    // existing entry), so a redelivery converges.
    if let Err(e) = crate::control::array_sync::catalog_register::register_array_catalog_entry(
        state,
        tenant_id,
        database_id,
        array,
    ) {
        warn!(
            group_id, index = log_index, array = %array, error = %e,
            "apply_array_schema: register_array_catalog_entry failed"
        );
        tracker.complete(
            group_id,
            log_index,
            applied_key,
            Err(crate::Error::Internal {
                detail: format!("array catalog register: {e}"),
            }),
        );
        return false;
    }

    // A schema import touches the registries, not a Data-Plane collection, so it
    // publishes no per-collection write-version.
    tracker.complete(
        group_id,
        log_index,
        applied_key,
        Ok(AppliedWrite::unversioned(Vec::new())),
    );
    true
}
