// SPDX-License-Identifier: BUSL-1.1

//! CDC change-event publishing for dispatched writes: turning the metadata
//! [`super::extract`] derived from a plan into `ChangeEvent`s on the local
//! change stream plus the cluster-wide NOTIFY fan-out.

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_plan::ClusterArrayOp;

use super::extract::{WriteChangeMeta, cluster_array_change_meta, extract_write_metadata};

/// Current wall-clock time as milliseconds since Unix epoch.
///
/// Returns 0 if the system clock is before the epoch (should never happen
/// on correctly configured systems).
fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Check if a timeseries collection has CDC enabled.
///
/// Returns `false` (CDC off) by default for timeseries to prevent
/// high-cardinality metric streams from flooding the ChangeStream bus.
/// Users opt in via `CREATE TIMESERIES name WITH (cdc = 'true')`.
fn is_timeseries_cdc_enabled(
    shared: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
) -> bool {
    let catalog = shared.credentials.catalog();
    if let Ok(Some(coll)) = catalog.get_collection(database_id, tenant_id.as_u64(), collection)
        && coll.collection_type.is_timeseries()
    {
        if let Some(config) = coll.get_timeseries_config()
            && let Some(cdc_val) = config.get("cdc")
        {
            return cdc_val.as_str() == Some("true") || cdc_val.as_bool() == Some(true);
        }
        // Default: CDC off for timeseries.
        return false;
    }
    // Not timeseries or catalog unavailable — allow publishing.
    true
}

/// Publish a change event (and cluster-wide NOTIFY) for a successful write.
///
/// CDC opt-in check for timeseries: skip publishing unless `cdc_enabled`.
/// Document collections always publish (backward compatible).
fn publish_change_event(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    change_meta: WriteChangeMeta,
    lsn: nodedb_types::Lsn,
) {
    let (collection, document_id, op) = change_meta;
    if !is_timeseries_cdc_enabled(shared, database_id, tenant_id, &collection) {
        return;
    }

    use crate::control::change_stream::ChangeEvent;
    let event = ChangeEvent {
        lsn,
        tenant_id,
        collection,
        document_id,
        operation: op,
        timestamp_ms: current_timestamp_ms(),
        after: None,
    };

    // Cluster-wide NOTIFY: broadcast to all peers via QUIC.
    if let (Some(transport), Some(topology)) = (&shared.cluster_transport, &shared.cluster_topology)
    {
        use std::sync::atomic::Ordering;
        static NOTIFY_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let seq = NOTIFY_SEQ.fetch_add(1, Ordering::Relaxed);
        crate::control::change_stream::broadcast_notify_to_cluster(
            database_id,
            &event,
            shared.node_id,
            seq,
            transport,
            topology,
        );
    }

    shared.change_stream.publish_in_database(database_id, event);
}

/// The Control-Plane change events one write plan yields.
///
/// Extraction is split from publishing because the write funnel consumes the
/// plan — it moves into the `Request` — long before the `Response` the event's
/// LSN comes from exists. A caller that still owns its plan at publish time
/// uses [`publish_origin_change_events`] and never names this type.
pub(crate) struct WriteChangeSet {
    /// One tuple per logical row change — see `extract_write_metadata`.
    metas: Vec<WriteChangeMeta>,
}

/// Derive a plan's change events. Pure: it matches over the plan and clones out
/// collection / document identity, touching no shared state, so it is safe to
/// call at the one point where the plan is still owned.
pub(crate) fn extract_write_change_set(plan: &PhysicalPlan, tenant_id: TenantId) -> WriteChangeSet {
    WriteChangeSet {
        metas: extract_write_metadata(plan, tenant_id),
    }
}

/// Publish an already-extracted change set at an explicit LSN. Almost every
/// write plan yields exactly one event; a handful of multi-row /
/// multi-collection ops yield more than one, and reads / DDL / index
/// maintenance yield none.
pub(crate) fn publish_change_set_with_lsn(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    change_set: WriteChangeSet,
    lsn: nodedb_types::Lsn,
) {
    let WriteChangeSet { metas } = change_set;
    for meta in metas {
        publish_change_event(shared, tenant_id, database_id, meta, lsn);
    }
}

/// Publish an already-extracted change set, taking the LSN from a Data-Plane
/// [`Response`]'s watermark.
pub(crate) fn publish_change_set(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    change_set: WriteChangeSet,
    response: &Response,
) {
    publish_change_set_with_lsn(
        shared,
        tenant_id,
        database_id,
        change_set,
        response.watermark_lsn,
    );
}

/// Publish the Control-Plane change event(s) for a write this node originated
/// and has already had committed and applied.
///
/// The cluster Raft path cannot let the write funnel own its change feed: the
/// proposing node never reaches `submit_write` — it proposes, and every
/// replica's apply loop submits the committed entry independently. Publishing
/// from the apply loop would emit one event per replica plus a full NOTIFY
/// fan-out from each, so a subscriber would see the write once per replica. The
/// proposing node is the one node that handled the write exactly once, so it is
/// the one that publishes.
pub(crate) fn publish_origin_change_events(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
    response: &Response,
) {
    publish_change_set(
        shared,
        tenant_id,
        database_id,
        extract_write_change_set(plan, tenant_id),
        response,
    );
}

/// Publish the Control-Plane change event(s) for a `ClusterArray` write.
///
/// `ClusterArrayOp` never reaches the SPSC bridge / Data-Plane `Response`
/// path (see `PhysicalPlan::ClusterArray`'s own doc comment) — the coordinator
/// dispatch loop in `routing/cluster_array.rs` executes the op directly via
/// `ClusterArrayExecutor` and has no `Response::watermark_lsn` to read, so it
/// calls this entry point with the `wal_lsn` the op itself carries (allocated
/// by the Control Plane for the write) instead of going through
/// [`publish_origin_change_events`].
pub(crate) fn publish_cluster_array_change_events(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    op: &ClusterArrayOp,
    lsn: u64,
) {
    let change_set = WriteChangeSet {
        metas: cluster_array_change_meta(op),
    };
    publish_change_set_with_lsn(
        shared,
        tenant_id,
        database_id,
        change_set,
        nodedb_types::Lsn::new(lsn),
    );
}
