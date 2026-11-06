// SPDX-License-Identifier: BUSL-1.1

//! Retention decisions: one clock read, in the log, on every replica and mirror.
//!
//! The metadata leader's loops decide. Each decision enters every data group's
//! log, so the drop lands at one position against the group's writes on each
//! replica. A mirror's tail carries the entry, and a mirror decides nothing. A
//! node with no cluster runs the decision here.
//!
//! A core holds rows of every group it hosts. The first group's entry drops them
//! all, and each later group's entry finds nothing past the cutoff.

use std::time::Duration;

use futures::future::join_all;
use nodedb_types::{DatabaseId, TenantId};

use crate::control::server::exchange::gather::gather_all_cores;
use crate::control::server::shared::ddl::neutral::tenant::purge::data_groups;
use crate::control::state::SharedState;
use crate::control::version_cleanup::horizon::decided_cleanup_horizon;
use crate::control::wal_replication::propose::propose_replicated_entry;
use crate::control::wal_replication::types::RetentionOp;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::engine::bitemporal::BitemporalEngineKind;
use crate::engine::bitemporal::enforcement::parse_count_from_payload;
use crate::engine::timeseries::retention_policy::rooted_aggregates::aggregates_rooted_at;
use crate::types::TraceId;
use crate::wal::manager::NO_APPLY_KEY;

/// How long one decision gets to land in every group.
const PROPOSE_TIMEOUT: Duration = Duration::from_secs(300);

/// Whether this node decides retention: the metadata leader, or a node with no
/// cluster.
pub fn decides_retention(state: &SharedState) -> bool {
    state.cluster_topology.is_none() || state.is_singleton_worker()
}

/// Put one decision into effect: in every group's log, or here with no cluster.
pub async fn enact_retention(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    op: RetentionOp,
) -> crate::Result<()> {
    if state.cluster_topology.is_none() {
        return run_retention(state, tenant_id, database_id, &op).await;
    }
    let proposer = state.async_raft_proposer()?;
    let deadline = tokio::time::Instant::now() + PROPOSE_TIMEOUT;
    let proposals = data_groups(state)?.into_values().filter_map(|vshards| {
        let vshard_id = vshards.first().copied()?;
        let entry = ReplicatedEntry::new(
            tenant_id.as_u64(),
            database_id.as_u64(),
            vshard_id,
            ReplicatedWrite::RetentionDrop {
                op: op.clone(),
                vshards,
            },
        );
        Some(propose_replicated_entry(state, proposer, entry, deadline))
    });
    for applied in join_all(proposals).await {
        applied?;
    }
    Ok(())
}

/// One object's audit purge, before its cutoff is decided.
#[derive(Debug, Clone, Copy)]
pub struct AuditPurge<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub engine: BitemporalEngineKind,
    /// The bare collection name, or the array's name.
    pub object: &'a str,
    pub retain_ms: i64,
}

/// Decide one audit purge and put it into effect.
///
/// The cutoff is the older of `now - retain_ms` and the cleanup horizon the
/// decider computes across every data group. A replica reads no local horizon at
/// apply. An open snapshot and a commit stay safe all the same: the horizon sits a
/// full snapshot hold below a cut closed on every group, and no statement outlives
/// that hold (`control::version_cleanup::horizon`).
pub async fn enact_audit_retention(
    state: &SharedState,
    purge: AuditPurge<'_>,
) -> crate::Result<()> {
    let horizon = decided_cleanup_horizon(state).await?;
    let decided_ms = nodedb_types::ordinal_to_ms(horizon.closed);
    let safe_ms = nodedb_types::ordinal_to_ms(horizon.horizon.saturating_add(1)).saturating_sub(1);
    let cutoff_system_ms = decided_ms.saturating_sub(purge.retain_ms).min(safe_ms);
    let name = purge.object.to_string();
    let op = match purge.engine {
        BitemporalEngineKind::EdgeStore => RetentionOp::TemporalEdges {
            collection: name,
            cutoff_system_ms,
        },
        BitemporalEngineKind::DocumentStrict => RetentionOp::TemporalDocumentStrict {
            collection: name,
            cutoff_system_ms,
        },
        BitemporalEngineKind::Columnar => RetentionOp::TemporalColumnar {
            collection: name,
            cutoff_system_ms,
            decided_ms,
        },
        BitemporalEngineKind::Crdt => RetentionOp::TemporalCrdt {
            collection: name,
            cutoff_system_ms,
        },
        BitemporalEngineKind::Array => RetentionOp::TemporalArray {
            array: name,
            cutoff_system_ms,
        },
    };
    enact_retention(state, purge.tenant_id, purge.database_id, op).await
}

/// Run one decision on every local core, and record an audit purge in the WAL.
pub(crate) async fn run_retention(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    op: &RetentionOp,
) -> crate::Result<()> {
    // A partition drop folds into each aggregate the catalog roots here. This
    // node's catalog covers the entry: the apply waited for its metadata floor.
    let aggregates = match op {
        RetentionOp::TimeseriesPartitions { collection, .. }
        | RetentionOp::TemporalColumnar { collection, .. } => aggregates_rooted_at(
            state,
            &state.retention_policy_registry,
            database_id.as_u64(),
            tenant_id.as_u64(),
            collection,
        )?,
        RetentionOp::ContinuousAggregates { .. }
        | RetentionOp::TemporalEdges { .. }
        | RetentionOp::TemporalDocumentStrict { .. }
        | RetentionOp::TemporalCrdt { .. }
        | RetentionOp::TemporalArray { .. } => Vec::new(),
    };
    let plan = op.plan(tenant_id, database_id, aggregates);
    let outcome = gather_all_cores(
        state,
        tenant_id,
        database_id,
        plan,
        TraceId::generate(),
        None,
    )
    .await?;
    let Some((engine, object, cutoff)) = audited(op) else {
        return Ok(());
    };
    let purged: u64 = outcome
        .payloads()
        .iter()
        .map(|payload| parse_count_from_payload(engine, payload))
        .sum();
    if purged == 0 {
        return Ok(());
    }
    state.wal.appender(NO_APPLY_KEY).append_temporal_purge(
        tenant_id,
        engine.wire_tag(),
        object,
        cutoff,
        purged,
    )?;
    tracing::info!(
        tenant = tenant_id.as_u64(),
        object,
        ?engine,
        purged,
        cutoff_ms = cutoff,
        "bitemporal audit-retention purge"
    );
    Ok(())
}

/// The audit record's engine, object and cutoff. A partition drop by age and a
/// bucket pass are no audit purge.
fn audited(op: &RetentionOp) -> Option<(BitemporalEngineKind, &str, i64)> {
    match op {
        RetentionOp::TimeseriesPartitions { .. } | RetentionOp::ContinuousAggregates { .. } => None,
        RetentionOp::TemporalEdges {
            collection,
            cutoff_system_ms,
        } => Some((
            BitemporalEngineKind::EdgeStore,
            collection,
            *cutoff_system_ms,
        )),
        RetentionOp::TemporalDocumentStrict {
            collection,
            cutoff_system_ms,
        } => Some((
            BitemporalEngineKind::DocumentStrict,
            collection,
            *cutoff_system_ms,
        )),
        RetentionOp::TemporalColumnar {
            collection,
            cutoff_system_ms,
            ..
        } => Some((
            BitemporalEngineKind::Columnar,
            collection,
            *cutoff_system_ms,
        )),
        RetentionOp::TemporalCrdt {
            collection,
            cutoff_system_ms,
        } => Some((BitemporalEngineKind::Crdt, collection, *cutoff_system_ms)),
        RetentionOp::TemporalArray {
            array,
            cutoff_system_ms,
        } => Some((BitemporalEngineKind::Array, array, *cutoff_system_ms)),
    }
}
