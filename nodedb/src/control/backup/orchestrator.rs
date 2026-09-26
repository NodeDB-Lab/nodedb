// SPDX-License-Identifier: BUSL-1.1

//! BACKUP TENANT orchestrator.
//!
//! Discovers every node that holds a vShard owning some of the
//! tenant's data, dispatches `MetaOp::CreateTenantSnapshot` to each
//! (local SPSC for self, `RaftRpc::ExecuteRequest` for remotes),
//! and packs the gathered per-node snapshots into a `BackupEnvelope`.
//!
//! Single-node mode is the degenerate case: routing table absent
//! (or 1 node) → 1 section, origin = self.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use nodedb_cluster::routing::VSHARD_COUNT;
use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, RaftRpc, TypedClusterError};
use nodedb_types::backup_envelope::{EnvelopeMeta, EnvelopeWriter};

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::ddl::sync_dispatch;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};
use nodedb_physical::physical_plan::{MetaOp, wire as plan_wire};

/// Default per-node snapshot dispatch timeout.
const NODE_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(120);

/// Build a complete tenant backup envelope by fanning out across the
/// cluster, gathering each node's slice, and framing the result.
///
/// Single-node and cluster paths converge here — a single-node server
/// produces a one-section envelope with origin = self.
pub async fn backup_tenant(state: &Arc<SharedState>, tenant_id: u64) -> Result<Bytes, Error> {
    // Assign every vshard to exactly ONE source node (the leader of its Raft
    // group, or — when no leader is elected yet — the lowest-id member), and
    // gather only from those source nodes. Under RF>1 every replica holds the
    // full vshard data, so gathering from all members and merging would
    // MULTIPLY append-style engine rows (columnar / timeseries) by the
    // replication factor. Filtering each source node's snapshot to the vshards
    // it owns makes the union cover each vshard exactly once.
    let assignment = source_assignment(state);

    // The envelope watermark is the consistent cut: every user write
    // committed below it has applied before the snapshots below, and every
    // write committed at or above it refuses a restore of this envelope.
    let snapshot_watermark = super::cut::consistent_cut(state, tenant_id).await?;
    // This node took the cut already. A remote source node takes it at the
    // same watermark before it snapshots.
    let local_plan = PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot {
        tenant_id,
        cut_watermark: None,
    });
    let remote_plan = PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot {
        tenant_id,
        cut_watermark: Some(snapshot_watermark),
    });

    // Collect per-node sections.
    let mut sections = Vec::with_capacity(assignment.len());
    for (node_id, source_vshards) in assignment {
        let body = if is_self(state, node_id) {
            snapshot_self(state, tenant_id, &local_plan).await?
        } else {
            snapshot_remote(state, node_id, tenant_id, &remote_plan).await?
        };
        // Single-node / single-replica: the node owns every vshard it leads, so
        // the filter retains everything (no-op). Under RF>1: keep only the
        // vshards this node is the assigned source for; the other replicas'
        // copies are dropped here so the restore merge sums disjoint sections.
        let body = filter_node_snapshot(body, tenant_id, &source_vshards)?;
        sections.push((node_id, body));
    }

    let meta = EnvelopeMeta {
        tenant_id,
        source_vshard_count: VSHARD_COUNT as u16,
        hash_seed: 0, // VSHARD_COUNT-derived hash; no seed today
        snapshot_watermark,
    };
    let mut writer = EnvelopeWriter::new(meta);

    for (node_id, body) in sections {
        writer
            .push_section(node_id, body)
            .map_err(|e| Error::Internal {
                detail: format!("backup envelope: {e}"),
            })?;
    }

    // Metadata sections: catalog rows + source-side tombstones. These
    // live in dedicated sections with sentinel origin_node_ids so the
    // restore path can distinguish them from per-node engine data.
    // Without these, a backup taken during a collection's retention
    // window loses its soft-deleted row (UNDROP can't work after
    // restore), and a restore whose source has already purged a
    // collection can resurrect rows that were properly reaped.
    push_metadata_sections(state, tenant_id, &mut writer)?;

    // A backup KEK must be configured; plaintext backup envelopes are no
    // longer supported.
    let envelope_bytes = match &state.backup_kek {
        Some(kek) => writer
            .finalize_encrypted(kek)
            .map_err(|e| Error::Internal {
                detail: format!("backup envelope encryption: {e}"),
            })?,
        None => {
            return Err(Error::Internal {
                detail: "backup: no [backup_encryption] KEK configured; \
                         plaintext backup envelopes are no longer supported"
                    .into(),
            });
        }
    };
    Ok(Bytes::from(envelope_bytes))
}

/// Assign every vshard to exactly ONE source node, returning the per-source
/// `(node_id, owned_vshard_set)` pairs to gather from.
///
/// Source of a vshard = the leader of its Raft group; when the group has no
/// elected leader yet (`leader == 0`), fall back deterministically to the
/// lowest-id member so the vshard is still captured exactly once (dropping it
/// would be data loss; assigning it to two nodes would duplicate it). The
/// metadata group (0) owns no vshards, so it never appears.
///
/// In single-node mode (no routing table) returns `[(self.node_id, ALL vshards)]`
/// — the filter is then a no-op and every section is captured once, matching
/// the previous single-section behavior.
fn source_assignment(state: &SharedState) -> Vec<(u64, HashSet<u32>)> {
    let Some(routing) = state.cluster_routing.as_ref() else {
        return vec![(state.node_id, (0..VSHARD_COUNT).collect())];
    };
    let table = routing.read().unwrap_or_else(|p| p.into_inner());

    let mut by_node: BTreeMap<u64, HashSet<u32>> = BTreeMap::new();
    for vshard in 0..VSHARD_COUNT {
        let Ok(group_id) = table.group_for_vshard(vshard) else {
            continue;
        };
        let Some(info) = table.group_info(group_id) else {
            continue;
        };
        let source = if info.leader != 0 {
            info.leader
        } else {
            // No elected leader: deterministic lowest-id member so the vshard
            // is captured by exactly one node.
            match info.members.iter().copied().min() {
                Some(m) => m,
                None => continue,
            }
        };
        by_node.entry(source).or_default().insert(vshard);
    }

    if by_node.is_empty() {
        return vec![(state.node_id, (0..VSHARD_COUNT).collect())];
    }
    by_node.into_iter().collect()
}

/// Decode a gathered per-node `TenantDataSnapshot`, filter it in place to the
/// vshards this node is the assigned source for, and re-encode it.
///
/// The per-section vshard classification is shared with the Raft snapshot SEND
/// builder via `snapshot_keys::retain_tenant_data_for_vshards`. The vshard-of
/// closure is the canonical routing function, matching the snapshot builder.
fn filter_node_snapshot(
    body: Vec<u8>,
    tenant_id: u64,
    source_vshards: &HashSet<u32>,
) -> Result<Vec<u8>, Error> {
    let mut snap: crate::types::TenantDataSnapshot =
        zerompk::from_msgpack(&body).map_err(|e| Error::Internal {
            detail: format!("backup: decode per-node snapshot: {e}"),
        })?;
    crate::control::backup::snapshot_keys::retain_tenant_data_for_vshards(
        &mut snap,
        tenant_id,
        source_vshards,
        |collection| {
            nodedb_cluster::routing::vshard_for_collection(DatabaseId::DEFAULT, collection)
        },
    );
    zerompk::to_msgpack_vec(&snap).map_err(|e| Error::Internal {
        detail: format!("backup: re-encode filtered snapshot: {e}"),
    })
}

/// Push the metadata sections: the tenant's catalog rows, its PK-to-surrogate
/// binds, and its source-side tombstones. A catalog read or encode error fails
/// the backup: an envelope without these sections restores rows a point
/// lookup cannot find, or resurrects a purged collection.
fn push_metadata_sections(
    state: &SharedState,
    tenant_id: u64,
    writer: &mut EnvelopeWriter,
) -> Result<(), Error> {
    let catalog = state.credentials.catalog();
    let collections: Vec<_> = catalog
        .load_all_collections(DatabaseId::DEFAULT)?
        .into_iter()
        .filter(|coll| coll.tenant_id == tenant_id)
        .collect();

    let mut blobs = Vec::with_capacity(collections.len());
    for coll in &collections {
        blobs.push(nodedb_types::backup_envelope::StoredCollectionBlob {
            name: coll.name.clone(),
            bytes: encode_section_part("catalog row", coll)?,
        });
    }
    if !blobs.is_empty() {
        push_encoded(
            writer,
            nodedb_types::backup_envelope::SECTION_ORIGIN_CATALOG_ROWS,
            "catalog rows",
            &blobs,
        )?;
    }

    // PK→surrogate identity map for the tenant's collections. This is
    // DATA-derived per-node state that the per-node engine sections do NOT
    // carry (the Data-Plane snapshot handler has no catalog access). Without
    // it a restored node has documents but cannot resolve PK point-lookups
    // (`WHERE id=<pk>`): full scans work, point-lookups silently miss. The
    // restore path rebinds these into the destination catalog.
    let mut binds: Vec<nodedb_types::backup_envelope::SurrogateBindBlob> = Vec::new();
    for coll in &collections {
        let rows = catalog.scan_surrogates_for_collection(
            DatabaseId::DEFAULT,
            TenantId::new(tenant_id),
            &coll.name,
        )?;
        for (pk, surrogate) in rows {
            binds.push(nodedb_types::backup_envelope::SurrogateBindBlob {
                tenant_id,
                collection: coll.name.clone(),
                pk,
                surrogate: surrogate.as_u32(),
            });
        }
    }
    if !binds.is_empty() {
        push_encoded(
            writer,
            nodedb_types::backup_envelope::SECTION_ORIGIN_SURROGATE_PK,
            "surrogate pk",
            &binds,
        )?;
    }

    let mut tombs: Vec<nodedb_types::backup_envelope::SourceTombstoneEntry> = Vec::new();
    for (database_id, tid, name, purge_lsn) in catalog.load_wal_tombstones()?.iter() {
        if database_id == DatabaseId::DEFAULT.as_u64() && tid == tenant_id {
            tombs.push(nodedb_types::backup_envelope::SourceTombstoneEntry {
                collection: name.to_string(),
                purge_lsn,
            });
        }
    }
    if !tombs.is_empty() {
        push_encoded(
            writer,
            nodedb_types::backup_envelope::SECTION_ORIGIN_SOURCE_TOMBSTONES,
            "source tombstones",
            &tombs,
        )?;
    }
    Ok(())
}

/// Encode one part of a metadata section.
fn encode_section_part<T: zerompk::ToMessagePack>(what: &str, value: &T) -> Result<Vec<u8>, Error> {
    zerompk::to_msgpack_vec(value).map_err(|e| Error::Serialization {
        format: "msgpack".into(),
        detail: format!("backup envelope ({what}): encode: {e}"),
    })
}

/// Encode `value` and push it as the section `origin`.
fn push_encoded<T: zerompk::ToMessagePack>(
    writer: &mut EnvelopeWriter,
    origin: u64,
    what: &str,
    value: &T,
) -> Result<(), Error> {
    let body = encode_section_part(what, value)?;
    writer
        .push_section(origin, body)
        .map_err(|e| Error::Internal {
            detail: format!("backup envelope ({what}): {e}"),
        })
}

fn is_self(state: &SharedState, node_id: u64) -> bool {
    node_id == state.node_id || node_id == 0 || state.cluster_transport.is_none()
}

async fn snapshot_self(
    state: &Arc<SharedState>,
    tenant_id: u64,
    plan: &PhysicalPlan,
) -> Result<Vec<u8>, Error> {
    sync_dispatch::dispatch_system(
        state,
        sync_dispatch::SystemTask::new(
            sync_dispatch::SystemReason::BackupRestore,
            TenantId::new(tenant_id),
            // TODO(A8-followup): backup/restore not yet multi-database.
            DatabaseId::DEFAULT,
            "__system",
            plan.clone(),
        ),
        NODE_SNAPSHOT_TIMEOUT,
    )
    .await
}

async fn snapshot_remote(
    state: &Arc<SharedState>,
    node_id: u64,
    tenant_id: u64,
    plan: &PhysicalPlan,
) -> Result<Vec<u8>, Error> {
    let transport = state
        .cluster_transport
        .as_ref()
        .ok_or_else(|| Error::Internal {
            detail: format!("backup: cluster_transport unavailable but node {node_id} is remote"),
        })?;

    let plan_bytes = plan_wire::encode(plan).map_err(|e| Error::Internal {
        detail: format!("backup: plan encode failed: {e}"),
    })?;
    let req = RaftRpc::ExecuteRequest(ExecuteRequest {
        plan_bytes,
        tenant_id,
        database_id: DatabaseId::DEFAULT.as_u64(),
        deadline_remaining_ms: NODE_SNAPSHOT_TIMEOUT.as_millis() as u64,
        trace_id: TraceId::generate().0,
        descriptor_versions: Vec::new(),
        // Backup snapshot dispatch is not session-transaction-scoped.
        txn_id: None,
    });

    let resp = transport
        .send_rpc(node_id, req)
        .await
        .map_err(|e| Error::Internal {
            detail: format!("backup: snapshot RPC to node {node_id} failed: {e}"),
        })?;
    match resp {
        RaftRpc::ExecuteResponse(ExecuteResponse {
            success: true,
            mut payloads,
            ..
        }) => {
            // CreateTenantSnapshot returns exactly one payload.
            if payloads.len() != 1 {
                return Err(Error::Internal {
                    detail: format!(
                        "backup: expected 1 payload from node {node_id}, got {}",
                        payloads.len()
                    ),
                });
            }
            Ok(payloads.remove(0))
        }
        RaftRpc::ExecuteResponse(ExecuteResponse {
            error: Some(err), ..
        }) => Err(map_typed_error(err, node_id)),
        RaftRpc::ExecuteResponse(_) => Err(Error::Internal {
            detail: format!("backup: empty error response from node {node_id}"),
        }),
        other => Err(Error::Internal {
            detail: format!(
                "backup: unexpected RPC response variant from node {node_id}: {other:?}"
            ),
        }),
    }
}

fn map_typed_error(err: TypedClusterError, node_id: u64) -> Error {
    match err {
        TypedClusterError::Internal { message, .. } => Error::Internal {
            detail: format!("backup node {node_id}: {message}"),
        },
        TypedClusterError::DeadlineExceeded { elapsed_ms } => Error::Internal {
            detail: format!("backup node {node_id}: deadline exceeded after {elapsed_ms}ms"),
        },
        TypedClusterError::NotLeader { .. } => Error::Internal {
            detail: format!("backup node {node_id}: snapshot RPC routed to non-leader"),
        },
        TypedClusterError::DescriptorMismatch { collection, .. } => Error::Internal {
            detail: format!(
                "backup node {node_id}: descriptor mismatch on collection {collection}"
            ),
        },
        // Keep the shard's verdict typed: a backup snapshot refused by the
        // Data Plane must not read as a generic internal backup fault.
        TypedClusterError::DataPlane { code } => Error::DataPlane(code.into()),
        // A constraint verdict keeps its collection and kind, so the client
        // reads the SQLSTATE the refusing shard meant.
        TypedClusterError::RejectedConstraint {
            collection,
            constraint,
            detail,
        } => Error::RejectedConstraint {
            collection,
            constraint,
            detail,
        },
    }
}
