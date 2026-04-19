//! RESTORE TENANT orchestrator.
//!
//! Validates a backup envelope, merges all sections into a single
//! `TenantDataSnapshot`, then splits the merged snapshot into per-node
//! sub-snapshots according to the *current* cluster topology and
//! dispatches `MetaOp::RestoreTenantSnapshot` to each owning node.
//!
//! This is what makes the restore topology-drift safe: keys are
//! re-bucketed under the live `vshard_for_collection` mapping, not
//! the source-cluster mapping recorded in the envelope.
//!
//! Single-node mode is the degenerate case: routing absent → one
//! bucket, dispatched locally.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::routing::{VSHARD_COUNT, vshard_for_collection};
use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, RaftRpc, TypedClusterError};
use nodedb_types::backup_envelope::{
    DEFAULT_MAX_TOTAL_BYTES, EnvelopeError, parse as parse_envelope,
};
use serde::Serialize;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{MetaOp, wire as plan_wire};
use crate::control::server::pgwire::ddl::sync_dispatch;
use crate::control::state::SharedState;
use crate::types::{TenantDataSnapshot, TenantId};

const NODE_RESTORE_TIMEOUT: Duration = Duration::from_secs(120);

/// Aggregate stats returned to the client at the end of a restore.
#[derive(Debug, Default, Clone, Serialize)]
pub struct RestoreStats {
    pub tenant_id: u32,
    pub dry_run: bool,
    pub sections: u16,
    pub source_vshard_count: u16,
    pub documents: usize,
    pub indexes: usize,
    pub edges: usize,
    pub vectors: usize,
    pub kv_tables: usize,
    pub crdt_state: usize,
    pub timeseries: usize,
    pub nodes_dispatched: usize,
    /// Entries whose key/collection name didn't parse — likely a
    /// corrupted snapshot. Each is logged at WARN with its key prefix.
    /// A non-zero count after a "successful" restore means the operator
    /// should investigate the source backup.
    pub malformed_keys: usize,
    /// Entries whose vshard owner couldn't be resolved at restore time
    /// (no current leader for the target vshard). Each is delivered to
    /// the local node — Raft replication then redistributes once the
    /// leader is back. Non-zero is informational, not necessarily an
    /// error.
    pub route_fallbacks: usize,
}

/// Restore a tenant from a fully-buffered backup envelope.
///
/// `tenant_id` is the request-scoped tenant ID; the envelope's
/// embedded `tenant_id` must match.
pub async fn restore_tenant(
    state: &Arc<SharedState>,
    tenant_id: u32,
    envelope_bytes: &[u8],
    dry_run: bool,
) -> Result<RestoreStats, Error> {
    let env = parse_envelope(envelope_bytes, DEFAULT_MAX_TOTAL_BYTES).map_err(envelope_to_err)?;
    if env.meta.tenant_id != tenant_id {
        // Mismatch is a hard error — request is for a different tenant.
        return Err(Error::Internal {
            detail: format!(
                "backup tenant mismatch: envelope has {}, request is for {}",
                env.meta.tenant_id, tenant_id
            ),
        });
    }

    // Staleness gate. An envelope whose captured watermark is older
    // than any dispatch this destination cluster has already observed
    // for the same tenant would silently roll back newer committed
    // writes. Reject unconditionally today — a `FORCE` override is
    // the operator's escape hatch and is a separate DDL decision.
    // Envelopes with `snapshot_watermark == 0` come from a source
    // that did not capture a watermark (pre-gate envelopes); let them
    // through for compatibility — the gate only rejects real
    // ordered-against-ordered comparisons.
    if !dry_run && env.meta.snapshot_watermark != 0 {
        let current_high_water = state
            .tenant_write_hlc
            .lock()
            .ok()
            .and_then(|map| map.get(&tenant_id).copied())
            .unwrap_or(0);
        if env.meta.snapshot_watermark < current_high_water {
            return Err(Error::Internal {
                detail: format!(
                    "restore refused: envelope watermark {} is older than the \
                     destination cluster's last observed write-HLC {} for tenant \
                     {} — newer writes would be silently overwritten",
                    env.meta.snapshot_watermark, current_high_water, tenant_id
                ),
            });
        }
    }

    let mut stats = RestoreStats {
        tenant_id,
        dry_run,
        sections: env.sections.len() as u16,
        source_vshard_count: env.meta.source_vshard_count,
        ..Default::default()
    };

    // Merge every section into one snapshot.
    let merged = merge_sections(&env.sections)?;
    stats.documents = merged.documents.len();
    stats.indexes = merged.indexes.len();
    stats.edges = merged.edges.len();
    stats.vectors = merged.vectors.len();
    stats.kv_tables = merged.kv_tables.len();
    stats.crdt_state = merged.crdt_state.len();
    stats.timeseries = merged.timeseries.len();

    if dry_run {
        return Ok(stats);
    }

    // Split per current topology, dispatch each.
    let SplitOutput {
        buckets,
        malformed_keys,
        route_fallbacks,
    } = split_by_current_topology(state, tenant_id, merged);
    stats.nodes_dispatched = buckets.len();
    stats.malformed_keys = malformed_keys;
    stats.route_fallbacks = route_fallbacks;
    if malformed_keys > 0 {
        tracing::warn!(
            tenant_id,
            count = malformed_keys,
            "restore: snapshot contained keys that did not parse — possible corruption"
        );
    }
    if route_fallbacks > 0 {
        tracing::warn!(
            tenant_id,
            count = route_fallbacks,
            "restore: routed some entries to local node because no current leader was visible"
        );
    }
    let split = buckets;
    for (node_id, sub) in split {
        let payload = zerompk::to_msgpack_vec(&sub).map_err(|e| Error::Internal {
            detail: format!("restore: snapshot encode failed: {e}"),
        })?;
        let plan = PhysicalPlan::Meta(MetaOp::RestoreTenantSnapshot {
            tenant_id,
            snapshot: payload,
        });
        if is_self(state, node_id) {
            sync_dispatch::dispatch_async(
                state,
                TenantId::new(tenant_id),
                "__system",
                plan,
                NODE_RESTORE_TIMEOUT,
            )
            .await?;
        } else {
            dispatch_remote(state, node_id, tenant_id, plan).await?;
        }
    }

    Ok(stats)
}

fn merge_sections(
    sections: &[nodedb_types::backup_envelope::Section],
) -> Result<TenantDataSnapshot, Error> {
    let mut merged = TenantDataSnapshot::default();
    for section in sections {
        // Decoder errors must NOT echo deserializer context to the client.
        let snap: TenantDataSnapshot =
            zerompk::from_msgpack(&section.body).map_err(|_| Error::Internal {
                detail: "invalid backup format: section payload is not a tenant snapshot".into(),
            })?;
        merged.documents.extend(snap.documents);
        merged.indexes.extend(snap.indexes);
        merged.edges.extend(snap.edges);
        merged.vectors.extend(snap.vectors);
        merged.kv_tables.extend(snap.kv_tables);
        merged.crdt_state.extend(snap.crdt_state);
        merged.timeseries.extend(snap.timeseries);
    }
    Ok(merged)
}

/// Bucketed output from `split_by_current_topology`.
struct SplitOutput {
    buckets: BTreeMap<u64, TenantDataSnapshot>,
    /// Entries whose key/collection couldn't be parsed (corrupt snapshot).
    malformed_keys: usize,
    /// Entries whose target vshard had no current leader; routed to self
    /// as a fallback (Raft replication redistributes once a leader exists).
    route_fallbacks: usize,
}

/// Routing decision for a single entry.
enum RouteOutcome {
    /// Resolved cleanly to this node.
    Routed(u64),
    /// Key/collection parse failed — corrupt snapshot signal.
    Malformed,
    /// Parsed fine but no current leader — fallback to self.
    NoLeader,
}

/// Bucket the merged snapshot per current vshard ownership.
///
/// Replicated-by-design data (graph edges, CRDT state) goes to every
/// owning node — those engines tolerate (and rely on) replicated state.
fn split_by_current_topology(
    state: &SharedState,
    tenant_id: u32,
    merged: TenantDataSnapshot,
) -> SplitOutput {
    let routing = state.cluster_routing.as_ref().map(|r| r.read().unwrap());
    let single_node = routing.is_none() || state.cluster_transport.is_none();

    if single_node {
        // Degenerate case: everything to self.
        let mut out = BTreeMap::new();
        out.insert(state.node_id, merged);
        return SplitOutput {
            buckets: out,
            malformed_keys: 0,
            route_fallbacks: 0,
        };
    }
    let routing = routing.unwrap();

    // Pre-compute every owning node so we can broadcast the by-design
    // replicated engines without missing one.
    let mut all_owners = BTreeMap::<u64, TenantDataSnapshot>::new();
    for vshard in 0..VSHARD_COUNT {
        if let Ok(node) = routing.leader_for_vshard(vshard)
            && node != 0
        {
            all_owners.entry(node).or_default();
        }
    }
    if all_owners.is_empty() {
        all_owners.insert(state.node_id, TenantDataSnapshot::default());
    }

    let route_collection = |coll: &str| -> RouteOutcome {
        let v = vshard_for_collection(coll);
        match routing.leader_for_vshard(v) {
            Ok(leader) if leader != 0 => RouteOutcome::Routed(leader),
            _ => RouteOutcome::NoLeader,
        }
    };
    let route_key = |key: &str| -> RouteOutcome {
        match extract_collection(key, tenant_id) {
            Some(coll) => route_collection(coll),
            None => RouteOutcome::Malformed,
        }
    };

    let mut malformed = 0usize;
    let mut fallbacks = 0usize;
    let mut resolve = |outcome: RouteOutcome, key: Option<&str>| -> u64 {
        match outcome {
            RouteOutcome::Routed(node) => node,
            RouteOutcome::Malformed => {
                malformed += 1;
                if let Some(k) = key {
                    let prefix: String = k.chars().take(64).collect();
                    tracing::warn!(tenant_id, key_prefix = %prefix, "restore: malformed key");
                }
                state.node_id
            }
            RouteOutcome::NoLeader => {
                fallbacks += 1;
                state.node_id
            }
        }
    };

    for entry in merged.documents {
        let node = resolve(route_key(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().documents.push(entry);
    }
    for entry in merged.indexes {
        let node = resolve(route_key(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().indexes.push(entry);
    }
    for entry in merged.vectors {
        // vector key prefix is "{tid}:{collection}".
        let node = resolve(route_key(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().vectors.push(entry);
    }
    for entry in merged.kv_tables {
        // kv collection name is the key (no tenant prefix here).
        let node = resolve(route_collection(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().kv_tables.push(entry);
    }
    for entry in merged.timeseries {
        let node = resolve(route_key(&entry.0), Some(&entry.0));
        all_owners.entry(node).or_default().timeseries.push(entry);
    }

    // Replicated-by-design: every owning node gets a copy.
    for entry in &merged.edges {
        for snap in all_owners.values_mut() {
            snap.edges.push(entry.clone());
        }
    }
    for entry in &merged.crdt_state {
        for snap in all_owners.values_mut() {
            snap.crdt_state.push(entry.clone());
        }
    }

    SplitOutput {
        buckets: all_owners,
        malformed_keys: malformed,
        route_fallbacks: fallbacks,
    }
}

fn extract_collection(key: &str, tenant_id: u32) -> Option<&str> {
    let prefix_owned = format!("{tenant_id}:");
    let after = key.strip_prefix(prefix_owned.as_str())?;
    let coll = after.split(['\0', ':']).next()?;
    if coll.is_empty() { None } else { Some(coll) }
}

fn is_self(state: &SharedState, node_id: u64) -> bool {
    node_id == state.node_id || node_id == 0 || state.cluster_transport.is_none()
}

async fn dispatch_remote(
    state: &Arc<SharedState>,
    node_id: u64,
    tenant_id: u32,
    plan: PhysicalPlan,
) -> Result<(), Error> {
    let transport = state
        .cluster_transport
        .as_ref()
        .ok_or_else(|| Error::Internal {
            detail: format!("restore: cluster_transport unavailable but node {node_id} is remote"),
        })?;
    let plan_bytes = plan_wire::encode(&plan).map_err(|e| Error::Internal {
        detail: format!("restore: plan encode failed: {e}"),
    })?;
    let req = RaftRpc::ExecuteRequest(ExecuteRequest {
        plan_bytes,
        tenant_id,
        deadline_remaining_ms: NODE_RESTORE_TIMEOUT.as_millis() as u64,
        trace_id: 0,
        descriptor_versions: Vec::new(),
    });
    let resp = transport
        .send_rpc(node_id, req)
        .await
        .map_err(|e| Error::Internal {
            detail: format!("restore RPC to node {node_id} failed: {e}"),
        })?;
    match resp {
        RaftRpc::ExecuteResponse(ExecuteResponse { success: true, .. }) => Ok(()),
        RaftRpc::ExecuteResponse(ExecuteResponse {
            error: Some(err), ..
        }) => Err(map_typed_error(err, node_id)),
        RaftRpc::ExecuteResponse(_) => Err(Error::Internal {
            detail: format!("restore: empty error response from node {node_id}"),
        }),
        other => Err(Error::Internal {
            detail: format!(
                "restore: unexpected RPC response variant from node {node_id}: {other:?}"
            ),
        }),
    }
}

fn map_typed_error(err: TypedClusterError, node_id: u64) -> Error {
    match err {
        TypedClusterError::Internal { message, .. } => Error::Internal {
            detail: format!("restore node {node_id}: {message}"),
        },
        TypedClusterError::DeadlineExceeded { elapsed_ms } => Error::Internal {
            detail: format!("restore node {node_id}: deadline exceeded after {elapsed_ms}ms"),
        },
        TypedClusterError::NotLeader { .. } => Error::Internal {
            detail: format!("restore node {node_id}: routed to non-leader"),
        },
        TypedClusterError::DescriptorMismatch { collection, .. } => Error::Internal {
            detail: format!(
                "restore node {node_id}: descriptor mismatch on collection {collection}"
            ),
        },
    }
}

/// Map envelope-level errors to a generic `Error::Internal` with a
/// scrubbed message — never echoes deserializer context to the client.
fn envelope_to_err(e: EnvelopeError) -> Error {
    let msg = match e {
        EnvelopeError::TenantMismatch { expected, actual } => {
            format!("backup tenant mismatch: expected {expected}, got {actual}")
        }
        EnvelopeError::OverSizeTotal { cap } => format!("backup exceeds size cap of {cap} bytes"),
        EnvelopeError::OverSizeSection { cap } => {
            format!("backup section exceeds size cap of {cap} bytes")
        }
        EnvelopeError::UnsupportedVersion(v) => format!("unsupported backup version: {v}"),
        // Everything else — bad magic, crc mismatch, truncation — gets the
        // same generic message so attackers learn nothing about format.
        _ => "invalid backup format".to_string(),
    };
    Error::Internal { detail: msg }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_collection_strips_prefix() {
        assert_eq!(extract_collection("7:users:doc1", 7), Some("users"));
        assert_eq!(extract_collection("7:users\u{0}doc1", 7), Some("users"));
        assert_eq!(extract_collection("7:users", 7), Some("users"));
        assert_eq!(extract_collection("8:users:doc1", 7), None); // wrong tenant
        assert_eq!(extract_collection("7:", 7), None);
    }
}
