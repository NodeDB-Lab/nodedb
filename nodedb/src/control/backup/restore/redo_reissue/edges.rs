// SPDX-License-Identifier: BUSL-1.1

//! Restored graph edges as edge units.
//!
//! A backup carries every edge version under its versioned key,
//! `"{collection}\x00{src}\x00{label}\x00{dst}\x00{system_from:020}"`. Each
//! version re-issues at its original `system_from`, so the restored edge keeps
//! its history and its valid-from time. A tombstone version re-issues as a
//! tombstone at its `system_from`. Every replica updates its CSR index and its
//! node identities as it installs each version.
//!
//! Each version re-issues to both endpoint homes, `from_key(src)` and
//! `from_key(dst)`, as every live edge write reaches them. Endpoint identities
//! bind through the routed surrogate exchange, as a live edge write binds
//! them.
//!
//! The key's collection is the name the source Data Plane stored it under.
//! Each version re-issues under the destination-qualified name, and binds its
//! node identities under the bare name in the destination database.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_physical::physical_plan::RestoredEdgeVersion;
use nodedb_types::Surrogate;

use crate::control::server::surrogate_exchange::assign_surrogates_routed;
use crate::control::state::SharedState;
use crate::control::surrogate::CarriedIdentity;
use crate::engine::graph::edge_store::{
    EdgeValuePayload, is_gdpr_erasure, is_tombstone, parse_versioned_edge_key,
};
use crate::types::{DatabaseId, RecordHomes, TenantId, TraceId};

use super::super::target::{DatabaseTarget, RestoredName};
use super::units::{CollectionEdges, EdgeUnit};

fn malformed(key: &str) -> crate::Error {
    let prefix: String = key.chars().take(64).collect();
    crate::Error::Serialization {
        format: "backup".into(),
        detail: format!("restore: edge key '{prefix:?}' is malformed"),
    }
}

/// The surrogate of every endpoint node, by bare edge collection, then node id.
type NodeSurrogates = BTreeMap<String, BTreeMap<String, Surrogate>>;

/// Resolve every endpoint node of `nodes` (bare collection → node ids), one
/// routed batch per collection.
///
/// The backup's bind wins: the restore rebinds it on this node before any
/// re-issue, and the routed exchange answers this node's catalog first. A
/// node the backup carries no bind for is bound on the leader of the
/// collection's home, as a live edge write binds it. Either way the batch
/// carries the identity, and every replica of each home binds it first-wins.
async fn resolve_node_surrogates(
    state: &SharedState,
    database_id: DatabaseId,
    tenant: TenantId,
    nodes: BTreeMap<String, BTreeSet<String>>,
) -> crate::Result<NodeSurrogates> {
    let mut out = NodeSurrogates::new();
    for (collection, ids) in nodes {
        let key = nodedb_types::CollectionKey::from_bare(database_id, &collection);
        let pks: Vec<&[u8]> = ids.iter().map(|id| id.as_bytes()).collect();
        let bound = assign_surrogates_routed(state, key, tenant, &pks, TraceId::ZERO).await?;
        if bound.len() != ids.len() {
            return Err(crate::Error::Internal {
                detail: format!(
                    "restore: {} of {} edge endpoints in '{collection}' got a surrogate",
                    bound.len(),
                    ids.len()
                ),
            });
        }
        let by_id = ids.into_iter().zip(bound).collect();
        out.insert(collection, by_id);
    }
    Ok(out)
}

/// The identity of node `node_id` in edge collection `collection`, resolved
/// by [`resolve_node_surrogates`].
fn node_identity(
    resolved: &NodeSurrogates,
    collection: &str,
    node_id: &str,
) -> crate::Result<CarriedIdentity> {
    let surrogate = resolved
        .get(collection)
        .and_then(|by_id| by_id.get(node_id))
        .copied()
        .ok_or_else(|| crate::Error::Internal {
            detail: format!("restore: edge endpoint '{node_id}' in '{collection}' is unresolved"),
        })?;
    Ok(CarriedIdentity {
        collection: collection.to_string(),
        pk_bytes: node_id.as_bytes().to_vec(),
        surrogate,
    })
}

/// One edge version of the collection `name` as a unit.
fn edge_unit(
    resolved: &NodeSurrogates,
    name: &RestoredName,
    key: &str,
    value: &[u8],
) -> crate::Result<EdgeUnit> {
    let (_, src_id, label, dst_id, system_from) =
        parse_versioned_edge_key(key).ok_or_else(|| malformed(key))?;
    let collection = name.bare.as_str();
    if is_gdpr_erasure(value) {
        return Err(crate::Error::Serialization {
            format: "backup".into(),
            detail: format!(
                "restore: edge version {system_from} in '{collection}' is an erasure marker, \
                 which no write path records"
            ),
        });
    }
    let (properties, valid_time) = if is_tombstone(value) {
        (None, None)
    } else {
        let payload = EdgeValuePayload::decode(value)?;
        (
            Some(payload.properties),
            Some((payload.valid_from_ms, payload.valid_until_ms)),
        )
    };
    let src = node_identity(resolved, collection, src_id)?;
    let dst = node_identity(resolved, collection, dst_id)?;
    Ok(EdgeUnit {
        version: RestoredEdgeVersion {
            collection: name.stored.to_string(),
            src_id: src_id.to_string(),
            label: label.to_string(),
            dst_id: dst_id.to_string(),
            src_surrogate: src.surrogate.as_u32(),
            dst_surrogate: dst.surrogate.as_u32(),
            system_from,
            properties,
            valid_time,
        },
        homes: RecordHomes::edge(src_id, dst_id),
        identities: vec![src, dst],
    })
}

/// Every restored edge version of `tenant_id` in one database, one unit per
/// version, grouped by edge collection in key order: each edge's versions in
/// system-time order. The edge section of a database's data section holds
/// that database's edges only.
///
/// Returns the number of distinct edge versions and the grouped units.
pub(super) async fn edge_units(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    edges: Vec<(String, Vec<u8>)>,
) -> crate::Result<(usize, Vec<CollectionEdges>)> {
    let database_id = target.dest;
    let tenant = TenantId::new(tenant_id);
    let mut by_key: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for (key, value) in edges {
        by_key.insert(key, value);
    }
    // Every endpoint node, by bare collection, resolves before any unit is
    // built: one routed batch per collection.
    let mut nodes: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for key in by_key.keys() {
        let (stored, src_id, _, dst_id, _) =
            parse_versioned_edge_key(key).ok_or_else(|| malformed(key))?;
        let name = target.resolve(stored)?;
        let ids = nodes.entry(name.bare).or_default();
        ids.insert(src_id.to_string());
        ids.insert(dst_id.to_string());
    }
    let resolved = resolve_node_surrogates(state, database_id, tenant, nodes).await?;
    let mut grouped: BTreeMap<String, Vec<EdgeUnit>> = BTreeMap::new();
    for (key, value) in &by_key {
        let (stored, ..) = parse_versioned_edge_key(key).ok_or_else(|| malformed(key))?;
        let name = target.resolve(stored)?;
        let unit = edge_unit(&resolved, &name, key, value)?;
        grouped.entry(name.bare).or_default().push(unit);
    }
    let versions = by_key.len();
    let collections = grouped
        .into_iter()
        .map(|(collection, units)| CollectionEdges {
            database_id,
            collection,
            units,
        })
        .collect();
    Ok((versions, collections))
}
