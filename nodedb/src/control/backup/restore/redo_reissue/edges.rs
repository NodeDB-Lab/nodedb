// SPDX-License-Identifier: BUSL-1.1

//! Restored graph edges as redo units.
//!
//! A backup carries every edge version under its versioned key,
//! `"{collection}\x00{src}\x00{label}\x00{dst}\x00{system_from:020}"`. Each
//! version re-issues at its original `system_from`, so the restored edge keeps
//! its history and its valid-from time. A tombstone version re-issues as a
//! delete at its `system_from`. Every replica updates its CSR index and its
//! node identities as it installs each version.

use std::collections::BTreeMap;

use crate::control::state::SharedState;
use crate::control::surrogate::CarriedIdentity;
use crate::engine::graph::edge_store::{
    EdgeValuePayload, is_gdpr_erasure, is_tombstone, parse_versioned_edge_key,
};
use crate::types::{DatabaseId, TenantId};
use crate::wal::{EdgeDeleteRedo, EdgePutRedo};

use super::sub_record::{edge_delete, edge_put};
use super::units::{CollectionUnits, RowUnit};

fn malformed(key: &str) -> crate::Error {
    let prefix: String = key.chars().take(64).collect();
    crate::Error::Serialization {
        format: "backup".into(),
        detail: format!("restore: edge key '{prefix:?}' is malformed"),
    }
}

/// The identity of node `node_id` in edge collection `collection`, bound
/// through the surrogate assigner as a live edge write binds it.
fn node_identity(
    state: &SharedState,
    database_id: DatabaseId,
    tenant: TenantId,
    collection: &str,
    node_id: &str,
) -> crate::Result<CarriedIdentity> {
    let surrogate =
        state
            .surrogate_assigner
            .assign(database_id, tenant, collection, node_id.as_bytes())?;
    Ok(CarriedIdentity {
        collection: collection.to_string(),
        pk_bytes: node_id.as_bytes().to_vec(),
        surrogate,
    })
}

/// One edge version as a unit.
fn edge_unit(
    state: &SharedState,
    database_id: DatabaseId,
    tenant: TenantId,
    key: &str,
    value: &[u8],
) -> crate::Result<RowUnit> {
    let (collection, src_id, label, dst_id, system_from) =
        parse_versioned_edge_key(key).ok_or_else(|| malformed(key))?;
    if is_tombstone(value) {
        let op = edge_delete(&EdgeDeleteRedo {
            collection: collection.to_string(),
            src_id: src_id.to_string(),
            label: label.to_string(),
            dst_id: dst_id.to_string(),
            system_from: Some(system_from),
        })?;
        return Ok(RowUnit {
            ops: vec![op],
            identities: Vec::new(),
        });
    }
    if is_gdpr_erasure(value) {
        return Err(crate::Error::Serialization {
            format: "backup".into(),
            detail: format!(
                "restore: edge version {system_from} in '{collection}' is an erasure marker, \
                 which no write path records"
            ),
        });
    }
    let payload = EdgeValuePayload::decode(value)?;
    let src = node_identity(state, database_id, tenant, collection, src_id)?;
    let dst = node_identity(state, database_id, tenant, collection, dst_id)?;
    let op = edge_put(&EdgePutRedo {
        collection: collection.to_string(),
        src_id: src_id.to_string(),
        label: label.to_string(),
        dst_id: dst_id.to_string(),
        properties: payload.properties,
        src_surrogate: src.surrogate.as_u32(),
        dst_surrogate: dst.surrogate.as_u32(),
        system_from: Some(system_from),
    })?;
    Ok(RowUnit {
        ops: vec![op],
        identities: vec![src, dst],
    })
}

/// Every restored edge version of `tenant_id`, one unit per version, grouped
/// by edge collection in key order: each edge's versions in system-time order.
pub(super) fn edge_units(
    state: &SharedState,
    tenant_id: u64,
    edges: Vec<(String, Vec<u8>)>,
) -> crate::Result<Vec<CollectionUnits>> {
    // The edge section carries no database: a tenant backup reads the default
    // database's edge store.
    let database_id = DatabaseId::DEFAULT;
    let tenant = TenantId::new(tenant_id);
    let mut by_key: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for (key, value) in edges {
        by_key.insert(key, value);
    }
    let mut grouped: BTreeMap<String, Vec<RowUnit>> = BTreeMap::new();
    for (key, value) in &by_key {
        let (collection, ..) = parse_versioned_edge_key(key).ok_or_else(|| malformed(key))?;
        let unit = edge_unit(state, database_id, tenant, key, value)?;
        grouped
            .entry(collection.to_string())
            .or_default()
            .push(unit);
    }
    Ok(grouped
        .into_iter()
        .map(|(collection, units)| CollectionUnits {
            database_id,
            collection,
            units,
        })
        .collect())
}
