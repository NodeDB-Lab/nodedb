// SPDX-License-Identifier: BUSL-1.1

//! Capture named collections of one tenant in one database from the whole
//! cluster.
//!
//! The capture takes the same consistent cut a backup takes. Each vShard is
//! read from exactly one source node: the leader of its group. Each record is
//! kept by the source of its owner home, so a graph edge comes from the leader
//! of its `from_key(src)` vShard. The result is one merged
//! `TenantDataSnapshot`, in the shape the restore re-issue reads.

use std::collections::{BTreeSet, HashSet};

use futures::future::join_all;
use nodedb_physical::physical_plan::MetaOp;
use nodedb_types::DatabaseId;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::{RecordHomes, TenantDataSnapshot};

use super::node_snapshot::{is_self, snapshot_remote, snapshot_self};
use super::orchestrator::source_assignment;
use super::restore::sections::append_snapshot;
use super::snapshot_keys::{
    StoredRecord, extract_db_tenant_scoped_collection, homes_of_stored,
    retain_tenant_data_for_vshards, stored_collection_key,
};

/// Capture `collections` (bare names) of `tenant_id` in `database_id` from
/// every node that is the source of a vShard. `arrays` captures the arrays
/// among the names too. Any node error fails the whole capture.
pub async fn capture_collections(
    state: &SharedState,
    tenant_id: u64,
    database_id: DatabaseId,
    collections: &BTreeSet<String>,
    arrays: bool,
) -> Result<TenantDataSnapshot, Error> {
    let assignment = source_assignment(state)?;
    let watermark = super::cut::consistent_cut(state).await?;

    let per_node = join_all(
        assignment
            .into_iter()
            .map(|(node_id, source_vshards)| async move {
                let body = if is_self(state, node_id) {
                    snapshot_self(state, tenant_id, database_id, arrays).await?
                } else {
                    // The remote node takes the cut at the same watermark
                    // before it snapshots.
                    let plan = PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot {
                        tenant_id,
                        cut_watermark: Some(watermark),
                        cut_capture: None,
                        arrays,
                    });
                    snapshot_remote(state, node_id, tenant_id, database_id, &plan).await?
                };
                decode_owned(
                    &body,
                    node_id,
                    tenant_id,
                    database_id,
                    &source_vshards,
                    collections,
                )
            }),
    )
    .await;

    let mut merged = TenantDataSnapshot::default();
    for snap in per_node {
        append_snapshot(&mut merged, snap?);
    }
    // The Data Plane snapshot carries no surrogate binds. They live in the
    // catalog of each node that holds their homes.
    let names: Vec<String> = collections.iter().cloned().collect();
    merged
        .surrogate_pk
        .extend(super::bind_capture::capture_binds(state, tenant_id, database_id, &names).await?);
    Ok(merged)
}

/// Decode one node's snapshot and keep only the records of the captured
/// collections whose owner home that node is the source for.
fn decode_owned(
    body: &[u8],
    node_id: u64,
    tenant_id: u64,
    database_id: DatabaseId,
    source_vshards: &HashSet<u32>,
    collections: &BTreeSet<String>,
) -> Result<TenantDataSnapshot, Error> {
    let mut snap: TenantDataSnapshot =
        zerompk::from_msgpack(body).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!(
                "capture: decode the tenant snapshot of node {node_id}, database {}: {e}",
                database_id.as_u64()
            ),
        })?;
    let homes_of = |record: StoredRecord<'_>| homes_if_captured(database_id, collections, record);
    retain_tenant_data_for_vshards(&mut snap, tenant_id, source_vshards, homes_of);
    // The vector build parameters are keyed like `vectors`. The shared filter
    // leaves them alone, and a re-issue configures a collection the
    // capture does not name.
    let owned = |key: &str| {
        extract_db_tenant_scoped_collection(key, tenant_id).is_some_and(|collection| {
            homes_of(StoredRecord::Row { collection })
                .is_some_and(|homes| homes.owned_by(source_vshards))
        })
    };
    snap.vector_params.retain(|(key, _)| owned(key));
    snap.index_configs.retain(|(key, _)| owned(key));
    Ok(snap)
}

/// The homes of `record`, or `None` when its collection is not one of
/// `collections`.
fn homes_if_captured(
    database_id: DatabaseId,
    collections: &BTreeSet<String>,
    record: StoredRecord<'_>,
) -> Option<RecordHomes> {
    let key = stored_collection_key(database_id, record.collection());
    collections
        .contains(key.name())
        .then(|| homes_of_stored(database_id, record))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{CollectionKey, QualifiedCollection};

    const DB: DatabaseId = DatabaseId::new(1025);

    fn captured(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|n| (*n).to_string()).collect()
    }

    #[test]
    fn a_captured_collection_keeps_its_home_vshard() {
        let names = captured(&["orders"]);
        let stored = QualifiedCollection::new(DB, "orders");
        let expected = CollectionKey::from_bare(DB, "orders").vshard();
        for collection in [stored.as_str(), "orders"] {
            let homes = homes_if_captured(DB, &names, StoredRecord::Row { collection })
                .expect("captured collection");
            assert_eq!(homes.owner(), expected);
            assert!(homes.is_single());
        }
    }

    #[test]
    fn an_uncaptured_collection_has_no_homes() {
        let names = captured(&["orders"]);
        let stored = QualifiedCollection::new(DB, "invoices");
        let row = StoredRecord::Row {
            collection: stored.as_str(),
        };
        assert!(homes_if_captured(DB, &names, row).is_none());
        let edge = StoredRecord::Edge {
            collection: stored.as_str(),
            src: "a",
            dst: "b",
        };
        assert!(homes_if_captured(DB, &names, edge).is_none());
    }

    #[test]
    fn a_captured_edge_is_homed_on_its_endpoints() {
        let names = captured(&["follows"]);
        let stored = QualifiedCollection::new(DB, "follows");
        let edge = StoredRecord::Edge {
            collection: stored.as_str(),
            src: "a",
            dst: "b",
        };
        assert_eq!(
            homes_if_captured(DB, &names, edge),
            Some(RecordHomes::edge("a", "b"))
        );
    }

    /// Four nodes, four RF=3 groups. Each node holds the edges homed on the
    /// groups it replicates and is the source of the group it leads. Every
    /// edge of a captured collection is captured exactly once, and no edge of
    /// an uncaptured collection is captured.
    #[test]
    fn every_edge_is_captured_once_when_nodes_outnumber_rf() {
        const NODES: u32 = 4;
        const RF: u32 = 3;
        const TID: u64 = 7;
        let group_of = |vshard: crate::types::VShardId| vshard.as_u32() % NODES;
        let replicates = |node: u32, group: u32| (0..RF).any(|i| (group + i) % NODES == node);

        let names = captured(&["follows"]);
        let follows = QualifiedCollection::new(DB, "follows");
        let other = QualifiedCollection::new(DB, "likes");
        let key = |collection: &str, i: u32| {
            crate::engine::graph::edge_store::versioned_edge_key(
                collection,
                &format!("u{i}"),
                "L",
                &format!("v{}", i * 7 + 3),
                1,
            )
            .expect("edge key")
        };
        let edges: Vec<String> = (0..256).map(|i| key(follows.as_str(), i)).collect();
        let uncaptured: Vec<String> = (0..16).map(|i| key(other.as_str(), i)).collect();
        let cross = edges
            .iter()
            .filter_map(|k| StoredRecord::from_edge_key(k))
            .filter(|r| !homes_of_stored(DB, *r).is_single())
            .count();
        assert!(cross > 0, "the fixture holds cross-shard edges");

        let mut seen: std::collections::BTreeMap<String, usize> = Default::default();
        for node in 0..NODES {
            let held: Vec<(String, Vec<u8>)> = edges
                .iter()
                .chain(&uncaptured)
                .filter(|k| {
                    StoredRecord::from_edge_key(k).is_some_and(|r| {
                        homes_of_stored(DB, r)
                            .iter()
                            .any(|home| replicates(node, group_of(home)))
                    })
                })
                .map(|k| (k.clone(), vec![]))
                .collect();
            let body = zerompk::to_msgpack_vec(&TenantDataSnapshot {
                edges: held,
                ..Default::default()
            })
            .expect("encode");
            let source: HashSet<u32> = (0..nodedb_cluster::routing::VSHARD_COUNT)
                .filter(|v| v % NODES == node)
                .collect();
            let kept =
                decode_owned(&body, u64::from(node), TID, DB, &source, &names).expect("decode");
            for (k, _) in kept.edges {
                *seen.entry(k).or_default() += 1;
            }
        }
        for k in &edges {
            assert_eq!(seen.get(k), Some(&1), "edge {k:?} captured once");
        }
        assert_eq!(seen.len(), edges.len(), "no uncaptured edge is kept");
    }

    #[test]
    fn the_filter_drops_every_entry_of_an_uncaptured_collection() {
        let names = captured(&["orders"]);
        let all: HashSet<u32> = (0..nodedb_cluster::routing::VSHARD_COUNT).collect();
        let kept = QualifiedCollection::new(DB, "orders");
        let dropped = QualifiedCollection::new(DB, "invoices");
        let mut snap = TenantDataSnapshot {
            kv_tables: vec![
                (format!("1025:7:{}", kept.as_str()), vec![1]),
                (format!("1025:7:{}", dropped.as_str()), vec![2]),
            ],
            ..Default::default()
        };
        retain_tenant_data_for_vshards(&mut snap, 7, &all, |record| {
            homes_if_captured(DB, &names, record)
        });
        assert_eq!(
            snap.kv_tables,
            vec![(format!("1025:7:{}", kept.as_str()), vec![1])]
        );
    }
}
