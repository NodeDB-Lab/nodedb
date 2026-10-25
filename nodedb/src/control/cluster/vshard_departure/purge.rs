// SPDX-License-Identifier: BUSL-1.1

//! One departure purge: the WAL barrier, then every core's purge.
//!
//! The purge removes rows the WAL can still hold records of. Replay
//! re-applies the redb-backed engines' records with no floor, so a pre-purge
//! record brings a purged row back at the next boot. The barrier is one
//! `CollectionTombstoned` record per purged collection, all naming the WAL
//! position before the purge, fsynced before any core purges: replay then
//! skips every record of those collections below it. Edge, label and array
//! records of other collections stay replayable, so a boot purges again
//! while the WAL still holds a record below the barrier (see
//! [`super::pass`]).

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::{DepartedCollection, MetaOp, PhysicalPlan};
use nodedb_types::id::{CollectionKey, QualifiedCollection};

use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};
use crate::wal::WalManager;
use crate::wal::manager::NO_APPLY_KEY;

/// How long every core gets to answer one purge.
const PURGE_DEADLINE: Duration = Duration::from_secs(120);

/// The key of the removed-record count every core answers.
const PURGED_COUNT: &str = "purged";

/// One collection homed on a departed vShard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomedCollection {
    pub database_id: u64,
    pub tenant_id: u64,
    /// Bare catalog name.
    pub name: String,
}

/// Every active collection of the local catalog homed on `departed`.
pub fn departed_collections(
    catalog: &SystemCatalog,
    departed: &BTreeSet<u32>,
) -> crate::Result<Vec<HomedCollection>> {
    if departed.is_empty() {
        return Ok(Vec::new());
    }
    Ok(catalog
        .load_all_collections_across_databases()?
        .into_iter()
        .filter(|c| {
            c.is_active
                && departed.contains(&nodedb_cluster::routing::vshard_for_collection(
                    CollectionKey::from_bare(c.database_id, &c.name),
                ))
        })
        .map(|c| HomedCollection {
            database_id: c.database_id.as_u64(),
            tenant_id: c.tenant_id,
            name: c.name,
        })
        .collect())
}

/// The oldest WAL position this node's WAL still holds a record at or
/// above.
pub fn wal_oldest_lsn(wal: &WalManager) -> crate::Result<u64> {
    let (segments, active_first) = wal.segments_with_active()?;
    Ok(segments
        .iter()
        .map(|segment| segment.first_lsn)
        .chain(std::iter::once(active_first))
        .min()
        .unwrap_or(active_first))
}

/// Append and fsync the purge barrier for `collections`. Returns the WAL
/// position the barrier names: the next position at the time of the call,
/// above every record the purge removes the rows of.
pub fn append_purge_barrier(
    wal: &WalManager,
    collections: &[HomedCollection],
) -> crate::Result<u64> {
    let barrier = wal.next_lsn().as_u64();
    let appender = wal.appender(NO_APPLY_KEY);
    for collection in collections {
        appender.append_collection_tombstone(
            TenantId::new(collection.tenant_id),
            DatabaseId::new(collection.database_id),
            &collection.name,
            barrier,
        )?;
    }
    wal.sync()?;
    Ok(barrier)
}

/// Purge `departed` on every Data-Plane core of this node. Edges with an
/// endpoint home in `resident` stay. Returns the documents, edge versions
/// and label versions removed, summed over the cores. Every core answers
/// before it returns, and any core's error fails it.
pub async fn purge_on_every_core(
    shared: &SharedState,
    departed: &BTreeSet<u32>,
    resident: &BTreeSet<u32>,
    collections: &[HomedCollection],
) -> crate::Result<u64> {
    injected_fault(shared.node_id)?;
    let plan = PhysicalPlan::Meta(MetaOp::PurgeDepartedVShards {
        departed: departed.iter().copied().collect(),
        resident: resident.iter().copied().collect(),
        collections: collections
            .iter()
            .map(|c| DepartedCollection {
                database_id: c.database_id,
                tenant_id: c.tenant_id,
                collection: QualifiedCollection::new(DatabaseId::new(c.database_id), &c.name)
                    .as_str()
                    .to_string(),
            })
            .collect(),
    });
    let response = crate::control::server::broadcast::broadcast_count_to_all_cores_until(
        shared,
        TenantId::new(0),
        DatabaseId::DEFAULT,
        plan,
        TraceId::generate(),
        PURGED_COUNT,
        Instant::now() + PURGE_DEADLINE,
    )
    .await?;
    let counts: std::collections::BTreeMap<String, u64> =
        zerompk::from_msgpack(response.payload.as_bytes()).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("departure purge count: {e}"),
            }
        })?;
    counts
        .get(PURGED_COUNT)
        .copied()
        .ok_or(crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("departure purge count: the response carries no `{PURGED_COUNT}` key"),
        })
}

/// Fail point `vshard_departure::purge`, scoped to node `node_id`: the purge
/// fails before any core runs, as an interrupted purge leaves it.
fn injected_fault(node_id: u64) -> crate::Result<()> {
    #[cfg(feature = "failpoints")]
    if let Some(detail) = crate::fail_point::eval_fail(
        crate::fail_point::FailScope::Node(node_id),
        "vshard_departure::purge",
    ) {
        return Err(crate::Error::Internal { detail });
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = node_id;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_collections_homed_on_a_departed_vshard_are_named() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        let names = ["dp_a", "dp_b", "dp_c", "dp_d", "dp_e", "dp_f"];
        for name in names {
            let stored =
                crate::control::security::catalog::StoredCollection::stamped_for_test(1, name, "o");
            catalog
                .put_collection(DatabaseId::DEFAULT, &stored)
                .expect("put");
        }
        let home = |name: &str| {
            nodedb_cluster::routing::vshard_for_collection(CollectionKey::from_bare(
                DatabaseId::DEFAULT,
                name,
            ))
        };
        let departed = BTreeSet::from([home("dp_a")]);
        let named: Vec<String> = departed_collections(&catalog, &departed)
            .expect("collections")
            .into_iter()
            .map(|c| c.name)
            .collect();
        let expected: Vec<String> = names
            .iter()
            .filter(|name| home(name) == home("dp_a"))
            .map(|name| name.to_string())
            .collect();
        assert_eq!(named, expected);
        assert!(
            departed_collections(&catalog, &BTreeSet::new())
                .expect("none")
                .is_empty()
        );
    }
}
