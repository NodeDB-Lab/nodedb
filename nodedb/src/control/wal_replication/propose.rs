// SPDX-License-Identifier: BUSL-1.1

//! Propose a `ReplicatedEntry` through Raft with transparent leader-change retry.
//!
//! Shared by the pgwire write dispatch path and the durable RESTORE re-issue
//! path: both must replicate a write to the vshard's Raft group and tolerate a
//! mid-flight leader change (the previous leader's entry being overwritten by a
//! new leader's election no-op) by re-proposing the same payload.

use std::sync::Arc;

use super::types::{AsyncRaftProposer, ReplicatedEntry};
use crate::control::state::SharedState;

/// First backoff before a re-proposal. Each retry doubles it up to
/// [`MAX_BACKOFF`].
const FIRST_BACKOFF: std::time::Duration = std::time::Duration::from_millis(10);

/// Longest wait between two re-proposals.
const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_millis(200);

/// Stamp this node's metadata floor on `entry`: the catalog the write was
/// planned against, including the batch being applied now (see
/// `AppliedIndexWatcher::floor`). Every replica applies the entry only once
/// its own metadata apply reached this index. Every proposal of a
/// user-collection write stamps it before encoding.
pub(crate) fn stamp_metadata_floor(state: &SharedState, entry: &mut ReplicatedEntry) {
    entry.metadata_floor = state
        .applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID)
        .floor();
}

/// Stamp the incarnation this node's catalogs hold for each collection and
/// array `entry` names. A collection with no catalog row stays `Hlc::ZERO`,
/// and a replica applies its write by key alone.
pub(crate) fn stamp_collection_incarnations(
    state: &SharedState,
    entry: &mut ReplicatedEntry,
) -> crate::Result<()> {
    use crate::control::array_catalog::named_incarnation::{array_incarnation_of, read_mirror};
    use nodedb_cluster::calvin::types::IncarnationKind;

    let catalog = state.credentials.catalog();
    let mirror = read_mirror(&state.array_catalog);
    let database_id = crate::types::DatabaseId::new(entry.database_id);
    let tenant_id = crate::types::TenantId::new(entry.tenant_id);
    for named in &mut entry.incarnations {
        named.incarnation = match named.kind {
            IncarnationKind::Collection => {
                catalog.incarnation_of(database_id, entry.tenant_id, &named.collection)?
            }
            IncarnationKind::Array => {
                array_incarnation_of(&mirror, tenant_id, database_id, &named.collection)?
            }
        };
    }
    Ok(())
}

/// Stamp every catalog read `entry` carries, in the one order that keeps
/// them consistent on every replica:
///
/// 1. The incarnations. Each names the collection the write targets.
/// 2. The config pins. They are read at or after the incarnations, so a
///    pin is never older than the incarnation it travels with. A drop and
///    recreate between the two reads leaves a stale incarnation, and every
///    replica then supersedes the write instead of applying it under an
///    old config.
/// 3. The metadata floor, last. The floor never moves backward, so it
///    covers every catalog state the reads above saw.
pub(crate) fn stamp_catalog_reads(
    state: &SharedState,
    entry: &mut ReplicatedEntry,
) -> crate::Result<()> {
    crate::control::array_catalog::cell_route::stamp_incarnation(state, entry);
    stamp_collection_incarnations(state, entry)?;
    stamp_config_pins(state, entry)?;
    stamp_metadata_floor(state, entry);
    Ok(())
}

/// Stamp the config pins `entry`'s write requires, from this node's catalog:
/// the config of each collection it derives rows in or folds into, and the
/// CRS set. Every replica derives the write's row bodies and judges its
/// rules under them. A write that requires no pin takes none.
///
/// An entry that carries pins keeps them, so a mirror's entry keeps its
/// source's pins.
///
/// A written collection or sum target with no active committed catalog row
/// refuses the write with `CollectionNotFound`.
pub(crate) fn stamp_config_pins(
    state: &SharedState,
    entry: &mut ReplicatedEntry,
) -> crate::Result<()> {
    stamp_config_pins_from(&state.config_pins, state.credentials.catalog(), entry)
}

/// [`stamp_config_pins`] over `catalog` through `cache`.
fn stamp_config_pins_from(
    cache: &crate::control::state::config_pin_cache::ConfigPinCache,
    catalog: &crate::control::security::catalog::SystemCatalog,
    entry: &mut ReplicatedEntry,
) -> crate::Result<()> {
    use super::transaction_redo::pins::{PinScope, PinnedNames, pins_from_catalog};

    if entry.config_pins.pinned() {
        return Ok(());
    }
    let names = entry.write.pinned_names();
    if names.is_empty() {
        return Ok(());
    }
    entry.config_pins = pins_from_catalog(
        cache,
        catalog,
        PinScope {
            database_id: crate::types::DatabaseId::new(entry.database_id),
            tenant_id: crate::types::TenantId::new(entry.tenant_id),
        },
        PinnedNames {
            written: names.required.iter().map(String::as_str),
            required: &names.required,
            sum_targets: &names.sum_targets,
        },
    )?;
    Ok(())
}

/// Propose `entry` via `proposer` and return the Data Plane apply payload
/// bytes.
///
/// An edge or label write is not proposed. It runs as a Calvin transaction
/// (`planner::calvin::graph_sequencing`), and its applied payload comes back
/// the same way.
///
/// `deadline` is the caller's deadline. Every attempt, each attempt's wait at
/// the leader's write gate, and each wait for the local apply end at it.
///
/// Re-proposes the same payload until `deadline` while the group has no
/// leader to take it:
/// - [`crate::Error::RetryableLeaderChange`]: a new leader's election no-op
///   overwrote the previous leader's entry;
/// - [`crate::Error::NoLeader`]: the group is electing, or a leadership
///   transfer is in flight.
///
/// The encoded `ReplicatedEntry` carries enough identity (collection, PK,
/// surrogate) to be replayable, and its idempotency key makes a copy that
/// committed twice apply once. Only propose-layer machinery failures map to
/// [`crate::Error::Dispatch`]; a classified apply verdict passes through.
pub(crate) async fn propose_replicated_entry(
    state: &SharedState,
    proposer: &Arc<AsyncRaftProposer>,
    mut entry: ReplicatedEntry,
    deadline: tokio::time::Instant,
) -> crate::Result<super::types::AppliedOutput> {
    // An edge or label write runs as a Calvin transaction, never as a
    // data-group entry, so every edge and label version takes a Calvin
    // ordinal.
    if let Some(response) =
        crate::control::planner::calvin::sequence_replicated_graph_write(state, &entry).await?
    {
        return Ok(response.payload.to_vec());
    }
    // The write's commit instant. Stamped once, before the first propose, so
    // every re-proposal and every replica's apply carries the same value.
    entry.write_hlc = state.hlc_clock.now().wall_ns;
    stamp_catalog_reads(state, &mut entry)?;
    let idempotency_key = entry.idempotency_key;
    let data = entry.encode()?;
    let vshard_id = entry.vshard_id;

    let mut backoff = FIRST_BACKOFF;
    let mut attempt: u32 = 0;
    let payload = loop {
        attempt += 1;
        let error = match proposer(vshard_id, idempotency_key, data.clone(), deadline).await {
            Ok(p) => break Ok(p),
            Err(error @ crate::Error::RetryableLeaderChange { .. }) => {
                state
                    .raft_propose_leader_change_retries
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error
            }
            Err(error @ crate::Error::NoLeader { .. }) => error,
            // Only a machinery failure is re-wrapped. A state-machine verdict
            // (constraint, authz, conflict) carries the client's SQLSTATE.
            Err(other) if crate::error_classify::is_unclassified_failure(&other) => {
                return Err(crate::Error::Dispatch {
                    detail: format!("raft propose failed: {other}"),
                });
            }
            Err(other) => return Err(other),
        };
        if tokio::time::Instant::now() + backoff >= deadline {
            break Err(error);
        }
        tracing::warn!(
            attempt,
            vshard_id,
            error = %error,
            "raft proposal found no leader to take it; re-proposing"
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    };
    // The waiter resolved only once this node's own apply ran the entry, and
    // that apply recorded the entry's commit stamp on the tenant's observed
    // write high-water before resolving it.
    payload
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::{StoredCollection, SystemCatalog};
    use crate::control::state::config_pin_cache::ConfigPinCache;
    use crate::control::wal_replication::{ReplicatedSumTarget, ReplicatedWrite};
    use crate::types::DatabaseId;

    fn catalog_with(names: &[&str]) -> SystemCatalog {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        for name in names {
            catalog
                .put_collection(
                    DatabaseId::DEFAULT,
                    &StoredCollection::stamped_for_test(1, name, "owner"),
                )
                .expect("put");
        }
        catalog
    }

    fn put(targets: &[&str]) -> ReplicatedEntry {
        ReplicatedEntry::new(
            1,
            0,
            0,
            ReplicatedWrite::PointPut {
                collection: "entries".into(),
                document_id: "e1".into(),
                value: vec![0x80],
                surrogate: 1,
                resolved_sum_targets: targets
                    .iter()
                    .map(|target| ReplicatedSumTarget {
                        target_collection: (*target).into(),
                        join_value: "a1".into(),
                        surrogate: 7,
                    })
                    .collect(),
                returning: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            },
        )
    }

    fn pinned_names(entry: &ReplicatedEntry) -> Vec<&str> {
        entry
            .config_pins
            .configs
            .iter()
            .map(|pin| pin.key.collection.as_str())
            .collect()
    }

    #[test]
    fn a_document_write_pins_its_collection_and_its_sum_targets() {
        let catalog = catalog_with(&["entries", "accounts"]);
        let mut entry = put(&["accounts"]);
        stamp_config_pins_from(&ConfigPinCache::new(), &catalog, &mut entry).expect("stamp");
        assert!(entry.config_pins.pinned());
        assert_eq!(pinned_names(&entry), vec!["accounts", "entries"]);
        // The stamped entry covers its write on every replica.
        assert!(entry.take_applied_config_pins().expect("covered").is_some());
    }

    #[test]
    fn a_write_that_requires_no_pin_takes_none() {
        let catalog = catalog_with(&["cache"]);
        let mut entry = ReplicatedEntry::new(
            1,
            0,
            0,
            ReplicatedWrite::KvPersist {
                collection: "cache".into(),
                key: b"k".to_vec(),
            },
        );
        stamp_config_pins_from(&ConfigPinCache::new(), &catalog, &mut entry).expect("stamp");
        assert!(entry.config_pins.is_empty());
    }

    #[test]
    fn a_missing_collection_is_refused() {
        let catalog = catalog_with(&["accounts"]);
        let mut entry = put(&["accounts"]);
        let refused = stamp_config_pins_from(&ConfigPinCache::new(), &catalog, &mut entry)
            .expect_err("refused");
        assert!(
            matches!(&refused, crate::Error::CollectionNotFound { collection, .. } if collection == "entries"),
            "{refused:?}"
        );
    }

    #[test]
    fn a_missing_sum_target_is_refused() {
        let catalog = catalog_with(&["entries"]);
        let mut entry = put(&["accounts"]);
        let refused = stamp_config_pins_from(&ConfigPinCache::new(), &catalog, &mut entry)
            .expect_err("refused");
        assert!(
            matches!(&refused, crate::Error::CollectionNotFound { collection, .. } if collection == "accounts"),
            "{refused:?}"
        );
    }

    /// A mirror's entry arrives pinned by its source and keeps those pins.
    #[test]
    fn a_pinned_entry_keeps_its_pins() {
        let source = catalog_with(&["entries"]);
        let mut entry = put(&[]);
        stamp_config_pins_from(&ConfigPinCache::new(), &source, &mut entry).expect("stamp");
        let carried = entry.config_pins.clone();
        // This node's catalog holds no row of the collection, so a restamp
        // refuses.
        let empty = catalog_with(&[]);
        stamp_config_pins_from(&ConfigPinCache::new(), &empty, &mut entry).expect("kept");
        assert_eq!(entry.config_pins, carried);
    }
}
