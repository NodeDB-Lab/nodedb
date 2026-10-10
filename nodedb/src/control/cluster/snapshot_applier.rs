// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane-facing [`SnapshotApplier`] implementation for the Raft snapshot
//! RECEIVE path.
//!
//! `nodedb-cluster` defines the [`nodedb_cluster::SnapshotApplier`] trait but
//! cannot depend on `nodedb` (circular), so the host crate supplies this
//! implementation. The install-snapshot finalize path calls it on the FOLLOWER
//! after staging the snapshot and before advancing Raft.
//!
//! The per-group snapshot bytes are the vshard-filtered `TenantDataSnapshot`
//! the leader's `DataPlaneSnapshotBuilder` ships. The install:
//!
//! 0. waits until this node's metadata group applied the snapshot's
//!    `metadata_floor`, the same hold a live replicated write takes. A new
//!    collection incarnation clears its storage when it applies, so the clear
//!    runs before the snapshot's rows of that incarnation land, never after.
//!    The wait is bounded: a group 0 snapshot this node still needs can queue
//!    behind this install, so a lagging node refuses the install and the
//!    leader resends it;
//! 1. splits the snapshot into one share per local core, routed the way live
//!    writes route ([`crate::control::cluster::snapshot_install::split`]);
//! 2. appends and fsyncs the WAL install barrier, so restart replay never
//!    applies a pre-install record over the installed state
//!    ([`crate::control::cluster::snapshot_install::barrier`]);
//! 3. sends every core its share and its clear list as a
//!    `MetaOp::RestoreTenantSnapshot` with `replace_mode: true`, so a Raft
//!    install replaces local state instead of failing against it. Each core
//!    checkpoints its memory-only engines before it answers, so an
//!    acknowledged install survives a restart without the WAL;
//! 4. settles only after every core acknowledged: rebinds the PK→surrogate
//!    identities and takes the group's tenant write marks.
//!
//! Any error returns a [`SnapshotInstallError`]. The caller then leaves the
//! Raft boundary and durable floor unmoved. A re-install clears every core
//! first, so it converges from whatever share a failed attempt left.
//!
//! Metadata group 0 ships a [`crate::control::cluster::metadata_image`]
//! image instead, installed by
//! [`crate::control::cluster::metadata_image::install_metadata_image`].

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use nodedb_types::Surrogate;
use nodedb_types::id::{CollectionKey, DatabaseId};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::cluster::snapshot_install::{
    CoreMap, CoreShares, SettleStep, SnapshotInstallError, append_install_barrier,
    append_install_marker, clear_targets_per_core, group_collections, install_on_every_core,
    split_by_core, version_floor,
};
use crate::control::state::SharedState;
use crate::types::{SurrogateBindEntry, TenantDataSnapshot, TenantId, VShardId};
use nodedb_physical::physical_plan::MetaOp;

use crate::control::cluster::metadata_image::{RaftOwnedState, install_metadata_image};
use nodedb_cluster::METADATA_GROUP_ID;

/// Per-group snapshot apply dispatch timeout (mirrors the restore orchestrator's
/// node timeout and the snapshot builder's per-tenant timeout).
const SNAPSHOT_APPLY_TIMEOUT: Duration = Duration::from_secs(120);

/// How long an install waits for this node's metadata group to reach the
/// snapshot's floor before it refuses the install.
const METADATA_FLOOR_WAIT: Duration = Duration::from_secs(10);

/// Applies received per-group snapshots to the local Data Plane on the Raft
/// snapshot RECEIVE path.
pub struct DataPlaneSnapshotApplier {
    shared: Arc<SharedState>,
    /// What a group 0 image install needs beyond `shared`. `None` on an
    /// applier built for data groups only.
    metadata: Option<MetadataInstallParts>,
    /// The Calvin sequencer group's snapshot store. `None` on an applier
    /// built for data groups only.
    sequencer: Option<Arc<crate::control::cluster::sequencer_snapshot::SequencerSnapshotStore>>,
}

/// The state a group 0 image install writes that `SharedState` does not hold.
struct MetadataInstallParts {
    /// The cluster catalog the image writes routing and epoch into.
    cluster_catalog: Arc<nodedb_cluster::ClusterCatalog>,
    /// Registries the Raft wiring owns that the image reloads.
    raft: RaftOwnedState,
}

impl DataPlaneSnapshotApplier {
    /// Construct an applier bound to the node's shared state. It installs
    /// data-group snapshots only.
    pub fn new(shared: Arc<SharedState>) -> Self {
        Self {
            shared,
            metadata: None,
            sequencer: None,
        }
    }

    /// Let this applier install sequencer-group snapshots through `store`.
    #[must_use]
    pub fn with_sequencer(
        mut self,
        store: Arc<crate::control::cluster::sequencer_snapshot::SequencerSnapshotStore>,
    ) -> Self {
        self.sequencer = Some(store);
        self
    }

    /// Let this applier install group 0 images too.
    pub fn with_metadata(
        mut self,
        cluster_catalog: Arc<nodedb_cluster::ClusterCatalog>,
        raft: RaftOwnedState,
    ) -> Self {
        self.metadata = Some(MetadataInstallParts {
            cluster_catalog,
            raft,
        });
        self
    }

    /// Install a non-empty data-group snapshot on every local core, then
    /// settle its Control-Plane state.
    pub async fn install(
        &self,
        group_id: u64,
        snapshot_bytes: &[u8],
    ) -> Result<(), SnapshotInstallError> {
        self.install_within(group_id, snapshot_bytes, METADATA_FLOOR_WAIT)
            .await
    }

    /// [`Self::install`], waiting at most `floor_wait` for the metadata floor.
    async fn install_within(
        &self,
        group_id: u64,
        snapshot_bytes: &[u8],
        floor_wait: Duration,
    ) -> Result<(), SnapshotInstallError> {
        let snap: TenantDataSnapshot =
            zerompk::from_msgpack(snapshot_bytes).map_err(|e| SnapshotInstallError::Decode {
                group_id,
                detail: e.to_string(),
            })?;
        // Before the catalog and routing reads below: both come from the
        // metadata group, and the floor brings them to the builder's view.
        self.await_metadata_floor(group_id, snap.metadata_floor, floor_wait)
            .await?;

        let group_vshards = self.group_vshards(group_id)?;
        let cores = {
            let dispatcher = self
                .shared
                .dispatcher
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            CoreMap::from_router(dispatcher.router())
        };
        let CoreShares {
            per_core,
            surrogate_pk,
            group_write_marks,
            proposal_keys,
            proposal_keys_complete_from,
            cut_index,
            event_lane,
            calvin,
            redo_streams,
        } = split_by_core(group_id, snap, &cores)?;
        // Refused before any core changes: storage installed without its
        // Calvin cut will hold Calvin transactions no applied state names.
        let calvin = calvin.ok_or(SnapshotInstallError::NoCalvinCut { group_id })?;

        let collections =
            group_collections(group_id, self.shared.credentials.catalog(), &group_vshards)?;
        let clears = clear_targets_per_core(group_id, &collections, &cores)?;
        // Every core replaces its array cells and graph edges of the group's
        // vShards. A store with none of them stays as it is.
        let mut vshards: Vec<u32> = group_vshards.iter().copied().collect();
        vshards.sort_unstable();
        let version_floor = version_floor(&self.shared, group_id, &vshards, cut_index);

        let plans = per_core
            .into_iter()
            .zip(clears)
            .enumerate()
            .map(|(core_id, (share, collections_to_clear))| {
                let snapshot =
                    zerompk::to_msgpack_vec(&share).map_err(|e| SnapshotInstallError::Encode {
                        group_id,
                        core_id,
                        detail: e.to_string(),
                    })?;
                Ok(PhysicalPlan::Meta(MetaOp::RestoreTenantSnapshot {
                    tenant_id: 0,
                    snapshot,
                    replace_mode: true,
                    collections_to_clear,
                    group_vshards: vshards.clone(),
                    version_floor: version_floor.clone(),
                }))
            })
            .collect::<Result<Vec<_>, SnapshotInstallError>>()?;

        append_install_barrier(&self.shared.wal, group_id, &collections)?;
        install_on_every_core(&self.shared, group_id, plans, SNAPSHOT_APPLY_TIMEOUT).await?;
        // The installed rows ride no WAL record: the install is a boundary a
        // point-in-time restore starts after, from a base this forces.
        append_install_marker(&self.shared.wal, group_id)?;
        // After the install marker: boot recovery drops the vShards' applied
        // markers from before it, and this replaces the rest.
        crate::control::cluster::calvin_snapshot::install_calvin_cut(
            &self.shared,
            group_id,
            &group_vshards,
            calvin,
        )?;
        // After the install marker too: boot drops the group's earlier streams
        // there and rebuilds these.
        self.shared
            .redo_chunks
            .install_group(group_id, redo_streams)
            .await
            .map_err(|source| SnapshotInstallError::Settle {
                group_id,
                step: SettleStep::RedoStreams,
                source: source.into(),
            })?;
        crate::control::pitr::force_base_after_install(&self.shared);
        self.settle(
            group_id,
            &surrogate_pk,
            &group_write_marks,
            &proposal_keys,
            proposal_keys_complete_from,
        )?;
        // The snapshot's trigger actions and messages, so this node can own
        // the group's firing and delivery without skipping an event.
        crate::event::trigger::lane::snapshot::install(
            &self.shared,
            &group_vshards,
            cut_index,
            event_lane,
        )
        .map_err(|source| SnapshotInstallError::Settle {
            group_id,
            step: SettleStep::EventLane,
            source,
        })?;
        // The snapshot holds every entry through its cut, which is at or
        // above its Raft index. The entries between the two reach the apply
        // loop from the log, and it concludes them without applying. Recorded
        // while the install holds the group's apply gate, so no such entry
        // starts before it.
        if let Some(tracker) = self.shared.propose_tracker.get() {
            tracker.cover_through(group_id, cut_index);
        }
        // Last: the group's streams here are the snapshot's now, so a
        // snapshot this node owed the group is paid. An install that fails
        // earlier leaves the debt, and the group keeps requiring one.
        self.shared
            .redo_chunks
            .settle_owed(self.shared.credentials.catalog(), group_id)
            .map_err(|source| SnapshotInstallError::Settle {
                group_id,
                step: SettleStep::RedoStreams,
                source,
            })?;
        Ok(())
    }

    /// Wait until this node's metadata group applied `floor`, for at most
    /// `limit`. `0` holds nothing.
    async fn await_metadata_floor(
        &self,
        group_id: u64,
        floor: u64,
        limit: Duration,
    ) -> Result<(), SnapshotInstallError> {
        let watcher = self.shared.applied_index_watcher(METADATA_GROUP_ID);
        if floor == 0 || watcher.current() >= floor {
            return Ok(());
        }
        let waiting = Arc::clone(&watcher);
        let outcome = tokio::task::spawn_blocking(move || waiting.wait_for(floor, limit)).await;
        match outcome {
            Ok(nodedb_cluster::WaitOutcome::Reached) => Ok(()),
            Ok(nodedb_cluster::WaitOutcome::TimedOut | nodedb_cluster::WaitOutcome::GroupGone)
            | Err(_) => Err(SnapshotInstallError::MetadataBehind {
                group_id,
                floor,
                applied: watcher.current(),
            }),
        }
    }

    /// The group's vShards in the LOCAL routing table, resolved exactly as the
    /// builder resolves them. No routing table or an ownerless group
    /// yields an empty set, so nothing is cleared: such a node has no stale
    /// state to remove.
    fn group_vshards(&self, group_id: u64) -> Result<HashSet<u32>, SnapshotInstallError> {
        match self.shared.cluster_routing.as_ref() {
            Some(routing) => {
                let table = routing
                    .read()
                    .map_err(|_| SnapshotInstallError::RoutingPoisoned { group_id })?;
                Ok(table.vshards_for_group(group_id).into_iter().collect())
            }
            None => Ok(HashSet::new()),
        }
    }

    /// Control-Plane steps that run once every core holds its share.
    fn settle(
        &self,
        group_id: u64,
        surrogate_pk: &[SurrogateBindEntry],
        group_write_marks: &[(u64, u64, u8, String, u64)],
        proposal_keys: &[(u64, u64)],
        proposal_keys_complete_from: u64,
    ) -> Result<(), SnapshotInstallError> {
        // The Data Plane has no catalog access, so the PK→surrogate identities
        // bind here. Without them PK point-lookups (`WHERE id=<pk>`) resolve to
        // nothing on the caught-up follower even though full scans work.
        let catalog = self.shared.credentials.catalog();
        for e in surrogate_pk {
            catalog
                .put_surrogate(
                    CollectionKey::from_bare(DatabaseId::new(e.database_id), &e.collection),
                    TenantId::new(e.tenant_id),
                    &e.pk,
                    Surrogate::new(e.surrogate),
                )
                .map_err(|source| SnapshotInstallError::Settle {
                    group_id,
                    step: SettleStep::SurrogateRebind,
                    source,
                })?;
        }

        // Take the group's tenant write marks, durably, before this node
        // reports the group applied through the snapshot.
        if !group_write_marks.is_empty() {
            self.shared
                .tenant_marks
                .raise_group_entries(group_id, group_write_marks);
            self.shared
                .tenant_marks
                .persist(catalog)
                .map_err(|source| SnapshotInstallError::Settle {
                    group_id,
                    step: SettleStep::WriteMarks,
                    source,
                })?;
        }

        self.restore_proposal_keys(group_id, proposal_keys, proposal_keys_complete_from)?;

        // The install emitted no per-row events, so the permission cache
        // reloads before this node reports coverage of the group again.
        self.shared.authorization_fence.note_snapshot_installed();
        Ok(())
    }
}

impl DataPlaneSnapshotApplier {
    /// Make the covered entries' proposal keys this node's own.
    ///
    /// A `ProposalApplied` WAL marker per key, fsynced, lets the proposal
    /// ledger recover them after a restart. The tracker then answers the
    /// group's covered waiters by them at adopt, and hands them to the apply
    /// loop's ledger, so a later copy of one of those proposals is skipped.
    fn restore_proposal_keys(
        &self,
        group_id: u64,
        proposal_keys: &[(u64, u64)],
        complete_from: u64,
    ) -> Result<(), SnapshotInstallError> {
        let settle_error = |source| SnapshotInstallError::Settle {
            group_id,
            step: SettleStep::ProposalKeys,
            source,
        };
        for &(_, key) in proposal_keys {
            self.shared
                .wal
                .appender(key)
                .append_proposal_applied(TenantId::new(0), VShardId::new(0), DatabaseId::DEFAULT)
                .map_err(settle_error)?;
        }
        if !proposal_keys.is_empty() {
            self.shared.wal.sync().map_err(settle_error)?;
        }
        // Recorded even with no keys: `complete_from` tells adopt which
        // covered indexes the missing keys say nothing about.
        if let Some(tracker) = self.shared.propose_tracker.get() {
            tracker.restore_committed_keys(group_id, proposal_keys, complete_from);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl nodedb_cluster::SnapshotApplier for DataPlaneSnapshotApplier {
    async fn apply_snapshot(
        &self,
        group_id: u64,
        snapshot_bytes: &[u8],
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if group_id == METADATA_GROUP_ID {
            let parts = self.metadata.as_ref().ok_or_else(|| {
                Box::new(crate::Error::Internal {
                    detail: "this snapshot applier installs no metadata images".into(),
                }) as Box<dyn std::error::Error + Send + Sync>
            })?;
            return install_metadata_image(
                &self.shared,
                &parts.cluster_catalog,
                &parts.raft,
                snapshot_bytes,
            )
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
        }
        // Empty payload is the bootstrap stub — nothing to restore.
        if snapshot_bytes.is_empty() {
            return Ok(());
        }
        // The sequencer group's state is its state machine, not engine data.
        if group_id == nodedb_cluster::calvin::SEQUENCER_GROUP_ID {
            let store = self.sequencer.as_ref().ok_or_else(|| {
                Box::new(crate::Error::Internal {
                    detail: "this snapshot applier installs no sequencer snapshots".into(),
                }) as Box<dyn std::error::Error + Send + Sync>
            })?;
            // The install fsyncs its file: off the async workers.
            let store = Arc::clone(store);
            let shared = Arc::clone(&self.shared);
            let bytes = snapshot_bytes.to_vec();
            return tokio::task::spawn_blocking(move || {
                store.install(&bytes, &shared.calvin.bases, shared.credentials.catalog())
            })
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
        }
        self.install(group_id, snapshot_bytes)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    /// Answer every propose waiter the snapshot covers. Their entries
    /// committed, but no apply on this node produces their results.
    fn snapshot_adopted(&self, group_id: u64, last_included_index: u64) {
        if group_id == METADATA_GROUP_ID {
            // No entry at or below the index applies here any more, so the
            // cache's applied index starts at it.
            self.shared
                .metadata_cache
                .write()
                .unwrap_or_else(|p| p.into_inner())
                .advance_applied_index(last_included_index);
        }
        if let Some(tracker) = self.shared.propose_tracker.get() {
            tracker.resolve_covered(group_id, last_included_index);
        }
        // The covered entries' change events never route on this node. A
        // data-group snapshot covers every entry through its cut, which can
        // sit above the Raft index.
        let covered = match self.shared.propose_tracker.get() {
            Some(tracker) if group_id != METADATA_GROUP_ID => {
                tracker.covered_through(group_id).max(last_included_index)
            }
            _ => last_included_index,
        };
        if group_id != METADATA_GROUP_ID {
            self.shared
                .change_stream
                .install_group_floor(group_id, covered);
        }
        crate::event::cdc::position::record_install_floor(&self.shared, group_id, covered);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    /// A data-group snapshot that holds a new incarnation's rows reaches this
    /// node before the metadata entry that created the incarnation. No core
    /// receives the rows until the metadata group applied the snapshot's
    /// floor, so the incarnation's storage clear, which runs inside that
    /// apply, precedes them.
    #[tokio::test(flavor = "multi_thread")]
    async fn new_incarnation_rows_wait_for_the_metadata_floor() {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("floor.wal")).expect("test WAL"),
        );
        let (dispatcher, mut sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).expect("shared state");
        let watcher = shared.applied_index_watcher(METADATA_GROUP_ID);
        let floor = watcher.current() + 3;
        let snapshot = TenantDataSnapshot {
            documents: vec![("1:1:recreated:doc1".to_string(), b"new".to_vec())],
            metadata_floor: floor,
            group_calvin: Some(crate::types::GroupCalvinCut::default()),
            ..Default::default()
        };
        let bytes = zerompk::to_msgpack_vec(&snapshot).expect("encode snapshot");
        let applier = Arc::new(DataPlaneSnapshotApplier::new(Arc::clone(&shared)));

        let refused = applier
            .install_within(7, &bytes, Duration::from_millis(100))
            .await;
        assert!(matches!(
            refused,
            Err(SnapshotInstallError::MetadataBehind { floor: f, .. }) if f == floor
        ));
        assert!(
            sides[0].request_rx.try_pop().is_err(),
            "no core receives the rows while the metadata group lags the floor"
        );

        let installing = {
            let applier = Arc::clone(&applier);
            let bytes = bytes.clone();
            tokio::spawn(async move {
                applier
                    .install_within(7, &bytes, Duration::from_secs(30))
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(sides[0].request_rx.try_pop().is_err());

        watcher.bump(floor);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let delivered = loop {
            if sides[0].request_rx.try_pop().is_ok() {
                break true;
            }
            if std::time::Instant::now() > deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        installing.abort();
        assert!(delivered, "the rows reach the core once the floor applied");
    }
}
