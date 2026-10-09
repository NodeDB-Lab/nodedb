// SPDX-License-Identifier: BUSL-1.1

//! Top-level `apply_host_side_effects` dispatcher and the
//! `impl MetadataApplier for MetadataCommitApplier` trait entry point.

use tracing::{debug, error, warn};

use nodedb_cluster::{
    CommittedMetadata, MetadataApplier, MetadataEntry, MetadataPayload, RoutingChange,
    TopologyChange,
};

use super::types::{CatalogChangeEvent, MetadataCommitApplier};

/// The future of one entry's host-side effects.
pub(super) type HostEffects<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), crate::Error>> + Send + 'a>>;

impl MetadataCommitApplier {
    /// Apply a single decoded `MetadataEntry`'s host-side effects.
    ///
    /// - `CatalogDdl` → decode payload as `CatalogEntry`, write
    ///   through to redb via `catalog_entry::apply_to`, spawn async
    ///   post-apply side effects if `SharedState` is reachable.
    /// - Non-DDL variants (topology, routing, lease, version) have
    ///   no host-side redb effects in this crate — the cluster crate
    ///   already tracks them in the `MetadataCache`.
    ///
    /// `Ok(())` means the entry is fully applied (or its only failure was a
    /// best-effort durability shortcut whose source of truth is the replicated
    /// log). `Err` means a durable, replicated-state write failed — the caller
    /// MUST NOT advance the apply watermark past this entry, so Raft re-delivers
    /// it and the apply is retried. This is the canonical "never advance the
    /// state machine past an entry you couldn't apply" rule: a transient I/O
    /// failure clears on retry; a persistent one leaves the watermark loudly
    /// stuck (proposer waiters time out) rather than silently diverging from the
    /// quorum with a false-success ACK.
    ///
    /// The future is boxed: a prepared DDL and a batch apply their inner
    /// entries through this function again.
    pub(super) fn apply_host_side_effects<'a>(
        &'a self,
        entry: &'a MetadataEntry,
        raft_index: u64,
    ) -> HostEffects<'a> {
        Box::pin(async move {
            let result = self.apply_host_side_effects_inner(entry, raft_index).await;
            // A drain this entry ended wakes the statements waiting it out,
            // now that every effect of the entry is visible to their re-plan.
            if let Ok(shared) = self.shared_state() {
                shared.lease_drain.settle();
            }
            result
        })
    }

    async fn apply_host_side_effects_inner(
        &self,
        entry: &MetadataEntry,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        // Handle non-CatalogDdl variants that still have host-side
        // effects. Drain start/end land on `shared.lease_drain` on
        // every node so the next `force_refresh_lease` check sees
        // the replicated drain state.
        match entry {
            MetadataEntry::DdlPrepared { token, entry } => {
                return self.apply_prepared_ddl(*token, entry, raft_index).await;
            }
            MetadataEntry::Batch { entries } => {
                return self.apply_batch(entries, raft_index).await;
            }
            MetadataEntry::DescriptorDrainStart {
                descriptor_id,
                up_to_version,
                expires_at,
                proposer_node_id,
                owner,
            } => {
                return self.apply_drain_start(
                    descriptor_id,
                    owner,
                    *up_to_version,
                    *expires_at,
                    *proposer_node_id,
                );
            }
            MetadataEntry::DescriptorDrainEnd {
                descriptor_id,
                owner,
            } => {
                return self.apply_drain_end(descriptor_id, owner);
            }
            MetadataEntry::DescriptorLeaseRelease {
                node_id,
                descriptor_ids,
            } => {
                return self.apply_lease_release(*node_id, descriptor_ids);
            }
            MetadataEntry::DdlPrepareAcquire { token, node_id } => {
                return self.apply_ddl_prepare_acquire(*token, *node_id);
            }
            MetadataEntry::DescriptorLeaseGrant(lease) => {
                return self.apply_lease_grant(lease);
            }
            MetadataEntry::ClusterVersionBump { to, .. } => {
                return self.apply_cluster_version(*to);
            }
            MetadataEntry::DdlPendingPropose {
                token,
                objects,
                proposed_at,
            } => {
                return self.apply_ddl_pending_propose(*token, objects, *proposed_at);
            }
            MetadataEntry::DdlPendingFinalize { token } => {
                return self.apply_ddl_pending_finalize(*token, raft_index).await;
            }
            MetadataEntry::DdlPendingCancel { token } => {
                return self.apply_ddl_pending_cancel(*token);
            }
            MetadataEntry::DdlPrepareRelease { token } => {
                return self.apply_ddl_prepare_release(*token);
            }
            MetadataEntry::CaTrustChange {
                add_ca_cert,
                remove_ca_fingerprint,
            } => {
                return self.apply_ca_trust(
                    add_ca_cert.as_deref(),
                    remove_ca_fingerprint.as_ref(),
                    raft_index,
                );
            }
            MetadataEntry::SurrogateAlloc { hwm } => {
                return self.apply_surrogate_alloc(*hwm, raft_index);
            }
            MetadataEntry::DatabaseIdReserve {
                node_id,
                request_id,
            } => {
                return self.apply_database_id_reserve(*node_id, *request_id, raft_index);
            }
            MetadataEntry::RestorePoint { hlc, created_at_ms } => {
                return self.apply_restore_point(*hlc, *created_at_ms, raft_index);
            }
            MetadataEntry::SurrogateReserve {
                node_id,
                request_id,
                batch_size,
            } => {
                return self.apply_surrogate_reserve(
                    *node_id,
                    *request_id,
                    *batch_size,
                    raft_index,
                );
            }
            MetadataEntry::SyncProducerRegister {
                lite_id,
                producer_id,
                tenant_id,
                user_id,
                epoch,
                created_ms,
            } => {
                return self.apply_sync_producer_register(
                    super::sync_and_routing::SyncProducerRegistrationApply {
                        lite_id,
                        producer_id: *producer_id,
                        tenant_id: *tenant_id,
                        user_id: *user_id,
                        epoch: *epoch,
                        created_ms: *created_ms,
                    },
                    raft_index,
                );
            }
            MetadataEntry::SyncProducerFence { lite_id, new_epoch } => {
                return self.apply_sync_producer_fence(lite_id, *new_epoch, raft_index);
            }
            MetadataEntry::SyncPeerBind {
                database_id,
                tenant_id,
                collection,
                peer_id,
                producer_id,
                bound_ms,
            } => {
                return self.apply_sync_peer_bind(
                    super::sync_and_routing::SyncPeerBindApply {
                        database_id: *database_id,
                        tenant_id: *tenant_id,
                        collection,
                        peer_id: *peer_id,
                        producer_id: *producer_id,
                        bound_ms: *bound_ms,
                    },
                    raft_index,
                );
            }
            MetadataEntry::JoinTokenTransition {
                token_hash,
                transition,
                ts_ms,
            } => {
                return self.apply_join_token_transition(token_hash, transition, *ts_ms);
            }
            MetadataEntry::EnrollmentPreauthorization {
                spki,
                expires_at_ms,
            } => {
                return self.apply_enrollment_preauthorization(spki, *expires_at_ms);
            }
            MetadataEntry::EnrollmentPreauthorizationRevoke {
                spki,
                expires_at_ms,
            } => {
                return self.apply_enrollment_revoke(spki, *expires_at_ms);
            }
            MetadataEntry::RoutingChange(RoutingChange::SetPlacement {
                group_id,
                placement,
            }) => {
                return self.apply_set_placement(*group_id, placement, raft_index);
            }
            MetadataEntry::RoutingChange(RoutingChange::ReassignVShard {
                vshard_id,
                new_group_id,
                new_leaseholder_node_id,
            }) => {
                return self.apply_reassign_vshard(
                    *vshard_id,
                    *new_group_id,
                    *new_leaseholder_node_id,
                    raft_index,
                );
            }
            MetadataEntry::TopologyChange(TopologyChange::Leave { node_id }) => {
                return self.apply_node_leave(*node_id, raft_index);
            }
            _ => {}
        }

        self.apply_catalog_ddl(entry, raft_index).await
    }

    /// Apply a prepared DDL under the replicated owner token. A superseded
    /// proposal is a deterministic no-op: rejecting a committed stale token
    /// will wedge the Raft apply watermark forever.
    async fn apply_prepared_ddl(
        &self,
        token: u64,
        entry: &MetadataEntry,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        if !crate::control::metadata_proposer::ddl_owner::owns_ddl_lease(&shared, token) {
            debug!(token, raft_index, "skipping superseded prepared DDL");
            return Ok(());
        }
        self.apply_host_side_effects(entry, raft_index).await?;
        shared
            .metadata_ddl
            .applied_token
            .store(token, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Apply an atomic batch one level deep. Each sub-entry applies on its
    /// own, so each gets its own audit record stamped with the same
    /// `raft_index` (they committed at the same log position).
    async fn apply_batch(
        &self,
        entries: &[MetadataEntry],
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        let shared = self.shared.get().and_then(std::sync::Weak::upgrade);
        for sub in entries {
            self.apply_host_side_effects(sub, raft_index).await?;
            if let Some(shared) = &shared {
                shared
                    .metadata_apply_progress
                    .fetch_add(1, std::sync::atomic::Ordering::Release);
            }
        }
        Ok(())
    }

    /// Publish a permanent apply failure on the node-wide readiness marker.
    ///
    /// Best-effort by construction: unit-test appliers are built without a
    /// `SharedState`, and a node that has already torn its shared state down
    /// has nothing left to report readiness to. The structured faultbox report
    /// and the error log at the call site are unconditional, so the failure is
    /// never lost when this cannot land.
    fn record_permanent_wedge(
        &self,
        error: &crate::Error,
        entry: &MetadataEntry,
        raft_index: u64,
        last_applied_watermark: u64,
    ) {
        let Some(shared) = self.shared.get().and_then(std::sync::Weak::upgrade) else {
            return;
        };
        shared
            .metadata_apply_wedge
            .record(super::wedge::WedgeReport {
                raft_index,
                last_applied_watermark,
                entry_kind: crate::diag::entry_kind(entry),
                error: error.to_string(),
            });
    }
}

/// The fail point that holds back node `node_id`'s whole metadata apply.
pub fn metadata_apply_hold_point(node_id: u64) -> String {
    format!("metadata_apply::hold::node{node_id}")
}

#[async_trait::async_trait]
impl MetadataApplier for MetadataCommitApplier {
    async fn apply_decoded(&self, entries: &[CommittedMetadata<'_>]) -> u64 {
        let shared = self.shared.get().and_then(std::sync::Weak::upgrade);
        // Holds back this node's whole metadata apply, so a test can make
        // its catalog lag the metadata group. Nothing applies and Raft
        // re-delivers the batch once the hold is released.
        #[cfg(feature = "failpoints")]
        if let Some(shared) = &shared
            && crate::control::fail_gate::holds_metadata_apply(shared.node_id)
        {
            return 0;
        }
        // Before any entry mutates a registry or the catalog, declare the
        // batch's end. A reader that sees an effect of this batch then stamps
        // a floor at or above the entry that made it visible.
        if let (Some(batch_end), Some(shared)) = (entries.last(), &shared) {
            shared
                .applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID)
                .begin_batch(batch_end.index);
        }
        // `last` is the highest index whose state is GUARANTEED visible. We
        // only advance it past an entry that fully applied — a durable apply
        // failure stops the batch here so Raft re-delivers the entry and the
        // apply is retried (never a silent divergence with a false-success ACK).
        let mut last = 0u64;
        for committed in entries {
            let index = &committed.index;
            let data = committed.data;
            let entry = match &committed.payload {
                MetadataPayload::Empty => {
                    // Raft no-op: nothing to apply, but advance the cache
                    // watermark in lockstep with the Raft applied index the
                    // tick loop reports from our return value. Skipping this
                    // leaves `cache.applied_index` behind the watcher and the
                    // startup applied-index sanity check fails the boot with a
                    // spurious gap (every group's first committed entry on a
                    // fresh start is an election no-op).
                    self.cache
                        .write()
                        .unwrap_or_else(|p| p.into_inner())
                        .advance_applied_index(*index);
                    last = *index;
                    continue;
                }
                MetadataPayload::Undecodable(e) => {
                    // Undecodable committed entry: deterministic poison, won't
                    // decode on retry — skip (advance) rather than wedge.
                    warn!(index = *index, error = %e, "metadata decode failed");
                    last = *index;
                    continue;
                }
                MetadataPayload::Decoded(entry) => entry,
            };
            if let Err(e) = self.observe_stamp(data) {
                error!(
                    index = *index,
                    last_applied = last,
                    error = %e,
                    "metadata apply: recording the entry stamp failed; not advancing \
                     watermark — Raft will re-deliver and retry"
                );
                break;
            }
            // 1. Cluster-owned cache state (topology, routing,
            //    leases, catalog_entries_applied counter).
            {
                let mut guard = self.cache.write().unwrap_or_else(|p| p.into_inner());
                guard.apply(*index, entry);
            }
            // 2. Host side effects (redb writeback + awaited post-apply). A
            //    durable failure halts the watermark at the last good index.
            // Every WAL record the effects append carries the entry's stamp.
            let applied = crate::wal::manager::effect_stamp::with_effect_stamp(
                nodedb_cluster::entry_stamp(data),
                self.apply_host_side_effects(entry, *index),
            )
            .await;
            if let Err(e) = applied {
                // Both classes stop the batch — skipping a committed metadata
                // entry is silent divergence from the quorum and is strictly
                // worse than halting. What differs is whether waiting for a
                // re-delivery is an honest plan.
                let class = super::wedge::classify(&e);
                // A deterministic failure here re-fails on every re-delivery and
                // wedges this node's applier forever while /healthz stays green,
                // so it is filed as a structured report — not only a log line —
                // at the one site that detects it.
                crate::diag::metadata_apply_wedged(&e, entry, *index, last, class.is_permanent());
                if class.is_permanent() {
                    // Retrying cannot help, so the node must stop advertising
                    // readiness rather than serve queries that will all die on
                    // an unrelated-looking descriptor-lease timeout.
                    self.record_permanent_wedge(&e, entry, *index, last);
                    error!(
                        index = *index,
                        last_applied = last,
                        error = %e,
                        "metadata apply: PERMANENT host-side effect failure; watermark halted \
                         and this node is no longer ready — re-delivery cannot clear this, \
                         operator intervention is required"
                    );
                } else {
                    error!(
                        index = *index,
                        last_applied = last,
                        error = %e,
                        "metadata apply: durable host-side effect failed; not advancing \
                         watermark — Raft will re-deliver and retry"
                    );
                }
                break;
            }
            last = *index;
        }
        if last > 0 {
            // The Raft tick loop bumps the per-group apply watcher
            // directly after `advance_applied`; this applier only
            // owns the catalog-change broadcast.
            let _ = self.catalog_change_tx.send(CatalogChangeEvent {
                applied_index: last,
            });
            debug!(
                applied_index = last,
                "metadata applier broadcast catalog-change event"
            );
        }
        last
    }

    /// Host effects land in redb, or in state boot rebuilds from redb, before
    /// `apply` returns an index.
    fn durable_effects(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex, RwLock};

    use tokio::sync::broadcast;

    use nodedb_cluster::{MetadataCache, PendingDdlObject, encode_entry};
    use nodedb_types::{DatabaseId, Hlc};

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::catalog_entry;
    use crate::control::catalog_entry::CatalogEntry;
    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::credential::CredentialStore;
    use crate::control::state::SharedState;
    use crate::wal::WalManager;

    fn make_applier() -> (
        MetadataCommitApplier,
        Arc<RwLock<MetadataCache>>,
        Arc<CredentialStore>,
        tempfile::TempDir,
    ) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let credentials =
            Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let (tx, _rx) = broadcast::channel(16);
        let token_state = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let applier =
            MetadataCommitApplier::new(cache.clone(), tx, credentials.clone(), token_state);
        (applier, cache, credentials, tmp)
    }

    fn put_collection_entry(name: &str) -> MetadataEntry {
        let stored = StoredCollection::stamped_for_test(7, name, "tester");
        let catalog_entry = CatalogEntry::PutCollection(Box::new(stored));
        MetadataEntry::CatalogDdl {
            payload: catalog_entry::encode(&catalog_entry).unwrap(),
        }
    }

    fn pending_create_object(name: &str) -> PendingDdlObject {
        PendingDdlObject::Create {
            entry: Box::new(put_collection_entry(name)),
        }
    }

    /// Install `token` as the DDL preparation owner, as an applied
    /// `DdlPrepareAcquire` does. A pending propose and finalize apply only
    /// under the owner's token.
    fn own_ddl_lease(state: &SharedState, token: u64) {
        *state.metadata_ddl.owner.lock().unwrap() =
            Some(crate::control::metadata_proposer::DdlPrepareOwner {
                token,
                node_id: state.node_id,
                acquired_at: std::time::Instant::now(),
            });
    }

    /// A reclaimed owner's late propose and finalize apply nothing on any
    /// replica, and a prepared entry under its token is skipped too.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_reclaimed_owners_late_entries_apply_nothing() {
        let (applier, state, _core, _tmp) = make_applier_with_acknowledging_core();
        let dead = 21;
        let acquire =
            |token: u64, node_id: u64| MetadataEntry::DdlPrepareAcquire { token, node_id };
        let entries = [
            acquire(dead, 2),
            // The metadata leader reclaims the dead owner's lease.
            MetadataEntry::DdlPrepareRelease { token: dead },
            acquire(22, 3),
            MetadataEntry::DdlPendingPropose {
                token: dead,
                objects: vec![pending_create_object("late_pending")],
                proposed_at: Hlc::default(),
            },
            MetadataEntry::DdlPendingFinalize { token: dead },
            MetadataEntry::DdlPrepared {
                token: dead,
                entry: Box::new(put_collection_entry("late_prepared")),
            },
        ];
        let batch: Vec<_> = entries
            .iter()
            .zip(1u64..)
            .map(|(entry, index)| (index, encode_entry(entry).unwrap()))
            .collect();
        assert_eq!(applier.apply(&batch).await, 6, "no late entry wedges apply");

        assert!(
            !state.pending_ddl.contains(dead),
            "the late propose reserved nothing"
        );
        let catalog = state.credentials.catalog();
        for name in ["late_pending", "late_prepared"] {
            assert!(
                catalog
                    .get_collection(DatabaseId::DEFAULT, 7, name)
                    .unwrap()
                    .is_none(),
                "{name} must not apply under a reclaimed token"
            );
        }
        assert_ne!(
            state
                .metadata_ddl
                .applied_token
                .load(std::sync::atomic::Ordering::Acquire),
            dead,
            "the dead owner's proposer must see its entries superseded"
        );
        assert_eq!(catalog.load_ddl_owner().unwrap(), Some((22, 3)));
    }

    /// An applier wired to a real `SharedState` (weak handle installed). Every
    /// entry with host effects needs it: without it the apply returns a
    /// transient error and the watermark stays put.
    fn make_applier_with_shared() -> (MetadataCommitApplier, Arc<SharedState>, tempfile::TempDir) {
        let (applier, state, _data_sides, tmp) = applier_over_shared_state();
        // `_data_sides` drops here, so no Data Plane request is ever answered.
        (applier, state, tmp)
    }

    /// [`make_applier_with_shared`] with one Data Plane core that
    /// acknowledges every request. A create's post-apply clears the name's
    /// storage on every core before it registers, so an applied create needs
    /// the answer. Runs inside a multi-thread tokio runtime: the core is a
    /// spawned task, and the applier blocks its own worker for the post-apply.
    fn make_applier_with_acknowledging_core() -> (
        MetadataCommitApplier,
        Arc<SharedState>,
        tokio::task::JoinHandle<()>,
        tempfile::TempDir,
    ) {
        let (applier, state, mut data_sides, tmp) = applier_over_shared_state();
        let side = data_sides.pop().expect("one data side");
        let core = tokio::spawn(crate::control::state::test_core::acknowledge_every_request(
            Arc::clone(&state),
            side,
        ));
        (applier, state, core, tmp)
    }

    /// An applier over a one-core `SharedState`, and that core's data side.
    fn applier_over_shared_state() -> (
        MetadataCommitApplier,
        Arc<SharedState>,
        Vec<crate::bridge::dispatch::CoreChannelDataSide>,
        tempfile::TempDir,
    ) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let wal =
            Arc::new(WalManager::open_for_testing(&tmp.path().join("test.wal")).expect("open wal"));
        let credentials =
            Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open catalog"));
        let (dispatcher, data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new_with_credentials(dispatcher, wal, credentials, false)
            .expect("construct shared state");
        // A fixture whose data side drops never answers a Data Plane request.
        // Keep the request deadline short: the tests cover apply semantics,
        // not the production deadline. Sole reference here, so `get_mut`
        // always succeeds.
        Arc::get_mut(&mut state)
            .expect("sole reference to the fixture's SharedState")
            .tuning
            .network
            .default_deadline_secs = 1;
        let (tx, _rx) = broadcast::channel(16);
        let token_state = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let applier = MetadataCommitApplier::new(
            state.metadata_cache.clone(),
            tx,
            state.credentials.clone(),
            token_state,
        );
        applier.install_shared(Arc::downgrade(&state));
        (applier, state, data_sides, tmp)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn propose_then_finalize_applies_and_clears_the_record() {
        let (applier, state, _core, _tmp) = make_applier_with_acknowledging_core();
        let token = 1;
        own_ddl_lease(&state, token);
        let propose = MetadataEntry::DdlPendingPropose {
            token,
            objects: vec![pending_create_object("pending_orders")],
            proposed_at: Hlc::default(),
        };
        assert_eq!(
            applier.apply(&[(1, encode_entry(&propose).unwrap())]).await,
            1
        );
        assert!(
            state.pending_ddl.contains(token),
            "propose reserves the record"
        );
        assert!(
            state
                .credentials
                .catalog()
                .get_collection(DatabaseId::DEFAULT, 7, "pending_orders")
                .unwrap()
                .is_none(),
            "propose alone must not write the catalog"
        );

        let finalize = MetadataEntry::DdlPendingFinalize { token };
        assert_eq!(
            applier
                .apply(&[(2, encode_entry(&finalize).unwrap())])
                .await,
            2
        );
        assert!(
            !state.pending_ddl.contains(token),
            "finalize clears the record"
        );
        assert!(
            state
                .credentials
                .catalog()
                .get_collection(DatabaseId::DEFAULT, 7, "pending_orders")
                .unwrap()
                .is_some(),
            "finalize replays the reserved object's host-side effects"
        );

        // Double-apply (Raft re-delivery): no record left, so this must be a
        // silent no-op rather than an error or a repeat write.
        assert_eq!(
            applier
                .apply(&[(3, encode_entry(&finalize).unwrap())])
                .await,
            3
        );
        assert!(!state.pending_ddl.contains(token));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn propose_then_cancel_clears_without_touching_the_catalog() {
        let (applier, state, _tmp) = make_applier_with_shared();
        let token = 2;
        own_ddl_lease(&state, token);
        let propose = MetadataEntry::DdlPendingPropose {
            token,
            objects: vec![pending_create_object("pending_widgets")],
            proposed_at: Hlc::default(),
        };
        assert_eq!(
            applier.apply(&[(1, encode_entry(&propose).unwrap())]).await,
            1
        );
        assert!(state.pending_ddl.contains(token));

        let cancel = MetadataEntry::DdlPendingCancel { token };
        assert_eq!(
            applier.apply(&[(2, encode_entry(&cancel).unwrap())]).await,
            2
        );
        assert!(
            !state.pending_ddl.contains(token),
            "cancel clears the record"
        );
        // The fixture's Data Plane never answers, so the spawned teardown
        // cannot succeed and remove the row: it was queued before the spawn.
        let queued = state
            .credentials
            .catalog()
            .load_pending_reclaim_queue()
            .unwrap();
        assert!(
            queued
                .iter()
                .any(|entry| entry.tenant_id == 7 && entry.name == "pending_widgets"),
            "cancel queues a durable reclaim for the teardown: {queued:?}"
        );
        assert!(
            state
                .credentials
                .catalog()
                .get_collection(DatabaseId::DEFAULT, 7, "pending_widgets")
                .unwrap()
                .is_none(),
            "cancel must never write the catalog"
        );

        // Double-apply (Raft re-delivery): no record left, must stay a no-op.
        assert_eq!(
            applier.apply(&[(3, encode_entry(&cancel).unwrap())]).await,
            3
        );
        assert!(!state.pending_ddl.contains(token));
    }

    /// A reclaim that fails before any durable retry is queued stops the
    /// batch at its entry. The re-delivered entry runs the reclaim again.
    #[cfg(feature = "failpoints")]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_reclaim_with_no_retry_queued_stops_the_batch_and_redelivery_retries_it() {
        let (applier, state, _tmp) = make_applier_with_shared();
        let purge = MetadataEntry::CatalogDdl {
            payload: catalog_entry::encode(&CatalogEntry::PurgeCollection {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tenant_id: 7,
                name: "orders".to_string(),
                target_descriptor_version: 0,
                target_hlc: Hlc::ZERO,
            })
            .unwrap(),
        };
        let batch = [(1, encode_entry(&purge).unwrap()), (2, Vec::new())];

        {
            let _fail = nodedb_types::fail_point::FailGuard::fail(
                "collection_reclaim::before_tombstone",
                "injected tombstone write error",
            );
            assert_eq!(
                applier.apply(&batch).await,
                0,
                "the failed reclaim stops the batch before its entry"
            );
        }
        assert!(
            state
                .credentials
                .catalog()
                .load_pending_reclaim_queue()
                .unwrap()
                .is_empty(),
            "the injected error precedes every durable reclaim step"
        );

        // Re-delivery: the reclaim runs again. The fixture's Data Plane never
        // answers, so it ends with a queued durable retry, which counts as done.
        assert_eq!(applier.apply(&batch).await, 2);
        let queued = state
            .credentials
            .catalog()
            .load_pending_reclaim_queue()
            .unwrap();
        assert!(
            queued
                .iter()
                .any(|entry| entry.tenant_id == 7 && entry.name == "orders"),
            "the re-delivered purge ran its reclaim: {queued:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn finalize_and_cancel_for_an_unknown_token_are_noops() {
        let (applier, state, _tmp) = make_applier_with_shared();
        let unknown = 999;
        assert_eq!(
            applier
                .apply(&[(
                    1,
                    encode_entry(&MetadataEntry::DdlPendingFinalize { token: unknown }).unwrap()
                )])
                .await,
            1,
            "finalize with no matching propose must not wedge the watermark"
        );
        assert_eq!(
            applier
                .apply(&[(
                    2,
                    encode_entry(&MetadataEntry::DdlPendingCancel { token: unknown }).unwrap()
                )])
                .await,
            2,
            "cancel with no matching propose must not wedge the watermark"
        );
        assert!(!state.pending_ddl.contains(unknown));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn apply_put_collection_writes_through_to_redb() {
        let (applier, state, _core, _tmp) = make_applier_with_acknowledging_core();
        let bytes = encode_entry(&put_collection_entry("orders")).unwrap();
        assert_eq!(applier.apply(&[(11, bytes)]).await, 11);

        let cache_guard = state.metadata_cache.read().unwrap();
        assert_eq!(cache_guard.applied_index, 11);
        assert_eq!(cache_guard.catalog_entries_applied, 1);
        drop(cache_guard);

        let loaded = state
            .credentials
            .catalog()
            .get_collection(DatabaseId::DEFAULT, 7, "orders")
            .unwrap()
            .expect("present");
        assert_eq!(loaded.name, "orders");
        assert_eq!(loaded.owner, "tester");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn apply_deactivate_preserves_record() {
        let (applier, state, _core, _tmp) = make_applier_with_acknowledging_core();

        // Seed.
        assert_eq!(
            applier
                .apply(&[(1, encode_entry(&put_collection_entry("archived")).unwrap())])
                .await,
            1,
            "the seeding create applies"
        );

        let drop_entry = MetadataEntry::CatalogDdl {
            payload: catalog_entry::encode(&CatalogEntry::DeactivateCollection {
                database_id: 0,
                tenant_id: 7,
                name: "archived".into(),
                descriptor_version: 0,
                modification_hlc: nodedb_types::Hlc::ZERO,
            })
            .unwrap(),
        };
        applier
            .apply(&[(2, encode_entry(&drop_entry).unwrap())])
            .await;

        let loaded = state
            .credentials
            .catalog()
            .get_collection(DatabaseId::DEFAULT, 7, "archived")
            .unwrap()
            .expect("preserved");
        assert!(!loaded.is_active);
    }

    #[tokio::test]
    async fn join_token_transition_updates_and_persists_shared_mirror() {
        let (applier, _cache, credentials, _tmp) = make_applier();
        let hash = [0x44; 32];
        let entries = [
            MetadataEntry::JoinTokenTransition {
                token_hash: hash,
                transition: nodedb_cluster::JoinTokenTransitionKind::Register {
                    expires_at_ms: 10_000,
                },
                ts_ms: 1,
            },
            MetadataEntry::JoinTokenTransition {
                token_hash: hash,
                transition: nodedb_cluster::JoinTokenTransitionKind::BeginInFlight {
                    node_addr: "127.0.0.1:9000".into(),
                    lease_id: [0x55; 16],
                },
                ts_ms: 2,
            },
            MetadataEntry::JoinTokenTransition {
                token_hash: hash,
                transition: nodedb_cluster::JoinTokenTransitionKind::MarkConsumed {
                    node_addr: "127.0.0.1:9000".into(),
                    lease_id: [0x55; 16],
                    recovery_bundle: vec![1, 2, 3],
                },
                ts_ms: 3,
            },
        ];
        for (offset, entry) in entries.iter().enumerate() {
            let index = offset as u64 + 1;
            assert_eq!(
                applier
                    .apply(&[(index, encode_entry(entry).expect("encode"))])
                    .await,
                index
            );
        }
        let persisted = credentials
            .catalog()
            .list_join_token_states()
            .expect("load token state");
        assert!(matches!(
            persisted.as_slice(),
            [nodedb_cluster::JoinTokenState {
                lifecycle: nodedb_cluster::JoinTokenLifecycle::Consumed { ts_ms: 3, .. },
                ..
            }]
        ));
    }

    /// Without `SharedState` a catalog DDL cannot land its host effects, so
    /// the watermark stays below the entry and Raft re-delivers it.
    #[tokio::test]
    async fn catalog_ddl_without_shared_state_is_not_applied() {
        let (applier, _cache, credentials, _tmp) = make_applier();
        let bytes = encode_entry(&put_collection_entry("orders")).unwrap();
        assert_eq!(applier.apply(&[(4, bytes)]).await, 0);
        assert!(
            credentials
                .catalog()
                .get_collection(DatabaseId::DEFAULT, 7, "orders")
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn apply_empty_batch_is_noop() {
        let (applier, _cache, _credentials, _tmp) = make_applier();
        assert_eq!(applier.apply(&[]).await, 0);
    }

    #[tokio::test]
    async fn apply_noop_entry_advances_cache_watermark() {
        let (applier, cache, _credentials, _tmp) = make_applier();
        // A committed Raft no-op (empty payload) at index 1 — the shape of every
        // group's first entry on a fresh single-node start. It mutates nothing, but
        // the cache watermark must advance in lockstep with the Raft applied index
        // the tick loop takes from the return value; otherwise the startup
        // applied-index sanity check reads a spurious gap and fails the boot.
        assert_eq!(applier.apply(&[(1, Vec::new())]).await, 1);
        assert_eq!(cache.read().unwrap().applied_index, 1);
        assert_eq!(
            cache.read().unwrap().catalog_entries_applied,
            0,
            "a no-op applies no catalog entry"
        );
    }

    /// Wall clock in nanoseconds since the epoch, matching what `HlcClock`
    /// compares a remote observation against.
    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }

    /// A proposer's `proposed_at` must advance this node's clock, so a later
    /// local stamp is causally after the remote DDL that preceded it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_proposers_hlc_advances_the_local_clock() {
        let (applier, state, _tmp) = make_applier_with_shared();
        let ahead = Hlc::new(now_ns() + 500_000_000, 0);
        own_ddl_lease(&state, 9);
        let propose = MetadataEntry::DdlPendingPropose {
            token: 9,
            objects: vec![pending_create_object("hlc_fold")],
            proposed_at: ahead,
        };
        assert_eq!(
            applier.apply(&[(1, encode_entry(&propose).unwrap())]).await,
            1
        );
        assert!(
            state.hlc_clock.peek() >= ahead,
            "the local clock must absorb a proposer's observation"
        );
    }

    /// A proposer whose clock runs far ahead must not drag this node forward.
    /// The entry still applies — it is already committed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_far_future_proposer_hlc_is_refused_but_the_entry_still_applies() {
        let (applier, state, _tmp) = make_applier_with_shared();
        let far = Hlc::new(now_ns() + nodedb_types::MAX_CLOCK_SKEW_NS * 10, 0);
        own_ddl_lease(&state, 11);
        let propose = MetadataEntry::DdlPendingPropose {
            token: 11,
            objects: vec![pending_create_object("hlc_skew")],
            proposed_at: far,
        };
        assert_eq!(
            applier.apply(&[(1, encode_entry(&propose).unwrap())]).await,
            1
        );
        assert!(
            state.hlc_clock.peek() < far,
            "a skewed proposer must never move this node's clock"
        );
        assert!(
            state.pending_ddl.contains(11),
            "the committed entry must still apply — refusing the fold is the \
             protection, wedging the state machine is not"
        );
    }

    /// A topic the batch in progress creates is visible before the applied
    /// watermark reaches its entry. The floor a reader stamps once it sees
    /// the topic is at or above the entry that created it.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_object_visible_mid_batch_yields_a_floor_at_its_entry() {
        let (applier, state, _tmp) = make_applier_with_shared();
        let topic = crate::event::topic::TopicDef {
            database_id: DatabaseId::DEFAULT,
            tenant_id: 1,
            name: "floor_feed".into(),
            retention: crate::event::cdc::stream_def::RetentionConfig::default(),
            owner: "tester".into(),
            created_at: 0,
            last_sequence: 0,
            last_lsn: 0,
            last_epoch: 0,
            modification_hlc: Hlc::ZERO,
        };
        let create = MetadataEntry::CatalogDdl {
            payload: catalog_entry::encode(&CatalogEntry::CreateTopicIfAbsent(Box::new(topic)))
                .unwrap(),
        };
        let watcher = state.applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID);
        // The entry creating the topic sits inside a batch that ends later.
        let batch = [(7, encode_entry(&create).unwrap()), (8, Vec::new())];
        assert_eq!(applier.apply(&batch).await, 8);

        // The Raft loop bumps the applied watermark only after the batch
        // returns, so the topic is visible with `applied` still below it.
        assert!(
            state
                .ep_topic_registry
                .get(DatabaseId::DEFAULT, 1, "floor_feed")
                .is_some()
        );
        assert!(watcher.current() < 7);
        assert!(
            watcher.floor() >= 7,
            "a reader that sees the topic stamps a floor at or above its creation"
        );
    }
}
