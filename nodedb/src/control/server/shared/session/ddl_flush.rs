// SPDX-License-Identifier: BUSL-1.1

//! COMMIT-time drain of a connection's buffered transactional DDL.
//!
//! One transaction's DDL commits as a single unit. With a metadata raft group
//! the batch goes through it fenced by a preparation lease. Without one — no
//! `[cluster]` and `server.single_node_calvin = false`, the only shape that
//! skips `start_raft` — the entries land locally in statement order instead.
//!
//! A crash after `finalize_pending` but before buffered DML dispatch
//! completes leaves the metadata log unable to tell whether the DML landed —
//! true of any buffered-DML transaction, DDL or not.

use std::sync::Arc;

use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry, PendingDdlObject, encode_entry};

use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::metadata_proposer::MetadataRaftHandle;
use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;

use super::connection::SessionId;
use super::ddl_buffer::{DdlBuffer, take};
use super::outcome::AbortReason;
use super::store::SessionStore;

/// What COMMIT must do with this connection's buffered DDL before any
/// buffered DML dispatches. Built once by [`begin_commit`], which drains the
/// buffer — nothing after it may call [`take`] again for this COMMIT.
pub(super) enum DdlCommitPlan<'a> {
    /// Nothing was buffered: a pure-DML commit, unchanged.
    None,
    /// Buffered DDL with no replicated metadata group. Apply with
    /// [`flush_local`] at the same point single-node COMMIT always has —
    /// after dispatch — since there is no cross-node visibility problem to
    /// close for this shape.
    Local(DdlBuffer),
    /// Buffered DDL already reserved via `DdlPendingPropose`. The caller
    /// must call [`finalize_pending`] on this handle before dispatching any
    /// buffered DML, then, on any dispatch failure, propose compensation
    /// for `handle.objects()` via [`compensate_finalized`].
    Pending(PendingDdlHandle<'a>),
}

/// Drain the connection's DDL buffer and decide how COMMIT must handle it.
///
/// Takes the buffer exactly once for the whole COMMIT — replicated and
/// local dispatch both flow from the returned plan, never from a second
/// [`take`]. `sessions`/`session_id` identify the calling transaction so a
/// same-transaction ALTER can exclude its own buffered-write lease hold from
/// the descriptor drain instead of waiting on a hold it owns itself.
pub(super) fn begin_commit<'a>(
    state: &'a SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
) -> crate::Result<DdlCommitPlan<'a>> {
    let Some(buffered) = take() else {
        return Ok(DdlCommitPlan::None);
    };
    if buffered.is_empty() {
        return Ok(DdlCommitPlan::None);
    }
    match state.metadata_raft.get() {
        Some(handle) if replicated_ddl_active(state) => {
            propose_pending_buffered(state, sessions, session_id, handle, buffered)
                .map(DdlCommitPlan::Pending)
        }
        _ => Ok(DdlCommitPlan::Local(buffered)),
    }
}

/// True when DDL on this node replicates through the metadata raft group.
/// False only in mixed-version compat mode, where the originating node is the
/// sole writer. `MIN_WIRE_FORMAT_VERSION == WIRE_FORMAT_VERSION` today, so no
/// admissible peer reports below the gate and this is currently always true.
fn replicated_ddl_active(state: &SharedState) -> bool {
    state
        .cluster_version_view()
        .can_activate_feature(crate::control::rolling_upgrade::DISTRIBUTED_CATALOG_VERSION)
}

/// Apply every buffered entry locally, in statement order, then run the same
/// two post-apply phases the metadata applier runs. Skipping them leaves the
/// catalog and the live registries disagreeing until restart. Reached only
/// without a metadata raft group; the first failure aborts the COMMIT.
pub(super) fn flush_local(state: &SharedState, buffered: DdlBuffer) -> Option<AbortReason> {
    let shared = match state.self_arc() {
        Ok(shared) => shared,
        Err(error) => return Some(AbortReason::DdlPropose(error)),
    };
    let catalog = shared.credentials.catalog();
    let total = buffered.len();
    for (position, item) in buffered.into_iter().enumerate() {
        match crate::control::catalog_entry::apply::apply_to(&item.entry, catalog) {
            // Wrote nothing (if-absent create for an existing descriptor):
            // the applier suppresses post-apply here, so this must too.
            Ok(false) => continue,
            Ok(true) => {}
            Err(error) => {
                return Some(AbortReason::DdlPropose(crate::Error::Internal {
                    detail: format!(
                        "transactional DDL local apply failed on statement {} of {} \
                         (catalog entry {}): {error}; roll back and re-run the transaction",
                        position + 1,
                        total,
                        item.entry.kind()
                    ),
                }));
            }
        }
        crate::control::catalog_entry::post_apply::apply_post_apply_side_effects_sync(
            &item.entry,
            &shared,
        );
        // No raft index exists here; the async phase uses it purely as the
        // purge LSN for storage reclaim, which the local DDL paths take from
        // the WAL instead.
        let purge_lsn = shared.wal.next_lsn().as_u64();
        crate::control::catalog_entry::post_apply::spawn_post_apply_async_side_effects(
            item.entry,
            Arc::clone(&shared),
            purge_lsn,
        );
    }
    None
}

/// Fencing token, metadata log index, preparation lease, and reserved
/// objects of one propose/finalize window. `handle` is always passed by
/// value into [`finalize_pending`], which drops it — on success after the
/// node-local pending record is gone, or via an internal `?` on failure —
/// so the lease releases at exactly that moment either way. `objects` is
/// cloned out before that move so a dispatch failure that follows can still
/// build compensation via [`compensate_finalized`].
pub(super) struct PendingDdlHandle<'a> {
    token: u64,
    log_index: u64,
    /// Never read past construction — its type is the whole point: `Drop`
    /// proposes `DdlPrepareRelease`, and this handle must hold it exactly
    /// as long as `PendingDdlHandle` itself lives, so the lease releases at
    /// the same moment `finalize_pending` drops this handle.
    _guard: crate::control::metadata_proposer::DdlPrepareGuard<'a>,
    objects: Vec<PendingDdlObject>,
}

impl PendingDdlHandle<'_> {
    pub(super) fn token(&self) -> u64 {
        self.token
    }

    pub(super) fn log_index(&self) -> u64 {
        self.log_index
    }

    /// The reserved objects, for building compensation after
    /// `finalize_pending` has consumed this handle.
    pub(super) fn objects(&self) -> &[PendingDdlObject] {
        &self.objects
    }
}

/// Propose `entry` to the metadata group and block until this node's applied
/// watermark reaches its log index.
fn propose_and_await(
    state: &SharedState,
    handle: &dyn MetadataRaftHandle,
    entry: &MetadataEntry,
) -> crate::Result<u64> {
    let raw = encode_entry(entry).map_err(|e| crate::Error::Internal {
        detail: format!("metadata entry encode: {e}"),
    })?;
    let log_index = handle.propose(raw)?;
    let watcher = state.applied_index_watcher(METADATA_GROUP_ID);
    let outcome = tokio::task::block_in_place(|| {
        watcher.wait_for(
            log_index,
            crate::control::metadata_proposer::DEFAULT_PROPOSE_TIMEOUT,
        )
    });
    if !outcome.is_reached() {
        return Err(crate::Error::Internal {
            detail: format!(
                "metadata propose timed out waiting for log index {log_index} (current: {})",
                watcher.current()
            ),
        });
    }
    Ok(log_index)
}

/// The committed prior value for `entry`, encoded the same shape its
/// `CatalogDdl` payload carries — `None` for entry kinds `descriptor_stamp`
/// does not version, which always propose as a fresh create.
fn committed_before_image(
    entry: &CatalogEntry,
    catalog: &SystemCatalog,
) -> crate::Result<Option<Vec<u8>>> {
    let prior = match entry {
        CatalogEntry::PutCollection(stored) | CatalogEntry::PutCollectionIfAbsent(stored) => {
            catalog
                .get_committed_collection(stored.database_id, stored.tenant_id, &stored.name)?
                .map(|prior| CatalogEntry::PutCollection(Box::new(prior)))
        }
        CatalogEntry::PutMaterializedView(stored) => catalog
            .get_committed_materialized_view(stored.database_id, stored.tenant_id, &stored.name)?
            .map(|prior| CatalogEntry::PutMaterializedView(Box::new(prior))),
        CatalogEntry::PutFunction(stored) => catalog
            .get_committed_function_in_database(stored.database_id, stored.tenant_id, &stored.name)?
            .map(|prior| CatalogEntry::PutFunction(Box::new(prior))),
        CatalogEntry::PutProcedure(stored) => catalog
            .get_committed_procedure_in_database(
                stored.database_id,
                stored.tenant_id,
                &stored.name,
            )?
            .map(|prior| CatalogEntry::PutProcedure(Box::new(prior))),
        CatalogEntry::PutTrigger(stored) => catalog
            .get_committed_trigger_in_database(stored.database_id, stored.tenant_id, &stored.name)?
            .map(|prior| CatalogEntry::PutTrigger(Box::new(prior))),
        CatalogEntry::PutSequence(stored) => catalog
            .get_sequence(stored.database_id, stored.tenant_id, &stored.name)?
            .map(|prior| CatalogEntry::PutSequence(Box::new(prior))),
        CatalogEntry::PutContinuousAggregate(stored) => catalog
            .get_continuous_aggregate(stored.database_id, stored.tenant_id, &stored.name)?
            .map(|prior| CatalogEntry::PutContinuousAggregate(Box::new(prior))),
        _ => None,
    };
    prior.as_ref().map(catalog_entry::encode).transpose()
}

/// Reserve `buffered` under a fresh fencing token via `DdlPendingPropose`,
/// without dispatching any DML. Runs the same lease/drain/stamp sequence the
/// old atomic `flush_replicated` ran, then reserves each stamped statement
/// as a [`PendingDdlObject`] instead of applying it.
fn propose_pending_buffered<'a>(
    state: &'a SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    handle: &'a Arc<dyn MetadataRaftHandle>,
    buffered: DdlBuffer,
) -> crate::Result<PendingDdlHandle<'a>> {
    let _local_guard = state
        .metadata_ddl_lock
        .lock()
        .map_err(|_| crate::Error::Internal {
            detail: "metadata DDL preparation lock poisoned".into(),
        })?;
    let distributed_guard =
        crate::control::metadata_proposer::acquire_ddl_prepare_lease(state, handle.as_ref())?;

    for item in &buffered {
        if let Some((descriptor_id, prior_version)) =
            crate::control::lease::descriptor_id_and_prior_version(&item.entry, state)
            && prior_version > 0
        {
            // Exclude the calling transaction's own statement-time lease
            // hold on this descriptor (a buffered write to the collection
            // this same transaction is altering) — a session cannot
            // conflict with itself.
            let own_holds =
                sessions.own_lease_hold_count(session_id, &descriptor_id, prior_version);
            crate::control::lease::drain_for_ddl(
                state,
                descriptor_id,
                prior_version,
                crate::control::metadata_proposer::DEFAULT_DRAIN_TIMEOUT,
                own_holds,
            )?;
        }
    }

    let audits: Vec<_> = buffered.iter().map(|item| item.audit.clone()).collect();
    let entries: Vec<_> = buffered.into_iter().map(|item| item.entry).collect();
    let catalog = state.credentials.catalog();
    let stamped = if state
        .cluster_version_view()
        .can_activate_feature(crate::control::rolling_upgrade::DESCRIPTOR_VERSIONING_VERSION)
    {
        catalog_entry::descriptor_stamp::stamp_batch(entries, &state.hlc_clock, catalog)
    } else {
        entries
    };

    let mut objects = Vec::with_capacity(stamped.len());
    for (entry, audit) in stamped.iter().zip(audits) {
        let payload = catalog_entry::encode(entry)?;
        let wire = match audit {
            Some(ctx) => MetadataEntry::CatalogDdlAudited {
                payload,
                auth_user_id: ctx.auth_user_id,
                auth_user_name: ctx.auth_user_name,
                sql_text: ctx.sql_text,
            },
            None => MetadataEntry::CatalogDdl { payload },
        };
        objects.push(match committed_before_image(entry, catalog)? {
            Some(before_image) => PendingDdlObject::Alter {
                entry: Box::new(wire),
                before_image,
            },
            None => PendingDdlObject::Create {
                entry: Box::new(wire),
            },
        });
    }

    let token = distributed_guard.token();
    let pending = MetadataEntry::DdlPendingPropose {
        token,
        objects: objects.clone(),
        proposed_at: state.hlc_clock.now(),
    };
    let log_index = propose_and_await(state, handle.as_ref(), &pending)?;

    Ok(PendingDdlHandle {
        token,
        log_index,
        _guard: distributed_guard,
        objects,
    })
}

/// Commit the objects `handle` reserved: propose `DdlPendingFinalize`, wait
/// for local apply, then release the preparation lease `handle` has held
/// since [`begin_commit`] constructed it via `propose_pending_buffered`.
/// The only other place a `DdlPendingPropose` record can be resolved is the
/// lease-reclaim path in `metadata_proposer::acquire_ddl_prepare_lease`,
/// which proposes `DdlPendingCancel` directly for a dead owner's stranded
/// record — nothing between a successful propose and this call can fail, so
/// there is no in-process cancel path to mirror it.
pub(super) fn finalize_pending(
    state: &SharedState,
    handle: PendingDdlHandle<'_>,
) -> crate::Result<()> {
    let Some(raft_handle) = state.metadata_raft.get() else {
        return Err(crate::Error::Internal {
            detail: "finalize_pending: no metadata raft group installed".into(),
        });
    };
    tracing::debug!(
        token = handle.token(),
        log_index = handle.log_index(),
        "finalizing pending DDL"
    );
    let bears_authorization =
        super::ddl_authorization::objects_bear_authorization(handle.objects())?;
    let log_index = propose_and_await(
        state,
        raft_handle.as_ref(),
        &MetadataEntry::DdlPendingFinalize {
            token: handle.token(),
        },
    )?;
    if bears_authorization {
        super::ddl_authorization::barrier_at(state, log_index)?;
    }
    Ok(())
}

/// Undo every object `finalize_pending` already applied to the catalog,
/// after a buffered-DML dispatch failure. Proposed as one fenced batch, so
/// the compensation itself is atomic: a fresh `Create` is purged/deleted, an
/// `Alter` is restored from its captured `before_image`. A failure here is
/// returned, never swallowed — the caller surfaces it alongside the original
/// dispatch failure rather than logging and continuing.
pub(super) fn compensate_finalized(
    state: &SharedState,
    objects: &[PendingDdlObject],
) -> crate::Result<()> {
    let Some(handle) = state.metadata_raft.get() else {
        return Err(crate::Error::Internal {
            detail: "compensate_finalized: no metadata raft group installed".into(),
        });
    };
    let mut entries = Vec::with_capacity(objects.len());
    for object in objects {
        let payload = match object {
            PendingDdlObject::Alter { before_image, .. } => before_image.clone(),
            PendingDdlObject::Create { entry } => {
                let created = catalog_entry::decode(wire_payload(entry)?)?;
                catalog_entry::encode(&reverse_create(&created)?)?
            }
        };
        entries.push(MetadataEntry::CatalogDdl { payload });
    }
    let log_index = propose_and_await(state, handle.as_ref(), &MetadataEntry::Batch { entries })?;
    // The compensation restores prior authorization state, which binds every
    // node like any other authorization change.
    if super::ddl_authorization::objects_bear_authorization(objects)? {
        super::ddl_authorization::barrier_at(state, log_index)?;
    }
    Ok(())
}

/// The opaque catalog payload `entry` carries, regardless of audit wrapping.
fn wire_payload(entry: &MetadataEntry) -> crate::Result<&[u8]> {
    match entry {
        MetadataEntry::CatalogDdl { payload }
        | MetadataEntry::CatalogDdlAudited { payload, .. } => Ok(payload),
        other => Err(crate::Error::Internal {
            detail: format!(
                "commit compensation: pending DDL wire shape is not CatalogDdl: {other:?}"
            ),
        }),
    }
}

/// The catalog entry that undoes `entry` after `finalize_pending` has
/// already applied it as a fresh `PendingDdlObject::Create`. Covers the
/// object kinds transactional DDL can buffer as a create; anything else
/// reports a typed error instead of silently doing nothing.
fn reverse_create(entry: &CatalogEntry) -> crate::Result<CatalogEntry> {
    match entry {
        CatalogEntry::PutCollection(stored) | CatalogEntry::PutCollectionIfAbsent(stored) => {
            Ok(CatalogEntry::PurgeCollection {
                database_id: stored.database_id.as_u64(),
                tenant_id: stored.tenant_id,
                name: stored.name.clone(),
            })
        }
        CatalogEntry::PutSequence(stored) => Ok(CatalogEntry::DeleteSequence {
            database_id: stored.database_id,
            tenant_id: stored.tenant_id,
            name: stored.name.clone(),
        }),
        CatalogEntry::PutFunction(stored) => Ok(CatalogEntry::DeleteFunction {
            database_id: stored.database_id,
            tenant_id: stored.tenant_id,
            name: stored.name.clone(),
        }),
        CatalogEntry::PutTrigger(stored) => Ok(CatalogEntry::DeleteTrigger {
            database_id: stored.database_id,
            tenant_id: stored.tenant_id,
            name: stored.name.clone(),
        }),
        CatalogEntry::PutProcedure(stored) => Ok(CatalogEntry::DeleteProcedure {
            database_id: stored.database_id,
            tenant_id: stored.tenant_id,
            name: stored.name.clone(),
        }),
        CatalogEntry::PutIndexRecord(stored) => Ok(CatalogEntry::DeleteIndexRecord {
            database_id: stored.database_id,
            tenant_id: stored.tenant_id,
            name: stored.name.clone(),
            collection: stored.collection.clone(),
        }),
        CatalogEntry::PutMaterializedView(stored) => Ok(CatalogEntry::DeleteMaterializedView {
            database_id: stored.database_id,
            tenant_id: stored.tenant_id,
            name: stored.name.clone(),
        }),
        other => Err(crate::Error::Internal {
            detail: format!(
                "commit compensation: no reversal defined for catalog entry kind {}",
                other.kind()
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::catalog_entry::CatalogEntry;
    use crate::control::gateway::Gateway;
    use crate::control::security::catalog::sequence_types::StoredSequence;
    use crate::control::security::credential::CredentialStore;
    use crate::control::security::identity::Permission;
    use crate::wal::WalManager;

    use super::super::connection::{ConnectionId, SessionId};
    use super::super::store::SessionStore;
    use super::super::{conn_scope, ddl_buffer};
    use super::{
        DdlCommitPlan, MetadataEntry, PendingDdlHandle, PendingDdlObject, SharedState,
        begin_commit, compensate_finalized, finalize_pending, flush_local, reverse_create,
    };

    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Weak};

    /// A fresh `SessionStore` with no session registered under the returned
    /// id, so `own_lease_hold_count` reports `0` — the tests in this module
    /// exercise no self-drain scenario, and this fixture must not change
    /// their outcome.
    fn test_session() -> (SessionStore, SessionId) {
        (
            SessionStore::new(),
            SessionId::from(ConnectionId::new(1).expect("nonzero connection id")),
        )
    }

    /// Construct a bare `SharedState` with no metadata raft group installed.
    fn build_shared_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let credentials = Arc::new(
            CredentialStore::open(&dir.path().join("system.redb")).expect("open credential store"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new_with_credentials(dispatcher, wal, credentials, false)
            .expect("construct shared state");
        let gateway = Arc::new(Gateway::new(Arc::clone(&state)));
        assert!(state.gateway.set(gateway).is_ok(), "gateway installs once");
        (state, dir)
    }

    /// A `SharedState` shaped like the one deployment that reaches
    /// `flush_local`: no metadata raft group (`[cluster]` absent AND
    /// `server.single_node_calvin = false`). The gateway is installed as
    /// `bootstrap::state_wiring` installs it, so `self_arc` resolves.
    fn local_only_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let (state, dir) = build_shared_state();
        // Guards the branch under test: with a handle installed, `begin_commit`
        // would take the `Pending` branch and none of these assertions would
        // mean anything.
        assert!(
            state.metadata_raft.get().is_none(),
            "fixture must have no metadata raft group, or begin_commit takes the pending branch"
        );
        (state, dir)
    }

    /// Test double for `MetadataRaftHandle`: applies the narrow subset of
    /// `MetadataEntry` variants the pending-DDL path touches directly against
    /// `SharedState` (in place of a real raft group) and bumps the local
    /// applied watermark so `wait_for` unblocks synchronously.
    struct FakeMetadataRaftHandle {
        shared: Weak<SharedState>,
        next_index: AtomicU64,
    }

    impl crate::control::metadata_proposer::MetadataRaftHandle for FakeMetadataRaftHandle {
        fn propose(&self, bytes: Vec<u8>) -> crate::Result<u64> {
            let shared = self
                .shared
                .upgrade()
                .ok_or_else(|| crate::Error::Internal {
                    detail: "fake metadata raft: state dropped".into(),
                })?;
            let entry =
                nodedb_cluster::decode_entry(&bytes).map_err(|e| crate::Error::Internal {
                    detail: format!("fake metadata raft: decode: {e}"),
                })?;
            match entry {
                MetadataEntry::DdlPrepareAcquire { token } => {
                    let mut owner = shared
                        .metadata_ddl_owner
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    if owner.is_none() || owner.is_some_and(|(current, _)| current == token) {
                        *owner = Some((token, std::time::Instant::now()));
                    }
                }
                MetadataEntry::DdlPrepareRelease { token } => {
                    let mut owner = shared
                        .metadata_ddl_owner
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    if owner.is_some_and(|(current, _)| current == token) {
                        *owner = None;
                    }
                }
                MetadataEntry::DdlPendingPropose {
                    token,
                    objects,
                    proposed_at,
                } => {
                    shared.pending_ddl.insert(token, objects, proposed_at);
                }
                MetadataEntry::DdlPendingFinalize { token }
                | MetadataEntry::DdlPendingCancel { token } => {
                    shared.pending_ddl.take(token);
                }
                _ => {}
            }
            let index = self.next_index.fetch_add(1, Ordering::SeqCst) + 1;
            shared
                .applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID)
                .bump(index);
            Ok(index)
        }
    }

    /// A `SharedState` with a fake metadata raft group installed, so
    /// `begin_commit` / `finalize_pending` take the replicated path
    /// against an in-process stand-in instead of a real raft group.
    fn replicated_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let (state, dir) = build_shared_state();
        let fake = FakeMetadataRaftHandle {
            shared: Arc::downgrade(&state),
            next_index: AtomicU64::new(0),
        };
        assert!(
            state.metadata_raft.set(Arc::new(fake)).is_ok(),
            "metadata raft installs once"
        );
        (state, dir)
    }

    /// Buffer one `PutSequence` and reserve it via `begin_commit`, which
    /// must take the `Pending` branch on this fixture's fake metadata raft
    /// group.
    async fn propose_one_sequence<'a>(state: &'a SharedState, name: &str) -> PendingDdlHandle<'a> {
        let stored = StoredSequence::new(0, 7, name.into(), "alice".into());
        let (sessions, session_id) = test_session();
        let plan = conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(CatalogEntry::PutSequence(Box::new(
                stored
            ))));
            begin_commit(state, &sessions, session_id)
        })
        .await
        .expect("begin_commit must succeed");
        match plan {
            DdlCommitPlan::Pending(handle) => handle,
            _ => panic!("fixture has a metadata raft group; expected a Pending plan"),
        }
    }

    /// Buffer `entries` in one connection scope and flush them as single-node
    /// COMMIT does: `begin_commit` then `flush_local` on the `Local` plan.
    /// True when the flush reported no abort.
    async fn buffer_and_flush(state: &SharedState, entries: Vec<CatalogEntry>) -> bool {
        let (sessions, session_id) = test_session();
        conn_scope::scoped(async {
            ddl_buffer::activate();
            for entry in entries {
                assert!(ddl_buffer::try_buffer(entry), "buffer is active");
            }
            match begin_commit(state, &sessions, session_id).expect("begin_commit must not error") {
                DdlCommitPlan::Local(buffered) => flush_local(state, buffered).is_none(),
                DdlCommitPlan::None => panic!("buffer was non-empty"),
                DdlCommitPlan::Pending(_) => panic!("fixture has no metadata raft group"),
            }
        })
        .await
    }

    #[tokio::test]
    async fn local_flush_populates_the_sequence_registry() {
        let (state, _dir) = local_only_state();
        assert!(
            !state.sequence_registry.exists(0, 7, "orders_seq"),
            "registry starts empty"
        );

        let stored = StoredSequence::new(0, 7, "orders_seq".into(), "alice".into());
        let ok = buffer_and_flush(&state, vec![CatalogEntry::PutSequence(Box::new(stored))]).await;

        assert!(ok, "local flush must not abort");
        assert!(
            state
                .credentials
                .catalog()
                .get_sequence(0, 7, "orders_seq")
                .expect("catalog read")
                .is_some(),
            "the catalog write is the part that already worked"
        );
        assert!(
            state.sequence_registry.exists(0, 7, "orders_seq"),
            "flush_local must run the post-apply sync phase: without it the catalog \
             and the live registry disagree until restart, and NEXTVAL / DROP SEQUENCE \
             report the sequence as missing"
        );
    }

    #[tokio::test]
    async fn local_flush_installs_a_replicated_grant() {
        let (state, _dir) = local_only_state();
        let stored =
            state
                .permissions
                .prepare_permission("widgets", "analyst", Permission::Read, "alice");
        assert!(
            !state
                .permissions
                .permission_exists("widgets", "analyst", Permission::Read),
            "grant cache starts empty"
        );

        let ok =
            buffer_and_flush(&state, vec![CatalogEntry::PutPermission(Box::new(stored))]).await;

        assert!(ok, "local flush must not abort");
        assert!(
            state
                .permissions
                .permission_exists("widgets", "analyst", Permission::Read),
            "flush_local must install the replicated grant, or the evaluator keeps \
             refusing a grant the catalog already holds"
        );
    }

    #[tokio::test]
    async fn local_flush_hooks_every_buffered_entry() {
        let (state, _dir) = local_only_state();
        let entries = vec![
            CatalogEntry::PutSequence(Box::new(StoredSequence::new(
                0,
                7,
                "first_seq".into(),
                "alice".into(),
            ))),
            CatalogEntry::PutSequence(Box::new(StoredSequence::new(
                0,
                7,
                "second_seq".into(),
                "alice".into(),
            ))),
        ];

        assert!(
            buffer_and_flush(&state, entries).await,
            "flush must not abort"
        );
        assert!(state.sequence_registry.exists(0, 7, "first_seq"));
        assert!(
            state.sequence_registry.exists(0, 7, "second_seq"),
            "the hook must run per entry, not once for the batch"
        );
    }

    #[tokio::test]
    async fn flush_outside_a_transaction_is_inert() {
        let (state, _dir) = local_only_state();
        let (sessions, session_id) = test_session();
        let is_none = conn_scope::scoped(async {
            matches!(
                begin_commit(&state, &sessions, session_id).expect("begin_commit must not error"),
                DdlCommitPlan::None
            )
        })
        .await;
        assert!(is_none, "no buffer means nothing to flush");
    }

    #[tokio::test]
    async fn begin_commit_on_empty_active_buffer_is_none() {
        let (state, _dir) = local_only_state();
        let (sessions, session_id) = test_session();
        let result = conn_scope::scoped(async {
            ddl_buffer::activate();
            begin_commit(&state, &sessions, session_id)
        })
        .await;
        assert!(
            matches!(
                result.expect("begin_commit must not error on an empty buffer"),
                DdlCommitPlan::None
            ),
            "an empty (but activated) buffer has nothing to reserve"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn propose_then_finalize_drops_the_pending_record_and_releases_the_lease() {
        let (state, _dir) = replicated_state();
        let handle = propose_one_sequence(&state, "orders_seq").await;
        let token = handle.token();
        assert!(
            state.pending_ddl.contains(token),
            "begin_commit must reserve a pending record"
        );

        finalize_pending(&state, handle).expect("finalize_pending must succeed");

        assert!(
            !state.pending_ddl.contains(token),
            "finalize_pending must drop the pending record"
        );
        assert!(
            state
                .metadata_ddl_owner
                .lock()
                .expect("owner lock")
                .is_none(),
            "finalize_pending must release the preparation lease"
        );
    }

    #[test]
    fn reverse_create_purges_a_created_collection() {
        let stored = crate::control::security::catalog::StoredCollection::new(7, "orders", "alice");
        let reversed = reverse_create(&CatalogEntry::PutCollection(Box::new(stored)))
            .expect("PutCollection reverses");
        assert!(matches!(
            reversed,
            CatalogEntry::PurgeCollection { tenant_id: 7, name, .. } if name == "orders"
        ));
    }

    #[test]
    fn reverse_create_deletes_a_created_sequence() {
        let stored = StoredSequence::new(0, 7, "orders_seq".into(), "alice".into());
        let reversed = reverse_create(&CatalogEntry::PutSequence(Box::new(stored)))
            .expect("PutSequence reverses");
        assert!(matches!(
            reversed,
            CatalogEntry::DeleteSequence {
                database_id: 0,
                tenant_id: 7,
                name
            } if name == "orders_seq"
        ));
    }

    #[test]
    fn reverse_create_rejects_an_unreversible_kind() {
        let err = reverse_create(&CatalogEntry::DeleteSequence {
            database_id: 0,
            tenant_id: 7,
            name: "orders_seq".into(),
        });
        assert!(
            err.is_err(),
            "no create-shaped reversal exists for a Delete entry"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn compensate_finalized_proposes_a_reversal_batch_for_a_create() {
        let (state, _dir) = replicated_state();
        let handle = propose_one_sequence(&state, "orders_seq").await;
        let objects: Vec<PendingDdlObject> = handle.objects().to_vec();
        finalize_pending(&state, handle).expect("finalize_pending must succeed");

        compensate_finalized(&state, &objects)
            .expect("compensate_finalized must propose a reversal batch for the finalized create");
    }
}
