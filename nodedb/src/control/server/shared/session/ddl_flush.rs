// SPDX-License-Identifier: BUSL-1.1

//! COMMIT-time drain of a connection's buffered transactional DDL.
//!
//! One transaction's DDL commits as a single unit through the metadata raft
//! group, fenced by a preparation lease. Every node runs a metadata group, a
//! one-node cluster included. A state with none refuses the COMMIT with a
//! typed error.
//!
//! A crash after `finalize_pending` but before buffered DML dispatch
//! completes leaves the metadata log unable to tell whether the DML landed —
//! true of any buffered-DML transaction, DDL or not.

use std::sync::Arc;

use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry, PendingDdlObject, encode_entry};

use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::metadata_proposer::{DdlPrepareLease, MetadataRaftHandle};
use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;

use super::connection::SessionId;
use super::ddl_buffer::{DdlBuffer, take};
use super::store::SessionStore;

/// What COMMIT must do with this connection's buffered DDL before any
/// buffered DML dispatches. Built once by [`begin_commit`], which drains the
/// buffer — nothing after it can call [`take`] again for this COMMIT.
pub(super) enum DdlCommitPlan<'a> {
    /// Nothing needs the pending path: nothing was buffered, or only consumer
    /// offset commits, which already applied without the preparation lease.
    None,
    /// Buffered DDL already reserved via `DdlPendingPropose`. The caller
    /// must call [`finalize_pending`] on this handle before dispatching any
    /// buffered DML, then, on any dispatch failure, propose compensation
    /// for `handle.objects()` via `ddl_compensate::compensate_finalized`.
    Pending(PendingDdlHandle<'a>),
}

/// Drain the connection's DDL buffer and reserve it through the metadata
/// group.
///
/// Takes the buffer exactly once for the whole COMMIT — the returned plan
/// carries it, never a second [`take`]. `sessions`/`session_id` identify the
/// calling transaction so a same-transaction ALTER can exclude its own
/// buffered-write lease hold from the descriptor drain instead of waiting on
/// a hold it owns itself.
pub(super) async fn begin_commit<'a>(
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
    let buffered = match commit_offsets_only(state, buffered).await? {
        Some(rest) => rest,
        None => return Ok(DdlCommitPlan::None),
    };
    let handle = state.metadata_raft_handle()?;
    propose_pending_buffered(state, sessions, session_id, handle, buffered)
        .await
        .map(DdlCommitPlan::Pending)
}

/// Commit a buffer that holds only consumer offset commits, without the DDL
/// preparation lease, and return `None`. Return any other buffer untouched.
///
/// An offset commit applies as a monotonic max and stamps no descriptor
/// version, so it needs no lease. It applies at the point a finalize does,
/// before the buffered DML dispatches, and a failed DML never lowers it, as
/// with a finalized batch. A buffer that mixes offsets with other DDL stays
/// one pending batch under the lease, so the offsets commit with that DDL or
/// not at all.
async fn commit_offsets_only(
    state: &SharedState,
    buffered: DdlBuffer,
) -> crate::Result<Option<DdlBuffer>> {
    let commits: Option<Vec<_>> = buffered
        .iter()
        .map(|item| match &item.entry {
            CatalogEntry::CommitConsumerOffsets(commit) => {
                Some((commit.as_ref().clone(), item.audit.clone()))
            }
            _ => None,
        })
        .collect();
    let Some(commits) = commits else {
        return Ok(Some(buffered));
    };
    for (commit, audit) in commits {
        crate::control::metadata_proposer::propose_cursor_commit_audited(state, commit, audit)
            .await?;
    }
    Ok(None)
}

/// Fencing token, metadata log index, preparation lease, and reserved
/// objects of one propose/finalize window. `handle` is always passed by
/// value into [`finalize_pending`], which releases the lease on success and
/// on failure. `objects` is cloned out before that move so a dispatch
/// failure that follows can still build compensation via
/// `ddl_compensate::compensate_finalized`.
pub(super) struct PendingDdlHandle<'a> {
    token: u64,
    log_index: u64,
    /// The preparation lease this window holds. [`finalize_pending`]
    /// releases it. A handle dropped unfinalized releases it from `Drop`.
    lease: DdlPrepareLease<'a>,
    objects: Vec<PendingDdlObject>,
}

impl PendingDdlHandle<'_> {
    /// The reserved objects, for building compensation after
    /// `finalize_pending` has consumed this handle.
    pub(super) fn objects(&self) -> &[PendingDdlObject] {
        &self.objects
    }
}

/// Propose `entry` to the metadata group and wait until this node's applied
/// watermark reaches its log index.
pub(super) async fn propose_and_await(
    state: &SharedState,
    handle: &dyn MetadataRaftHandle,
    entry: &MetadataEntry,
) -> crate::Result<u64> {
    let raw = encode_entry(entry).map_err(|e| crate::Error::Internal {
        detail: format!("metadata entry encode: {e}"),
    })?;
    let log_index = handle.propose_async(raw).await?;
    let watcher = state.applied_index_watcher(METADATA_GROUP_ID);
    let outcome = crate::control::metadata_proposer::wait::wait_applied(
        Arc::clone(&watcher),
        log_index,
        crate::control::metadata_proposer::DEFAULT_PROPOSE_TIMEOUT,
    )
    .await?;
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
/// without dispatching any DML. Takes the preparation lock and lease, drains
/// and stamps the batch, then reserves each stamped statement as a
/// [`PendingDdlObject`] instead of applying it. A failure releases the lease
/// before it returns.
async fn propose_pending_buffered<'a>(
    state: &'a SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    handle: &'a Arc<dyn MetadataRaftHandle>,
    buffered: DdlBuffer,
) -> crate::Result<PendingDdlHandle<'a>> {
    let _local_guard = crate::control::metadata_proposer::lock_ddl_preparation_async(state).await;
    let lease =
        crate::control::metadata_proposer::acquire_ddl_prepare_lease_async(state, handle.as_ref())
            .await?;
    let reserved = reserve_buffered(
        state,
        sessions,
        session_id,
        handle.as_ref(),
        lease.token(),
        buffered,
    )
    .await;
    match reserved {
        Ok((log_index, objects)) => Ok(PendingDdlHandle {
            token: lease.token(),
            log_index,
            lease,
            objects,
        }),
        Err(error) => {
            lease.release().await;
            Err(error)
        }
    }
}

/// Drain, stamp, and reserve `buffered` under the preparation lease `token`.
/// Returns the reservation's log index and its reserved objects.
async fn reserve_buffered(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    handle: &dyn MetadataRaftHandle,
    token: u64,
    buffered: DdlBuffer,
) -> crate::Result<(u64, Vec<PendingDdlObject>)> {
    // Checked under the preparation lease, which every DDL takes: no other
    // role or user change commits between this check and the finalize, so
    // the finalize never applies part of the batch.
    crate::control::catalog_entry::role_rules::check_batch(
        buffered.iter().map(|item| &item.entry),
        state.credentials.catalog(),
    )?;

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
            crate::control::lease::drain_for_ddl_async(
                state,
                descriptor_id,
                prior_version,
                crate::control::metadata_proposer::DEFAULT_DRAIN_TIMEOUT,
                own_holds,
            )
            .await?;
        }
    }

    let audits: Vec<_> = buffered.iter().map(|item| item.audit.clone()).collect();
    let entries: Vec<_> = buffered.into_iter().map(|item| item.entry).collect();
    let catalog = state.credentials.catalog();
    let stamped = catalog_entry::descriptor_stamp::stamp_batch(entries, &state.hlc_clock, catalog)?;

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

    let pending = MetadataEntry::DdlPendingPropose {
        token,
        objects: objects.clone(),
        proposed_at: state.hlc_clock.now(),
    };
    let log_index = propose_and_await(state, handle, &pending).await?;
    // The propose reserves only while `token` owns the lease. A lease the
    // metadata leader reclaimed meanwhile left no record.
    if !state.pending_ddl.contains(token) {
        return Err(superseded(token, "reserved"));
    }
    Ok((log_index, objects))
}

/// The error for a pending DDL whose preparation lease the metadata leader
/// reclaimed before its `step` applied.
fn superseded(token: u64, step: &str) -> crate::Error {
    crate::Error::Config {
        detail: format!(
            "metadata DDL preparation ownership of token {token} was reclaimed before the \
             buffered DDL was {step}; nothing was applied"
        ),
    }
}

/// Commit the objects `handle` reserved: propose `DdlPendingFinalize`, wait
/// for local apply, then release the preparation lease `handle` has held
/// since [`begin_commit`] constructed it via `propose_pending_buffered`.
/// The authorization barrier runs after the release, as on the
/// single-statement path.
///
/// The only other place a `DdlPendingPropose` record can be resolved is the
/// metadata leader's lease reclaim (`metadata_proposer::ddl_reclaim`), which
/// proposes `DdlPendingCancel` for a reclaimed owner's stranded record. A
/// finalize after that reclaim applies nothing and fails here, so the client
/// never sees a commit the cluster dropped.
pub(super) async fn finalize_pending(
    state: &SharedState,
    handle: PendingDdlHandle<'_>,
) -> crate::Result<()> {
    let PendingDdlHandle {
        token,
        log_index,
        lease,
        objects,
    } = handle;
    tracing::debug!(token, log_index, "finalizing pending DDL");
    let finalized = finalize_reserved(state, token, &objects).await;
    lease.release().await;
    let (log_index, bears_authorization) = finalized?;
    if bears_authorization {
        super::ddl_authorization::barrier_at(state, log_index).await?;
    }
    Ok(())
}

/// Propose `DdlPendingFinalize` for `token` and wait until it applied here.
/// Returns its log index, and whether `objects` change authorization state.
async fn finalize_reserved(
    state: &SharedState,
    token: u64,
    objects: &[PendingDdlObject],
) -> crate::Result<(u64, bool)> {
    let raft_handle = state.metadata_raft_handle()?;
    let bears_authorization = super::ddl_authorization::objects_bear_authorization(objects)?;
    let log_index = propose_and_await(
        state,
        raft_handle.as_ref(),
        &MetadataEntry::DdlPendingFinalize { token },
    )
    .await?;
    // The finalize applies only while `token` owns the lease. A reclaim
    // cancels the record first, so a finalize that found nothing applied
    // nothing.
    if state
        .metadata_ddl
        .applied_token
        .load(std::sync::atomic::Ordering::Acquire)
        != token
    {
        return Err(superseded(token, "finalized"));
    }
    Ok((log_index, bears_authorization))
}

/// The catalog entry that undoes `entry` after `finalize_pending` has
/// already applied it as a fresh `PendingDdlObject::Create`. Covers the
/// object kinds transactional DDL can buffer as a create; anything else
/// reports a typed error instead of silently doing nothing.
///
/// The reversal is proposed without a stamp, so each delete targets the exact
/// incarnation the create wrote.
pub(super) fn reverse_create(entry: &CatalogEntry) -> crate::Result<CatalogEntry> {
    match entry {
        CatalogEntry::PutCollection(stored) | CatalogEntry::PutCollectionIfAbsent(stored) => {
            Ok(CatalogEntry::PurgeCollection {
                database_id: stored.database_id.as_u64(),
                tenant_id: stored.tenant_id,
                name: stored.name.clone(),
                target_descriptor_version: stored.descriptor_version,
                target_hlc: stored.modification_hlc,
            })
        }
        CatalogEntry::PutSequence(stored) => Ok(CatalogEntry::DeleteSequence {
            database_id: stored.database_id,
            tenant_id: stored.tenant_id,
            name: stored.name.clone(),
            target_descriptor_version: stored.descriptor_version,
            target_hlc: stored.modification_hlc,
        }),
        CatalogEntry::PutFunction(stored) => Ok(CatalogEntry::DeleteFunction {
            database_id: stored.database_id,
            tenant_id: stored.tenant_id,
            name: stored.name.clone(),
            target_descriptor_version: stored.descriptor_version,
            target_hlc: stored.modification_hlc,
        }),
        CatalogEntry::PutTrigger(stored) => Ok(CatalogEntry::DeleteTrigger {
            database_id: stored.database_id,
            tenant_id: stored.tenant_id,
            name: stored.name.clone(),
            target_descriptor_version: stored.descriptor_version,
            target_hlc: stored.modification_hlc,
        }),
        CatalogEntry::PutProcedure(stored) => Ok(CatalogEntry::DeleteProcedure {
            database_id: stored.database_id,
            tenant_id: stored.tenant_id,
            name: stored.name.clone(),
            target_descriptor_version: stored.descriptor_version,
            target_hlc: stored.modification_hlc,
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
            target_descriptor_version: stored.descriptor_version,
            target_hlc: stored.modification_hlc,
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
    use crate::control::cluster::test_one_node;
    use crate::control::security::catalog::sequence_types::StoredSequence;
    use crate::control::security::credential::CredentialStore;
    use crate::wal::WalManager;

    use super::super::connection::{ConnectionId, SessionId};
    use super::super::ddl_compensate::compensate_finalized;
    use super::super::store::SessionStore;
    use super::super::{conn_scope, ddl_buffer};
    use super::{
        DdlCommitPlan, PendingDdlHandle, PendingDdlObject, SharedState, begin_commit,
        finalize_pending, reverse_create,
    };

    use std::sync::Arc;

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

    /// A state no boot wired to a cluster. A COMMIT with nothing buffered
    /// never reaches the metadata group, so it needs none.
    fn unbooted_state() -> (Arc<SharedState>, tempfile::TempDir) {
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
        crate::bootstrap::state_wiring::install_gateway(&state).expect("install gateway");
        (state, dir)
    }

    /// Buffer one `PutSequence` and reserve it via `begin_commit` through the
    /// metadata group of `state`.
    async fn propose_one_sequence<'a>(state: &'a SharedState, name: &str) -> PendingDdlHandle<'a> {
        let stored = StoredSequence::new(0, 7, name.into(), "alice".into());
        let (sessions, session_id) = test_session();
        let plan = conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(CatalogEntry::PutSequence(Box::new(
                stored
            ))));
            begin_commit(state, &sessions, session_id).await
        })
        .await
        .expect("begin_commit must succeed");
        match plan {
            DdlCommitPlan::Pending(handle) => handle,
            DdlCommitPlan::None => panic!("a buffered sequence must reserve a pending record"),
        }
    }

    /// A COMMIT that buffered DDL on a state with no metadata group is refused
    /// with a typed error. No catalog row is written.
    #[tokio::test]
    async fn buffered_ddl_without_a_metadata_group_is_refused() {
        let (state, _dir) = unbooted_state();
        let (sessions, session_id) = test_session();
        let refused = conn_scope::scoped(async {
            ddl_buffer::activate();
            let stored = StoredSequence::new(0, 7, "orders_seq".into(), "alice".into());
            assert!(ddl_buffer::try_buffer(CatalogEntry::PutSequence(Box::new(
                stored
            ))));
            begin_commit(&state, &sessions, session_id).await.is_err()
        })
        .await;
        assert!(refused, "no metadata group, no COMMIT of buffered DDL");
        assert!(
            state
                .credentials
                .catalog()
                .get_sequence(0, 7, "orders_seq")
                .expect("catalog read")
                .is_none(),
            "a refused COMMIT writes no catalog row"
        );
    }

    /// Buffer `entries` in one connection scope and commit them as COMMIT
    /// does: `begin_commit`, then `finalize_pending` on the pending plan.
    /// True when both succeeded.
    async fn buffer_and_commit(state: &SharedState, entries: Vec<CatalogEntry>) -> bool {
        let (sessions, session_id) = test_session();
        conn_scope::scoped(async {
            ddl_buffer::activate();
            for entry in entries {
                assert!(ddl_buffer::try_buffer(entry), "buffer is active");
            }
            match begin_commit(state, &sessions, session_id)
                .await
                .expect("begin_commit must not error")
            {
                DdlCommitPlan::Pending(handle) => finalize_pending(state, handle).await.is_ok(),
                DdlCommitPlan::None => panic!("buffer was non-empty"),
            }
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn commit_populates_the_sequence_registry() {
        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
        assert!(
            !state.sequence_registry.exists(0, 7, "orders_seq"),
            "registry starts empty"
        );

        let stored = StoredSequence::new(0, 7, "orders_seq".into(), "alice".into());
        let ok = buffer_and_commit(state, vec![CatalogEntry::PutSequence(Box::new(stored))]).await;

        assert!(ok, "the commit must not abort");
        let written = state
            .credentials
            .catalog()
            .get_sequence(0, 7, "orders_seq")
            .expect("catalog read")
            .expect("the commit wrote the sequence");
        assert_eq!(written.descriptor_version, 1);
        assert_ne!(written.modification_hlc, nodedb_types::Hlc::ZERO);
        assert!(
            state.sequence_registry.exists(0, 7, "orders_seq"),
            "the applied commit must run the post-apply sync phase: without it the catalog \
             and the live registry disagree until restart, and NEXTVAL / DROP SEQUENCE \
             report the sequence as missing"
        );
        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn commit_installs_a_replicated_grant() {
        use crate::control::security::identity::Permission;

        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
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
            buffer_and_commit(state, vec![CatalogEntry::PutPermission(Box::new(stored))]).await;

        assert!(ok, "the commit must not abort");
        assert!(
            state
                .permissions
                .permission_exists("widgets", "analyst", Permission::Read),
            "the applied commit must install the replicated grant, or the evaluator keeps \
             refusing a grant the catalog already holds"
        );
        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn commit_hooks_every_buffered_entry() {
        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
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
            buffer_and_commit(state, entries).await,
            "the commit must not abort"
        );
        assert!(state.sequence_registry.exists(0, 7, "first_seq"));
        assert!(
            state.sequence_registry.exists(0, 7, "second_seq"),
            "the hook must run per entry, not once for the batch"
        );
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn flush_outside_a_transaction_is_inert() {
        let (state, _dir) = unbooted_state();
        let (sessions, session_id) = test_session();
        let is_none = conn_scope::scoped(async {
            matches!(
                begin_commit(&state, &sessions, session_id)
                    .await
                    .expect("begin_commit must not error"),
                DdlCommitPlan::None
            )
        })
        .await;
        assert!(is_none, "no buffer means nothing to flush");
    }

    #[tokio::test]
    async fn begin_commit_on_empty_active_buffer_is_none() {
        let (state, _dir) = unbooted_state();
        let (sessions, session_id) = test_session();
        let result = conn_scope::scoped(async {
            ddl_buffer::activate();
            begin_commit(&state, &sessions, session_id).await
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
        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
        let handle = propose_one_sequence(state, "orders_seq").await;
        let token = handle.token;
        assert!(
            state.pending_ddl.contains(token),
            "begin_commit must reserve a pending record"
        );

        finalize_pending(state, handle)
            .await
            .expect("finalize_pending must succeed");

        assert!(
            !state.pending_ddl.contains(token),
            "finalize_pending must drop the pending record"
        );
        assert!(
            state
                .metadata_ddl
                .owner
                .lock()
                .expect("owner lock")
                .is_none(),
            "finalize_pending must release the preparation lease"
        );
        cluster.shutdown().await;
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
                name,
                ..
            } if name == "orders_seq"
        ));
    }

    #[test]
    fn reverse_create_rejects_an_unreversible_kind() {
        let err = reverse_create(&CatalogEntry::DeleteSequence {
            database_id: 0,
            tenant_id: 7,
            name: "orders_seq".into(),
            target_descriptor_version: 0,
            target_hlc: nodedb_types::Hlc::ZERO,
        });
        assert!(
            err.is_err(),
            "no create-shaped reversal exists for a Delete entry"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn compensate_finalized_proposes_a_reversal_batch_for_a_create() {
        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
        let handle = propose_one_sequence(state, "orders_seq").await;
        let objects: Vec<PendingDdlObject> = handle.objects().to_vec();
        finalize_pending(state, handle)
            .await
            .expect("finalize_pending must succeed");

        compensate_finalized(state, &objects)
            .await
            .expect("compensate_finalized must propose a reversal batch for the finalized create");
        assert!(
            state
                .credentials
                .catalog()
                .get_sequence(0, 7, "orders_seq")
                .expect("read sequence")
                .is_none(),
            "the reversal must delete the sequence the transaction created"
        );
        cluster.shutdown().await;
    }

    /// A DDL that lands between the finalize and the compensation owns the
    /// descriptor. The reversal is refused, and the later row survives.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn compensate_finalized_refuses_a_descriptor_changed_after_finalize() {
        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
        let handle = propose_one_sequence(state, "orders_seq").await;
        let objects: Vec<PendingDdlObject> = handle.objects().to_vec();
        finalize_pending(state, handle)
            .await
            .expect("finalize_pending must succeed");

        let catalog = state.credentials.catalog();
        let mut later = catalog
            .get_sequence(0, 7, "orders_seq")
            .expect("read sequence")
            .expect("finalize created the sequence");
        later.descriptor_version += 1;
        later.modification_hlc = state.hlc_clock.now();
        catalog
            .put_sequence(&later)
            .expect("write later incarnation");

        assert!(
            compensate_finalized(state, &objects).await.is_err(),
            "a reversal over a later DDL must fail, never report success"
        );
        let surviving = catalog
            .get_sequence(0, 7, "orders_seq")
            .expect("read sequence")
            .expect("the later incarnation must survive");
        assert_eq!(surviving.modification_hlc, later.modification_hlc);
        cluster.shutdown().await;
    }
}
