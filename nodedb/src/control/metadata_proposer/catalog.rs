// SPDX-License-Identifier: BUSL-1.1

//! Propose a catalog entry and wait until this node applied it.

use std::sync::atomic::Ordering;
use std::time::Instant;

use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry, encode_entry};

use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::constraint_install::{ConstraintTarget, constraint_targets};
use crate::control::lease::DrainHolds;
use crate::control::propose_outcome::ProposeOutcome;
use crate::control::state::SharedState;
use crate::error::Error;

use super::ddl_prepare::{acquire_ddl_prepare_lease_async, lock_ddl_preparation_async};
use super::handle::MetadataRaftHandle;
use super::timeouts::DEFAULT_DRAIN_TIMEOUT;
use super::wait::await_applied;
use crate::control::server::shared::session::StatementDeadline;

/// Propose a `CatalogEntry` and wait until this node applied it, on any
/// runtime flavor.
///
/// The returned [`ProposeOutcome`] tells the caller whether the entry applied
/// here through the metadata group, or is held for COMMIT. The caller never
/// writes the catalog itself.
///
/// Every wait is awaited: the DDL preparation lock and lease, the descriptor
/// drain, the metadata commit's apply, and the authorization barrier.
///
/// An entry that changes authorization state returns only once it binds
/// every node: the authorization barrier runs after the local apply, with the
/// DDL preparation lock and lease already released.
///
/// An entry that changes a collection's CRDT constraint version installs the
/// new set through the collection's data Raft log before it returns. The
/// entry is committed by then, so a failed install never fails it.
pub async fn propose_catalog_entry_async(
    shared: &SharedState,
    entry: &CatalogEntry,
) -> Result<ProposeOutcome, Error> {
    let (outcome, constraint_targets) = propose_and_apply_locally(shared, entry).await?;
    // A changed constraint set is live on the collection's data log before
    // the DDL returns, so the first CRDT apply after it is not fenced.
    crate::control::constraint_install::install_constraints(shared, &constraint_targets).await;
    if let ProposeOutcome::Replicated { log_index } = outcome
        && entry.bears_authorization()
    {
        crate::control::security::auth_lease::authorization_barrier(
            shared,
            vec![nodedb_cluster::GroupCoverage {
                group_id: METADATA_GROUP_ID,
                through: log_index,
            }],
            crate::control::server::shared::session::StatementDeadline::of_statement(shared)
                .instant(),
        )
        .await?;
    }
    Ok(outcome)
}

/// Propose `entry` and wait until this node applied it. The preparation lock
/// and lease are released before it returns. Also returns the collections
/// whose constraint version the entry changed.
async fn propose_and_apply_locally(
    shared: &SharedState,
    entry: &CatalogEntry,
) -> Result<(ProposeOutcome, Vec<ConstraintTarget>), Error> {
    // Buffering is decided first, ahead of every replication-mode gate: an open
    // transaction owns the entry regardless of whether this deployment
    // replicates DDL, and COMMIT re-runs the mode choice for the whole batch.
    // Entries also stay unstamped until then, so repeated mutations of one
    // descriptor receive distinct versions in commit order.
    // A buffered entry installs its constraints when its COMMIT finalizes.
    if crate::control::server::shared::session::ddl_buffer::try_buffer(entry.clone()) {
        return Ok((ProposeOutcome::Buffered, Vec::new()));
    }

    let handle = shared.metadata_raft_handle()?;

    // Serialize preparation through local apply confirmation. Without this,
    // concurrent proposers can both observe persisted version N and emit N+1.
    let _local_ddl_guard = lock_ddl_preparation_async(shared).await;

    let lease = acquire_ddl_prepare_lease_async(shared, handle.as_ref()).await?;
    let mut drains = DrainHolds::new(shared);
    let proposed = async {
        drain_prior_version(shared, entry, &mut drains).await?;
        let (stamped, targets) = stamp(shared, entry)?;
        let log_index = propose_prepared(
            shared,
            handle.as_ref(),
            lease.token(),
            catalog_ddl_entry(&stamped)?,
            StatementDeadline::of_statement(shared).instant(),
        )
        .await?;
        Ok::<_, Error>((log_index, targets))
    }
    .await;
    // The applied entry ended the drain. Any failure ends it here: a drain has
    // no wall-clock expiry. The end is proposed after the entry, so it is a
    // no-op when an entry whose outcome was unknown applied after all.
    match &proposed {
        Ok(_) => drains.keep(),
        Err(_) => drains.end().await,
    }
    lease.release().await;
    let (log_index, targets) = proposed?;
    Ok((ProposeOutcome::Replicated { log_index }, targets))
}

/// Drain the prior version of the descriptor `entry` changes, and record the
/// drain in `drains`. The entry's apply ends it.
///
/// Leases acquired at plan time are refcounted and held through execute; when
/// the last in-flight query using a descriptor completes, its
/// `QueryLeaseScope` drops and the refcount hits zero, releasing the lease.
/// The drain is the barrier: the proposer waits for every prior-version lease
/// to release before committing the new `Put*`, giving long-running in-flight
/// queries a bounded window (`DEFAULT_DRAIN_TIMEOUT`) to finish.
async fn drain_prior_version(
    shared: &SharedState,
    entry: &CatalogEntry,
    drains: &mut DrainHolds<'_>,
) -> Result<(), Error> {
    let Some((descriptor_id, prior_version)) =
        crate::control::lease::descriptor_id_and_prior_version(entry, shared)
    else {
        return Ok(());
    };
    if prior_version == 0 {
        return Ok(());
    }
    drains.hold(descriptor_id.clone(), nodedb_cluster::DrainOwner::Ddl);
    crate::control::lease::drain_for_ddl_async(
        shared,
        descriptor_id,
        prior_version,
        DEFAULT_DRAIN_TIMEOUT,
        // No transactional lease scope of its own: this is a bare,
        // unbuffered DDL statement, not a COMMIT finalizing buffered DDL
        // alongside a buffered write to the same descriptor.
        0,
    )
    .await
}

/// Stamp `entry`, and name the collections whose constraint version the
/// stamped entry changes.
///
/// Freezes the descriptor_version / constraint_version / modification_hlc
/// HERE, at propose time, so the value is computed exactly once from this
/// node's local catalog (`prior + 1`) and then replicated verbatim inside the
/// entry. Every node applies the frozen value without re-deriving it, which
/// makes replay-from-log on restart and re-delivery during learner catch-up
/// idempotent.
fn stamp(
    shared: &SharedState,
    entry: &CatalogEntry,
) -> Result<(CatalogEntry, Vec<ConstraintTarget>), Error> {
    let catalog = shared.credentials.catalog();
    let stamped =
        catalog_entry::descriptor_stamp::stamp(entry.clone(), &shared.hlc_clock, catalog)?;
    let targets = constraint_targets(catalog, std::slice::from_ref(&stamped))?;
    Ok((stamped, targets))
}

/// Wrap `entry` as a `CatalogDdl` metadata entry.
///
/// Carries the statement's audit context when the statement boundary
/// installed one. Internal callers (descriptor lease grant/release, drain
/// proposer) run outside that scope and emit the plain `CatalogDdl` variant.
pub(super) fn catalog_ddl_entry(entry: &CatalogEntry) -> Result<MetadataEntry, Error> {
    catalog_ddl_entry_with(
        entry,
        crate::control::server::shared::session::audit_context::current(),
    )
}

/// Wrap `entry` as a `CatalogDdl` metadata entry carrying `audit`, the
/// context of the statement that issued it.
pub(super) fn catalog_ddl_entry_with(
    entry: &CatalogEntry,
    audit: Option<crate::control::server::shared::session::audit_context::AuditCtx>,
) -> Result<MetadataEntry, Error> {
    let payload = catalog_entry::encode(entry)?;
    Ok(match audit {
        Some(ctx) => MetadataEntry::CatalogDdlAudited {
            payload,
            auth_user_id: ctx.auth_user_id,
            auth_user_name: ctx.auth_user_name,
            sql_text: ctx.sql_text,
        },
        None => MetadataEntry::CatalogDdl { payload },
    })
}

/// Propose `entry` under the preparation lease `token` and wait until this
/// node applied it, until `deadline`. Returns the log index.
///
/// - Past `deadline`: `Error::DeadlineExceeded`. The entry was proposed, so
///   its outcome is unknown and no caller resends it.
/// - Another lease owner superseded `token` before the apply:
///   `Error::MetadataDdlSuperseded`. The apply skipped the entry.
pub(super) async fn propose_prepared(
    shared: &SharedState,
    handle: &dyn MetadataRaftHandle,
    token: u64,
    entry: MetadataEntry,
    deadline: Instant,
) -> Result<u64, Error> {
    let raw = encode_entry(&MetadataEntry::DdlPrepared {
        token,
        entry: Box::new(entry),
    })
    .map_err(|e| Error::Config {
        detail: format!("metadata entry encode: {e}"),
    })?;
    let log_index = handle.propose_async(raw).await?;
    let watcher = shared.applied_index_watcher(METADATA_GROUP_ID);
    await_applied(&watcher, log_index, deadline).await?;
    if shared.metadata_ddl.applied_token.load(Ordering::Acquire) == token {
        Ok(log_index)
    } else {
        Err(Error::MetadataDdlSuperseded { log_index })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    /// A handle that assigns every proposal log index 5.
    struct AtFive;

    impl MetadataRaftHandle for AtFive {
        fn propose_async<'a>(
            &'a self,
            _bytes: Vec<u8>,
        ) -> crate::control::metadata_proposer::ProposeFuture<'a> {
            Box::pin(async { Ok::<u64, Error>(5) })
        }
    }

    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(
            crate::wal::WalManager::open_for_testing(&dir.path().join("propose.wal")).expect("wal"),
        );
        let (dispatcher, _sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        (state, dir)
    }

    fn ddl() -> MetadataEntry {
        MetadataEntry::CatalogDdl {
            payload: Vec::new(),
        }
    }

    /// An apply that lands after a 1 s quiet gap, inside a 2 s deadline,
    /// answers the log index.
    #[tokio::test]
    async fn a_slow_apply_inside_the_deadline_succeeds() {
        const TOKEN: u64 = 77;
        let (state, _dir) = test_state();
        let applier = {
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                state
                    .metadata_ddl
                    .applied_token
                    .store(TOKEN, Ordering::Release);
                state.applied_index_watcher(METADATA_GROUP_ID).bump(5);
            })
        };
        let deadline = Instant::now() + Duration::from_secs(2);
        let applied = propose_prepared(&state, &AtFive, TOKEN, ddl(), deadline).await;
        applier.await.expect("join applier");
        assert_eq!(applied.expect("the entry applied"), 5);
    }

    /// An entry that never applies ends at the deadline as `DeadlineExceeded`,
    /// never as a config error: it was proposed, so its outcome is unknown.
    #[tokio::test]
    async fn an_entry_that_never_applies_ends_at_the_deadline() {
        let (state, _dir) = test_state();
        let budget = Duration::from_millis(300);
        let started = Instant::now();
        let result = propose_prepared(&state, &AtFive, 1, ddl(), started + budget).await;
        assert!(
            matches!(result, Err(Error::DeadlineExceeded { .. })),
            "{result:?}"
        );
        assert!(started.elapsed() >= budget);
    }

    /// An entry whose apply skipped it under another lease owner is
    /// superseded, not a success.
    #[tokio::test]
    async fn a_superseded_token_is_a_typed_refusal() {
        let (state, _dir) = test_state();
        state.metadata_ddl.applied_token.store(9, Ordering::Release);
        state.applied_index_watcher(METADATA_GROUP_ID).bump(5);
        let deadline = Instant::now() + Duration::from_secs(2);
        let result = propose_prepared(&state, &AtFive, 1, ddl(), deadline).await;
        assert!(
            matches!(result, Err(Error::MetadataDdlSuperseded { log_index: 5 })),
            "{result:?}"
        );
    }

    /// A one-node cluster stamps a create, commits it through its metadata
    /// group, and applies it before the propose returns: the row holds its
    /// first descriptor version and a non-zero incarnation.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_one_node_cluster_stamps_and_applies_through_its_metadata_group() {
        let cluster = crate::control::cluster::test_one_node::boot().await;

        let outcome = propose_catalog_entry_async(&cluster.state, &orders())
            .await
            .expect("propose");
        assert!(outcome.is_replicated(), "{outcome:?}");
        assert_orders_applied(&cluster.state);
        cluster.shutdown().await;
    }

    fn orders() -> CatalogEntry {
        use crate::control::security::catalog::StoredCollection;
        CatalogEntry::PutCollection(Box::new(StoredCollection::new(7, "orders", "admin")))
    }

    /// The create of [`orders`] landed with its first descriptor version and a
    /// non-zero incarnation.
    fn assert_orders_applied(state: &SharedState) {
        use nodedb_types::{DatabaseId, Hlc};
        let row = state
            .credentials
            .catalog()
            .get_committed_collection(DatabaseId::DEFAULT, 7, "orders")
            .expect("read")
            .expect("the propose applied the row");
        assert_eq!(row.descriptor_version, 1);
        assert_ne!(row.incarnation, Hlc::ZERO);
        assert_ne!(row.modification_hlc, Hlc::ZERO);
    }
}
