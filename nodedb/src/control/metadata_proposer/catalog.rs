// SPDX-License-Identifier: BUSL-1.1

//! Propose a catalog entry and wait until this node applied it.

use std::sync::atomic::Ordering;
use std::time::Duration;

use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry, WaitOutcome, encode_entry};

use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::propose_outcome::ProposeOutcome;
use crate::control::state::SharedState;
use crate::error::Error;

use super::ddl_prepare::{acquire_ddl_prepare_lease_async, lock_ddl_preparation_async};
use super::handle::MetadataRaftHandle;
use super::timeouts::{DEFAULT_DRAIN_TIMEOUT, DEFAULT_PROPOSE_TIMEOUT};
use super::wait::wait_tracking_progress_async;

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
pub async fn propose_catalog_entry_async(
    shared: &SharedState,
    entry: &CatalogEntry,
) -> Result<ProposeOutcome, Error> {
    let outcome = propose_and_apply_locally(shared, entry).await?;
    if let ProposeOutcome::Replicated { log_index } = outcome
        && entry.bears_authorization()
    {
        crate::control::security::auth_lease::authorization_barrier(
            shared,
            vec![nodedb_cluster::GroupCoverage {
                group_id: METADATA_GROUP_ID,
                through: log_index,
            }],
        )
        .await?;
    }
    Ok(outcome)
}

/// Propose `entry` and wait until this node applied it. The preparation lock
/// and lease are released before it returns.
async fn propose_and_apply_locally(
    shared: &SharedState,
    entry: &CatalogEntry,
) -> Result<ProposeOutcome, Error> {
    // Buffering is decided first, ahead of every replication-mode gate: an open
    // transaction owns the entry regardless of whether this deployment
    // replicates DDL, and COMMIT re-runs the mode choice for the whole batch.
    // Entries also stay unstamped until then, so repeated mutations of one
    // descriptor receive distinct versions in commit order.
    if crate::control::server::shared::session::ddl_buffer::try_buffer(entry.clone()) {
        return Ok(ProposeOutcome::Buffered);
    }

    let handle = shared.metadata_raft_handle()?;

    // Serialize preparation through local apply confirmation. Without this,
    // concurrent proposers can both observe persisted version N and emit N+1.
    let _local_ddl_guard = lock_ddl_preparation_async(shared).await;

    let lease = acquire_ddl_prepare_lease_async(shared, handle.as_ref()).await?;
    let proposed = async {
        let drained = drain_prior_version(shared, entry).await?;
        let stamped = stamp_or_end_drain(shared, entry, drained).await?;
        propose_prepared(
            shared,
            handle.as_ref(),
            lease.token(),
            catalog_ddl_entry(&stamped)?,
            DEFAULT_PROPOSE_TIMEOUT,
        )
        .await
    }
    .await;
    lease.release().await;
    Ok(ProposeOutcome::Replicated {
        log_index: proposed?,
    })
}

/// Drain the prior version of the descriptor `entry` changes. Returns the
/// drained descriptor, whose drain the entry's apply ends.
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
) -> Result<Option<nodedb_cluster::DescriptorId>, Error> {
    let Some((descriptor_id, prior_version)) =
        crate::control::lease::descriptor_id_and_prior_version(entry, shared)
    else {
        return Ok(None);
    };
    if prior_version == 0 {
        return Ok(None);
    }
    crate::control::lease::drain_for_ddl_async(
        shared,
        descriptor_id.clone(),
        prior_version,
        DEFAULT_DRAIN_TIMEOUT,
        // No transactional lease scope of its own: this is a bare,
        // unbuffered DDL statement, not a COMMIT finalizing buffered DDL
        // alongside a buffered write to the same descriptor.
        0,
    )
    .await?;
    Ok(Some(descriptor_id))
}

/// Stamp `entry`. A failed stamp ends the drain of `drained` and returns the
/// stamp error.
///
/// Freezes the descriptor_version / constraint_version / modification_hlc
/// HERE, at propose time, so the value is computed exactly once from this
/// node's local catalog (`prior + 1`) and then replicated verbatim inside the
/// entry. Every node applies the frozen value without re-deriving it, which
/// makes replay-from-log on restart and re-delivery during learner catch-up
/// idempotent.
async fn stamp_or_end_drain(
    shared: &SharedState,
    entry: &CatalogEntry,
    drained: Option<nodedb_cluster::DescriptorId>,
) -> Result<CatalogEntry, Error> {
    match catalog_entry::descriptor_stamp::stamp(
        entry.clone(),
        &shared.hlc_clock,
        shared.credentials.catalog(),
    ) {
        Ok(stamped) => Ok(stamped),
        Err(error) => Err(end_unproposed_drain(shared, drained, error).await),
    }
}

/// End the drain a DDL started when it fails before its entry is proposed.
/// A drain has no wall-clock expiry, and no apply of this entry will end it.
/// Returns `error`, the failure that stopped the DDL.
async fn end_unproposed_drain(
    shared: &SharedState,
    drained: Option<nodedb_cluster::DescriptorId>,
    error: Error,
) -> Error {
    if let Some(descriptor_id) = drained
        && let Err(end) = crate::control::lease::end_drain_async(
            shared,
            descriptor_id,
            nodedb_cluster::DrainOwner::Ddl,
        )
        .await
    {
        tracing::warn!(
            error = %end,
            "metadata propose: the drain of a DDL that failed before its propose did not end"
        );
    }
    error
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
/// node applied it. Returns the log index. Fails when another lease owner
/// superseded `token` before the apply.
///
/// `timeout` bounds a stall, not the whole wait, as
/// [`super::wait::wait_tracking_progress_async`] describes.
pub(super) async fn propose_prepared(
    shared: &SharedState,
    handle: &dyn MetadataRaftHandle,
    token: u64,
    entry: MetadataEntry,
    timeout: Duration,
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
    let outcome =
        wait_tracking_progress_async(std::sync::Arc::clone(&watcher), log_index, timeout, || {
            shared.metadata_apply_progress.load(Ordering::Acquire)
        })
        .await?;
    match outcome {
        WaitOutcome::Reached
            if shared.metadata_ddl.applied_token.load(Ordering::Acquire) == token =>
        {
            Ok(log_index)
        }
        WaitOutcome::Reached => Err(Error::Config {
            detail: "metadata DDL preparation ownership was superseded before apply".into(),
        }),
        WaitOutcome::TimedOut => Err(Error::Config {
            detail: format!(
                "metadata propose timed out after {timeout:?} without apply progress waiting \
                 for log index {log_index} (current: {})",
                watcher.current()
            ),
        }),
        WaitOutcome::GroupGone => Err(Error::Config {
            detail: "metadata group no longer hosted on this node".into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use nodedb_cluster::AppliedIndexWatcher;

    use super::*;

    const WINDOW: Duration = Duration::from_millis(60);

    /// Progress that keeps moving for many windows never times out; the wait
    /// ends when the entry applies.
    #[tokio::test]
    async fn steady_progress_never_times_out() {
        let watcher = Arc::new(AppliedIndexWatcher::new());
        let progress = Arc::new(AtomicU64::new(0));
        let driver = {
            let (watcher, progress) = (Arc::clone(&watcher), Arc::clone(&progress));
            std::thread::spawn(move || {
                for _ in 0..20 {
                    std::thread::sleep(Duration::from_millis(20));
                    progress.fetch_add(1, Ordering::Release);
                }
                watcher.bump(5);
            })
        };
        let outcome = wait_tracking_progress_async(Arc::clone(&watcher), 5, WINDOW, || {
            progress.load(Ordering::Acquire)
        })
        .await
        .expect("the wait finishes");
        driver.join().unwrap();
        assert!(outcome.is_reached(), "{outcome:?}");
    }

    /// A stall of one window times the wait out.
    #[tokio::test]
    async fn a_stall_times_out() {
        let watcher = Arc::new(AppliedIndexWatcher::new());
        let started = std::time::Instant::now();
        let outcome = wait_tracking_progress_async(watcher, 5, WINDOW, || 7)
            .await
            .expect("the wait finishes");
        assert!(matches!(outcome, WaitOutcome::TimedOut), "{outcome:?}");
        assert!(started.elapsed() < WINDOW * 4);
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
