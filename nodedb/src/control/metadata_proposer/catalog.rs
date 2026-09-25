// SPDX-License-Identifier: BUSL-1.1

//! Propose a catalog entry and wait until this node applied it.

use std::sync::atomic::Ordering;
use std::time::Duration;

use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry, WaitOutcome, encode_entry};

use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::propose_outcome::ProposeOutcome;
use crate::control::state::SharedState;
use crate::error::Error;

use super::ddl_prepare::{acquire_ddl_prepare_lease, lock_ddl_preparation};
use super::timeouts::{DEFAULT_DRAIN_TIMEOUT, DEFAULT_PROPOSE_TIMEOUT};

/// Propose a `CatalogEntry` and block until the local applied-index
/// watcher confirms the entry has been applied on this node.
///
/// The returned [`ProposeOutcome`] tells the caller whether to write the
/// catalog itself, leave it to the applier, or do nothing because the entry
/// is held for COMMIT.
pub fn propose_catalog_entry(
    shared: &SharedState,
    entry: &CatalogEntry,
) -> Result<ProposeOutcome, Error> {
    propose_catalog_entry_with_timeout(shared, entry, DEFAULT_PROPOSE_TIMEOUT)
}

/// Same as [`propose_catalog_entry`] but with an explicit timeout.
///
/// An entry that changes authorization state returns only once it binds
/// every node: the authorization barrier runs after the local apply, with the
/// DDL preparation lock already released.
pub fn propose_catalog_entry_with_timeout(
    shared: &SharedState,
    entry: &CatalogEntry,
    timeout: Duration,
) -> Result<ProposeOutcome, Error> {
    let outcome = propose_and_apply_locally(shared, entry, timeout)?;
    if let ProposeOutcome::Replicated { log_index } = outcome
        && entry.bears_authorization()
    {
        crate::control::security::auth_lease::block_on_barrier(
            shared,
            vec![nodedb_cluster::GroupCoverage {
                group_id: METADATA_GROUP_ID,
                through: log_index,
            }],
        )?;
    }
    Ok(outcome)
}

/// Propose `entry` and wait until this node applied it.
fn propose_and_apply_locally(
    shared: &SharedState,
    entry: &CatalogEntry,
    timeout: Duration,
) -> Result<ProposeOutcome, Error> {
    // Buffering is decided first, ahead of every replication-mode gate: an open
    // transaction owns the entry regardless of whether this deployment
    // replicates DDL, and COMMIT re-runs the mode choice for the whole batch.
    // Entries also stay unstamped until then, so repeated mutations of one
    // descriptor receive distinct versions in commit order.
    if crate::control::server::shared::session::ddl_buffer::try_buffer(entry.clone()) {
        return Ok(ProposeOutcome::Buffered);
    }

    let Some(handle) = shared.metadata_raft.get() else {
        return Ok(ProposeOutcome::LocalOnly);
    };

    // Rolling-upgrade gate: until every node in the cluster reports
    // at least `DISTRIBUTED_CATALOG_VERSION`, fall back to the legacy
    // direct-write path on the originating node. Mixing the
    // replicated and direct paths during a partial upgrade would
    // diverge catalog state across nodes — see
    // `control/rolling_upgrade.rs`.
    if !shared
        .cluster_version_view()
        .can_activate_feature(crate::control::rolling_upgrade::DISTRIBUTED_CATALOG_VERSION)
    {
        tracing::warn!(
            min_version = shared.cluster_version_view().min_version,
            required = crate::control::rolling_upgrade::DISTRIBUTED_CATALOG_VERSION,
            "metadata propose: cluster in compat mode (mixed-version), \
             falling back to legacy direct-write path"
        );
        return Ok(ProposeOutcome::LocalOnly);
    }

    // Serialize preparation through local apply confirmation. Without this,
    // concurrent proposers can both observe persisted version N and emit N+1.
    let _local_ddl_guard = lock_ddl_preparation(shared)?;
    let distributed_ddl_guard = acquire_ddl_prepare_lease(shared, handle.as_ref())?;

    // Drain for Put* variants that carry descriptor_version.
    // Leases acquired at plan time are refcounted and held
    // through execute; when the last in-flight query using a
    // descriptor completes, its `QueryLeaseScope` drops and the
    // refcount hits zero, releasing the lease. Drain is what
    // makes this an actual barrier: the proposer waits for all
    // prior-version leases to release before committing the new
    // `Put*`, giving long-running in-flight queries a bounded
    // window (DEFAULT_DRAIN_TIMEOUT) to finish.
    if let Some((descriptor_id, prior_version)) =
        crate::control::lease::descriptor_id_and_prior_version(entry, shared)
        && prior_version > 0
    {
        crate::control::lease::drain_for_ddl(
            shared,
            descriptor_id,
            prior_version,
            DEFAULT_DRAIN_TIMEOUT,
            // No transactional lease scope of its own: this is a bare,
            // unbuffered DDL statement, not a COMMIT finalizing buffered DDL
            // alongside a buffered write to the same descriptor.
            0,
        )?;
    }

    // Freeze the descriptor_version / constraint_version /
    // modification_hlc HERE, at propose time, so the value is computed
    // exactly once from this node's local catalog (`prior + 1`) and
    // then replicated verbatim inside the entry. Every node applies the
    // frozen value without re-deriving it, which makes replay-from-log
    // on restart and re-delivery during learner catch-up idempotent —
    // the divergence that a per-node apply-time stamp produced is gone.
    //
    // Gated on the same rolling-upgrade flag the apply path used to
    // gate on: only stamp once every node can activate descriptor
    // versioning; otherwise leave the entry's sentinel version `0`
    // (downstream resolvers treat `0` as `1`). Older nodes in a
    // mixed-version cluster lack the stamp logic, so a stamped value
    // would not be reproduced symmetrically there.
    let stamped_owned;
    let entry: &CatalogEntry = if shared
        .cluster_version_view()
        .can_activate_feature(crate::control::rolling_upgrade::DESCRIPTOR_VERSIONING_VERSION)
    {
        stamped_owned = catalog_entry::descriptor_stamp::stamp(
            entry.clone(),
            &shared.hlc_clock,
            shared.credentials.catalog(),
        );
        &stamped_owned
    } else {
        entry
    };

    let payload = catalog_entry::encode(entry)?;

    // Attach J.4 audit context when the pgwire statement boundary
    // installed one. Internal callers (descriptor lease grant/release,
    // drain proposer) run outside that scope and emit the plain
    // `CatalogDdl` variant — they have no SQL text to log.
    let catalog_entry = match crate::control::server::shared::session::audit_context::current() {
        Some(ctx) => MetadataEntry::CatalogDdlAudited {
            payload,
            auth_user_id: ctx.auth_user_id,
            auth_user_name: ctx.auth_user_name,
            sql_text: ctx.sql_text,
        },
        None => MetadataEntry::CatalogDdl { payload },
    };
    let metadata_entry = MetadataEntry::DdlPrepared {
        token: distributed_ddl_guard.token(),
        entry: Box::new(catalog_entry),
    };
    let raw = encode_entry(&metadata_entry).map_err(|e| Error::Config {
        detail: format!("metadata entry encode: {e}"),
    })?;

    let log_index = handle.propose(raw)?;

    let watcher = shared.applied_index_watcher(METADATA_GROUP_ID);
    // `wait_for` blocks the calling thread on a Condvar. When the
    // caller is already inside a tokio task (pgwire handlers always
    // are), parking the worker without telling tokio starves every
    // other task that lands on it — including the raft tick that
    // would otherwise bump the watcher. Wrap the blocking section
    // in `block_in_place` so tokio reassigns a fresh worker.
    let outcome = tokio::task::block_in_place(|| watcher.wait_for(log_index, timeout));
    match outcome {
        WaitOutcome::Reached
            if shared.metadata_ddl_applied_token.load(Ordering::Acquire)
                == distributed_ddl_guard.token() =>
        {
            Ok(ProposeOutcome::Replicated { log_index })
        }
        WaitOutcome::Reached => Err(Error::Config {
            detail: "metadata DDL preparation ownership was superseded before apply".into(),
        }),
        WaitOutcome::TimedOut => Err(Error::Config {
            detail: format!(
                "metadata propose timed out after {:?} waiting for log index {} (current: {})",
                timeout,
                log_index,
                watcher.current()
            ),
        }),
        WaitOutcome::GroupGone => Err(Error::Config {
            detail: "metadata group no longer hosted on this node".into(),
        }),
    }
}
