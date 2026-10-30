// SPDX-License-Identifier: BUSL-1.1

//! Propose several catalog entries as one metadata commit.

use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry};

use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::constraint_install::{ConstraintTarget, constraint_targets};
use crate::control::lease::DrainHolds;
use crate::control::security::catalog::SystemCatalog;
use crate::control::server::shared::session::ddl_buffer;
use crate::control::state::SharedState;
use crate::error::Error;

use super::catalog::{catalog_ddl_entry, propose_prepared};
use super::ddl_prepare::{acquire_ddl_prepare_lease_async, lock_ddl_preparation_async};
use super::handle::MetadataRaftHandle;
use super::timeouts::DEFAULT_DRAIN_TIMEOUT;

/// What happened to the entries a batch plan built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchOutcome {
    /// Replicated through the metadata group and applied here at this index.
    Replicated { log_index: u64 },
    /// Every entry joined the open DDL transaction.
    Buffered,
    /// The plan built no entries: nothing was proposed, buffered, or applied.
    Empty,
}

/// The entries a batch plan built, and what happened to them.
pub struct CatalogBatch {
    pub outcome: BatchOutcome,
    /// Stamped as applied on the `Replicated` path.
    pub entries: Vec<CatalogEntry>,
}

impl CatalogBatch {
    fn empty() -> Self {
        Self {
            outcome: BatchOutcome::Empty,
            entries: Vec::new(),
        }
    }
}

/// Build entries with `plan` and propose them as one metadata commit, on any
/// runtime flavor.
///
/// `plan` runs under the DDL preparation lock and lease, so no other DDL
/// changes the catalog between the plan and the commit. The batch applies at
/// one log index on every node, entry by entry in plan order, and a restart
/// replays it as one unit.
///
/// Every wait is awaited: the preparation lock and lease, the descriptor
/// drains, the commit's apply, and the authorization barrier.
pub async fn propose_catalog_batch_async(
    shared: &SharedState,
    plan: impl FnOnce(&SystemCatalog) -> crate::Result<Vec<CatalogEntry>>,
) -> Result<CatalogBatch, Error> {
    let catalog = shared.credentials.catalog();
    if ddl_buffer::is_active() {
        let entries = plan(catalog)?;
        if entries.is_empty() {
            return Ok(CatalogBatch::empty());
        }
        for entry in &entries {
            ddl_buffer::try_buffer(entry.clone());
        }
        return Ok(CatalogBatch {
            outcome: BatchOutcome::Buffered,
            entries,
        });
    }
    let handle = shared.metadata_raft_handle()?;

    // The guards drop before the authorization barrier below, as on the
    // single-entry path.
    let Some((log_index, entries, constraint_targets)) =
        propose_replicated(shared, handle.as_ref(), plan).await?
    else {
        return Ok(CatalogBatch::empty());
    };
    // A changed constraint set is live on the collection's data log before
    // the batch returns, so the first CRDT apply after it is not fenced. The
    // batch is committed, so a failed install never fails it.
    crate::control::constraint_install::install_constraints(shared, &constraint_targets).await;
    if entries.iter().any(CatalogEntry::bears_authorization) {
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
    Ok(CatalogBatch {
        outcome: BatchOutcome::Replicated { log_index },
        entries,
    })
}

/// A replicated batch: its log index, its stamped entries, and the
/// collections whose constraint version it changed.
type ReplicatedBatch = (u64, Vec<CatalogEntry>, Vec<ConstraintTarget>);

/// Plan, drain, stamp, and propose a batch under the preparation lock and
/// lease, and wait until this node applied it. Returns the
/// [`ReplicatedBatch`], or `None` for a plan with no entries. Both guards
/// are released before it returns.
async fn propose_replicated(
    shared: &SharedState,
    handle: &dyn MetadataRaftHandle,
    plan: impl FnOnce(&SystemCatalog) -> crate::Result<Vec<CatalogEntry>>,
) -> Result<Option<ReplicatedBatch>, Error> {
    let _local_ddl_guard = lock_ddl_preparation_async(shared).await;
    let lease = acquire_ddl_prepare_lease_async(shared, handle).await?;
    let mut drains = DrainHolds::new(shared);
    let proposed: Result<Option<ReplicatedBatch>, Error> = async {
        let Some((entries, targets)) = plan_drain_and_stamp(shared, plan, &mut drains).await?
        else {
            return Ok::<_, Error>(None);
        };
        let mut wrapped = Vec::with_capacity(entries.len());
        for entry in &entries {
            wrapped.push(catalog_ddl_entry(entry)?);
        }
        // A batch of slow purges applies for as long as the statement lets it.
        let log_index = propose_prepared(
            shared,
            handle,
            lease.token(),
            MetadataEntry::Batch { entries: wrapped },
            crate::control::server::shared::session::StatementDeadline::of_statement(shared)
                .instant(),
        )
        .await?;
        Ok::<_, Error>(Some((log_index, entries, targets)))
    }
    .await;
    // The applied batch ended its drains. Any failure ends them here: a drain
    // has no wall-clock expiry. The ends are proposed after the batch, so they
    // are no-ops when a batch whose outcome was unknown applied after all.
    match &proposed {
        Ok(_) => drains.keep(),
        Err(_) => drains.end().await,
    }
    lease.release().await;
    proposed
}

/// Plan, drain, and stamp a batch, and name the collections whose constraint
/// version the stamped batch changes. The caller holds the DDL preparation
/// lock and lease, and owns every drain recorded in `drains`. `None` is a plan
/// with no entries.
async fn plan_drain_and_stamp(
    shared: &SharedState,
    plan: impl FnOnce(&SystemCatalog) -> crate::Result<Vec<CatalogEntry>>,
    drains: &mut DrainHolds<'_>,
) -> Result<Option<(Vec<CatalogEntry>, Vec<ConstraintTarget>)>, Error> {
    let catalog = shared.credentials.catalog();
    let entries = plan(catalog)?;
    if entries.is_empty() {
        return Ok(None);
    }
    drain_batch(shared, &entries, drains).await?;
    let stamped =
        catalog_entry::descriptor_stamp::stamp_batch(entries, &shared.hlc_clock, catalog)?;
    let targets = constraint_targets(catalog, &stamped)?;
    Ok(Some((stamped, targets)))
}

/// Drain the prior version of every descriptor the batch changes, and record
/// each drain in `drains`.
async fn drain_batch(
    shared: &SharedState,
    entries: &[CatalogEntry],
    drains: &mut DrainHolds<'_>,
) -> Result<(), Error> {
    for entry in entries {
        if let Some((descriptor_id, prior_version)) =
            crate::control::lease::descriptor_id_and_prior_version(entry, shared)
            && prior_version > 0
        {
            drains.hold(descriptor_id.clone(), nodedb_cluster::DrainOwner::Ddl);
            crate::control::lease::drain_for_ddl_async(
                shared,
                descriptor_id,
                prior_version,
                DEFAULT_DRAIN_TIMEOUT,
                0,
            )
            .await?;
        }
    }
    Ok(())
}
