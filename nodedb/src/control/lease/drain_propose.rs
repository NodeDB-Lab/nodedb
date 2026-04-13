//! Descriptor lease drain proposer flow.
//!
//! Wraps the replicated `DescriptorDrainStart` / `DescriptorDrainEnd`
//! raft path with a synchronous wait loop:
//!
//! 1. Propose `DescriptorDrainStart(id, up_to_version, expires_at)`
//!    through the metadata raft group. Every node's applier installs
//!    the drain entry into `shared.lease_drain`, so a subsequent
//!    `force_refresh_lease` on any node rejects new acquires at the
//!    drained version.
//! 2. Poll `metadata_cache.leases` every 50ms, filtering for
//!    entries on the same descriptor at `version <= up_to_version`.
//!    Return `Ok(())` once the filtered set is empty.
//! 3. On deadline, propose `DescriptorDrainEnd(id)` explicitly so
//!    the cluster can make progress, then return
//!    `Err::Config { "drain timed out" }`.
//!
//! On the happy path, the `DescriptorDrainEnd` raft entry is NOT
//! emitted: the subsequent `Put*` raft entry carries the new
//! descriptor version, and the metadata applier's post-apply hook
//! calls `shared.lease_drain.install_end` implicitly on every node.
//! This saves one raft round-trip per DDL on the common path.
//!
//! ## Rolling upgrade
//!
//! The `MetadataEntry::DescriptorDrainStart` / `End` variants are
//! wire-format v4. Mixed clusters running v3 binaries can't decode
//! them, so the proposer gates on
//! `cluster_version_state.can_activate_feature(DESCRIPTOR_DRAIN_VERSION)`
//! and returns `Ok(())` immediately in compat mode — the same
//! "degrade to no drain" fallback catalog DDL uses. Mixed clusters
//! behave without drain safety until all nodes are upgraded.

use std::time::{Duration, Instant};

use nodedb_cluster::{DescriptorId, DescriptorKind, MetadataEntry, encode_entry};
use nodedb_types::Hlc;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::rolling_upgrade::DESCRIPTOR_DRAIN_VERSION;
use crate::control::state::SharedState;
use crate::error::Error;

/// How often the drain wait loop re-polls `metadata_cache.leases`
/// to check whether the in-flight leases have drained.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Grace period added on top of the configured lease duration
/// when computing a drain entry's TTL. Prevents a drain entry
/// from expiring before the leases it's waiting on.
const DRAIN_TTL_GRACE: Duration = Duration::from_secs(30);

/// Orchestrate a full drain for a `Put*` DDL on the descriptor
/// identified by `id`, targeting prior version `up_to_version`.
///
/// Returns `Ok(())` when every lease at `version <= up_to_version`
/// has drained from `shared.metadata_cache.leases`, or when the
/// rolling-upgrade gate is closed (compat mode). Returns an error
/// on timeout, on propose failures, or if `prior_version == 0`
/// does not apply (callers should skip the call entirely for
/// creates).
pub fn drain_for_ddl(
    shared: &SharedState,
    id: DescriptorId,
    up_to_version: u64,
    max_wait: Duration,
) -> Result<(), Error> {
    // Rolling upgrade gate: no drain in mixed-version clusters.
    {
        let vs = shared
            .cluster_version_state
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if !vs.can_activate_feature(DESCRIPTOR_DRAIN_VERSION) {
            tracing::warn!(
                min_version = vs.min_version,
                required = DESCRIPTOR_DRAIN_VERSION,
                "descriptor lease drain: cluster in compat mode, skipping drain"
            );
            return Ok(());
        }
    }

    // Nothing to drain: no prior version means no lease could
    // have been acquired against this descriptor. Callers SHOULD
    // skip the call in that case but the guard is cheap.
    if up_to_version == 0 {
        return Ok(());
    }

    // Propose DrainStart. Every node's applier sees it and
    // installs into `shared.lease_drain`, so a subsequent
    // `force_refresh_lease` on any node rejects new acquires at
    // the drained version.
    let now_hlc = shared.hlc_clock.now();
    let ttl_ns: u64 = (max_wait + DRAIN_TTL_GRACE)
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX);
    let expires_at = Hlc::new(now_hlc.wall_ns.saturating_add(ttl_ns), 0);

    propose_drain(
        shared,
        MetadataEntry::DescriptorDrainStart {
            descriptor_id: id.clone(),
            up_to_version,
            expires_at,
        },
        "drain_start",
    )?;

    // Wait for matching leases to drain.
    match poll_leases_drained(shared, &id, up_to_version, max_wait) {
        Ok(()) => Ok(()),
        Err(e) => {
            // Timeout or other failure: emit DrainEnd explicitly
            // so the cluster isn't stuck rejecting acquires at
            // this version. Log and ignore errors from the
            // cleanup propose — the TTL on the drain entry is
            // the last line of defence.
            if let Err(cleanup_err) = propose_drain(
                shared,
                MetadataEntry::DescriptorDrainEnd {
                    descriptor_id: id.clone(),
                },
                "drain_end",
            ) {
                tracing::warn!(
                    error = %cleanup_err,
                    "descriptor lease drain: cleanup propose failed after timeout"
                );
            }
            Err(e)
        }
    }
}

/// Wait until `metadata_cache.leases` has no entries on `id` at
/// `version <= up_to_version`. Polls every [`POLL_INTERVAL`] until
/// the deadline.
pub(crate) fn poll_leases_drained(
    shared: &SharedState,
    id: &DescriptorId,
    up_to_version: u64,
    max_wait: Duration,
) -> Result<(), Error> {
    let deadline = Instant::now() + max_wait;
    loop {
        let remaining = count_matching_leases(shared, id, up_to_version);
        if remaining == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Error::Config {
                detail: format!(
                    "descriptor lease drain timed out after {max_wait:?} \
                     waiting for {id:?} up to version {up_to_version} \
                     (still held: {remaining})"
                ),
            });
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Count leases on `id` at `version <= up_to_version` currently in
/// the metadata cache. `0` means the drain target has cleared.
fn count_matching_leases(shared: &SharedState, id: &DescriptorId, up_to_version: u64) -> usize {
    let cache = shared
        .metadata_cache
        .read()
        .unwrap_or_else(|p| p.into_inner());
    cache
        .leases
        .iter()
        .filter(|((lid, _), l)| lid == id && l.version <= up_to_version)
        .count()
}

/// Encode + propose a drain variant through the shared
/// `metadata_proposer` helper, blocking until the local
/// applied-index watcher confirms the entry has been applied on
/// this node. Mirrors `lease::propose_and_wait` — extracted here
/// because drain variants are not `CatalogDdl` and go through a
/// different encode path.
fn propose_drain(
    shared: &SharedState,
    entry: MetadataEntry,
    operation: &'static str,
) -> Result<(), Error> {
    let Some(handle) = shared.metadata_raft.get() else {
        // Single-node fallback: apply drain directly to the local
        // tracker by wrapping the entry in the same code path the
        // applier uses. This keeps single-node DDL tests honest:
        // they exercise drain state even without a real raft loop.
        apply_drain_locally(shared, &entry);
        return Ok(());
    };
    let raw = encode_entry(&entry).map_err(|e| Error::Config {
        detail: format!("descriptor drain {operation} encode: {e}"),
    })?;
    let log_index = handle.propose(raw)?;
    let watcher = shared.applied_index_watcher();
    const DRAIN_PROPOSE_TIMEOUT: Duration = Duration::from_secs(5);
    let timed_out =
        tokio::task::block_in_place(|| !watcher.wait_for(log_index, DRAIN_PROPOSE_TIMEOUT));
    if timed_out {
        return Err(Error::Config {
            detail: format!(
                "descriptor drain {operation} timed out after {DRAIN_PROPOSE_TIMEOUT:?} \
                 waiting for log index {log_index} (current: {})",
                watcher.current()
            ),
        });
    }
    Ok(())
}

/// Single-node fallback: apply a drain variant directly to the
/// local tracker without going through raft. Single-node clusters
/// still install drains so DDL handlers that call `drain_for_ddl`
/// observe consistent semantics regardless of deployment mode.
fn apply_drain_locally(shared: &SharedState, entry: &MetadataEntry) {
    match entry {
        MetadataEntry::DescriptorDrainStart {
            descriptor_id,
            up_to_version,
            expires_at,
        } => {
            shared
                .lease_drain
                .install_start(descriptor_id.clone(), *up_to_version, *expires_at);
        }
        MetadataEntry::DescriptorDrainEnd { descriptor_id } => {
            shared.lease_drain.install_end(descriptor_id);
        }
        _ => {}
    }
}

/// For a `Put*` entry that carries `descriptor_version`, return
/// the `DescriptorId` whose drain should be implicitly cleared
/// after the entry applies. Returns `None` for variants without
/// descriptor versioning (auth, schedules, change streams, etc.).
///
/// Called from `MetadataCommitApplier::apply_host_side_effects`
/// on every node — after the `apply_to` succeeds, the applier
/// looks up the drained id via this helper and calls
/// `shared.lease_drain.install_end` on it. This is how drain
/// clears implicitly on the happy path without a second raft
/// round-trip.
pub fn descriptor_id_for_implicit_clear(entry: &CatalogEntry) -> Option<DescriptorId> {
    match entry {
        CatalogEntry::PutCollection(stored) => Some(DescriptorId::new(
            stored.tenant_id,
            DescriptorKind::Collection,
            stored.name.clone(),
        )),
        CatalogEntry::PutMaterializedView(stored) => Some(DescriptorId::new(
            stored.tenant_id,
            DescriptorKind::MaterializedView,
            stored.name.clone(),
        )),
        CatalogEntry::PutFunction(stored) => Some(DescriptorId::new(
            stored.tenant_id,
            DescriptorKind::Function,
            stored.name.clone(),
        )),
        CatalogEntry::PutProcedure(stored) => Some(DescriptorId::new(
            stored.tenant_id,
            DescriptorKind::Procedure,
            stored.name.clone(),
        )),
        CatalogEntry::PutTrigger(stored) => Some(DescriptorId::new(
            stored.tenant_id,
            DescriptorKind::Trigger,
            stored.name.clone(),
        )),
        CatalogEntry::PutSequence(stored) => Some(DescriptorId::new(
            stored.tenant_id,
            DescriptorKind::Sequence,
            stored.name.clone(),
        )),
        _ => None,
    }
}

/// For a `Put*` entry that carries `descriptor_version`, return
/// `(descriptor_id, prior_persisted_version)` so the proposer can
/// decide whether to run drain. `prior_persisted_version` is `0`
/// on create (no prior record) and causes `drain_for_ddl` to
/// return immediately.
///
/// Called from `metadata_proposer::propose_catalog_entry_with_timeout`
/// BEFORE the raft propose path. Reads from `SystemCatalog` under
/// a short read txn — the read is consistent with the subsequent
/// propose because the stamp logic in the applier increments
/// from the same prior value under its own write txn.
pub fn descriptor_id_and_prior_version(
    entry: &CatalogEntry,
    shared: &SharedState,
) -> Option<(DescriptorId, u64)> {
    let catalog = shared.credentials.catalog();
    let catalog = catalog.as_ref()?;
    match entry {
        CatalogEntry::PutCollection(stored) => {
            let prior = catalog
                .get_collection(stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|c| c.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.tenant_id,
                    DescriptorKind::Collection,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutMaterializedView(stored) => {
            let prior = catalog
                .get_materialized_view(stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|v| v.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.tenant_id,
                    DescriptorKind::MaterializedView,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutFunction(stored) => {
            let prior = catalog
                .get_function(stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|f| f.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.tenant_id,
                    DescriptorKind::Function,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutProcedure(stored) => {
            let prior = catalog
                .get_procedure(stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|p| p.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.tenant_id,
                    DescriptorKind::Procedure,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutTrigger(stored) => {
            let prior = catalog
                .get_trigger(stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|t| t.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.tenant_id,
                    DescriptorKind::Trigger,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutSequence(stored) => {
            let prior = catalog
                .get_sequence(stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|s| s.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.tenant_id,
                    DescriptorKind::Sequence,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        _ => None,
    }
}
