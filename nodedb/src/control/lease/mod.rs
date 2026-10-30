// SPDX-License-Identifier: BUSL-1.1

//! Descriptor lease acquisition and release via the metadata raft group.
//!
//! Wraps the `MetadataEntry::DescriptorLeaseGrant` /
//! `DescriptorLeaseRelease` raft path that already exists in
//! `nodedb-cluster`. The cluster crate owns the canonical lease
//! state in `MetadataCache.leases` (a
//! `HashMap<(DescriptorId, node_id), DescriptorLease>`), populated
//! by every node's commit applier as soon as a grant or release
//! entry commits on the metadata raft group.
//!
//! This module provides the host-side API surface — `acquire_lease`
//! and `release_leases` — that proposes those entries and awaits
//! the local applied watermark. Synchronous code hands a release to
//! the background `releaser` instead of waiting.
//!
//! The planner acquires a lease before reading a descriptor to prevent
//! stale reads across DDL. DDL drain consumes the `MetadataCache.leases`
//! view before committing a new descriptor version. On `SIGTERM`, leases
//! are released explicitly so they drain faster than expiry.

pub mod admission;
pub mod descriptor_lookup;
pub mod drain;
pub mod drain_apply;
mod drain_hold;
pub mod drain_propose;
pub mod drain_propose_async;
pub mod gc;
pub mod holders;
mod leader_wait;
pub mod leave_cleanup;
pub mod propose;
pub mod refcount;
pub mod release;
pub mod releaser;
pub mod renewal;
pub mod runtime;
mod self_fence;
pub mod shutdown_release;

pub(crate) use self_fence::lease_use_is_fenced;

pub use descriptor_lookup::{
    clear_implicit_drains, descriptor_id_and_prior_version, descriptor_id_for_implicit_clear,
    drains_for_implicit_clear, move_source_descriptor, move_tenant_drain_owner,
};
pub use drain::{DescriptorDrainTracker, DrainEntry};
pub use drain_apply::{apply_drain_ends, apply_drain_start};
pub(crate) use drain_hold::DrainHolds;
pub use drain_propose_async::{drain_for_ddl_async, drain_for_owner_async, end_drain_async};
pub use holders::{LeaseHolders, LeaseRevocation, revoke_if_fenced, revoke_on_release};
pub(super) use leader_wait::propose_and_wait;
pub use propose::{DEFAULT_LEASE_DURATION, acquire_lease, compute_expires_at, force_refresh_lease};
pub(crate) use propose::{acquire_lease_after_admission, drain_owner_list, ensure_not_draining};
pub use refcount::{LeaseRefCount, QueryLeaseScope};
pub use release::release_leases;
pub use renewal::{LeaseRenewalConfig, LeaseRenewalLoop};
pub use runtime::LeaseRuntime;
