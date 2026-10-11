// SPDX-License-Identifier: BUSL-1.1

//! Production metadata-group commit applier.
//!
//! Single branch for DDL: decode the opaque `CatalogDdl { payload }`
//! as a host-side [`CatalogEntry`], write through to `SystemCatalog`
//! redb via [`catalog_entry::apply_to`], and spawn the post-apply
//! side effects (Data Plane register, sequence registry sync, etc.).
//! All 16 per-DDL-object types are handled by adding a variant to
//! `CatalogEntry` — nothing in this file changes per type.
//!
//! The applier broadcasts `CatalogChangeEvent` (for future
//! prepared-statement / catalog cache invalidation). The per-group
//! apply watermark is maintained by the Raft tick loop directly via
//! [`nodedb_cluster::GroupAppliedWatchers`] — the applier owns
//! no watcher because that primitive is keyed by `group_id` and shared
//! across every Raft group on the node.
//!
//! Split by concern:
//! - [`types`]: the `MetadataCommitApplier` struct, construction, and
//!   `CatalogChangeEvent`.
//! - [`lease_events`]: descriptor-drain and CA-trust-change effects.
//! - [`membership_effects`]: join tokens, enrollment preauthorizations,
//!   and a node's leave.
//! - [`surrogate`]: cross-engine surrogate HWM + HiLo batch reservation.
//! - [`database_id`]: replicated database-id reservation.
//! - [`restore_point`]: cluster restore points.
//! - [`sync_and_routing`]: Lite sync-producer register/fence + live
//!   routing-table `SetPlacement`.
//! - [`catalog_ddl`]: `CatalogDdl` / `CatalogDdlAudited` decode + apply.
//! - [`pending_ddl`]: `DdlPendingPropose` / `DdlPendingFinalize` /
//!   `DdlPendingCancel` apply.
//! - [`host_state`]: durable leases, cluster version, and DDL preparation
//!   owner.
//! - [`boot_seed`]: loads the persisted host state before the Raft loop
//!   ticks.
//! - [`dispatch`]: the recursive `apply_host_side_effects` entry point
//!   and `impl MetadataApplier for MetadataCommitApplier`.
//! - [`audit`]: audit and CA-trust helpers (kept as its own file; used
//!   by [`catalog_ddl`] and [`lease_events`]).
//! - [`audit_describe`]: the name, version, and HLC an audit record reports
//!   for a catalog entry.
//! - [`wedge`]: transient-vs-permanent classification of an apply failure
//!   and the readiness marker a permanent one leaves behind.

mod audit;
mod audit_describe;
mod boot_seed;
mod catalog_ddl;
mod database_id;
mod dispatch;
mod host_state;
mod lease_events;
mod membership_effects;
mod pending_ddl;
mod restore_point;
mod surrogate;
mod sync_and_routing;
#[cfg(test)]
mod test_fixture;
mod types;
mod wedge;

pub use boot_seed::{seed_host_tables, seed_metadata_cache};
pub use catalog_ddl::BACKUP_MARK_FAIL_POINT;
pub use dispatch::METADATA_APPLY_HOLD_POINT;
pub use types::{CATALOG_CHANNEL_CAPACITY, CatalogChangeEvent, MetadataCommitApplier};
pub use wedge::{ApplyFailureClass, MetadataApplyWedge, WedgeReport, classify};
