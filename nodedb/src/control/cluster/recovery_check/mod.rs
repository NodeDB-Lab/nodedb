//! Catalog recovery sanity check — the `CatalogSanityCheck`
//! startup phase.
//!
//! This module is **not** a "derived schema vs persisted redb"
//! diff — the NodeDB applier writes directly into
//! `SystemCatalog` (redb), so there is no second catalog view
//! to compare. Instead, three genuine invariants are checked:
//!
//! 1. [`applied_index`] — the metadata raft group's
//!    `MetadataCache.applied_index` is ≥ the committed index
//!    observed on entry. A gap means replay hasn't finished;
//!    the node is serving against stale state and startup
//!    must abort.
//!
//! 2. [`integrity`] — cross-table referential integrity inside
//!    redb. Every `StoredCollection` has a matching
//!    `StoredOwner`; every owner references an existing user;
//!    every grant references both an existing user/role and
//!    an existing object. redb is NOT atomic across tables, so
//!    a crash mid-apply can leave any of these broken.
//!
//! 3. [`registry_verify`] — every in-memory registry loaded
//!    via `load_from(catalog)` at startup is re-checked
//!    against the current redb state using its `snapshot_*`
//!    methods. A `load_from` bug silently corrupts an entire
//!    feature's in-memory view; the sanity checker catches it
//!    by comparing element-wise and repairing via a fresh
//!    re-load into the same registry.
//!
//! The top-level entry point is [`verify::verify_and_repair`]
//! which runs all three in sequence and returns a
//! [`report::VerifyReport`] with per-phase outcomes.

pub mod applied_index;
pub mod divergence;
pub mod integrity;
pub mod registry_verify;
pub mod report;
pub mod verify;

pub use applied_index::check_applied_index;
pub use divergence::{Divergence, DivergenceKind};
pub use report::{RegistryDivergenceCount, VerifyReport};
pub use verify::verify_and_repair;
