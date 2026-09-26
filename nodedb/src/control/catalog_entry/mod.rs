// SPDX-License-Identifier: BUSL-1.1

//! Typed catalog-entry enum carried by every committed `CatalogDdl`
//! raft entry on the metadata group.
//!
//! The `CatalogEntry` enum is the single source of truth for "the set
//! of mutations a pgwire DDL handler can make to the replicated
//! catalog". Every DDL handler constructs a `CatalogEntry`, hands it
//! to [`crate::control::metadata_proposer::propose_catalog_entry`]
//! which encodes it into an opaque `Vec<u8>` payload inside a
//! `nodedb_cluster::MetadataEntry::CatalogDdl { payload }`, the raft
//! log commits it, and on every node the production
//! `MetadataCommitApplier` decodes the payload and calls
//! [`CatalogEntry::apply_to`] which writes the host-side redb record
//! via the existing `SystemCatalog` methods plus
//! [`CatalogEntry::spawn_post_apply_side_effects`] which handles
//! in-memory registry sync and Data Plane register dispatches.
//!
//! Adding a new DDL object type is a 5-step change:
//!
//! 1. Add a variant to the [`entry::CatalogEntry`] enum.
//! 2. Add an arm to [`apply::apply_to`].
//! 3. Add an arm to [`post_apply::spawn_post_apply_side_effects`]
//!    (often a no-op).
//! 4. Add a roundtrip test inline in the file that applies the new
//!    variant, as a `#[cfg(test)] mod tests` block.
//! 5. Migrate the pgwire DDL handler to build the new variant and
//!    call `propose_catalog_entry`.
//!
//! Every match in this module is exhaustive — adding a variant is a
//! compile error everywhere a caller needs to handle it.

pub mod apply;
pub mod authorization;
pub mod codec;
pub mod descriptor_stamp;
pub mod descriptor_validate;
pub mod entry;
pub mod kind;
pub mod persist_collection;
pub mod post_apply;
pub mod role_rules;

pub use codec::{decode, encode};
pub use entry::CatalogEntry;
pub use persist_collection::{merge_collection_fields_replicated, persist_collection_replicated};
