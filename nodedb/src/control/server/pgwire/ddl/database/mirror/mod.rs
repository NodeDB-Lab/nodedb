// SPDX-License-Identifier: BUSL-1.1

//! Mirror database runtime helpers (read-consistency enforcement + DDL apply).
//!
//! The mirror DDL *router handlers* (`MIRROR DATABASE`, `ALTER DATABASE …
//! PROMOTE`, `SHOW DATABASE MIRROR STATUS`) have been migrated to the
//! protocol-neutral router (`shared::ddl::neutral::database::mirror`). What
//! remains here are the runtime helpers that are NOT part of the DDL router:
//!
//! - [`read::check_mirror_read_consistency`] gates reads by the session's
//!   `ReadConsistency` (used by every protocol through the write-admission
//!   module's `check_mirror_read`).
//! - [`ddl_apply::apply_mirror_ddl_entry`] applies Raft DDL entries from the
//!   source observer stream, atomically updating `_system.mirror_collection_map`
//!   and `_system.mirror_lag`.

pub mod ddl_apply;
pub mod read;

pub use ddl_apply::{MirrorDdlKind, apply_mirror_ddl_entry};
pub use read::{MirrorReadOutcome, check_mirror_read_consistency};
