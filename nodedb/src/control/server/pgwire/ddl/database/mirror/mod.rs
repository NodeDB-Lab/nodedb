// SPDX-License-Identifier: BUSL-1.1

//! Mirror database read-consistency enforcement.
//!
//! The mirror DDL *router handlers* (`MIRROR DATABASE`, `ALTER DATABASE …
//! PROMOTE`, `SHOW DATABASE MIRROR STATUS`) live in the protocol-neutral
//! router (`shared::ddl::neutral::database::mirror`).
//!
//! [`read::check_mirror_read_consistency`] gates reads by the session's
//! `ReadConsistency`. Every protocol reaches it through the write-admission
//! module's `check_mirror_read`.

pub mod read;

pub use read::{MirrorReadOutcome, check_mirror_read_consistency};
