// SPDX-License-Identifier: BUSL-1.1

//! Database DDL surface remaining on the pgwire side.
//!
//! The database-scoped DDL family has been migrated to the protocol-neutral
//! router (`shared::ddl::neutral::database`). What remains here:
//!
//! - [`use_database`]: session-coupled (`USE DATABASE` mutates the
//!   per-connection current database + resets txn / prepared statements),
//!   intercepted in `execute_single_sql` before the DDL router.
//! - [`mirror`]: the mirror read-consistency enforcement that backs the read
//!   path. It is no DDL router handler.

pub mod mirror;
pub mod use_database;

pub use mirror::{MirrorReadOutcome, check_mirror_read_consistency};
pub use use_database::handle_use_database;
