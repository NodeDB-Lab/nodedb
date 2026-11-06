// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral mirror database DDL handlers.
//!
//! The mirror *runtime* helper `check_mirror_read_consistency` remains on the
//! pgwire side (`pgwire::ddl::database::mirror`) because it backs the read
//! path, not the DDL router. Only the three router handlers
//! (`MIRROR DATABASE`, `ALTER DATABASE … PROMOTE`, `SHOW DATABASE MIRROR
//! STATUS`) are migrated here.

pub mod create;
pub mod promote;
pub mod show;
