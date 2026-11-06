// SPDX-License-Identifier: BUSL-1.1

//! RESTORE TENANT orchestrator logic.
//!
//! Validates a backup envelope, merges the sections of each backed-up
//! database into one `TenantDataSnapshot`, then re-issues every section as
//! durable, replicated writes into the destination database.
//!
//! [`database`] re-issues one database. Durable re-issue of
//! columnar/timeseries/vector rows lives in [`reissue`]; surrogate rebinding
//! and tombstone warnings live in [`rebind`].

mod database;
mod rebind;
mod reissue;
mod restore;
mod stats;

pub use database::reissue_into_database;
pub(crate) use database::reissue_mirror_capture;
pub use restore::restore_tenant;
pub use stats::{CollectionRows, RestoreStats};
