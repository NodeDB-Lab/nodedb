// SPDX-License-Identifier: BUSL-1.1

//! Durable images of the metadata group's host-side state.
//!
//! Each metadata apply that changes one of these writes its row before it
//! returns, and boot seeds the in-memory state from the rows. No state here
//! depends on replaying the metadata log.

pub mod ddl;
pub mod drains;
pub mod leases;
pub mod owed_post_apply;
pub mod scalars;
