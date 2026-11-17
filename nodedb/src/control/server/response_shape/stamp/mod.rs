// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane evaluation of per-row computed SELECT-list columns.

pub mod evaluate;
pub mod no_access;
pub(crate) mod substitute;

pub use evaluate::stamp_rows;
pub use no_access::NoSequenceAccess;
