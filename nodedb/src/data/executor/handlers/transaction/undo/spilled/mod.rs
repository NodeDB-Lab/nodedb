// SPDX-License-Identifier: BUSL-1.1

//! The undo entries that hold plain data, in the form a spill file stores.

pub(in crate::data::executor) mod convert;
pub(in crate::data::executor) mod entry;
pub(in crate::data::executor) mod restore;
#[cfg(test)]
mod round_trip_tests;
pub(in crate::data::executor) mod weight;

pub(in crate::data::executor) use entry::SpilledUndo;
