// SPDX-License-Identifier: Apache-2.0

//! System-time history of the inverted index.
//!
//! A write indexes a document's text at its system-time ordinal. An update
//! or a delete ends the indexed version at the write's ordinal, and the
//! version's analyzed tokens move to the history. A read at a cut sees the
//! index as a transaction sees its staged writes. The live documents
//! indexed after the cut are hidden. The ended versions visible at the cut
//! compete in the ranking from their tokens. Membership is exact at the
//! cut. BM25 scores use the current corpus statistics. A backend stores the
//! rows through [`crate::backend::FtsHistoryWrite`].

pub mod cut;
pub mod keys;
pub mod purge;
#[cfg(test)]
mod test_rows;
pub mod version;
pub mod write;

pub use cut::{TextCutView, cut_view};
pub use keys::{EndedKey, EndedRow, LiveKey, LiveRow};
pub use purge::purge_below;
pub use version::{EndedVersion, HistoryDoc, TextCut, UNVERSIONED};
pub use write::{UndoStep, end_version, start_version, undo_at};
