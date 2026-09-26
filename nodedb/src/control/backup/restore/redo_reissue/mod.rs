// SPDX-License-Identifier: BUSL-1.1

//! Durable, replicated RESTORE of document rows, their index entries, and
//! graph edges, as committed redo records.

mod commit;
mod documents;
mod edges;
mod reissue;
mod sub_record;
mod units;

pub(in crate::control::backup::restore) use reissue::{RestoredRows, reissue_rows_and_edges};
