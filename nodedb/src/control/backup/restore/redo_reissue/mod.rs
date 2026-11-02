// SPDX-License-Identifier: BUSL-1.1

//! Durable, replicated RESTORE of document rows, their index entries, KV
//! keys, columnar keys, timeseries rows, vector rows, graph edges, node
//! labels and array cells, as Calvin transactions.

mod arrays;
mod columnar;
mod commit;
mod documents;
mod edges;
mod kv;
mod labels;
mod prepared;
mod reissue;
mod sub_record;
mod timeseries;
mod units;
mod vectors;

pub(in crate::control::backup::restore) use arrays::reissue_array_cells;
pub(in crate::control::backup::restore) use columnar::reissue_columnar_keys;
pub(in crate::control::backup::restore) use documents::{max_row_surrogate, prepare_documents};
pub(in crate::control::backup::restore) use kv::reissue_kv_tables;
pub(in crate::control::backup::restore) use reissue::{RestoredRows, reissue_rows_and_edges};
pub(in crate::control::backup::restore) use timeseries::{
    BackedUpTimeseries, reissue_timeseries_rows,
};
pub(in crate::control::backup::restore) use vectors::{BackedUpVectors, reissue_vector_rows};
