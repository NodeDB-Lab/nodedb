// SPDX-License-Identifier: BUSL-1.1

//! Undo log types and rollback logic for transaction batches.

pub(super) mod apply;
pub(super) mod balanced;
pub(super) mod document;
pub(super) mod document_fts;
pub(super) mod entry;
pub(super) mod graph_node;
pub(super) mod kv;
pub(super) mod rollback;
pub(super) mod spatial;
pub(super) mod stats;
pub(super) mod timeseries;
pub(super) mod truncate_columnar;

pub(in crate::data::executor) use entry::{
    ColumnarTruncateUndo, TimeseriesIngestUndo, TimeseriesTruncateUndo, UndoEntry,
};
