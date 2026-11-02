// SPDX-License-Identifier: BUSL-1.1

//! Undo log types and rollback logic for transaction batches.

pub(super) mod apply;
pub(super) mod columnar_insert;
pub(in crate::data::executor) mod crdt_collection;
pub(in crate::data::executor) mod crdt_row;
pub(super) mod document;
pub(super) mod document_fts;
pub(in crate::data::executor::handlers) mod document_outcome;
pub(in crate::data::executor) mod edge_cut;
pub(in crate::data::executor) mod edge_write;
pub(super) mod entry;
pub(in crate::data::executor) mod error;
pub(in crate::data::executor) mod fts_doc;
pub(super) mod graph_node;
pub(in crate::data::executor) mod history_version;
pub(super) mod kv;
pub(in crate::data::executor) mod memory;
pub(super) mod rollback;
pub(in crate::data::executor) mod row_version;
pub(super) mod spatial;
pub(in crate::data::executor) mod spatial_row;
pub(super) mod stats;
pub(super) mod sync_hwm;
pub(super) mod timeseries;
pub(super) mod truncate_columnar;
pub(in crate::data::executor) mod vector_truncate;
pub(in crate::data::executor) mod vector_write;

pub(in crate::data::executor) use entry::{
    ColumnarTruncateUndo, TimeseriesEndedInstallUndo, TimeseriesIngestUndo, TimeseriesTruncateUndo,
    UndoEntry,
};
pub(in crate::data::executor) use error::UndoError;
