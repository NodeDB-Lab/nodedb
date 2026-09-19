// SPDX-License-Identifier: BUSL-1.1

mod array_merge;
mod array_staged;
mod columnar_merge;
mod fts_merge;
mod fts_score;
mod graph_staged;
mod lease;
mod merge;
mod spatial_merge;
mod staged;
mod staged_sidecar;
mod staged_vector;
mod timeseries_merge;
mod vector_merge;
mod vector_primary_merge;

pub(in crate::data::executor) use array_merge::ArrayOverlayMergeParams;
pub use array_staged::{ArrayTxnOverlay, StagedCellPut};
pub(in crate::data::executor) use columnar_merge::{
    ColumnarMatchedRow, ColumnarOverlayMergeParams, decode_staged_row,
};
pub(in crate::data::executor) use fts_merge::FtsMergeParams;
pub use graph_staged::{GraphCollKey, GraphTxnOverlay, NodeLabelDelta};
pub(in crate::data::executor) use merge::IndexOverlayMergeParams;
pub(in crate::data::executor) use spatial_merge::SpatialOverlayMergeParams;
pub use staged::{CollectionOverlay, MAX_TXN_OVERLAY_BYTES, Staged, TxnOverlay};
pub use staged_sidecar::{BitemporalStamp, StagedTtl};
pub use staged_vector::StagedVectorRow;
pub(in crate::data::executor) use timeseries_merge::TimeseriesOverlayMergeParams;
pub(in crate::data::executor) use vector_merge::VectorMergeParams;
pub(in crate::data::executor) use vector_primary_merge::{SidecarRowShape, staged_vector_sidecar};
