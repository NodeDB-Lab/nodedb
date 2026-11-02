// SPDX-License-Identifier: BUSL-1.1

pub mod cut;
pub mod definition;
pub mod fold;
pub mod image;
pub mod manager;
pub mod merge_names;
pub mod partial;
pub mod persist;
pub mod refresh;
pub mod rollup;
pub mod snapshot;
#[cfg(test)]
pub mod test_support;
pub mod watermark;

pub use cut::{CutOutcome, CutState, CutStatus, OrphanCut};
pub use definition::{AggFunction, AggregateExpr, ContinuousAggregateDef, RefreshPolicy};
pub use fold::{FoldScope, FoldTarget};
pub use image::{
    CAGG_IMAGE_FILE, CaggImage, CaggImageError, CaggImageView, decode_image, encode_image,
};
pub use manager::{AggKey, AggregateInfo, ContinuousAggregateManager};
pub use partial::{ColumnLayout, ColumnPartial, GroupCell, PartialAggregate};
pub use refresh::{RefreshSource, RowCut};
pub use watermark::WatermarkState;
