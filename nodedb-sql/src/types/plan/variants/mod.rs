// SPDX-License-Identifier: Apache-2.0

//! `SqlPlan` and its per-family payload structs.

mod array;
mod cte;
mod hybrid;
mod index_ddl;
mod index_reads;
mod lateral;
mod merge;
mod plan;
mod recursive;
mod text;
mod timeseries;
mod vector_primary;
mod writes;

pub use array::{
    AlterArrayPlan, ArrayAggPlan, ArrayElementwisePlan, ArrayProjectPlan, ArraySlicePlan,
    CreateArrayPlan, DeleteArrayPlan, InsertArrayPlan,
};
pub use cte::CtePlan;
pub use hybrid::{HybridSearchPlan, HybridSearchTriplePlan};
pub use index_ddl::{CreateIndexPlan, DropIndexPlan};
pub use index_reads::{DocumentIndexLookupPlan, RangeScanPlan};
pub use lateral::{LateralLoopPlan, LateralTopKPlan};
pub use merge::MergePlan;
pub use plan::{DistanceMetric, SqlPlan};
pub use recursive::{RecursiveScanPlan, RecursiveValuePlan};
pub use text::{TextScoreColumn, TextSearchPlan, TextSearchShape};
pub use timeseries::{TimeseriesIngestPlan, TimeseriesScanPlan};
pub use vector_primary::{
    VectorPrimaryDeletePlan, VectorPrimaryInsertPlan, VectorPrimaryTruncatePlan,
    VectorPrimaryUpdatePlan,
};
pub use writes::{InsertPlan, KvInsertPlan, UpsertPlan};
