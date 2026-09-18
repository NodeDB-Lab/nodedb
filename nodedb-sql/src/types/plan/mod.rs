// SPDX-License-Identifier: Apache-2.0

//! SqlPlan intermediate representation and supporting types.

mod cacheability;
mod expr_scan;
mod merge_types;
mod row_types;
mod search_cells;
mod variant_name;
mod variants;
mod vector_opts;
mod volatility_scan;

pub use cacheability::PlanCacheEligibility;
pub use expr_scan::{
    contains_sequence_accessor, first_sequence_accessor, projection_is_cp_computed,
    referenced_columns,
};
pub use merge_types::{MergeClauseKind, MergePlanAction, MergePlanClause};
pub use row_types::{KvInsertIntent, VectorPrimaryRow, WriteRoute};
pub use variants::{DistanceMetric, SqlPlan};
pub use vector_opts::{ArrayPrefilter, VectorAnnOptions, VectorQuantization};
pub use volatility_scan::expr_is_volatile;
