// SPDX-License-Identifier: Apache-2.0

//! SqlPlan intermediate representation and supporting types.

mod cacheability;
mod merge_types;
mod row_types;
mod variants;
mod vector_opts;

pub use cacheability::PlanCacheEligibility;
pub use merge_types::{MergeClauseKind, MergePlanAction, MergePlanClause};
pub use row_types::{KvInsertIntent, VectorPrimaryRow};
pub use variants::{DistanceMetric, SqlPlan};
pub use vector_opts::{ArrayPrefilter, VectorAnnOptions, VectorQuantization};
