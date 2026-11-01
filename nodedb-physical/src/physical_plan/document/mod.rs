// SPDX-License-Identifier: Apache-2.0

//! Document / sparse engine operations dispatched to the Data Plane.

pub mod declared_column;
pub mod enforcement_types;
pub mod index_probe;
pub mod merge_types;
pub mod ollp_edge;
pub mod op;
pub mod resolved_mutation;
pub mod row_source;
pub mod sum_target;
pub mod timeseries_schema;
pub mod types;
pub mod update_value;

pub use declared_column::DeclaredColumn;
pub use enforcement_types::{
    RetentionDuration, RetentionUnit, StateTransitionDef, TransitionCheckDef, TransitionRule,
};
pub use index_probe::IndexProbe;
pub use merge_types::{MergeActionOp, MergeClauseKind as MergeClauseKindOp, MergeClauseOp};
pub use ollp_edge::OllpPredictedEdge;
pub use op::DocumentOp;
pub use resolved_mutation::{DocumentResolveOutcome, DocumentResolvedMutation};
pub use row_source::{DocumentPageSource, QuerySource, RowSource};
pub use sum_target::{RedoSumTargets, ResolvedSumTarget, SumTargetKey, resolved_sum_surrogate};
pub use timeseries_schema::TimeseriesSchema;
pub use types::{
    BalancedDef, EnforcementOptions, GeneratedColumnSpec, MaterializedSumBinding, PeriodLockConfig,
    RegisteredIndex, RegisteredIndexState, ReturningColumns, ReturningItem, ReturningSpec,
    StorageMode,
};
pub use update_value::UpdateValue;
