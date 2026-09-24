// SPDX-License-Identifier: Apache-2.0

//! Physical plan types dispatched from Control Plane to Data Plane.
//!
//! The top-level [`PhysicalPlan`] enum delegates to per-engine sub-enums,
//! each defined in its own module. This keeps each engine's operations
//! isolated.

pub mod array;
pub mod cluster_array;
pub mod cluster_event;
pub mod collection;
pub mod columnar;
pub mod crdt;
pub mod document;
pub mod exchange;
pub mod graph;
pub mod kv;
pub mod meta;
pub mod meta_calvin;
pub mod plan;
pub mod query;
pub mod rls_write_check_accessor;
pub mod routing;
pub mod set_op;
pub mod sort_key;
pub mod spatial;
pub mod streaming;
pub mod text;
pub mod timeseries;
pub mod truncate_target;
pub mod vector;
pub mod wire;

pub use array::{ArrayBinaryOp, ArrayOp, ArrayReducer};
pub use cluster_array::ClusterArrayOp;
pub use cluster_event::{ClusterEventOp, MAX_REMOTE_CDC_COMMITTED_OFFSETS};
pub use columnar::{ColumnarInsertIntent, ColumnarOp};
pub use crdt::{CrdtOp, CrdtWriteVerb};
pub use document::{
    BalancedDef, DocumentOp, DocumentResolveOutcome, DocumentResolvedMutation, EnforcementOptions,
    GeneratedColumnSpec, MaterializedSumBinding, OllpPredictedEdge, PeriodLockConfig,
    RedoSumTargets, RegisteredIndex, RegisteredIndexState, ResolvedSumTarget, ReturningColumns,
    ReturningItem, ReturningSpec, StorageMode, SumTargetKey, TimeseriesSchema, UpdateValue,
    resolved_sum_surrogate,
};
pub use exchange::{ExchangeMode, ExchangeOp};
pub use graph::{
    BatchEdge, BspSuperstepPlan, BspSuperstepResult, GraphOp, WccSuperstepPlan, WccSuperstepResult,
};
pub use kv::{
    KvCounterShape, KvOp, KvResolveOutcome, KvResolvedMutation, SortedIndexRead, SortedIndexSpec,
};
pub use meta::{MetaOp, SAVEPOINT_MARKER_BYTES};
pub use plan::PhysicalPlan;
pub use query::{AggregateSpec, GroupKeySpec, JoinProjection, QueryOp};
pub use routing::plan_contains_cluster_partitioned_leaf;
pub use set_op::SetOpKind;
pub use sort_key::SortKeySpec;
pub use spatial::{SpatialOp, SpatialPredicate};
pub use text::TextOp;
pub use timeseries::{TimeseriesOp, UNBOUNDED_TIME_RANGE};
pub use vector::{
    VectorDirectWriteIntent, VectorOp, VectorResolveOutcome, VectorResolvedMutation,
    VectorWriteTargets,
};
pub use wire::{decode, encode};
