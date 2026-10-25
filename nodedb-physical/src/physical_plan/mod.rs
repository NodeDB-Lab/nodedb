// SPDX-License-Identifier: Apache-2.0

//! Physical plan types dispatched from Control Plane to Data Plane.
//!
//! The top-level [`PhysicalPlan`] enum delegates to per-engine sub-enums,
//! each defined in its own module. This keeps each engine's operations
//! isolated.

pub mod aggregate_functions;
pub mod array;
pub mod calvin_reply;
pub mod cluster_array;
pub mod cluster_event;
pub mod collection;
pub mod columnar;
pub mod crdt;
pub mod document;
pub mod exchange;
pub mod gathered_source;
pub mod graph;
pub mod kv;
pub mod meta;
pub mod meta_calvin;
pub mod meta_home;
pub mod meta_restore;
pub mod meta_snapshot;
pub mod plan;
pub mod query;
pub mod redo_origin;
pub mod rls_write_check_accessor;
pub mod routing;
pub mod set_op;
pub mod sort_key;
pub mod spatial;
pub mod streaming;
pub mod text;
pub mod tiering;
pub mod timeseries;
pub mod truncate_target;
pub mod vector;
pub mod wire;
pub mod written_collections;

pub use aggregate_functions::{
    AggregateParam, GENERIC_AGGREGATE_FUNCTIONS, PARAMETERIZED_AGGREGATE_FUNCTIONS,
};
pub use array::{ArrayBinaryOp, ArrayOp, ArrayReducer};
pub use calvin_reply::{
    CalvinInstall, CalvinInstalledTimeseriesSpec, CalvinPostImagesSpec, CalvinReplyRow,
    CalvinReplySpec, CalvinResolved, CalvinRowEngine,
};
pub use cluster_array::ClusterArrayOp;
pub use cluster_event::{ClusterEventOp, MAX_REMOTE_CDC_COMMITTED_OFFSETS};
pub use columnar::{ColumnarInsertIntent, ColumnarOp};
pub use crdt::{CrdtOp, CrdtWriteVerb};
pub use document::{
    BalancedDef, DeclaredColumn, DocumentOp, DocumentPageSource, DocumentResolveOutcome,
    DocumentResolvedMutation, EnforcementOptions, GeneratedColumnSpec, MaterializedSumBinding,
    OllpPredictedEdge, PeriodLockConfig, QuerySource, RedoSumTargets, RegisteredIndex,
    RegisteredIndexState, ResolvedSumTarget, ReturningColumns, ReturningItem, ReturningSpec,
    RowSource, StorageMode, SumTargetKey, TimeseriesSchema, UpdateValue, resolved_sum_surrogate,
};
pub use exchange::{ExchangeMode, ExchangeOp};
pub use gathered_source::GatheredSource;
pub use graph::{
    ALL_VERSIONS_REFUSED, AlgoEdge, AlgoStage, BatchEdge, BspSuperstepPlan, BspSuperstepResult,
    GraphOp, GraphReadScope, GraphSystemCut, RagBindingRow, RagLegs, RagStage, RagTextHit,
    RagVectorHit, WccSuperstepPlan, WccSuperstepResult,
};
pub use kv::{
    KvBatchIntent, KvCounterShape, KvOp, KvResolveOutcome, KvResolvedMutation, SortedIndexRead,
    SortedIndexSpec, TransferAmount,
};
pub use meta::MetaOp;
pub use meta_home::{HomeAnswer, HomeVersion, HomeVersionProbe};
pub use meta_restore::{
    RestoredEdgeVersion, RestoredIdentity, RestoredNodeLabels, RestoredRedo, RestoredRow,
};
pub use meta_snapshot::{CutCaptureRequest, DepartedCollection, SnapshotClearTarget};
pub use plan::PhysicalPlan;
pub use query::{AggregateSpec, GroupKeySpec, JoinProjection, QueryOp};
pub use redo_origin::RedoOrigin;
pub use routing::plan_contains_cluster_partitioned_leaf;
pub use set_op::SetOpKind;
pub use sort_key::SortKeySpec;
pub use spatial::{SpatialOp, SpatialPredicate};
pub use text::{ScoreScanBound, ScoreScanOrder, TextOp, TextScoreSpec};
pub use tiering::{TierRead, TierSegment, TierSource, TieringError, TieringPlan};
pub use timeseries::{
    AggregateIncarnation, PartitionsAction, RefreshCut, TimeseriesOp, TimeseriesResolve,
    UNBOUNDED_TIME_RANGE,
};
pub use vector::{
    VectorDirectRow, VectorDirectWriteIntent, VectorOp, VectorResolveOutcome,
    VectorResolvedMutation, VectorWriteTargets,
};
pub use wire::{decode, encode};
pub use written_collections::written_collections;
