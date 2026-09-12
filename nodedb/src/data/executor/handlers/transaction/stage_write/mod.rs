// SPDX-License-Identifier: BUSL-1.1

//! Statement-time execution of in-transaction point writes by STAGING them
//! into the per-transaction overlay.
//!
//! A point write issued inside a `BEGIN..COMMIT` block is evaluated here
//! against BASE ∪ OVERLAY: constraint violations surface immediately (at the
//! statement, not deferred to COMMIT), the real affected-row count is
//! computed, and the resulting encoded body (or a tombstone) is recorded in
//! the overlay so a later same-transaction read-modify-write observes it. The
//! write is NOT made durable here — the buffered plan is still replayed
//! through the real apply path inside the COMMIT `TransactionBatch`, which
//! remains the sole durable apply.

mod body;
mod constraint;
mod context;
mod dispatch;
mod stage_bulk_delete;
mod stage_bulk_update;
mod stage_columnar;
mod stage_columnar_dml;
mod stage_columnar_resolved_dml;
mod stage_graph;
mod stage_kv;
mod stage_kv_atomic;
mod stage_kv_conflict;
mod stage_kv_delete;
mod stage_kv_transfer;
mod stage_kv_ttl;
mod stage_point_document;
mod stage_rls;
mod stage_spatial;
mod stage_timeseries;
mod stage_upsert;

pub(in crate::data::executor) use body::stored_row_identity;
pub(in crate::data::executor) use context::StageCtx;
pub(in crate::data::executor) use stage_bulk_delete::StageBulkDeleteParams;
pub(in crate::data::executor) use stage_bulk_update::StageBulkUpdateParams;
pub(in crate::data::executor) use stage_columnar::StageColumnarInsertParams;
pub(in crate::data::executor) use stage_columnar_dml::{
    StageColumnarDeleteParams, StageColumnarUpdateParams,
};
pub(in crate::data::executor) use stage_columnar_resolved_dml::{
    StageColumnarResolvedDeleteParams, StageColumnarResolvedUpdateParams,
};
pub(in crate::data::executor) use stage_graph::GRAPH_LABEL_COLL_KEY;
pub(in crate::data::executor) use stage_kv::{kv_row_identity, unhex_key};
pub(in crate::data::executor) use stage_spatial::StageSpatialInsertParams;
pub(in crate::data::executor) use stage_timeseries::StageTimeseriesInsertParams;
