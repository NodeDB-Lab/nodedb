// SPDX-License-Identifier: BUSL-1.1

//! Per-engine rewrite stages: a read of the cloned collection becomes the
//! same read of the source collection.

use nodedb_physical::physical_plan::{ColumnarOp, DocumentOp, KvOp, PhysicalPlan, TimeseriesOp};

use super::refusal::{SourceRewrite, refuse_clone_read_shape};
use super::rewrite::{RewriteCtx, refuse_or_skip};

/// Document plans. A scan, point get, or indexed fetch of the cloned
/// collection reads the source collection instead.
pub(super) async fn rewrite_document(
    plan: &PhysicalPlan,
    op: &DocumentOp,
    ctx: &RewriteCtx<'_>,
) -> crate::Result<SourceRewrite> {
    match op {
        DocumentOp::Scan {
            collection,
            limit,
            offset,
            sort_keys,
            filters,
            distinct,
            projection,
            computed_columns,
            window_functions,
            system_time,
            valid_at_ms,
            prefilter,
        } if ctx.is_target(collection) => Ok(SourceRewrite::task(PhysicalPlan::Document(
            DocumentOp::Scan {
                collection: ctx.source_qualified.clone(),
                limit: *limit,
                offset: *offset,
                sort_keys: sort_keys.clone(),
                filters: filters.clone(),
                distinct: *distinct,
                projection: projection.clone(),
                computed_columns: computed_columns.clone(),
                window_functions: window_functions.clone(),
                system_time: ctx.system_time(*system_time)?,
                valid_at_ms: *valid_at_ms,
                prefilter: prefilter.clone(),
            },
        ))),

        DocumentOp::PointGet {
            collection,
            document_id,
            surrogate: _,
            pk_bytes,
            rls_filters,
            system_time,
            valid_at_ms,
        } if ctx.is_target(collection) => {
            // The target surrogate is invalid in the source database — each
            // maintains its own pk→surrogate mapping. The source collection's
            // home answers the binding through the async routed exchange. No
            // binding means the row never existed there; skip rather than use
            // a sentinel. Lookup errors are also treated as "skip" (visible in
            // the exchange's own logs instead).
            let system_time = ctx.system_time(*system_time)?;
            let Some(source_surrogate) =
                crate::control::server::surrogate_exchange::lookup_surrogate_routed(
                    ctx.state,
                    nodedb_types::CollectionKey::from_bare(ctx.source_db_id, ctx.source_coll),
                    ctx.tenant_id,
                    pk_bytes,
                    crate::types::TraceId::ZERO,
                )
                .await
                .ok()
                .flatten()
            else {
                return Ok(SourceRewrite::NoSourceTask);
            };
            Ok(SourceRewrite::task(PhysicalPlan::Document(
                DocumentOp::PointGet {
                    collection: ctx.source_qualified.clone(),
                    document_id: document_id.clone(),
                    surrogate: Some(source_surrogate),
                    pk_bytes: pk_bytes.clone(),
                    rls_filters: rls_filters.clone(),
                    system_time,
                    valid_at_ms: *valid_at_ms,
                },
            )))
        }

        DocumentOp::IndexedFetch {
            collection,
            path,
            value,
            filters,
            projection,
            limit,
            offset,
            sort_keys,
            system_time,
        } if ctx.is_target(collection) => Ok(SourceRewrite::task(PhysicalPlan::Document(
            DocumentOp::IndexedFetch {
                collection: ctx.source_qualified.clone(),
                path: path.clone(),
                value: value.clone(),
                filters: filters.clone(),
                projection: projection.clone(),
                limit: *limit,
                offset: *offset,
                sort_keys: sort_keys.clone(),
                system_time: ctx.system_time(*system_time)?,
            },
        ))),

        _ => refuse_or_skip(plan, ctx),
    }
}

/// KV plans. A scan or get of the cloned collection reads the source
/// collection under the clone's surrogate ceiling.
pub(super) fn rewrite_kv(
    plan: &PhysicalPlan,
    op: &KvOp,
    ctx: &RewriteCtx<'_>,
) -> crate::Result<SourceRewrite> {
    match op {
        KvOp::Scan {
            collection,
            cursor,
            count,
            filters,
            projection,
            computed_columns,
            match_pattern,
            sort_keys,
            // The original target-side scan never carries a ceiling
            // (clones-of-clones still funnel through here per-level);
            // the resolver overrides it for source delegation below.
            surrogate_ceiling: _,
        } if ctx.is_target(collection) => Ok(SourceRewrite::task(PhysicalPlan::Kv(KvOp::Scan {
            collection: ctx.source_qualified.clone(),
            cursor: cursor.clone(),
            count: *count,
            filters: filters.clone(),
            projection: projection.clone(),
            computed_columns: computed_columns.clone(),
            match_pattern: match_pattern.clone(),
            sort_keys: sort_keys.clone(),
            surrogate_ceiling: ctx.kv_surrogate_ceiling,
        }))),

        KvOp::Get {
            collection,
            key,
            rls_filters,
            surrogate_ceiling: _,
        } if ctx.is_target(collection) => Ok(SourceRewrite::task(PhysicalPlan::Kv(KvOp::Get {
            collection: ctx.source_qualified.clone(),
            key: key.clone(),
            rls_filters: rls_filters.clone(),
            surrogate_ceiling: ctx.kv_surrogate_ceiling,
        }))),

        _ => refuse_or_skip(plan, ctx),
    }
}

/// Columnar plans. A scan of the cloned collection reads the source
/// collection instead.
pub(super) fn rewrite_columnar(
    plan: &PhysicalPlan,
    op: &ColumnarOp,
    ctx: &RewriteCtx<'_>,
) -> crate::Result<SourceRewrite> {
    match op {
        ColumnarOp::Scan {
            collection,
            projection,
            limit,
            filters,
            rls_filters,
            sort_keys,
            system_time,
            valid_at_ms,
            prefilter,
            computed_columns,
        } if ctx.is_target(collection) => Ok(SourceRewrite::task(PhysicalPlan::Columnar(
            ColumnarOp::Scan {
                collection: ctx.source_qualified.clone(),
                projection: projection.clone(),
                limit: *limit,
                filters: filters.clone(),
                rls_filters: rls_filters.clone(),
                sort_keys: sort_keys.clone(),
                system_time: ctx.system_time(*system_time)?,
                valid_at_ms: *valid_at_ms,
                prefilter: prefilter.clone(),
                computed_columns: computed_columns.clone(),
            },
        ))),

        _ => refuse_or_skip(plan, ctx),
    }
}

/// Timeseries plans. A plain scan of the cloned collection reads the source
/// collection instead.
///
/// A bucketing or aggregating scan is the same unsound concatenation as
/// `Query::Aggregate`: target and source payloads are appended, so a bucket
/// present on both sides comes back twice and every sum/avg over the union is
/// wrong. The aggregating form is refused.
pub(super) fn rewrite_timeseries(
    plan: &PhysicalPlan,
    op: &TimeseriesOp,
    ctx: &RewriteCtx<'_>,
) -> crate::Result<SourceRewrite> {
    match op {
        TimeseriesOp::Scan {
            collection,
            bucket_interval_ms,
            group_by,
            aggregates,
            ..
        } if ctx.is_target(collection)
            && (!group_by.is_empty() || !aggregates.is_empty() || *bucket_interval_ms != 0) =>
        {
            Err(refuse_clone_read_shape(plan, ctx.target_coll))
        }

        TimeseriesOp::Scan {
            collection,
            time_range,
            projection,
            limit,
            filters,
            sort_keys,
            bucket_interval_ms,
            group_by,
            aggregates,
            gap_fill,
            computed_columns,
            rls_filters,
            system_time,
            valid_at_ms,
            having,
            tiering,
            output_columns,
        } if ctx.is_target(collection) => Ok(SourceRewrite::task(PhysicalPlan::Timeseries(
            TimeseriesOp::Scan {
                collection: ctx.source_qualified.clone(),
                time_range: *time_range,
                projection: projection.clone(),
                limit: *limit,
                filters: filters.clone(),
                sort_keys: sort_keys.clone(),
                bucket_interval_ms: *bucket_interval_ms,
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
                gap_fill: gap_fill.clone(),
                computed_columns: computed_columns.clone(),
                rls_filters: rls_filters.clone(),
                system_time: ctx.system_time(*system_time)?,
                valid_at_ms: *valid_at_ms,
                having: having.clone(),
                tiering: tiering.clone(),
                output_columns: output_columns.clone(),
            },
        ))),

        _ => refuse_or_skip(plan, ctx),
    }
}
