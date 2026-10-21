// SPDX-License-Identifier: BUSL-1.1

//! Timeseries scan and ingest converters.

use nodedb_sql::types::SqlValue;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::*;

use super::super::aggregate::{
    agg_expr_to_pair, extract_computed_columns, extract_projection_names,
};
use super::super::expr::convert_sort_keys;
use super::super::filter::serialize_filters;
use super::super::scan_params::TimeseriesScanParams;
use super::super::value::{InstantForm, row_to_msgpack_with, write_msgpack_array_header};
use crate::control::planner::sql_plan_convert::valid_time_point;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub(in crate::control::planner::sql_plan_convert) fn convert_timeseries_scan(
    p: TimeseriesScanParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let TimeseriesScanParams {
        collection,
        time_range,
        bucket_interval_ms,
        group_by,
        aggregates,
        filters,
        having,
        projection,
        gap_fill,
        limit,
        sort_keys,
        tiered,
        tenant_id,
        ctx,
        temporal,
        output,
    } = p;
    // The scan writes each group row under the SELECT-list names, in order.
    let output_columns: Vec<(String, String)> = output
        .iter()
        .map(|column| {
            column
                .key_and_name(aggregates)
                .ok_or(crate::Error::Internal {
                    detail: format!(
                        "timeseries output column {column:?} names no aggregate of the scan"
                    ),
                })
        })
        .collect::<crate::Result<_>>()?;
    let collection_key = nodedb_types::CollectionKey::from_bare(ctx.database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let filter_bytes = serialize_filters(filters)?;
    let having_bytes = serialize_filters(having)?;
    let agg_pairs: Vec<(String, String)> = aggregates
        .iter()
        .map(agg_expr_to_pair)
        .collect::<crate::Result<_>>()?;

    // AUTO_TIER: the owning core routes each segment of the range to raw
    // rows or to a tier's buckets, then merges before HAVING, sort and limit.
    let tiering = match &ctx.retention_registry {
        // Policies are keyed by policy name. A collection's policy is found
        // by the bare collection name it targets.
        Some(registry) if *tiered => registry
            .get_for_collection(
                ctx.database_id.as_u64(),
                tenant_id.as_u64(),
                collection_key.name(),
            )
            .filter(|policy| policy.auto_tier)
            .map(|policy| super::super::super::auto_tier::tiering_plan(&policy)),
        Some(_) | None => None,
    };

    let proj_names = extract_projection_names(projection, &[]);
    let computed_bytes = timeseries_computed_columns(projection)?;
    let vshard = collection_key.vshard();
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: qualified_collection,
            time_range: *time_range,
            projection: proj_names,
            limit: *limit,
            filters: filter_bytes,
            sort_keys: convert_sort_keys(sort_keys),
            bucket_interval_ms: *bucket_interval_ms,
            group_by: group_by.to_vec(),
            aggregates: agg_pairs,
            gap_fill: gap_fill.to_string(),
            computed_columns: computed_bytes,
            rls_filters: Vec::new(),
            system_time: temporal.system_time,
            valid_at_ms: valid_time_point(temporal.valid_time, "timeseries")?,
            having: having_bytes,
            tiering,
            output_columns,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

/// The computed columns of a raw timeseries scan. A raw scan with computed
/// columns emits only those columns, so a plain column projected beside a
/// computed one (`SELECT code AS id, score`) rides along as an identity
/// column under its own name, in SELECT-list order. A projection with no
/// computed column, or with `*`, keeps every stored column as before.
fn timeseries_computed_columns(
    projection: &[nodedb_sql::types::Projection],
) -> crate::Result<Vec<u8>> {
    use nodedb_sql::types::Projection;
    let has_computed = projection
        .iter()
        .any(|p| matches!(p, Projection::Computed { .. }));
    let has_star = projection
        .iter()
        .any(|p| matches!(p, Projection::Star | Projection::QualifiedStar(_)));
    if !has_computed || has_star {
        return extract_computed_columns(projection, &[], false);
    }
    let columns: Vec<crate::bridge::expr_eval::ComputedColumn> = projection
        .iter()
        .filter_map(|p| match p {
            Projection::Column(name) => Some(crate::bridge::expr_eval::ComputedColumn {
                alias: name.clone(),
                expr: crate::bridge::expr_eval::SqlExpr::Column(name.clone()),
            }),
            Projection::Computed { expr, alias } => {
                Some(crate::bridge::expr_eval::ComputedColumn {
                    alias: alias.clone(),
                    expr: super::super::expr::sql_expr_to_bridge_expr(expr),
                })
            }
            Projection::CpComputed { .. } | Projection::Star | Projection::QualifiedStar(_) => None,
        })
        .collect();
    zerompk::to_msgpack_vec(&columns).map_err(|e| crate::Error::Internal {
        detail: format!("serialize computed columns: {e}"),
    })
}

pub(in crate::control::planner::sql_plan_convert) fn convert_timeseries_ingest(
    collection: &str,
    rows: &[Vec<(String, SqlValue)>],
    tenant_id: TenantId,
    ctx: &super::super::convert::ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let collection_key = nodedb_types::CollectionKey::from_bare(ctx.database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let vshard = collection_key.vshard();
    let mut payload = Vec::with_capacity(rows.len() * 128);
    write_msgpack_array_header(&mut payload, rows.len());
    let mut surrogates: Vec<nodedb_types::Surrogate> = Vec::with_capacity(rows.len());
    for row in rows {
        // A declared `TIMESTAMP` / `TIMESTAMPTZ` value travels as the typed
        // instant ext: the ingest decoder takes it on the same arm a restore
        // reissue's typed cell takes, so both spellings converge there.
        let row_bytes = row_to_msgpack_with(row, InstantForm::Ext)?;
        payload.extend_from_slice(&row_bytes);
        // A timeseries row's natural identity is its (timestamp, tag-set)
        // tuple, which is not a cross-engine surrogate and carries no PK
        // column. Mint a FRESH unique surrogate per row (mirroring the
        // document-storage-key path a keyless schemaless row takes in
        // `dml/insert.rs`), so every row holds its own transaction-overlay
        // slot for read-your-own-writes staging. Content-addressing an empty
        // PK collapses every row onto `Surrogate::ZERO` and merges distinct
        // rows. Nothing looks a timeseries row up by this binding, so the
        // identity string is discarded.
        let (s, _) = ctx.fresh_surrogate(collection_key)?;
        surrogates.push(s);
    }
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: qualified_collection,
            payload,
            format: "msgpack".into(),
            wal_lsn: None,
            surrogates,
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}
