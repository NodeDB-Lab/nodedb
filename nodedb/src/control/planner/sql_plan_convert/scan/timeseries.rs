// SPDX-License-Identifier: BUSL-1.1

//! Timeseries scan and ingest converters.

use nodedb_sql::types::SqlValue;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::*;

use super::super::aggregate::{
    agg_expr_to_pair, extract_computed_columns, extract_projection_names,
};
use super::super::expr::convert_sort_keys;
use super::super::filter::serialize_filters;
use super::super::scan_params::TimeseriesScanParams;
use super::super::value::{row_to_msgpack, write_msgpack_array_header};
use super::helpers::valid_at_from_scope;
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
        projection,
        gap_fill,
        limit,
        sort_keys,
        tiered,
        tenant_id,
        ctx,
        temporal,
    } = p;
    let coll_qualified = super::super::convert::db_qualified(ctx.database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let filter_bytes = serialize_filters(filters)?;
    let agg_pairs: Vec<(String, String)> = aggregates.iter().map(agg_expr_to_pair).collect();

    // AUTO_TIER: split query across retention tiers if enabled.
    if *tiered
        && let Some(registry) = &ctx.retention_registry
        && let Some(policy) = registry.get(ctx.database_id.as_u64(), tenant_id.as_u64(), collection)
        && policy.auto_tier
    {
        return Ok(super::super::super::auto_tier::plan_tiered_scan(
            &policy,
            super::super::super::auto_tier::ScopeIds {
                tenant_id,
                database_id: ctx.database_id,
            },
            *time_range,
            filter_bytes,
            group_by.to_vec(),
            agg_pairs,
            gap_fill.to_string(),
        ));
    }

    let proj_names = extract_projection_names(projection, &[]);
    let computed_bytes = extract_computed_columns(projection, &[])?;
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
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
            valid_at_ms: valid_at_from_scope(temporal),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

pub(in crate::control::planner::sql_plan_convert) fn convert_timeseries_ingest(
    collection: &str,
    rows: &[Vec<(String, SqlValue)>],
    tenant_id: TenantId,
    ctx: &super::super::convert::ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = super::super::convert::db_qualified(ctx.database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    let mut payload = Vec::with_capacity(rows.len() * 128);
    write_msgpack_array_header(&mut payload, rows.len());
    let mut surrogates: Vec<nodedb_types::Surrogate> = Vec::with_capacity(rows.len());
    for row in rows {
        let row_bytes = row_to_msgpack(row)?;
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
        let (s, _) = ctx.fresh_surrogate(
            collection,
            nodedb_physical::FreshSurrogateKind::DocumentStorageKey,
        )?;
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
