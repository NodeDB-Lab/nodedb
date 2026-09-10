// SPDX-License-Identifier: BUSL-1.1

//! Generic scan converters: row scan, secondary-index lookup, point get.

use nodedb_sql::types::{EngineType, Filter, SqlValue};
use nodedb_types::SystemTimeScope;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::*;

use super::super::aggregate::{
    extract_computed_columns, extract_projection_names, serialize_window_functions,
};
use super::super::expr::convert_sort_keys;
use super::super::filter::serialize_filters;
use super::super::scan_params::ScanParams;
use super::super::value::{sql_value_to_bytes, sql_value_to_nodedb_value, sql_value_to_string};
use super::helpers::valid_at_from_scope;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub(in crate::control::planner::sql_plan_convert) fn convert_scan(
    p: ScanParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let ScanParams {
        collection,
        engine,
        filters,
        projection,
        sort_keys,
        limit,
        offset,
        distinct,
        window_functions,
        tenant_id,
        temporal,
        database_id,
    } = p;

    // Catalog tables (pg_class, information_schema.*, _system.*, etc.) are
    // surfaced as `ProviderScan{Some(name)}` so the coordinator materializes
    // their rows per-request from the identity-scoped catalog producer.
    // `rows` is left empty here; the coordinator fills it post-cache via
    // `materialize_providers`. Using an empty-coordinator vshard (empty
    // collection string) keeps the task coordinator-local.
    if crate::control::server::pgwire::catalog::schema::catalog_collection_info(collection)
        .is_some()
    {
        let filter_bytes = serialize_filters(filters)?;
        let proj_names = extract_projection_names(projection, window_functions);
        let sort = convert_sort_keys(sort_keys);
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: VShardId::from_collection_in_database(database_id, ""),
            database_id,
            plan: PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: Some(collection.to_string()),
                rows: Vec::new(),
                filters: filter_bytes,
                projection: proj_names,
                computed_columns: Vec::new(),
                window_functions: Vec::new(),
                sort_keys: sort,
                limit: *limit,
                offset: *offset,
                distinct: *distinct,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    let coll_qualified = super::super::convert::db_qualified(database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(database_id, collection);
    let collection = coll_qualified.as_str();
    let filter_bytes = serialize_filters(filters)?;
    let proj_names = extract_projection_names(projection, window_functions);
    let sort = convert_sort_keys(sort_keys);
    let vshard = VShardId::from_collection_in_database(database_id, collection);
    let computed_bytes = extract_computed_columns(projection, window_functions)?;
    let window_bytes = serialize_window_functions(window_functions)?;

    let physical = match engine {
        EngineType::Timeseries => {
            // The time range is derived in the Data Plane, from the query's
            // bounds on the collection's declared TIME_KEY column. Only the
            // core holding the collection knows which column that is.
            PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection: qualified_collection.clone(),
                time_range: UNBOUNDED_TIME_RANGE,
                sort_keys: sort.clone(),
                projection: proj_names,
                limit: limit.unwrap_or(usize::MAX),
                filters: filter_bytes,
                bucket_interval_ms: 0,
                group_by: Vec::new(),
                aggregates: Vec::new(),
                gap_fill: String::new(),
                computed_columns: computed_bytes,
                rls_filters: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
            })
        }
        EngineType::Columnar => PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection: qualified_collection.clone(),
            projection: proj_names,
            limit: limit.unwrap_or(usize::MAX),
            filters: filter_bytes,
            rls_filters: Vec::new(),
            sort_keys: sort.clone(),
            system_time: temporal.system_time,
            valid_at_ms: valid_at_from_scope(temporal),
            prefilter: None,
            computed_columns: computed_bytes.clone(),
        }),
        EngineType::Spatial => PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection: qualified_collection.clone(),
            projection: proj_names,
            limit: limit.unwrap_or(10000),
            filters: filter_bytes,
            rls_filters: Vec::new(),
            sort_keys: sort.clone(),
            system_time: SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
            computed_columns: computed_bytes.clone(),
        }),
        EngineType::KeyValue => PhysicalPlan::Kv(KvOp::Scan {
            collection: qualified_collection.clone(),
            cursor: Vec::new(),
            count: limit.unwrap_or(usize::MAX),
            filters: filter_bytes,
            match_pattern: None,
            sort_keys: sort.clone(),
            // Original SQL planner output never carries a clone ceiling;
            // the clone resolver overrides it when delegating to source.
            surrogate_ceiling: None,
        }),
        EngineType::DocumentSchemaless | EngineType::DocumentStrict => {
            PhysicalPlan::Document(DocumentOp::Scan {
                collection: qualified_collection,
                // A no-LIMIT document scan is unbounded by row count; the Data
                // Plane bounds it by a memory budget (`max_scan_result_bytes`)
                // and surfaces a deterministic error rather than silently
                // truncating. KV, columnar, and timeseries now take the same
                // budget-bounded `usize::MAX` path (their handlers enforce the
                // memory budget). Spatial alone still caps at 10k — its R-tree
                // scan path is not yet budget-aware (out of scope here).
                limit: limit.unwrap_or(usize::MAX),
                offset: *offset,
                sort_keys: sort,
                filters: filter_bytes,
                distinct: *distinct,
                projection: proj_names,
                computed_columns: computed_bytes,
                window_functions: window_bytes,
                system_time: temporal.system_time,
                valid_at_ms: valid_at_from_scope(temporal),
                prefilter: None,
            })
        }
        EngineType::Array => {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "scan on '{collection}': array engine has no table-shaped scan; use ARRAY_SLICE"
                ),
            });
        }
    };
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id,
        plan: physical,
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

/// Map `SqlPlan::DocumentIndexLookup` to a `DocumentOp::IndexedFetch` task.
///
/// The handler resolves doc IDs through the sparse index, fetches each
/// document, applies any remaining filters + projection, and emits rows
/// in the same wire format as a document scan.
/// Bundled arguments for [`convert_document_index_lookup`].
pub(in crate::control::planner::sql_plan_convert) struct DocumentIndexLookupArgs<'a> {
    pub collection: &'a str,
    pub field: &'a str,
    pub value: &'a SqlValue,
    pub filters: &'a [Filter],
    pub projection: &'a [nodedb_sql::types::Projection],
    pub limit: Option<usize>,
    pub offset: usize,
    pub tenant_id: TenantId,
    pub database_id: crate::types::DatabaseId,
}

pub(in crate::control::planner::sql_plan_convert) fn convert_document_index_lookup(
    args: DocumentIndexLookupArgs<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let DocumentIndexLookupArgs {
        collection,
        field,
        value,
        filters,
        projection,
        limit,
        offset,
        tenant_id,
        database_id,
    } = args;
    let coll_qualified = super::super::convert::db_qualified(database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(database_id, collection);
    let collection = coll_qualified.as_str();
    let filter_bytes = serialize_filters(filters)?;
    let proj_names = extract_projection_names(projection, &[]);
    let vshard = VShardId::from_collection_in_database(database_id, collection);
    let physical = PhysicalPlan::Document(DocumentOp::IndexedFetch {
        collection: qualified_collection,
        path: field.into(),
        value: sql_value_to_string(value),
        filters: filter_bytes,
        projection: proj_names,
        limit: limit.unwrap_or(10_000),
        offset,
    });
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id,
        plan: physical,
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

pub(in crate::control::planner::sql_plan_convert) fn convert_point_get(
    collection: &str,
    engine: &EngineType,
    key_column: &str,
    key_value: &SqlValue,
    tenant_id: TenantId,
    ctx: &super::super::convert::ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = super::super::convert::db_qualified(ctx.database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    let physical = match engine {
        EngineType::KeyValue => PhysicalPlan::Kv(KvOp::Get {
            collection: qualified_collection.clone(),
            key: sql_value_to_bytes(key_value),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        }),
        EngineType::DocumentSchemaless | EngineType::DocumentStrict => {
            let pk_string = sql_value_to_string(key_value);
            let pk_bytes = pk_string.clone().into_bytes();
            let surrogate = match ctx.surrogate_assigner.as_ref() {
                Some(a) => match a.lookup(ctx.database_id, ctx.tenant_id, collection, &pk_bytes)? {
                    Some(s) => s,
                    None => {
                        // No surrogate bound in the target database yet.
                        // Emit a sentinel task so the clone CoW resolver can
                        // intercept and fetch the row from the source database.
                        // For non-clone databases the Data Plane looks up
                        // the sentinel key, finds nothing, and returns empty
                        // — identical behaviour to the zero-tasks path.
                        nodedb_types::Surrogate::ZERO
                    }
                },
                None => nodedb_types::Surrogate::ZERO,
            };
            PhysicalPlan::Document(DocumentOp::PointGet {
                collection: qualified_collection.clone(),
                document_id: pk_string,
                surrogate,
                pk_bytes,
                rls_filters: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
            })
        }
        // Columnar point get: emit a ColumnarOp::Scan with an `Eq` filter
        // on the PK column and limit=1. Columnar collections have no
        // document store, so routing to `DocumentOp::PointGet` silently
        // returns zero rows.
        EngineType::Columnar | EngineType::Spatial => {
            use nodedb_query::scan_filter::{FilterOp, ScanFilter};
            let scan_filter = ScanFilter {
                field: key_column.to_string(),
                op: FilterOp::Eq,
                value: sql_value_to_nodedb_value(key_value),
                clauses: Vec::new(),
                expr: None,
            };
            let filter_bytes = zerompk::to_msgpack_vec(&vec![scan_filter]).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("columnar point-get filter: {e}"),
                }
            })?;
            PhysicalPlan::Columnar(ColumnarOp::Scan {
                collection: qualified_collection.clone(),
                projection: Vec::new(),
                limit: 1,
                filters: filter_bytes,
                rls_filters: Vec::new(),
                sort_keys: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
                prefilter: None,
                computed_columns: Vec::new(),
            })
        }
        // Timeseries should never reach here — nodedb-sql rejects point gets.
        EngineType::Timeseries => {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "point get on '{collection}': timeseries does not support point lookups"
                ),
            });
        }
        // Array reads do not have a key column.
        EngineType::Array => {
            return Err(crate::Error::PlanError {
                detail: format!("point get on '{collection}': array engine has no primary key"),
            });
        }
    };
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: physical,
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}
