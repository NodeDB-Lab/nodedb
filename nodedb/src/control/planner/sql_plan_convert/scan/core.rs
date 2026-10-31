// SPDX-License-Identifier: BUSL-1.1

//! Generic scan converters: row scan, secondary-index lookup, point get.

use nodedb_sql::types::{EngineType, Filter, SqlValue};
use nodedb_types::SystemTimeScope;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::*;

use super::super::aggregate::{
    extract_computed_columns, extract_projection_names, serialize_window_functions,
};
use super::super::expr::convert_sort_keys;
use super::super::filter::serialize_filters;
use super::super::scan_params::ScanParams;
use super::super::value::{sql_value_to_bytes, sql_value_to_nodedb_value, sql_value_to_string};
use crate::control::planner::sql_plan_convert::valid_time_point;
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
    // collection string) keeps the task coordinator-local. A continuous
    // aggregate reads the same way: the coordinator fetches its finalized
    // rows from the core that owns its root source.
    let computed_bytes = extract_computed_columns(projection, window_functions, false)?;
    let window_bytes = serialize_window_functions(window_functions, false)?;

    if is_provider_relation(collection, engine) {
        let filter_bytes = serialize_filters(filters)?;
        // The provider tail evaluates computed columns before it prunes, so
        // the pruning list keeps each computed alias the output schema reads.
        let proj_names =
            super::super::set_ops::lower_subquery_projection(projection, window_functions)?;
        let sort = convert_sort_keys(sort_keys);
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: nodedb_types::CollectionKey::from_bare(database_id, "").vshard(),
            database_id,
            plan: PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: Some(collection.to_string()),
                rows: Vec::new(),
                sources: Vec::new(),
                filters: filter_bytes,
                projection: proj_names,
                computed_columns: computed_bytes,
                window_functions: window_bytes,
                sort_keys: sort,
                limit: *limit,
                offset: *offset,
                distinct: *distinct,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    let collection_key = nodedb_types::CollectionKey::from_bare(database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(database_id, collection);
    let filter_bytes = serialize_filters(filters)?;
    let proj_names = extract_projection_names(projection, window_functions);
    let sort = convert_sort_keys(sort_keys);
    let vshard = collection_key.vshard();

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
                system_time: temporal.system_time,
                valid_at_ms: valid_time_point(temporal.valid_time, "timeseries")?,
                having: Vec::new(),
                tiering: None,
                output_columns: Vec::new(),
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
            valid_at_ms: valid_time_point(temporal.valid_time, "columnar")?,
            prefilter: None,
            computed_columns: computed_bytes.clone(),
        }),
        // A spatial collection stores its rows in the columnar engine, so its
        // scan is the columnar scan: no LIMIT reads every row under the byte
        // budget. The planner refuses `AS OF` on a spatial collection, so the
        // scope carried here is always the current one.
        EngineType::Spatial => PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection: qualified_collection.clone(),
            projection: proj_names,
            limit: limit.unwrap_or(usize::MAX),
            filters: filter_bytes,
            rls_filters: Vec::new(),
            sort_keys: sort.clone(),
            system_time: temporal.system_time,
            valid_at_ms: valid_time_point(temporal.valid_time, "spatial")?,
            prefilter: None,
            computed_columns: computed_bytes.clone(),
        }),
        EngineType::KeyValue => PhysicalPlan::Kv(KvOp::Scan {
            collection: qualified_collection.clone(),
            cursor: Vec::new(),
            count: limit.unwrap_or(usize::MAX),
            filters: filter_bytes,
            // A kv scan emits the full row: the clone-source merge keys
            // tombstone suppression on the row key, so the response shaper
            // projects by output schema instead. Computed columns still
            // evaluate per row on the Data Plane.
            projection: Vec::new(),
            computed_columns: computed_bytes,
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
                // truncating. Every other engine's scan takes the same
                // budget-bounded `usize::MAX` path.
                limit: limit.unwrap_or(usize::MAX),
                offset: *offset,
                sort_keys: sort,
                filters: filter_bytes,
                distinct: *distinct,
                projection: proj_names,
                computed_columns: computed_bytes,
                window_functions: window_bytes,
                system_time: temporal.system_time,
                valid_at_ms: valid_time_point(temporal.valid_time, "document")?,
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
        // `is_provider_relation` sends every continuous aggregate to the
        // provider scan above.
        EngineType::ContinuousAggregate => {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "scan on '{collection}': a continuous aggregate reads through its provider"
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

/// Whether a read of `collection` is a provider scan the coordinator
/// materializes: a catalog table, or a continuous aggregate.
pub(in crate::control::planner::sql_plan_convert) fn is_provider_relation(
    collection: &str,
    engine: &EngineType,
) -> bool {
    *engine == EngineType::ContinuousAggregate
        || crate::control::server::pgwire::catalog::schema::catalog_collection_info(collection)
            .is_some()
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
    pub sort_keys: &'a [nodedb_sql::types::SortKey],
    pub limit: Option<usize>,
    pub offset: usize,
    pub tenant_id: TenantId,
    pub database_id: crate::types::DatabaseId,
    /// The read's time scope. Only the current state and a system-time
    /// cutoff plan an index lookup.
    pub temporal: &'a nodedb_sql::TemporalScope,
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
        sort_keys,
        limit,
        offset,
        tenant_id,
        database_id,
        temporal,
    } = args;
    // The versioned index answers at a system-time cutoff. It holds no valid
    // time and lists no history, so the planner never routes those here.
    if !matches!(temporal.valid_time, nodedb_sql::ValidTime::Any) {
        return Err(crate::Error::PlanError {
            detail: format!("index lookup on '{collection}' cannot answer a valid-time read"),
        });
    }
    let system_time = match temporal.system_time {
        scope @ (SystemTimeScope::Current
        | SystemTimeScope::AsOf(_)
        | SystemTimeScope::Pinned { .. }) => scope,
        SystemTimeScope::AllVersions => {
            return Err(crate::Error::PlanError {
                detail: format!("index lookup on '{collection}' cannot list every version"),
            });
        }
    };
    let collection_key = nodedb_types::CollectionKey::from_bare(database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(database_id, collection);
    let filter_bytes = serialize_filters(filters)?;
    let proj_names = extract_projection_names(projection, &[]);
    let vshard = collection_key.vshard();
    let physical = PhysicalPlan::Document(DocumentOp::IndexedFetch {
        collection: qualified_collection,
        path: field.into(),
        value: sql_value_to_string(value),
        filters: filter_bytes,
        projection: proj_names,
        // No LIMIT fetches every matching row under the byte budget.
        limit: limit.unwrap_or(usize::MAX),
        offset,
        sort_keys: convert_sort_keys(sort_keys),
        system_time,
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
    let collection_key = nodedb_types::CollectionKey::from_bare(ctx.database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let vshard = collection_key.vshard();
    let physical = match engine {
        EngineType::KeyValue => PhysicalPlan::Kv(KvOp::Get {
            collection: qualified_collection.clone(),
            key: sql_value_to_bytes(key_value)?,
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        }),
        EngineType::DocumentSchemaless | EngineType::DocumentStrict => {
            let pk_string = sql_value_to_string(key_value);
            let pk_bytes = pk_string.clone().into_bytes();
            // An unbound key carries `None`: it matches no row in this
            // database, so the Data Plane returns empty. The clone CoW
            // resolver still sees the task and fetches the row from the
            // source database.
            let surrogate = ctx.surrogate_for_existing_pk(collection_key, &pk_bytes)?;
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
        // Timeseries must never reach here — nodedb-sql rejects point gets.
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
        // nodedb-sql never rewrites a continuous-aggregate read into a
        // point lookup.
        EngineType::ContinuousAggregate => {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "point get on '{collection}': a continuous aggregate has no primary key"
                ),
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
