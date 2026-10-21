// SPDX-License-Identifier: BUSL-1.1

//! Lower the source SELECT of an orchestrated write (`INSERT ... SELECT`,
//! `MERGE`, `UPDATE ... FROM`) to the relation the Control Plane reads.

use nodedb_sql::types::{EngineType, SqlExpr, SqlPlan, SqlValue};
use nodedb_types::SystemTimeScope;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::{DocumentOp, DocumentPageSource, QuerySource, RowSource};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::body::convert_body_to_single_plan;
use super::convert::ConvertContext;

/// How the copied rows map onto the target's columns.
#[derive(Clone, Copy)]
pub(super) struct ColumnMaps<'a> {
    /// `(target_column, expression)` over a stored source row, for a source
    /// read as stored rows. Empty passes each row through.
    pub stored: &'a [(String, SqlExpr)],
    /// `(target_column, output_column)` over a SELECT output row, for a
    /// source read as a query. Empty passes each row through.
    pub output: &'a [(String, String)],
}

impl ColumnMaps<'_> {
    /// No mapping: every source row passes through unchanged.
    pub(super) const PASSTHROUGH: ColumnMaps<'static> = ColumnMaps {
        stored: &[],
        output: &[],
    };
}

/// Lower a source SELECT to the relation the Control Plane reads.
///
/// A plain document scan (schemaless or strict, no ordering, DISTINCT,
/// window, or valid-time scope, and not `AS OF SYSTEM TIME NULL`) reads as
/// [`RowSource::DocumentPages`]: a stable paged scan whose WHERE, offset,
/// limit, and stored-row column map the Control Plane applies per page.
/// Every other SELECT reads as [`RowSource::Query`]: the normal converter
/// lowers it, so its engine, ordering, distinct, windows, joins, `AS OF`,
/// and read policies follow the normal pipeline.
///
/// `paged_writer` names the collection a paged copy writes into while it
/// still reads. A scan of that same collection reads as a query instead, so
/// the whole source is read before the first row lands and the copy never
/// reads its own output.
pub(super) fn lower_row_source(
    source: &SqlPlan,
    maps: ColumnMaps<'_>,
    paged_writer: Option<&str>,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<RowSource> {
    if let SqlPlan::Scan {
        collection,
        engine: EngineType::DocumentSchemaless | EngineType::DocumentStrict,
        filters,
        sort_keys,
        limit,
        offset,
        distinct,
        window_functions,
        temporal,
        ..
    } = source
        && paged_writer != Some(collection.as_str())
        && sort_keys.is_empty()
        && !*distinct
        && window_functions.is_empty()
        && matches!(temporal.valid_time, nodedb_sql::ValidTime::Any)
    {
        let system_time = match temporal.system_time {
            scope @ (SystemTimeScope::Current
            | SystemTimeScope::AsOf(_)
            | SystemTimeScope::Pinned { .. }) => Some(scope),
            SystemTimeScope::AllVersions => None,
        };
        if let Some(system_time) = system_time {
            return Ok(RowSource::DocumentPages(DocumentPageSource {
                collection: nodedb_types::QualifiedCollection::new(ctx.database_id, collection),
                filters: super::filter::serialize_filters(filters)?,
                system_time,
                offset: *offset,
                limit: *limit,
                column_map: super::aggregate::serialize_column_map(maps.stored)?,
            }));
        }
    }
    // A sequence accessor in the SELECT list is evaluated by the session's
    // response stage, which a source read does not pass through. Refuse it
    // rather than write NULL under its column.
    if let SqlPlan::Scan { projection, .. } = source
        && nodedb_sql::types::plan::projection_is_cp_computed(projection)
    {
        return Err(crate::Error::PlanError {
            detail: "a sequence function in the SELECT list of a write's source is not \
                     supported; call it in the VALUES of a separate INSERT"
                .into(),
        });
    }
    let output_map: Vec<(String, SqlExpr)> = maps
        .output
        .iter()
        .map(|(target, output)| {
            (
                target.clone(),
                SqlExpr::Column {
                    table: None,
                    name: output.clone(),
                },
            )
        })
        .collect();
    Ok(RowSource::Query(QuerySource {
        plan: Box::new(convert_body_to_single_plan(source, tenant_id, ctx)?),
        column_map: super::aggregate::serialize_column_map(&output_map)?,
    }))
}

/// Lower `SqlPlan::InsertSelect` to its `DocumentOp::InsertSelect` task.
///
/// `column_map` binds each target column over a stored source row, and
/// `source_columns` pairs each target column with a SELECT output column.
/// The source relation decides which one shapes the rows.
pub(super) fn convert_insert_select(
    target: &str,
    source: &SqlPlan,
    maps: ColumnMaps<'_>,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let bare_target = target;
    let target_key = nodedb_types::CollectionKey::from_bare(ctx.database_id, target);
    let target_qualified = super::convert::db_qualified(ctx.database_id, target);
    let qualified_target = nodedb_types::QualifiedCollection::new(ctx.database_id, target);
    let target = target_qualified.as_str();

    // A declared PRIMARY KEY implies NOT NULL. A literal `SELECT NULL` into
    // the pk column is knowable at plan time, before any row is scanned.
    if let Some(declared) = super::dml::declared_primary_key_name(ctx, target)?
        && maps.stored.iter().any(|(field, expr)| {
            field == &declared && matches!(expr, SqlExpr::Literal(SqlValue::Null))
        })
    {
        return Err(crate::Error::RejectedConstraint {
            collection: target.to_string(),
            constraint: "not_null".to_string(),
            detail: format!("primary key '{declared}' cannot be set to NULL"),
        });
    }

    // A target column the engine computes on every write cannot take a
    // selected value.
    let gates = super::dml::document_collection_write_gates(ctx, target)?;
    crate::control::security::catalog::collection_generated::refuse_generated_write(
        target,
        &gates.generated,
        maps.output.iter().map(|(field, _)| field.as_str()),
    )?;

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: target_key.vshard(),
        database_id: ctx.database_id,
        plan: PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection: qualified_target,
            source: lower_row_source(source, maps, Some(bare_target), tenant_id, ctx)?,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_sql::types::{Filter, FilterExpr, SortKey};

    fn ctx() -> ConvertContext {
        ConvertContext {
            purpose: crate::control::planner::sql_plan_convert::PlanningPurpose::Execute,
            retention_registry: None,
            array_catalog: None,
            credentials: None,
            wal: None,
            surrogate_assigner:
                crate::control::planner::sql_plan_convert::test_support::test_assigner(),
            cluster_enabled: false,
            bitemporal_retention_registry: None,
            max_vector_dim: 0,
            force_shuffle_join: false,
            shuffle_num_parts: 0,
            force_shuffle_agg: false,
            shuffle_agg_num_parts: 0,
            broadcast_threshold_bytes: 8 * 1024 * 1024,
            shuffle_agg_threshold: 10_000,
            prefetched: Default::default(),
            database_id: crate::types::DatabaseId::DEFAULT,
            tenant_id: crate::types::TenantId::new(0),
        }
    }

    fn scan(engine: EngineType, limit: Option<usize>, sort: bool) -> SqlPlan {
        SqlPlan::Scan {
            collection: "src".into(),
            alias: None,
            engine,
            filters: vec![Filter {
                expr: FilterExpr::Comparison {
                    field: "g".into(),
                    op: nodedb_sql::types::CompareOp::Eq,
                    value: SqlValue::Int(1),
                },
            }],
            projection: Vec::new(),
            sort_keys: if sort {
                vec![SortKey {
                    expr: SqlExpr::Column {
                        table: None,
                        name: "score".into(),
                    },
                    ascending: false,
                    nulls_first: false,
                }]
            } else {
                Vec::new()
            },
            limit,
            offset: 2,
            distinct: false,
            window_functions: Vec::new(),
            temporal: nodedb_sql::TemporalScope::default(),
        }
    }

    fn source_of(tasks: &[PhysicalTask]) -> &RowSource {
        match &tasks[0].plan {
            PhysicalPlan::Document(DocumentOp::InsertSelect { source, .. }) => source,
            other => panic!("expected DocumentOp::InsertSelect, got {other:?}"),
        }
    }

    /// A plain document scan reads page by page, with no row cap: no LIMIT
    /// stays `None`. The scan used to stop at 10,000 rows.
    #[test]
    fn plain_document_scan_lowers_to_document_pages() {
        let source = scan(EngineType::DocumentSchemaless, None, false);
        let tasks = convert_insert_select(
            "dst",
            &source,
            ColumnMaps::PASSTHROUGH,
            TenantId::new(1),
            &ctx(),
        )
        .expect("converts");
        let RowSource::DocumentPages(page) = source_of(&tasks) else {
            panic!("expected a page source");
        };
        assert_eq!(page.collection.as_str(), "src");
        assert_eq!(page.limit, None);
        assert_eq!(page.offset, 2);
        assert!(!page.filters.is_empty(), "the WHERE clause travels");
    }

    #[test]
    fn document_scan_limit_travels_to_the_page_source() {
        let source = scan(EngineType::DocumentStrict, Some(50), false);
        let tasks = convert_insert_select(
            "dst",
            &source,
            ColumnMaps::PASSTHROUGH,
            TenantId::new(1),
            &ctx(),
        )
        .expect("converts");
        let RowSource::DocumentPages(page) = source_of(&tasks) else {
            panic!("expected a page source");
        };
        assert_eq!(page.limit, Some(50));
    }

    /// An ordered scan needs the whole relation in order, so it reads as a
    /// query. It used to be refused.
    #[test]
    fn sorted_document_scan_lowers_to_a_query_source() {
        let source = scan(EngineType::DocumentSchemaless, Some(2), true);
        let tasks = convert_insert_select(
            "dst",
            &source,
            ColumnMaps::PASSTHROUGH,
            TenantId::new(1),
            &ctx(),
        )
        .expect("converts");
        assert!(matches!(source_of(&tasks), RowSource::Query(_)));
    }

    /// A key-value source reads as a query through its own engine. It used
    /// to be read as a document scan and copied no rows.
    #[test]
    fn kv_scan_lowers_to_a_query_source() {
        let source = scan(EngineType::KeyValue, None, false);
        let output = [("v".to_string(), "__nodedb_insert_select_0".to_string())];
        let tasks = convert_insert_select(
            "dst",
            &source,
            ColumnMaps {
                stored: &[],
                output: &output,
            },
            TenantId::new(1),
            &ctx(),
        )
        .expect("converts");
        let RowSource::Query(query) = source_of(&tasks) else {
            panic!("expected a query source");
        };
        assert!(
            !query.column_map.is_empty(),
            "the output columns are renamed to the target columns"
        );
        assert!(
            matches!(query.plan.collection(), Some("src")),
            "{:?}",
            query.plan
        );
    }

    /// `AS OF SYSTEM TIME NULL` reads every version, which a page scan does
    /// not return, so it reads as a query.
    #[test]
    fn all_versions_scan_lowers_to_a_query_source() {
        let mut source = scan(EngineType::DocumentSchemaless, None, false);
        if let SqlPlan::Scan { temporal, .. } = &mut source {
            temporal.system_time = SystemTimeScope::AllVersions;
        }
        let tasks = convert_insert_select(
            "dst",
            &source,
            ColumnMaps::PASSTHROUGH,
            TenantId::new(1),
            &ctx(),
        )
        .expect("converts");
        assert!(matches!(source_of(&tasks), RowSource::Query(_)));
    }

    /// A copy into its own source reads the whole source before writing, so
    /// the paged read never sees the rows the copy inserts.
    #[test]
    fn self_copy_lowers_to_a_query_source() {
        let source = scan(EngineType::DocumentSchemaless, None, false);
        let tasks = convert_insert_select(
            "src",
            &source,
            ColumnMaps::PASSTHROUGH,
            TenantId::new(1),
            &ctx(),
        )
        .expect("converts");
        assert!(matches!(source_of(&tasks), RowSource::Query(_)));
    }

    #[test]
    fn as_of_scan_carries_its_cut_to_the_page_source() {
        let mut source = scan(EngineType::DocumentSchemaless, None, false);
        if let SqlPlan::Scan { temporal, .. } = &mut source {
            temporal.system_time = SystemTimeScope::AsOf(1_700_000_000_000);
        }
        let tasks = convert_insert_select(
            "dst",
            &source,
            ColumnMaps::PASSTHROUGH,
            TenantId::new(1),
            &ctx(),
        )
        .expect("converts");
        let RowSource::DocumentPages(page) = source_of(&tasks) else {
            panic!("expected a page source");
        };
        assert_eq!(page.system_time, SystemTimeScope::AsOf(1_700_000_000_000));
    }
}
