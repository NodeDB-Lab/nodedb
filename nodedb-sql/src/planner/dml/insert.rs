// SPDX-License-Identifier: Apache-2.0

//! INSERT planning.

use sqlparser::ast;

use super::super::dml_helpers::{
    KvInsertParams, VectorPrimaryInsertParams, bind_insert_select_columns, build_kv_insert_plan,
    build_vector_primary_insert_plan, is_vector_primary, resolve_insert_columns,
};
use super::target::{
    OnConflict, classify_on_conflict, column_schema, insert_columns, resolve_target, target_scope,
    typed_rows, values_rows,
};
use super::upsert::plan_upsert_with_on_conflict;
use crate::engine_rules::{self, InsertParams};
use crate::error::Result;
use crate::types::*;

/// Plan an INSERT statement.
pub fn plan_insert(
    ins: &ast::Insert,
    catalog: &dyn SqlCatalog,
    functions: &crate::functions::registry::FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<Vec<SqlPlan>> {
    let (table_name, info) = resolve_target(ins, "INSERT", catalog)?;
    let target_scope = target_scope(&table_name, &info)?;

    // `INSERT ... ON CONFLICT DO UPDATE SET` reroutes to the upsert path
    // with the assignments carried through. `DO NOTHING` stays on the
    // INSERT path with `if_absent=true`.
    let if_absent = match classify_on_conflict(ins, &target_scope)? {
        OnConflict::None => false,
        OnConflict::DoNothing => true,
        OnConflict::DoUpdate(updates) => {
            return plan_upsert_with_on_conflict(ins, catalog, updates);
        }
    };

    let columns = insert_columns(&ins.columns, &target_scope)?;

    // Check for INSERT...SELECT.
    if let Some(source) = &ins.source
        && let ast::SetExpr::Select(select) = &*source.body
    {
        let column_map =
            bind_insert_select_columns(catalog, functions, temporal, &columns, select, &info)?;
        let source_plan = super::super::select::plan_query(source, catalog, functions, temporal)?;
        return Ok(vec![SqlPlan::InsertSelect {
            target: table_name,
            source: Box::new(source_plan),
            limit: 0,
            column_map,
        }]);
    }

    let rows_ast = values_rows(ins, "INSERT")?;

    // KV engine: key and value are fundamentally separate — handle directly.
    // Positional column binding (below) does not apply here: the KV path
    // matches columns by name against `pk_col`/`"key"`/`"ttl"`, which is
    // orthogonal to declared column order.
    if info.engine == EngineType::KeyValue {
        let intent = if if_absent {
            KvInsertIntent::InsertIfAbsent
        } else {
            KvInsertIntent::Insert
        };
        return build_kv_insert_plan(KvInsertParams {
            collection: table_name,
            columns: &columns,
            rows_ast,
            intent,
            on_conflict_updates: Vec::new(),
            pk_col: info.primary_key.as_deref(),
            declared_columns: &info.columns,
            catalog,
        });
    }

    // Positional INSERT (no column list): bind values to the collection's
    // declared column order so named projections/predicates can find them.
    // No-op for named inserts and schemaless collections.
    let columns = resolve_insert_columns(columns, &info, rows_ast)?;

    // One typing pass for every remaining engine: defaults materialized,
    // then every cell coerced and range-checked against its declared type.
    let typed = typed_rows(&info, &columns, rows_ast, catalog)?;

    // Vector-primary collection: bypass document encoding. The row's
    // existence intent travels with the plan, as it does for KV.
    if is_vector_primary(&info)
        && let Some(ref vpc) = info.vector_primary
    {
        let intent = if if_absent {
            VectorPrimaryInsertIntent::InsertIfAbsent
        } else {
            VectorPrimaryInsertIntent::Insert
        };
        return build_vector_primary_insert_plan(VectorPrimaryInsertParams {
            collection: &table_name,
            vpc,
            rows: typed.rows,
            volatile_defaults: typed.volatile_defaults,
            intent,
            on_conflict_updates: Vec::new(),
            primary_key: info.primary_key.clone(),
        });
    }

    // All other engines: delegate to engine rules.
    let column_schema = column_schema(&info);
    let rules = engine_rules::resolve_engine_rules(info.engine);
    rules.plan_insert(InsertParams {
        collection: table_name,
        columns,
        rows: typed.rows,
        volatile_defaults: typed.volatile_defaults,
        if_absent,
        column_schema,
        primary_key: info.primary_key.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{SqlCatalog, SqlCatalogError};
    use crate::parser::statement::parse_sql;
    use nodedb_types::columnar::IntWidth;
    use nodedb_types::datetime::NdbDateTime;

    fn column(name: &str, data_type: SqlDataType, default: Option<&str>) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: true,
            is_primary_key: false,
            default: default.map(str::to_string),
            raw_type: None,
            int_width: None,
            float_width: None,
        }
    }

    fn collection(engine: EngineType, columns: Vec<ColumnInfo>) -> CollectionInfo {
        let primary_key = columns
            .iter()
            .find(|c| c.is_primary_key)
            .map(|c| c.name.clone());
        CollectionInfo {
            name: "t".into(),
            engine,
            columns,
            primary_key,
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: CollectionInfo::open_schema_for(engine),
        }
    }

    /// A catalog with one collection named `t`.
    struct OneCollection(CollectionInfo);

    impl SqlCatalog for OneCollection {
        fn get_collection(
            &self,
            _: nodedb_types::DatabaseId,
            name: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            Ok((name == "t").then(|| self.0.clone()))
        }
    }

    fn plan(sql: &str, catalog: &dyn SqlCatalog) -> Result<SqlPlan> {
        let statements = parse_sql(sql)?;
        let sqlparser::ast::Statement::Insert(ins) = &statements[0] else {
            panic!("expected an INSERT statement");
        };
        let mut plans = plan_insert(
            ins,
            catalog,
            &crate::functions::registry::FunctionRegistry::new(),
            crate::TemporalScope::default(),
        )?;
        Ok(plans.remove(0))
    }

    /// `2020-03-05T10:00:00Z` as microseconds since the Unix epoch.
    const EARLY_MICROS: i64 = 1_583_402_400_000_000;

    fn cell<'a>(row: &'a [(String, SqlValue)], name: &str) -> &'a SqlValue {
        &row.iter()
            .find(|(n, _)| n == name)
            .unwrap_or_else(|| panic!("row carries {name}"))
            .1
    }

    /// A numeric DEFAULT on a TIMESTAMP column is materialized, then coerced
    /// like a supplied literal: the plan carries the typed instant.
    #[test]
    fn a_numeric_timestamp_default_is_materialized_then_coerced() {
        let mut id = column("id", SqlDataType::String, None);
        id.is_primary_key = true;
        let catalog = OneCollection(collection(
            EngineType::DocumentSchemaless,
            vec![
                id,
                column("at", SqlDataType::Timestamp, Some("1583402400000")),
            ],
        ));
        let plan = plan("INSERT INTO t (id) VALUES ('r1')", &catalog).expect("plans");
        let SqlPlan::Insert {
            rows,
            volatile_defaults,
            ..
        } = plan
        else {
            panic!("expected SqlPlan::Insert, got {plan:?}");
        };
        assert!(!volatile_defaults, "a literal DEFAULT is not volatile");
        assert_eq!(
            cell(&rows[0], "at"),
            &SqlValue::Timestamp(NdbDateTime::from_micros(EARLY_MICROS))
        );
    }

    /// A DEFAULT the column cannot hold is refused at the insert, naming the
    /// column, exactly as the same literal in VALUES is.
    #[test]
    fn a_default_out_of_declared_range_is_refused() {
        let mut id = column("id", SqlDataType::String, None);
        id.is_primary_key = true;
        let mut small = column("s", SqlDataType::Int64, Some("999999"));
        small.int_width = Some(IntWidth::I16);
        let catalog = OneCollection(collection(EngineType::DocumentStrict, vec![id, small]));
        let err = plan("INSERT INTO t (id) VALUES ('r1')", &catalog)
            .expect_err("DEFAULT 999999 does not fit SMALLINT");
        assert!(
            matches!(err, crate::SqlError::IntegerOutOfRange { ref column, .. } if column == "s"),
            "{err}"
        );
    }

    /// A timeseries plan carries the declared default of an omitted column
    /// in its rows, the TIME_KEY column included.
    #[test]
    fn a_timeseries_plan_carries_defaults_in_its_rows() {
        let catalog = OneCollection(collection(
            EngineType::Timeseries,
            vec![
                column("at", SqlDataType::Timestamp, Some("1583402400000")),
                column("host", SqlDataType::String, Some("'h0'")),
                column("v", SqlDataType::Float64, None),
            ],
        ));
        let plan = plan("INSERT INTO t (v) VALUES (1.0)", &catalog).expect("plans");
        let SqlPlan::TimeseriesIngest {
            rows,
            volatile_defaults,
            ..
        } = plan
        else {
            panic!("expected SqlPlan::TimeseriesIngest, got {plan:?}");
        };
        assert!(!volatile_defaults);
        assert_eq!(
            cell(&rows[0], "at"),
            &SqlValue::Timestamp(NdbDateTime::from_micros(EARLY_MICROS))
        );
        assert_eq!(cell(&rows[0], "host"), &SqlValue::String("h0".into()));
        assert_eq!(cell(&rows[0], "v"), &SqlValue::Float(1.0));
    }

    /// A volatile DEFAULT marks the plan so the plan cache never replays it.
    #[test]
    fn a_volatile_default_marks_the_plan() {
        let mut id = column("id", SqlDataType::String, Some("UUID_V7"));
        id.is_primary_key = true;
        let catalog = OneCollection(collection(
            EngineType::DocumentStrict,
            vec![id, column("n", SqlDataType::Int64, None)],
        ));
        let plan = plan("INSERT INTO t (n) VALUES (1)", &catalog).expect("plans");
        assert!(
            !plan.cache_eligibility().is_cacheable(),
            "a plan carrying a volatile DEFAULT is never cached"
        );
    }
}
