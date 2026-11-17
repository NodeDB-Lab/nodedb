// SPDX-License-Identifier: BUSL-1.1

//! Per-side column types for a join's output schema.
//!
//! A join has no single source collection, so each projected column resolves
//! against the catalog of the side it came from. Two sides can carry the same
//! bare column name with different types, so the map is keyed on the
//! qualified name the executor emits (`orders.ts`). The bare name is added
//! too, and only while exactly one side claims it: an ambiguous bare name
//! stays `Text`, which every client parses.

use std::collections::HashMap;

use nodedb_sql::SqlError;
use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::{
    CtePlan, DocumentIndexLookupPlan, HybridSearchPlan, HybridSearchTriplePlan, RangeScanPlan,
    RecursiveScanPlan, SqlPlan, TextSearchPlan, TimeseriesScanPlan,
};

use crate::control::server::response_shape::types::DdlColType;

use super::columns::column_types_for;
use super::cte_types::{CatalogRef, CteTypesCatalog};

/// One join side: the relation it reads, the alias qualifying its columns
/// in the merged row, and the relation's column types. `None` means the
/// columns are qualified by the relation name.
struct JoinSide {
    collection: String,
    alias: Option<String>,
    types: HashMap<String, DdlColType>,
}

/// Column types of every side of a join, keyed by qualified name.
///
/// Each side contributes `<alias>.<column>` and `<collection>.<column>` — a
/// projection can spell either — plus the bare `<column>` when no other side
/// declares that name with a different type. A catalog lookup error is
/// returned.
pub(super) fn join_column_types<C: SqlCatalog + ?Sized>(
    left: &SqlPlan,
    right: &SqlPlan,
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
) -> Result<HashMap<String, DdlColType>, SqlError> {
    let stored = CatalogRef(catalog);
    let mut sides = Vec::new();
    collect_sides(left, &stored, database_id, &mut sides)?;
    collect_sides(right, &stored, database_id, &mut sides)?;

    let mut qualified: HashMap<String, DdlColType> = HashMap::new();
    // Bare names seen so far, and whether one side still owns the name. A
    // second side declaring the same name with a different type marks it
    // ambiguous, and an ambiguous bare name resolves to `Text`.
    let mut bare: HashMap<String, DdlColType> = HashMap::new();
    let mut ambiguous: Vec<String> = Vec::new();

    for side in &sides {
        for (column, ty) in &side.types {
            qualified.insert(format!("{}.{column}", side.collection), *ty);
            if let Some(alias) = &side.alias {
                qualified.insert(format!("{alias}.{column}"), *ty);
            }
            // `copied` ends the borrow on `bare` before the arms write to it.
            match bare.get(column).copied() {
                Some(seen) if seen == *ty => {}
                Some(_) => {
                    if !ambiguous.iter().any(|c| c == column) {
                        ambiguous.push(column.clone());
                    }
                }
                None => {
                    bare.insert(column.clone(), *ty);
                }
            }
        }
    }

    for column in &ambiguous {
        bare.insert(column.clone(), DdlColType::Text);
    }
    for (column, ty) in bare {
        qualified.entry(column).or_insert(ty);
    }
    Ok(qualified)
}

/// Collect the scan-like leaves of one join side, each typed from `catalog`.
///
/// A nested join contributes both of its own sides. A CTE plan, which is
/// what a derived table plans to, contributes the leaves of its outer query,
/// typed against its definitions' output schemas. A plan shape with no
/// single source relation contributes none, so every column it projects
/// stays `Text`. A catalog lookup error is returned.
fn collect_sides(
    plan: &SqlPlan,
    catalog: &dyn SqlCatalog,
    database_id: nodedb_types::DatabaseId,
    out: &mut Vec<JoinSide>,
) -> Result<(), SqlError> {
    match plan {
        SqlPlan::Join { left, right, .. } => {
            collect_sides(left, catalog, database_id, out)?;
            collect_sides(right, catalog, database_id, out)?;
        }
        SqlPlan::Cte(CtePlan { definitions, outer }) => {
            let overlay = CteTypesCatalog::build(catalog, definitions, database_id)?;
            collect_sides(outer, &overlay, database_id, out)?;
        }
        // The three read shapes that carry a FROM-clause alias.
        SqlPlan::Scan {
            collection, alias, ..
        }
        | SqlPlan::PointGet {
            collection, alias, ..
        }
        | SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan {
            collection, alias, ..
        }) => out.push(JoinSide {
            collection: collection.clone(),
            alias: alias.clone(),
            types: column_types_for(catalog, database_id, collection)?,
        }),
        // Reads over one collection that carry no alias slot: the merged row
        // qualifies their columns by the collection name.
        SqlPlan::RangeScan(RangeScanPlan { collection, .. })
        | SqlPlan::TimeseriesScan(TimeseriesScanPlan { collection, .. })
        | SqlPlan::SpatialScan { collection, .. }
        | SqlPlan::VectorSearch { collection, .. }
        | SqlPlan::MultiVectorSearch { collection, .. }
        | SqlPlan::SparseSearch { collection, .. }
        | SqlPlan::TextSearch(TextSearchPlan { collection, .. })
        | SqlPlan::HybridSearch(HybridSearchPlan { collection, .. })
        | SqlPlan::HybridSearchTriple(HybridSearchTriplePlan { collection, .. })
        | SqlPlan::RecursiveScan(RecursiveScanPlan { collection, .. }) => out.push(JoinSide {
            collection: collection.clone(),
            alias: None,
            types: column_types_for(catalog, database_id, collection)?,
        }),
        // No single source relation to attribute a column to: set
        // operations, aggregates, subquery bodies, lateral shapes, constant
        // and recursive-value rows, every write, and the whole array and
        // index DDL family.
        SqlPlan::ConstantResult { .. }
        | SqlPlan::Insert { .. }
        | SqlPlan::KvInsert { .. }
        | SqlPlan::Upsert { .. }
        | SqlPlan::InsertSelect { .. }
        | SqlPlan::Update { .. }
        | SqlPlan::UpdateFrom { .. }
        | SqlPlan::Delete { .. }
        | SqlPlan::Truncate { .. }
        | SqlPlan::VectorPrimaryTruncate { .. }
        | SqlPlan::Aggregate { .. }
        | SqlPlan::TimeseriesIngest { .. }
        | SqlPlan::Union { .. }
        | SqlPlan::Intersect { .. }
        | SqlPlan::Except { .. }
        | SqlPlan::RecursiveValue { .. }
        | SqlPlan::Subquery { .. }
        | SqlPlan::CreateArray { .. }
        | SqlPlan::DropArray { .. }
        | SqlPlan::AlterArray { .. }
        | SqlPlan::InsertArray { .. }
        | SqlPlan::DeleteArray { .. }
        | SqlPlan::ArraySlice { .. }
        | SqlPlan::ArrayAgg { .. }
        | SqlPlan::ArrayElementwise { .. }
        | SqlPlan::ArrayFlush { .. }
        | SqlPlan::ArrayCompact { .. }
        | SqlPlan::Merge { .. }
        | SqlPlan::LateralTopK { .. }
        | SqlPlan::LateralLoop { .. }
        | SqlPlan::VectorPrimaryInsert { .. }
        | SqlPlan::VectorPrimaryDelete { .. }
        | SqlPlan::VectorPrimaryUpdate { .. }
        | SqlPlan::CreateIndex { .. }
        | SqlPlan::DropIndex { .. } => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_sql::types::collection::ColumnInfo;
    use nodedb_sql::types::query::EngineType;
    use nodedb_sql::types_expr::SqlDataType;

    /// Two collections whose `id` columns differ in type, plus a `ts`
    /// declared only on `events`.
    struct TwoSideCatalog;

    fn column(name: &str, ty: SqlDataType) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: ty,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: None,
        }
    }

    impl SqlCatalog for TwoSideCatalog {
        fn get_collection(
            &self,
            _database_id: nodedb_types::DatabaseId,
            name: &str,
        ) -> Result<Option<nodedb_sql::types::CollectionInfo>, nodedb_sql::catalog::SqlCatalogError>
        {
            let columns = match name {
                "events" => vec![
                    column("ts", SqlDataType::Timestamp),
                    column("id", SqlDataType::Int64),
                ],
                "hosts" => vec![column("id", SqlDataType::String)],
                _ => return Ok(None),
            };
            Ok(Some(nodedb_sql::types::CollectionInfo {
                name: name.to_string(),
                engine: EngineType::DocumentStrict,
                columns,
                primary_key: None,
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                open_schema: nodedb_sql::types::CollectionInfo::open_schema_for(
                    EngineType::DocumentStrict,
                ),
                text_indexes: None,
            }))
        }
    }

    fn scan(collection: &str, alias: Option<&str>) -> SqlPlan {
        SqlPlan::Scan {
            collection: collection.to_string(),
            alias: alias.map(str::to_string),
            engine: EngineType::DocumentStrict,
            filters: Vec::new(),
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: nodedb_sql::temporal::TemporalScope::default(),
        }
    }

    /// A column declared on one side only keeps that side's catalog type,
    /// under both its qualified and its bare name.
    #[test]
    fn a_column_unique_to_one_side_keeps_its_type() {
        let types = join_column_types(
            &scan("events", None),
            &scan("hosts", None),
            &TwoSideCatalog,
            nodedb_types::DatabaseId::DEFAULT,
        )
        .expect("types derive");
        assert_eq!(types.get("events.ts"), Some(&DdlColType::Timestamp));
        assert_eq!(types.get("ts"), Some(&DdlColType::Timestamp));
    }

    /// An alias qualifies the same columns the collection name does.
    #[test]
    fn an_alias_names_the_same_side() {
        let types = join_column_types(
            &scan("events", Some("e")),
            &scan("hosts", None),
            &TwoSideCatalog,
            nodedb_types::DatabaseId::DEFAULT,
        )
        .expect("types derive");
        assert_eq!(types.get("e.ts"), Some(&DdlColType::Timestamp));
        assert_eq!(types.get("events.ts"), Some(&DdlColType::Timestamp));
    }

    /// A bare name two sides declare with different types cannot be
    /// attributed, so it stays `Text` while the qualified names keep theirs.
    #[test]
    fn an_ambiguous_bare_name_stays_text() {
        let types = join_column_types(
            &scan("events", None),
            &scan("hosts", None),
            &TwoSideCatalog,
            nodedb_types::DatabaseId::DEFAULT,
        )
        .expect("types derive");
        assert_eq!(types.get("id"), Some(&DdlColType::Text));
        assert_eq!(types.get("events.id"), Some(&DdlColType::Int8));
        assert_eq!(types.get("hosts.id"), Some(&DdlColType::Text));
    }

    /// A derived side plans to a one-definition CTE. Its columns carry the
    /// types the body announces, under the derived alias.
    #[test]
    fn a_derived_side_takes_its_body_types() {
        let body = SqlPlan::Scan {
            collection: "events".to_string(),
            alias: None,
            engine: EngineType::DocumentStrict,
            filters: Vec::new(),
            projection: vec![
                nodedb_sql::types::query::Projection::Column("ts".to_string()),
                nodedb_sql::types::query::Projection::Computed {
                    expr: nodedb_sql::types_expr::SqlExpr::Column {
                        table: None,
                        name: "id".to_string(),
                    },
                    alias: "event_id".to_string(),
                },
            ],
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: nodedb_sql::temporal::TemporalScope::default(),
        };
        let derived = SqlPlan::Cte(CtePlan {
            definitions: vec![("d".to_string(), body)],
            outer: Box::new(scan("d", Some("d"))),
        });
        let types = join_column_types(
            &scan("hosts", Some("h")),
            &derived,
            &TwoSideCatalog,
            nodedb_types::DatabaseId::DEFAULT,
        )
        .expect("types derive");
        assert_eq!(types.get("d.ts"), Some(&DdlColType::Timestamp));
        assert_eq!(types.get("d.event_id"), Some(&DdlColType::Int8));
        assert_eq!(types.get("h.id"), Some(&DdlColType::Text));
    }
}
