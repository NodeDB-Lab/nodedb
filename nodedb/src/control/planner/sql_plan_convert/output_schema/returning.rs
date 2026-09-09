// SPDX-License-Identifier: BUSL-1.1

//! Derives the announced [`OutputSchema`] of a DML `RETURNING` clause.
//!
//! A `RETURNING` clause is a projection over the target collection, so it is
//! typed by the same rule a `SELECT` projection is: the clause's column list is
//! mapped to [`Projection`] entries and handed to
//! [`schema_from_projection`](super::columns::schema_from_projection), against
//! the same catalog column types and declared order. There is one derivation,
//! so a write and a read of the same column can never announce different types.
//!
//! `RETURNING *` sets `is_star`, exactly as `SELECT *` does. The concrete
//! column list of a star is only knowable from the returned rows — a
//! schemaless row carries fields no catalog column declares — so the shaper
//! keeps the row-derived list and renders those cells as text, the same answer
//! `SELECT *` gives for the same row.

use nodedb_physical::physical_plan::{ReturningColumns, ReturningSpec};
use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::query::Projection;
use nodedb_sql::types_expr::SqlExpr;

use crate::control::server::response_shape::schema::OutputSchema;

use super::columns::{column_types_for, ordered_columns_for, schema_from_projection};

/// The output schema a write announces for `returning` against `collection`.
///
/// `None` — the statement carries no `RETURNING` clause — announces nothing,
/// which is what a write with no result set must say.
pub fn build_returning_schema<C: SqlCatalog + ?Sized>(
    returning: Option<&ReturningSpec>,
    collection: &str,
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
) -> OutputSchema {
    let Some(spec) = returning else {
        return OutputSchema::default();
    };
    let projection = returning_projection(spec);
    let types = column_types_for(catalog, database_id, collection);
    let ordered_cols = ordered_columns_for(catalog, database_id, collection);
    schema_from_projection(&projection, &types, &ordered_cols)
}

/// Maps a `RETURNING` column list to the projection entries the shared
/// derivation reads.
///
/// The clause's grammar admits a bare column name and an optional alias, and
/// nothing else: `parse_returning_columns` rejects every expression form with a
/// typed error before a spec exists. So an aliased item maps to a
/// `Projection::Computed` wrapping the column reference — the form that keeps
/// the alias as the display name while the value is still looked up under the
/// source column — and a bare item maps to `Projection::Column`.
fn returning_projection(spec: &ReturningSpec) -> Vec<Projection> {
    match &spec.columns {
        ReturningColumns::Star => vec![Projection::Star],
        ReturningColumns::Named(items) => items
            .iter()
            .map(|item| match &item.alias {
                Some(alias) => Projection::Computed {
                    expr: SqlExpr::Column {
                        table: None,
                        name: item.name.clone(),
                    },
                    alias: alias.clone(),
                },
                None => Projection::Column(item.name.clone()),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::response_shape::types::DdlColType;
    use nodedb_physical::physical_plan::ReturningItem;

    /// Catalog exposing one `points` collection: a `TIMESTAMP` time key, a
    /// `TEXT` tag, and a `FLOAT` measurement — the shape a timeseries
    /// collection declares.
    struct PointsCatalog;

    impl SqlCatalog for PointsCatalog {
        fn get_collection(
            &self,
            _database_id: nodedb_types::DatabaseId,
            name: &str,
        ) -> Result<Option<nodedb_sql::types::CollectionInfo>, nodedb_sql::catalog::SqlCatalogError>
        {
            use nodedb_sql::types::collection::ColumnInfo;
            use nodedb_sql::types::query::EngineType;
            use nodedb_sql::types_expr::SqlDataType;

            if name != "points" {
                return Ok(None);
            }
            let col = |n: &str, t: SqlDataType| ColumnInfo {
                name: n.to_string(),
                data_type: t,
                nullable: true,
                is_primary_key: false,
                default: None,
                raw_type: None,
                int_width: None,
                float_width: None,
            };
            Ok(Some(nodedb_sql::types::CollectionInfo {
                name: "points".to_string(),
                engine: EngineType::Timeseries,
                columns: vec![
                    col("ts", SqlDataType::Timestamp),
                    col("host", SqlDataType::String),
                    col("v", SqlDataType::Float64),
                ],
                primary_key: None,
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                open_schema: nodedb_sql::types::CollectionInfo::open_schema_for(
                    EngineType::Timeseries,
                ),
            }))
        }
    }

    fn named(items: &[(&str, Option<&str>)]) -> ReturningSpec {
        ReturningSpec {
            columns: ReturningColumns::Named(
                items
                    .iter()
                    .map(|(name, alias)| ReturningItem {
                        name: (*name).to_string(),
                        alias: alias.map(str::to_string),
                    })
                    .collect(),
            ),
        }
    }

    fn schema(spec: Option<&ReturningSpec>) -> OutputSchema {
        let database_id = nodedb_types::DatabaseId::DEFAULT;
        build_returning_schema(spec, "points", &PointsCatalog, database_id)
    }

    /// Named columns carry the declared catalog type, in clause order — the
    /// same types `SELECT ts, host, v` announces for the same row.
    #[test]
    fn named_columns_carry_their_declared_types() {
        let spec = named(&[("ts", None), ("host", None), ("v", None)]);
        let out = schema(Some(&spec));
        assert!(!out.is_star);
        let got: Vec<(&str, DdlColType)> = out
            .columns
            .iter()
            .map(|c| (c.display_name.as_str(), c.ty))
            .collect();
        assert_eq!(
            got,
            vec![
                ("ts", DdlColType::Timestamp),
                ("host", DdlColType::Text),
                ("v", DdlColType::Float8),
            ]
        );
    }

    /// An alias names the output column while the value is still looked up
    /// under the source column, and it keeps the source column's type.
    #[test]
    fn an_alias_renames_the_column_and_keeps_its_type() {
        let spec = named(&[("v", Some("reading"))]);
        let out = schema(Some(&spec));
        assert_eq!(out.columns.len(), 1);
        assert_eq!(out.columns[0].display_name, "reading");
        assert_eq!(out.columns[0].lookup_key, "v");
        assert_eq!(out.columns[0].ty, DdlColType::Float8);
    }

    /// A column the catalog does not declare falls back to `Text`, the safe
    /// default for a schemaless field.
    #[test]
    fn an_undeclared_column_falls_back_to_text() {
        let spec = named(&[("undeclared", None)]);
        let out = schema(Some(&spec));
        assert_eq!(out.columns[0].ty, DdlColType::Text);
    }

    /// `RETURNING *` sets `is_star`, so the shaper keeps the row-derived
    /// column list — the same answer `SELECT *` gives.
    #[test]
    fn a_star_is_marked_as_one() {
        let spec = ReturningSpec {
            columns: ReturningColumns::Star,
        };
        let out = schema(Some(&spec));
        assert!(out.is_star);
        let names: Vec<&str> = out
            .columns
            .iter()
            .map(|c| c.display_name.as_str())
            .collect();
        assert_eq!(names, vec!["ts", "host", "v"]);
    }

    /// A write with no `RETURNING` clause announces nothing.
    #[test]
    fn no_clause_announces_nothing() {
        let out = schema(None);
        assert!(out.columns.is_empty());
        assert!(!out.is_star);
    }
}
