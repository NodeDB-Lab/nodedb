// SPDX-License-Identifier: BUSL-1.1

//! Derives the announced [`OutputSchema`] of a DML `RETURNING` clause.
//!
//! A `RETURNING` clause is a projection over the target collection, so it is
//! typed by the same rule a `SELECT` projection is: the resolved
//! [`Projection`] list is handed to
//! [`schema_from_projection`](super::columns::schema_from_projection), against
//! the same catalog column types and declared order. There is one derivation,
//! so a write and a read of the same column can never announce different types.
//!
//! A `CpComputed` entry is announced under its alias and recorded in
//! `cp_computed`, so the response shaper evaluates it per returned row.
//!
//! `RETURNING *` sets `is_star`, exactly as `SELECT *` does. The concrete
//! column list of a star is only knowable from the returned rows — a
//! schemaless row carries fields no catalog column declares — so the shaper
//! keeps the row-derived list and renders those cells as text, the same answer
//! `SELECT *` gives for the same row.

use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::query::Projection;

use crate::control::server::response_shape::schema::OutputSchema;

use super::columns::{column_types_for, ordered_columns_for, schema_from_projection};

/// The output schema a write announces for `returning` against `collection`.
///
/// `None` — the statement carries no `RETURNING` clause — announces nothing,
/// which is what a write with no result set must say.
pub fn build_returning_schema<C: SqlCatalog + ?Sized>(
    returning: Option<&[Projection]>,
    collection: &str,
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
) -> OutputSchema {
    let Some(projection) = returning else {
        return OutputSchema::default();
    };
    let types = column_types_for(catalog, database_id, collection);
    let ordered_cols = ordered_columns_for(catalog, database_id, collection);
    schema_from_projection(projection, &types, &ordered_cols)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::response_shape::types::DdlColType;
    use nodedb_sql::types_expr::{BinaryOp, SqlExpr, SqlValue};

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

    fn column(name: &str) -> SqlExpr {
        SqlExpr::Column {
            table: None,
            name: name.to_string(),
        }
    }

    fn schema(projection: Option<&[Projection]>) -> OutputSchema {
        let database_id = nodedb_types::DatabaseId::DEFAULT;
        build_returning_schema(projection, "points", &PointsCatalog, database_id)
    }

    /// Named columns carry the declared catalog type, in clause order — the
    /// same types `SELECT ts, host, v` announces for the same row.
    #[test]
    fn named_columns_carry_their_declared_types() {
        let projection = vec![
            Projection::Column("ts".into()),
            Projection::Column("host".into()),
            Projection::Column("v".into()),
        ];
        let out = schema(Some(&projection));
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
        assert!(out.cp_computed.is_empty());
    }

    /// An alias names the output column while the value is still looked up
    /// under the source column, and it keeps the source column's type.
    #[test]
    fn an_alias_renames_the_column_and_keeps_its_type() {
        let projection = vec![Projection::Computed {
            expr: column("v"),
            alias: "reading".into(),
        }];
        let out = schema(Some(&projection));
        assert_eq!(out.columns.len(), 1);
        assert_eq!(out.columns[0].display_name, "reading");
        assert_eq!(out.columns[0].lookup_key, "v");
        assert_eq!(out.columns[0].ty, DdlColType::Float8);
    }

    /// A Control-Plane computed entry is announced under its alias, looked up
    /// under that alias, and recorded for the shaper to evaluate.
    #[test]
    fn a_computed_entry_is_announced_and_recorded() {
        let projection = vec![
            Projection::Column("host".into()),
            Projection::CpComputed {
                expr: SqlExpr::BinaryOp {
                    left: Box::new(column("v")),
                    op: BinaryOp::Mul,
                    right: Box::new(SqlExpr::Literal(SqlValue::Int(2))),
                },
                alias: "d".into(),
            },
        ];
        let out = schema(Some(&projection));
        assert_eq!(out.columns.len(), 2);
        assert_eq!(out.columns[1].display_name, "d");
        assert_eq!(out.columns[1].lookup_key, "d");
        assert_eq!(out.cp_computed.len(), 1);
        assert_eq!(out.cp_computed[0].alias, "d");
    }

    /// A bare sequence accessor announces `bigint`.
    #[test]
    fn a_bare_accessor_is_a_bigint() {
        let projection = vec![Projection::CpComputed {
            expr: SqlExpr::Function {
                name: "nextval".into(),
                args: vec![SqlExpr::Literal(SqlValue::String("s".into()))],
                distinct: false,
            },
            alias: "n".into(),
        }];
        let out = schema(Some(&projection));
        assert_eq!(out.columns[0].ty, DdlColType::Int8);
    }

    /// A column the catalog does not declare falls back to `Text`, the safe
    /// default for a schemaless field.
    #[test]
    fn an_undeclared_column_falls_back_to_text() {
        let projection = vec![Projection::Column("undeclared".into())];
        let out = schema(Some(&projection));
        assert_eq!(out.columns[0].ty, DdlColType::Text);
    }

    /// `RETURNING *` sets `is_star`, so the shaper keeps the row-derived
    /// column list — the same answer `SELECT *` gives.
    #[test]
    fn a_star_is_marked_as_one() {
        let projection = vec![Projection::Star];
        let out = schema(Some(&projection));
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
