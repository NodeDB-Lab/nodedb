// SPDX-License-Identifier: Apache-2.0

//! Plan-time refusal of search functions outside a search plan.

use super::refuse_row_scoped_search_functions;
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::types::query::EngineType;
use crate::types::query::Projection;
use crate::types::{Filter, FilterExpr, SqlPlan};
use crate::types_expr::SqlExpr;
use crate::types_expr::SqlValue;

fn call(name: &str) -> SqlExpr {
    SqlExpr::Function {
        name: name.into(),
        args: vec![
            SqlExpr::Column {
                table: None,
                name: "body".into(),
            },
            SqlExpr::Literal(SqlValue::String("rust".into())),
        ],
        distinct: false,
    }
}

fn scan(projection: Vec<Projection>, filters: Vec<Filter>) -> SqlPlan {
    SqlPlan::Scan {
        collection: "docs".into(),
        alias: None,
        engine: EngineType::DocumentSchemaless,
        filters,
        projection,
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
        window_functions: Vec::new(),
        temporal: Default::default(),
    }
}

fn check(plan: &SqlPlan) -> Result<()> {
    refuse_row_scoped_search_functions(plan, &FunctionRegistry::new())
}

#[test]
fn a_score_in_a_scan_projection_is_refused() {
    let plan = scan(
        vec![Projection::Computed {
            expr: call("bm25_score"),
            alias: "s".into(),
        }],
        Vec::new(),
    );
    assert_eq!(
        check(&plan),
        Err(SqlError::SearchFunctionOutsideSearch {
            name: "bm25_score".into()
        })
    );
}

#[test]
fn a_match_nested_in_a_scan_filter_is_refused() {
    let nested = SqlExpr::BinaryOp {
        left: Box::new(call("text_match")),
        op: crate::types_expr::BinaryOp::Or,
        right: Box::new(SqlExpr::Literal(SqlValue::Bool(false))),
    };
    let plan = scan(
        Vec::new(),
        vec![Filter {
            expr: FilterExpr::Expr(nested),
        }],
    );
    assert!(matches!(
        check(&plan),
        Err(SqlError::SearchFunctionOutsideSearch { .. })
    ));
}

#[test]
fn row_scalars_in_a_scan_pass() {
    let plan = scan(
        vec![
            Projection::Computed {
                expr: call("vector_distance"),
                alias: "d".into(),
            },
            Projection::Computed {
                expr: call("doc_get"),
                alias: "g".into(),
            },
        ],
        Vec::new(),
    );
    assert_eq!(check(&plan), Ok(()));
}

#[test]
fn a_search_plan_projection_serves_its_score() {
    let plan = SqlPlan::TextSearch {
        collection: "docs".into(),
        field: None,
        query: crate::fts_types::FtsQuery::Plain {
            text: "rust".into(),
            fuzzy: true,
        },
        top_k: 10,
        filters: Vec::new(),
        score_alias: Some("s".into()),
        projection: vec![Projection::Computed {
            expr: call("bm25_score"),
            alias: "s".into(),
        }],
    };
    assert_eq!(check(&plan), Ok(()));
}

#[test]
fn a_subquery_tail_over_a_search_plan_is_not_checked() {
    let search = SqlPlan::TextSearch {
        collection: "docs".into(),
        field: None,
        query: crate::fts_types::FtsQuery::Plain {
            text: "rust".into(),
            fuzzy: true,
        },
        top_k: 10,
        filters: Vec::new(),
        score_alias: Some("s".into()),
        projection: Vec::new(),
    };
    let plan = SqlPlan::Subquery {
        input: Box::new(search),
        filters: Vec::new(),
        projection: vec![Projection::Computed {
            expr: call("bm25_score"),
            alias: "s".into(),
        }],
        window_functions: Vec::new(),
        sort_keys: Vec::new(),
        offset: 0,
        distinct: false,
        limit: None,
    };
    assert_eq!(check(&plan), Ok(()));
}
