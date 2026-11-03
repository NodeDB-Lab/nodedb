// SPDX-License-Identifier: Apache-2.0

//! ANN options parsing for vector distance SQL functions.
//!
//! Parses named arguments of the form `name => value` passed to
//! `vector_distance`, `vector_cosine_distance`, and
//! `vector_neg_inner_product`.
//!
//! Example:
//! ```sql
//! SELECT id FROM embeddings
//! ORDER BY vector_distance(embedding, [1.0, 0.0], quantization => 'rabitq', ef_search => 100)
//! LIMIT 10
//! ```

use sqlparser::ast::{self, FunctionArgOperator};

use crate::error::{Result, SqlError};
use crate::types::{VectorAnnOptions, VectorQuantization};

const CANONICAL_NAMES: &str =
    "quantization, oversample, query_dim, meta_token_budget, ef_search, target_recall";

/// Parse named arguments of a vector distance function call into
/// [`VectorAnnOptions`]. Positional args at positions 0 and 1 (field name and
/// query vector) are ignored — this function only reads named args. Positional
/// args at position ≥ 2 are rejected (old JSON-string form).
///
/// Returns `VectorAnnOptions::default()` when no named options are present.
pub fn parse_ann_options(func_args: &[ast::FunctionArg]) -> Result<VectorAnnOptions> {
    let mut opts = VectorAnnOptions::default();
    let mut seen = [false; 6]; // indexed by option slot

    let mut positional_idx: usize = 0;

    for arg in func_args {
        // Resolve the named arg to (key, value_expr, operator), or handle
        // positional/unsupported forms.
        let (key, value_expr, operator) = match arg {
            ast::FunctionArg::Unnamed(_) => {
                // First two positional args (field, query vector) are fine.
                if positional_idx >= 2 {
                    return Err(SqlError::Unsupported {
                        detail: "vector_distance: JSON-string options are no longer supported. \
                             Use named arguments instead: \
                             vector_distance(field, query, quantization => 'rabitq', ef_search => 100, oversample => 3)"
                            .into(),
                    });
                }
                positional_idx += 1;
                continue;
            }

            ast::FunctionArg::Named {
                name,
                arg,
                operator,
            } => {
                let key = name.value.to_ascii_lowercase();
                let expr = match arg {
                    ast::FunctionArgExpr::Expr(e) => e,
                    _ => {
                        return Err(SqlError::Unsupported {
                            detail: format!("ANN option '{key}': expected a value expression"),
                        });
                    }
                };
                (key, expr, operator)
            }

            // sqlparser may parse `ident => value` as ExprNamed when the name
            // is an identifier expression. Accept simple identifier names.
            ast::FunctionArg::ExprNamed {
                name: ast::Expr::Identifier(ident),
                arg,
                operator,
            } => {
                let key = ident.value.to_ascii_lowercase();
                let expr = match arg {
                    ast::FunctionArgExpr::Expr(e) => e,
                    _ => {
                        return Err(SqlError::Unsupported {
                            detail: format!("ANN option '{key}': expected a value expression"),
                        });
                    }
                };
                (key, expr, operator)
            }

            ast::FunctionArg::ExprNamed { .. } => {
                return Err(SqlError::Unsupported {
                    detail: "vector_distance: expression-named arguments are not supported. \
                         Use identifier names with '=>': quantization => 'rabitq'"
                        .into(),
                });
            }
        };

        if *operator != FunctionArgOperator::RightArrow {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "use '=>' not '{}' for ANN options (e.g. quantization => 'rabitq')",
                    operator
                ),
            });
        }

        match key.as_str() {
            "quantization" => {
                if seen[0] {
                    return Err(duplicate_error("quantization"));
                }
                seen[0] = true;
                let s = extract_string(value_expr, "quantization")?;
                opts.quantization = VectorQuantization::parse(&s).map(Some).ok_or_else(|| {
                    SqlError::Unsupported {
                        detail: format!(
                            "unknown quantization '{s}'; \
                             valid values: none, sq8, pq, rabitq, bbq, binary, ternary, opq"
                        ),
                    }
                })?;
            }
            "oversample" => {
                if seen[1] {
                    return Err(duplicate_error("oversample"));
                }
                seen[1] = true;
                let n = extract_u64(value_expr, "oversample")?;
                opts.oversample = Some(u8::try_from(n).map_err(|_| SqlError::Unsupported {
                    detail: format!(
                        "ANN option 'oversample' must fit in u8 (0..={}); got {n}",
                        u8::MAX
                    ),
                })?);
            }
            "query_dim" => {
                if seen[2] {
                    return Err(duplicate_error("query_dim"));
                }
                seen[2] = true;
                let n = extract_u64(value_expr, "query_dim")?;
                opts.query_dim = Some(u32::try_from(n).map_err(|_| SqlError::Unsupported {
                    detail: format!(
                        "ANN option 'query_dim' must fit in u32 (0..={}); got {n}",
                        u32::MAX
                    ),
                })?);
            }
            "meta_token_budget" => {
                if seen[3] {
                    return Err(duplicate_error("meta_token_budget"));
                }
                seen[3] = true;
                let n = extract_u64(value_expr, "meta_token_budget")?;
                opts.meta_token_budget =
                    Some(u8::try_from(n).map_err(|_| SqlError::Unsupported {
                        detail: format!(
                            "ANN option 'meta_token_budget' must fit in u8 (0..={}); got {n}",
                            u8::MAX
                        ),
                    })?);
            }
            "ef_search" => {
                if seen[4] {
                    return Err(duplicate_error("ef_search"));
                }
                seen[4] = true;
                let n = extract_u64(value_expr, "ef_search")?;
                opts.ef_search_override =
                    Some(usize::try_from(n).map_err(|_| SqlError::Unsupported {
                        detail: format!("ANN option 'ef_search' exceeds usize range; got {n}"),
                    })?);
            }
            "target_recall" => {
                if seen[5] {
                    return Err(duplicate_error("target_recall"));
                }
                seen[5] = true;
                let f = extract_f64(value_expr, "target_recall")?;
                if f.is_nan() || f <= 0.0 || f > 1.0 {
                    return Err(SqlError::Unsupported {
                        detail: format!(
                            "ANN option 'target_recall' must be in (0.0, 1.0]; got {f}"
                        ),
                    });
                }
                opts.target_recall = Some(f as f32);
            }
            other => {
                return Err(SqlError::Unsupported {
                    detail: format!(
                        "unknown ANN option '{other}'; valid options: {CANONICAL_NAMES}"
                    ),
                });
            }
        }
    }

    Ok(opts)
}

fn duplicate_error(name: &str) -> SqlError {
    SqlError::Unsupported {
        detail: format!("option '{name}' specified more than once"),
    }
}

/// Extract a string literal from a function argument expression.
fn extract_string(expr: &ast::Expr, option: &str) -> Result<String> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(s) => Ok(s.clone()),
            _ => Err(SqlError::Unsupported {
                detail: format!(
                    "ANN option '{option}' expects a string literal (e.g. => 'rabitq'), got: {expr}"
                ),
            }),
        },
        _ => Err(SqlError::Unsupported {
            detail: format!(
                "ANN option '{option}' expects a string literal (e.g. => 'rabitq'), got: {expr}"
            ),
        }),
    }
}

/// Extract a non-negative integer from a function argument expression.
fn extract_u64(expr: &ast::Expr, option: &str) -> Result<u64> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => n.parse::<u64>().map_err(|_| SqlError::Unsupported {
                detail: format!("ANN option '{option}' expects a non-negative integer, got: {n}"),
            }),
            _ => Err(SqlError::Unsupported {
                detail: format!(
                    "ANN option '{option}' expects a non-negative integer, got: {expr}"
                ),
            }),
        },
        _ => Err(SqlError::Unsupported {
            detail: format!("ANN option '{option}' expects a non-negative integer, got: {expr}"),
        }),
    }
}

/// Extract a float from a function argument expression.
fn extract_f64(expr: &ast::Expr, option: &str) -> Result<f64> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => n.parse::<f64>().map_err(|_| SqlError::Unsupported {
                detail: format!("ANN option '{option}' expects a number, got: {n}"),
            }),
            _ => Err(SqlError::Unsupported {
                detail: format!("ANN option '{option}' expects a number, got: {expr}"),
            }),
        },
        // Handle negative floats: -0.5 parses as UnaryOp { Minus, 0.5 }
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr: inner,
        } => extract_f64(inner, option).map(|f| -f),
        _ => Err(SqlError::Unsupported {
            detail: format!("ANN option '{option}' expects a number, got: {expr}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::functions::registry::FunctionRegistry;
    use crate::parser::preprocess::pipeline::preprocess;
    use crate::parser::statement::parse_sql;
    use crate::planner::select::plan_query;
    use crate::types::*;

    use sqlparser::ast::Statement;

    struct TestCatalog;

    impl SqlCatalog for TestCatalog {
        fn get_collection(
            &self,
            _: nodedb_types::DatabaseId,
            name: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            if name == "embeddings" {
                Ok(Some(CollectionInfo {
                    name: "embeddings".into(),
                    engine: EngineType::DocumentSchemaless,
                    columns: Vec::new(),
                    primary_key: Some("id".into()),
                    has_auto_tier: false,
                    indexes: Vec::new(),
                    bitemporal: false,
                    primary: nodedb_types::PrimaryEngine::Document,
                    vector_primary: None,
                    partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                    open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
                }))
            } else {
                Ok(None)
            }
        }

        fn lookup_array(&self, _name: &str) -> Option<crate::types::ArrayCatalogView> {
            None
        }
    }

    fn plan_ann(sql: &str) -> SqlPlan {
        let (preprocessed_sql, temporal) = match preprocess(sql).unwrap() {
            Some(p) => (p.sql, p.temporal),
            None => (sql.to_string(), crate::TemporalScope::default()),
        };
        let statements = parse_sql(&preprocessed_sql).unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query statement");
        };
        plan_query(query, &TestCatalog, &FunctionRegistry::new(), temporal).unwrap()
    }

    fn plan_ann_err(sql: &str) -> crate::error::SqlError {
        let (preprocessed_sql, temporal) = match preprocess(sql).unwrap() {
            Some(p) => (p.sql, p.temporal),
            None => (sql.to_string(), crate::TemporalScope::default()),
        };
        let statements = parse_sql(&preprocessed_sql).unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query statement");
        };
        plan_query(query, &TestCatalog, &FunctionRegistry::new(), temporal).unwrap_err()
    }

    #[test]
    fn two_positional_args_default_options() {
        let plan = plan_ann(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { ann_options, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ann_options, VectorAnnOptions::default());
    }

    #[test]
    fn named_quantization_rabitq() {
        let plan = plan_ann(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], quantization => 'rabitq') LIMIT 5",
        );
        let SqlPlan::VectorSearch { ann_options, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ann_options.quantization, Some(VectorQuantization::RaBitQ));
    }

    #[test]
    fn named_ef_search() {
        let plan = plan_ann(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], ef_search => 100) LIMIT 5",
        );
        let SqlPlan::VectorSearch {
            ann_options,
            ef_search,
            ..
        } = plan
        else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ann_options.ef_search_override, Some(100));
        assert_eq!(ef_search, 100);
    }

    #[test]
    fn named_oversample() {
        let plan = plan_ann(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], oversample => 3) LIMIT 5",
        );
        let SqlPlan::VectorSearch { ann_options, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ann_options.oversample, Some(3));
    }

    #[test]
    fn named_query_dim() {
        let plan = plan_ann(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], query_dim => 128) LIMIT 5",
        );
        let SqlPlan::VectorSearch { ann_options, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ann_options.query_dim, Some(128));
    }

    #[test]
    fn named_meta_token_budget() {
        let plan = plan_ann(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], meta_token_budget => 10) LIMIT 5",
        );
        let SqlPlan::VectorSearch { ann_options, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ann_options.meta_token_budget, Some(10));
    }

    #[test]
    fn named_target_recall() {
        let plan = plan_ann(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], target_recall => 0.95) LIMIT 5",
        );
        let SqlPlan::VectorSearch { ann_options, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert!((ann_options.target_recall.unwrap() - 0.95_f32).abs() < 0.001);
    }

    #[test]
    fn all_named_options() {
        let plan = plan_ann(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], quantization => 'pq', oversample => 5, query_dim => 64, meta_token_budget => 8, ef_search => 200, target_recall => 0.9) LIMIT 5",
        );
        let SqlPlan::VectorSearch { ann_options, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ann_options.quantization, Some(VectorQuantization::Pq));
        assert_eq!(ann_options.oversample, Some(5));
        assert_eq!(ann_options.query_dim, Some(64));
        assert_eq!(ann_options.meta_token_budget, Some(8));
        assert_eq!(ann_options.ef_search_override, Some(200));
        assert!((ann_options.target_recall.unwrap() - 0.9_f32).abs() < 0.001);
    }

    #[test]
    fn json_string_form_rejected() {
        let err = plan_ann_err(
            r#"SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], '{"quantization":"rabitq"}') LIMIT 5"#,
        );
        assert!(
            matches!(err, crate::error::SqlError::Unsupported { ref detail } if detail.contains("JSON-string options are no longer supported")),
            "expected Unsupported with JSON message, got: {err:?}"
        );
    }

    #[test]
    fn unknown_named_arg_rejected() {
        let err = plan_ann_err(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], future_key => 'val') LIMIT 5",
        );
        assert!(
            matches!(err, crate::error::SqlError::Unsupported { ref detail } if detail.contains("unknown ANN option")),
            "expected Unsupported with unknown option message, got: {err:?}"
        );
    }

    #[test]
    fn wrong_operator_rejected() {
        // quantization : 'rabitq' — PostgreSQL dialect parses `:` as a named
        // arg operator (Colon), which is not the required RightArrow.
        let err = plan_ann_err(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], quantization : 'rabitq') LIMIT 5",
        );
        assert!(
            matches!(err, crate::error::SqlError::Unsupported { ref detail } if detail.contains("use '=>'")),
            "expected Unsupported with operator message, got: {err:?}"
        );
    }

    #[test]
    fn duplicate_named_arg_rejected() {
        let err = plan_ann_err(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], ef_search => 100, ef_search => 200) LIMIT 5",
        );
        assert!(
            matches!(err, crate::error::SqlError::Unsupported { ref detail } if detail.contains("specified more than once")),
            "expected Unsupported duplicate message, got: {err:?}"
        );
    }

    #[test]
    fn out_of_range_oversample_rejected() {
        let err = plan_ann_err(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], oversample => 1000) LIMIT 5",
        );
        assert!(
            matches!(err, crate::error::SqlError::Unsupported { ref detail } if detail.contains("oversample")),
            "expected Unsupported oversample range message, got: {err:?}"
        );
    }

    #[test]
    fn target_recall_out_of_range_rejected() {
        let err = plan_ann_err(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], target_recall => 2.0) LIMIT 5",
        );
        assert!(
            matches!(err, crate::error::SqlError::Unsupported { ref detail } if detail.contains("target_recall")),
            "expected Unsupported target_recall range message, got: {err:?}"
        );
    }

    #[test]
    fn target_recall_zero_is_rejected() {
        let err = plan_ann_err(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], target_recall => 0.0) LIMIT 5",
        );
        assert!(
            matches!(err, crate::error::SqlError::Unsupported { ref detail } if detail.contains("(0.0, 1.0]")),
            "expected Unsupported target_recall range message, got: {err:?}"
        );
    }

    #[test]
    fn all_three_distance_functions_accept_named_args_l2() {
        let plan = plan_ann(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], quantization => 'sq8') LIMIT 5",
        );
        let SqlPlan::VectorSearch {
            ann_options,
            metric,
            ..
        } = plan
        else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ann_options.quantization, Some(VectorQuantization::Sq8));
        assert_eq!(metric, DistanceMetric::L2);
    }

    #[test]
    fn all_three_distance_functions_accept_named_args_cosine() {
        let plan = plan_ann(
            "SELECT id FROM embeddings ORDER BY vector_cosine_distance(embedding, [1.0, 0.0], quantization => 'sq8') LIMIT 5",
        );
        let SqlPlan::VectorSearch {
            ann_options,
            metric,
            ..
        } = plan
        else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ann_options.quantization, Some(VectorQuantization::Sq8));
        assert_eq!(metric, DistanceMetric::Cosine);
    }

    #[test]
    fn all_three_distance_functions_accept_named_args_inner_product() {
        let plan = plan_ann(
            "SELECT id FROM embeddings ORDER BY vector_neg_inner_product(embedding, [1.0, 0.0], quantization => 'sq8') LIMIT 5",
        );
        let SqlPlan::VectorSearch {
            ann_options,
            metric,
            ..
        } = plan
        else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ann_options.quantization, Some(VectorQuantization::Sq8));
        assert_eq!(metric, DistanceMetric::InnerProduct);
    }
}
