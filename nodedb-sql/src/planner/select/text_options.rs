// SPDX-License-Identifier: Apache-2.0

//! Named options of the text-search calls `text_match(column, 'q', ...)`,
//! `search(column, 'q', ...)`, and `bm25_score(column, 'q', ...)`.
//!
//! ```sql
//! SELECT id FROM docs WHERE text_match(body, 'rust db', mode => 'and', fuzzy => true)
//! ```
//!
//! The keys are a closed set: `mode` (`'or'` or `'and'`) and `fuzzy` (`true`
//! or `false`). Each takes `=>`. An omitted option takes its
//! [`TextSearchParams::default`] value, the default of the native `text_search`
//! API. A third positional argument, an unknown key, a repeated key, and `=`
//! in place of `=>` are typed errors.

use nodedb_types::text_search::{QueryMode, TextSearchParams};
use sqlparser::ast::{self, FunctionArgOperator};

use crate::error::{Result, SqlError};

/// The keys a text-search call accepts.
const OPTION_NAMES: &str = "mode, fuzzy";

/// The options of one text-search call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TextOptions {
    pub params: TextSearchParams,
    /// Whether the call names any option.
    pub named: bool,
}

/// Parse the named options of `function`'s argument list. The first two
/// positional arguments (column, query) are skipped.
pub(super) fn parse_text_options(function: &str, args: &[ast::FunctionArg]) -> Result<TextOptions> {
    let mut params = TextSearchParams::default();
    let mut mode_seen = false;
    let mut fuzzy_seen = false;
    let mut positional: usize = 0;
    for arg in args {
        let (key, value, operator) = match arg {
            ast::FunctionArg::Unnamed(arg_expr) => {
                if positional >= 2 {
                    return Err(positional_option(function, arg_expr));
                }
                positional += 1;
                continue;
            }
            ast::FunctionArg::Named {
                name,
                arg,
                operator,
            } => (name.value.to_ascii_lowercase(), arg, operator),
            ast::FunctionArg::ExprNamed {
                name: ast::Expr::Identifier(ident),
                arg,
                operator,
            } => (ident.value.to_ascii_lowercase(), arg, operator),
            ast::FunctionArg::ExprNamed { name, .. } => {
                return Err(SqlError::Unsupported {
                    detail: format!(
                        "{function}(): option name {name} is not an identifier; \
                         name an option: {OPTION_NAMES} (e.g. mode => 'or')"
                    ),
                });
            }
        };
        if *operator != FunctionArgOperator::RightArrow {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "{function}(): use '=>' not '{operator}' for text-search options \
                     (e.g. mode => 'or')"
                ),
            });
        }
        let ast::FunctionArgExpr::Expr(value) = value else {
            return Err(SqlError::Unsupported {
                detail: format!("{function}(): option '{key}' expects a value, got {value}"),
            });
        };
        match key.as_str() {
            "mode" => {
                if mode_seen {
                    return Err(duplicate(function, "mode"));
                }
                mode_seen = true;
                params.mode = parse_mode(function, value)?;
            }
            "fuzzy" => {
                if fuzzy_seen {
                    return Err(duplicate(function, "fuzzy"));
                }
                fuzzy_seen = true;
                params.fuzzy = parse_fuzzy(function, value)?;
            }
            other => {
                return Err(SqlError::Unsupported {
                    detail: format!(
                        "{function}(): unknown text-search option '{other}'; \
                         valid options: {OPTION_NAMES}"
                    ),
                });
            }
        }
    }
    Ok(TextOptions {
        params,
        named: mode_seen || fuzzy_seen,
    })
}

/// The error for a third positional argument. `key = value` names the `=>`
/// form it meant.
fn positional_option(function: &str, arg: &ast::FunctionArgExpr) -> SqlError {
    let is_eq_option = matches!(
        arg,
        ast::FunctionArgExpr::Expr(ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            ..
        }) if matches!(**left, ast::Expr::Identifier(_))
    );
    let detail = if is_eq_option {
        format!("{function}(): use '=>' not '=' for text-search options (e.g. mode => 'or')")
    } else {
        format!(
            "{function}() takes (column, 'query') and named options {OPTION_NAMES}; \
             a third positional argument is not accepted, got {arg}. \
             Use named options: {function}(column, 'query', mode => 'or', fuzzy => true)"
        )
    };
    SqlError::Unsupported { detail }
}

fn duplicate(function: &str, name: &str) -> SqlError {
    SqlError::Unsupported {
        detail: format!("{function}(): option '{name}' specified more than once"),
    }
}

/// `'or'` or `'and'`, in any ASCII case.
fn parse_mode(function: &str, value: &ast::Expr) -> Result<QueryMode> {
    let text = match value {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(s) => Some(s.as_str()),
            _ => None,
        },
        _ => None,
    };
    text.and_then(QueryMode::parse)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("{function}(): option 'mode' expects 'or' or 'and', got {value}"),
        })
}

/// `true` or `false`.
fn parse_fuzzy(function: &str, value: &ast::Expr) -> Result<bool> {
    let flag = match value {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Boolean(b) => Some(*b),
            _ => None,
        },
        _ => None,
    };
    flag.ok_or_else(|| SqlError::Unsupported {
        detail: format!("{function}(): option 'fuzzy' expects true or false, got {value}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::statement::parse_sql;

    /// The argument list of the first projected call of `SELECT <call>`.
    fn call_args(call: &str) -> Vec<ast::FunctionArg> {
        let statements = parse_sql(&format!("SELECT {call}")).expect("parse");
        let ast::Statement::Query(query) = &statements[0] else {
            panic!("expected a query");
        };
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected a SELECT");
        };
        let ast::SelectItem::UnnamedExpr(ast::Expr::Function(func)) = &select.projection[0] else {
            panic!("expected a function call");
        };
        let ast::FunctionArguments::List(list) = &func.args else {
            panic!("expected an argument list");
        };
        list.args.clone()
    }

    fn parse(call: &str) -> Result<TextOptions> {
        parse_text_options("text_match", &call_args(call))
    }

    fn detail(err: SqlError) -> String {
        match err {
            SqlError::Unsupported { detail } => detail,
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn no_options_take_the_trait_default() {
        let opts = parse("text_match(body, 'q')").expect("parse");
        assert_eq!(opts.params, TextSearchParams::default());
        assert!(!opts.named);
    }

    #[test]
    fn mode_and_fuzzy_parse() {
        let opts = parse("text_match(body, 'q', mode => 'AND', fuzzy => true)").expect("parse");
        assert_eq!(opts.params.mode, QueryMode::And);
        assert!(opts.params.fuzzy);
        assert!(opts.named);
        let opts = parse("text_match(body, 'q', fuzzy => false)").expect("parse");
        assert_eq!(opts.params.mode, QueryMode::Or);
        assert!(!opts.params.fuzzy);
    }

    #[test]
    fn an_unknown_key_is_refused() {
        let err = parse("text_match(body, 'q', boost => 2)").expect_err("unknown");
        assert!(detail(err).contains("unknown text-search option 'boost'"));
    }

    #[test]
    fn an_equals_operator_is_refused() {
        let err = parse("text_match(body, 'q', mode = 'or')").expect_err("=");
        assert!(detail(err).contains("use '=>'"));
    }

    #[test]
    fn a_positional_third_argument_is_refused() {
        let err = parse("text_match(body, 'q', '{\"fuzzy\":true}')").expect_err("positional");
        assert!(detail(err).contains("third positional argument"));
    }

    #[test]
    fn a_repeated_key_is_refused() {
        let err = parse("text_match(body, 'q', mode => 'or', mode => 'and')").expect_err("dup");
        assert!(detail(err).contains("more than once"));
    }

    #[test]
    fn a_bad_value_is_refused() {
        let err = parse("text_match(body, 'q', mode => 'xor')").expect_err("mode");
        assert!(detail(err).contains("'or' or 'and'"));
        let err = parse("text_match(body, 'q', fuzzy => 'yes')").expect_err("fuzzy");
        assert!(detail(err).contains("true or false"));
    }
}
