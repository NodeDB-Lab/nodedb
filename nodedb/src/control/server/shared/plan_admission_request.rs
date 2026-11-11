// SPDX-License-Identifier: BUSL-1.1

//! The planning step of admission, for a request of one or several
//! statements.
//!
//! A request of one statement plans its whole text through the
//! single-statement path. A request of several statements plans each
//! statement alone through the request planner. Both shapes give one task
//! set and its statement map.

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::planner::context::query::refuse_solo_between;
use crate::control::planner::context::{PlanSecurityContext, QueryContext};
use crate::control::planner::descriptor_set::DescriptorVersionSet;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::shared::returning::strip_returning;
use crate::control::server::shared::sql_split::split_sql_statements;
use crate::control::server::shared::statement_exec::{RequestError, StatementMap};
use crate::control::server::shared::txn_control::{TxnWrapper, classify_txn_wrapper};
use crate::types::{DatabaseId, TenantId};

/// The planned task set of one request, before authorization.
pub(super) struct PlannedRequest {
    pub(super) tasks: Vec<PhysicalTask>,
    /// The statement of each task in `tasks`, one entry per task.
    pub(super) statement_map: StatementMap,
    /// The schema the response folds read. See [`folded_output_schema`].
    pub(super) output_schema: OutputSchema,
    /// One output schema per statement. Index `k` is statement `k`.
    pub(super) output_schemas: Vec<OutputSchema>,
    pub(super) versions: DescriptorVersionSet,
}

/// The text of one request, split for planning.
pub(super) enum RequestText {
    /// One statement, or none. The whole text plans through the
    /// single-statement path. `sql` is that text with its `RETURNING`
    /// clause split off.
    Whole {
        sql: String,
        returning_items: Option<String>,
    },
    /// Several statements. Each plans alone through the request planner.
    Several(Vec<String>),
}

impl RequestText {
    /// Split `sql` at top-level semicolons.
    ///
    /// The planner does not parse a DML `RETURNING` clause. A text of one
    /// statement splits its clause off here. The item text resolves inside
    /// the planner against the planned target. A statement planned with the
    /// clause in its text drops it and answers a count with no rows.
    /// The request planner splits each statement's clause off on its own.
    pub(super) fn split(sql: &str) -> crate::Result<Self> {
        let statements = split_sql_statements(sql);
        if statements.len() > 1 {
            return Ok(Self::Several(statements));
        }
        let (sql, returning_items) = strip_returning(sql)?;
        Ok(Self::Whole {
            sql,
            returning_items,
        })
    }

    /// [`Self::split`] under the whole-request transaction wrapper policy.
    ///
    /// `wrapper` turns the policy on. Off, a request of several statements
    /// that holds transaction control takes the planner's 0A000 refusal. A
    /// request of one statement, or one with no transaction control, splits
    /// as [`Self::split`] splits it, policy on or off.
    ///
    /// On, the policy reads [`classify_txn_wrapper`] over the statements:
    ///
    /// | Shape | Text | Rolls back |
    /// |---|---|---|
    /// | `BEGIN; ..; COMMIT` | the inner statements | false |
    /// | `BEGIN; ..; ROLLBACK` | the inner statements | true |
    /// | any other transaction control | error 0A000 | - |
    ///
    /// The second value is true when the caller must apply nothing. Each
    /// inner statement passes the planner's solo check, even when only one
    /// sits inside the wrapper. The check numbers the inner statements.
    pub(super) fn split_with_wrapper(sql: &str, wrapper: bool) -> crate::Result<(Self, bool)> {
        let statements = split_sql_statements(sql);
        if !wrapper || statements.len() < 2 {
            return Self::split(sql).map(|text| (text, false));
        }
        match classify_txn_wrapper(&statements) {
            TxnWrapper::None => Ok((Self::Several(statements), false)),
            TxnWrapper::Unsupported => Err(unsupported_txn_control(statements.len())),
            TxnWrapper::Wrapped { commit } => {
                let end = statements.len().saturating_sub(1);
                refuse_solo_between(&statements, 1, end)?;
                let inner = statements.get(1..end).unwrap_or_default().to_vec();
                Self::inner(inner).map(|text| (text, !commit))
            }
        }
    }

    /// The text of the statements inside a wrapper. One statement plans
    /// whole, as a request of one statement. None plans no task.
    fn inner(mut statements: Vec<String>) -> crate::Result<Self> {
        if statements.len() != 1 {
            return Ok(Self::Several(statements));
        }
        let statement = statements.pop().unwrap_or_default();
        let (sql, returning_items) = strip_returning(&statement)?;
        Ok(Self::Whole {
            sql,
            returning_items,
        })
    }

    /// Plan the request against the committed catalog. A plan error of a
    /// request of several statements carries its statement.
    pub(super) async fn plan(
        self,
        query_ctx: &QueryContext,
        tenant_id: TenantId,
        database_id: DatabaseId,
        security: &PlanSecurityContext<'_>,
    ) -> Result<PlannedRequest, RequestError> {
        match self {
            Self::Whole {
                sql,
                returning_items,
            } => {
                let (tasks, output_schema, versions, _cache_eligibility) = query_ctx
                    .plan_sql_with_rls_and_versions(
                        &sql,
                        tenant_id,
                        database_id,
                        security,
                        returning_items.as_deref(),
                    )
                    .await?;
                Ok(PlannedRequest {
                    statement_map: StatementMap::single(tasks.len()),
                    output_schemas: vec![output_schema.clone()],
                    output_schema,
                    tasks,
                    versions,
                })
            }
            Self::Several(statements) => {
                let plan = query_ctx
                    .plan_request_with_rls_and_versions(
                        &statements,
                        tenant_id,
                        database_id,
                        security,
                    )
                    .await?;
                Ok(PlannedRequest {
                    output_schema: folded_output_schema(&plan.output_schemas),
                    tasks: plan.tasks,
                    statement_map: plan.statement_map,
                    output_schemas: plan.output_schemas,
                    versions: plan.versions,
                })
            }
        }
    }
}

/// The 0A000 refusal of transaction control the whole-request wrapper does
/// not cover, in a request of `count` statements.
fn unsupported_txn_control(count: usize) -> crate::Error {
    crate::Error::FeatureNotSupported {
        detail: format!(
            "a request of {count} statements holds transaction control that does not wrap \
             the whole request. Only BEGIN as the first statement, with no transaction mode, \
             and COMMIT or ROLLBACK as the last statement are accepted. Send the statements \
             as separate requests."
        ),
    }
}

/// The one schema a fold over the whole request reads: the schema of the
/// last statement that announces columns, else the first statement's.
///
/// A schema announces columns when it names a column or projects `*`. An
/// empty list gives the default schema.
pub(super) fn folded_output_schema(schemas: &[OutputSchema]) -> OutputSchema {
    folded_schema(schemas).cloned().unwrap_or_default()
}

/// The schema [`folded_output_schema`] picks, by reference. `None` for an
/// empty list.
pub(super) fn folded_schema(schemas: &[OutputSchema]) -> Option<&OutputSchema> {
    schemas
        .iter()
        .rev()
        .find(|schema| !schema.columns.is_empty() || schema.is_star)
        .or_else(|| schemas.first())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::response_shape::schema::OutputColumn;
    use crate::control::server::response_shape::types::DdlColType;

    fn announcing(name: &str) -> OutputSchema {
        OutputSchema {
            columns: vec![OutputColumn {
                display_name: name.to_owned(),
                lookup_key: name.to_owned(),
                ty: DdlColType::Text,
            }],
            ..OutputSchema::default()
        }
    }

    fn names(schema: &OutputSchema) -> Vec<&str> {
        schema
            .columns
            .iter()
            .map(|column| column.display_name.as_str())
            .collect()
    }

    #[test]
    fn the_last_statement_that_announces_columns_wins() {
        let schemas = [
            announcing("first"),
            announcing("second"),
            OutputSchema::default(),
        ];
        assert_eq!(names(&folded_output_schema(&schemas)), ["second"]);
    }

    #[test]
    fn no_announcing_statement_takes_the_first_schema() {
        let first = OutputSchema {
            declared_key: Some("key".to_owned()),
            ..OutputSchema::default()
        };
        let folded = folded_output_schema(&[first, OutputSchema::default()]);
        assert_eq!(folded.declared_key.as_deref(), Some("key"));
    }

    #[test]
    fn a_text_of_one_statement_plans_whole() {
        let text = RequestText::split("INSERT INTO docs (id) VALUES ('a') RETURNING id;")
            .expect("one statement splits");
        let RequestText::Whole {
            sql,
            returning_items,
        } = text
        else {
            panic!("one statement must plan whole");
        };
        assert!(!sql.contains("RETURNING"), "{sql}");
        assert!(returning_items.is_some(), "the clause is split off: {sql}");
    }

    #[test]
    fn a_text_of_two_statements_plans_each() {
        let text = RequestText::split("INSERT INTO a (id) VALUES ('x'); DELETE FROM b")
            .expect("two statements split");
        let RequestText::Several(statements) = text else {
            panic!("two statements must plan each alone");
        };
        assert_eq!(
            statements,
            ["INSERT INTO a (id) VALUES ('x')", "DELETE FROM b"]
        );
    }

    const INNER: &str = "INSERT INTO a (id) VALUES ('x'); DELETE FROM b";

    fn several(text: RequestText) -> Vec<String> {
        let RequestText::Several(statements) = text else {
            panic!("the text must plan each statement alone");
        };
        statements
    }

    fn sqlstate_0a000(result: crate::Result<(RequestText, bool)>) -> String {
        match result {
            Err(crate::Error::FeatureNotSupported { detail }) => detail,
            Err(other) => panic!("the request must answer 0A000, got {other:?}"),
            Ok(_) => panic!("the request must answer 0A000"),
        }
    }

    #[test]
    fn a_commit_wrapper_plans_the_inner_statements() {
        let (text, rolls_back) =
            RequestText::split_with_wrapper(&format!("BEGIN; {INNER}; COMMIT"), true)
                .expect("the wrapper is accepted");
        assert!(!rolls_back);
        assert_eq!(
            several(text),
            ["INSERT INTO a (id) VALUES ('x')", "DELETE FROM b"]
        );
    }

    #[test]
    fn a_rollback_wrapper_plans_the_inner_statements_and_rolls_back() {
        let (text, rolls_back) =
            RequestText::split_with_wrapper(&format!("BEGIN; {INNER}; ROLLBACK"), true)
                .expect("the wrapper is accepted");
        assert!(rolls_back);
        assert_eq!(several(text).len(), 2);
    }

    #[test]
    fn one_inner_statement_plans_whole() {
        let (text, _) = RequestText::split_with_wrapper(
            "BEGIN; INSERT INTO a (id) VALUES ('x') RETURNING id; COMMIT",
            true,
        )
        .expect("the wrapper is accepted");
        let RequestText::Whole {
            sql,
            returning_items,
        } = text
        else {
            panic!("one inner statement must plan whole");
        };
        assert_eq!(sql.trim(), "INSERT INTO a (id) VALUES ('x')");
        assert!(returning_items.is_some());
        let (empty, _) =
            RequestText::split_with_wrapper("BEGIN; COMMIT", true).expect("an empty wrapper");
        assert!(several(empty).is_empty());
    }

    #[test]
    fn transaction_control_outside_the_wrapper_answers_0a000() {
        for sql in [
            "INSERT INTO a (id) VALUES ('x'); COMMIT; DELETE FROM b",
            "BEGIN; INSERT INTO a (id) VALUES ('x')",
            "SAVEPOINT s; INSERT INTO a (id) VALUES ('x')",
            "BEGIN ISOLATION LEVEL SERIALIZABLE; INSERT INTO a (id) VALUES ('x'); COMMIT",
        ] {
            let detail = sqlstate_0a000(RequestText::split_with_wrapper(sql, true));
            assert!(detail.contains("separate requests"), "{sql}: {detail}");
        }
    }

    /// The solo check covers a session command or DDL inside the wrapper,
    /// numbered in the inner statements.
    #[test]
    fn a_solo_statement_inside_the_wrapper_answers_0a000() {
        for (sql, needle) in [
            ("BEGIN; SET x = 1; COMMIT", "statement 1 of 1"),
            (
                "BEGIN; INSERT INTO a (id) VALUES ('x'); RESET x; COMMIT",
                "statement 2 of 2",
            ),
            ("BEGIN; CREATE COLLECTION c; COMMIT", "statement 1 of 1"),
        ] {
            let detail = sqlstate_0a000(RequestText::split_with_wrapper(sql, true));
            assert!(detail.contains(needle), "{sql}: {detail}");
        }
    }

    /// Off, the wrapper splits into every statement. The planner refuses it.
    #[test]
    fn the_policy_off_splits_every_statement() {
        let (text, rolls_back) =
            RequestText::split_with_wrapper(&format!("BEGIN; {INNER}; COMMIT"), false)
                .expect("the split itself never refuses");
        assert!(!rolls_back);
        assert_eq!(several(text).len(), 4);
    }

    /// A request of one statement plans whole, policy on.
    #[test]
    fn one_statement_alone_plans_whole() {
        for sql in ["BEGIN", "COMMIT", "SAVEPOINT s"] {
            let (text, rolls_back) =
                RequestText::split_with_wrapper(sql, true).expect("one statement splits");
            assert!(!rolls_back);
            assert!(matches!(text, RequestText::Whole { .. }), "{sql}");
        }
    }
}
