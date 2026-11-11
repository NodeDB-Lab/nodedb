// SPDX-License-Identifier: BUSL-1.1

//! The planning step of admission, for a request of one or several
//! statements.
//!
//! A request of one statement plans its whole text through the
//! single-statement path. A request of several statements plans each
//! statement alone through the request planner. Both shapes give one task
//! set and its statement map.

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::planner::context::{PlanSecurityContext, QueryContext};
use crate::control::planner::descriptor_set::DescriptorVersionSet;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::shared::returning::strip_returning;
use crate::control::server::shared::sql_split::split_sql_statements;
use crate::control::server::shared::statement_exec::StatementMap;
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

    /// Plan the request against the committed catalog.
    pub(super) async fn plan(
        self,
        query_ctx: &QueryContext,
        tenant_id: TenantId,
        database_id: DatabaseId,
        security: &PlanSecurityContext<'_>,
    ) -> crate::Result<PlannedRequest> {
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

/// The one schema a fold over the whole request reads: the schema of the
/// last statement that announces columns, else the first statement's.
///
/// A schema announces columns when it names a column or projects `*`. An
/// empty list gives the default schema.
pub(super) fn folded_output_schema(schemas: &[OutputSchema]) -> OutputSchema {
    schemas
        .iter()
        .rev()
        .find(|schema| !schema.columns.is_empty() || schema.is_star)
        .or_else(|| schemas.first())
        .cloned()
        .unwrap_or_default()
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
}
