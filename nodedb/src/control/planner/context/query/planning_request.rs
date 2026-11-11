// SPDX-License-Identifier: BUSL-1.1

//! Planning of a request that holds one or more statements.
//!
//! Each statement plans alone through the single-statement path. The task
//! lists join into one set, in statement order. A [`StatementMap`] records
//! the statement of each task.

use nodedb_physical::physical_task::PhysicalTask;
use nodedb_sql::parser::preprocess::lex::first_sql_word;
use nodedb_sql::types::PlanCacheEligibility;

use super::QueryContext;
use crate::control::planner::context::security::PlanSecurityContext;
use crate::control::planner::descriptor_set::DescriptorVersionSet;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::shared::ddl::neutral::router;
use crate::control::server::shared::returning::strip_returning;
use crate::control::server::shared::statement_exec::StatementMap;

/// The joined plan of every statement of one request.
#[derive(Debug)]
pub struct RequestPlan {
    /// Every statement's tasks, in statement order.
    pub tasks: Vec<PhysicalTask>,
    /// The statement of each task in `tasks`, one entry per task.
    pub statement_map: StatementMap,
    /// One output schema per statement. Index `k` is statement `k`.
    pub output_schemas: Vec<OutputSchema>,
    /// The descriptor and policy versions of every statement, merged.
    pub versions: DescriptorVersionSet,
    /// `DataDependent` when any statement is not cacheable.
    pub cache_eligibility: PlanCacheEligibility,
}

impl QueryContext {
    /// Plan each statement of one request alone and join the tasks into one
    /// set.
    ///
    /// `statements` is the request text split at top-level semicolons.
    /// Statement `k` splits off its own `RETURNING` clause. It then plans
    /// through [`Self::plan_sql_with_rls_and_versions`], the single-statement
    /// path. So its tasks take that path's row filters, policy checks,
    /// RETURNING spec and output schema. The map holds `k` once per task of
    /// statement `k`.
    ///
    /// Each statement plans against the committed catalog. A later statement
    /// never sees what an earlier one changes. So a request of several
    /// statements that holds DDL, transaction control or a session command
    /// returns `FeatureNotSupported` (SQLSTATE `0A000`). A statement the DDL
    /// router runs, such as `UPSERT .. VALUES` or `SELECT KV_INCR(..)`, takes
    /// the same refusal: `plan_sql` skips the router's own steps. A request
    /// of one statement is never refused here. A request of no statement
    /// plans no task.
    pub async fn plan_request_with_rls_and_versions(
        &self,
        statements: &[String],
        tenant_id: crate::types::TenantId,
        database_id: crate::types::DatabaseId,
        sec: &PlanSecurityContext<'_>,
    ) -> crate::Result<RequestPlan> {
        refuse_solo_statements(statements)?;
        let mut tasks = Vec::new();
        let mut statement_map = StatementMap::default();
        let mut output_schemas = Vec::with_capacity(statements.len());
        let mut versions: Option<DescriptorVersionSet> = None;
        let mut cache_eligibility = PlanCacheEligibility::Cacheable;
        for (index, statement) in statements.iter().enumerate() {
            let map_index = statement_index(index)?;
            // Split off this statement alone. A clause split off the joined
            // text swallows every later statement into its item list.
            let (sql, returning_items) = strip_returning(statement)?;
            let (statement_tasks, output_schema, statement_versions, eligibility) = self
                .plan_sql_with_rls_and_versions(
                    &sql,
                    tenant_id,
                    database_id,
                    sec,
                    returning_items.as_deref(),
                )
                .await?;
            for _ in 0..statement_tasks.len() {
                statement_map.push(map_index);
            }
            tasks.extend(statement_tasks);
            output_schemas.push(output_schema);
            cache_eligibility = join_cache_eligibility(cache_eligibility, eligibility);
            versions = Some(match versions.take() {
                None => statement_versions,
                Some(mut joined) => {
                    joined.merge_statement(&statement_versions)?;
                    joined
                }
            });
        }
        statement_map.debug_check_covers(&tasks);
        Ok(RequestPlan {
            tasks,
            statement_map,
            output_schemas,
            versions: versions.unwrap_or_default(),
            cache_eligibility,
        })
    }
}

/// The index of statement `index` as a [`StatementMap`] entry.
fn statement_index(index: usize) -> crate::Result<u32> {
    u32::try_from(index).map_err(|_| crate::Error::BadRequest {
        detail: format!(
            "a request holds more than {} statements. Split it into smaller requests.",
            u32::MAX
        ),
    })
}

/// `Cacheable` only when both inputs are `Cacheable`.
pub(super) fn join_cache_eligibility(
    left: PlanCacheEligibility,
    right: PlanCacheEligibility,
) -> PlanCacheEligibility {
    if left.is_cacheable() && right.is_cacheable() {
        PlanCacheEligibility::Cacheable
    } else {
        PlanCacheEligibility::DataDependent
    }
}

/// A statement kind that plans only as a request of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SoloKind {
    /// DDL, or another command the DDL router runs.
    Ddl,
    /// `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT` and their spellings.
    TransactionControl,
    /// `SET`, `SHOW` or `RESET`.
    SessionCommand,
    /// A statement the DDL router runs itself, by `router::claims`. Its
    /// router steps, such as field inference, never run in `plan_sql`.
    Routed,
}

impl SoloKind {
    fn describe(self) -> &'static str {
        match self {
            Self::Ddl => "DDL or an engine command",
            Self::TransactionControl => "transaction control",
            Self::SessionCommand => "a session command",
            Self::Routed => "a statement the DDL router runs",
        }
    }

    /// The rule a request of several statements breaks with this kind.
    fn rule(self) -> &'static str {
        match self {
            Self::Routed => {
                "A request of several statements plans each statement without the DDL \
                 router, so it skips the router's own steps."
            }
            Self::Ddl | Self::TransactionControl | Self::SessionCommand => {
                "A request of several statements can hold only DML and queries."
            }
        }
    }
}

/// Every first word of a transaction-control statement.
///
/// `txn_control::classify` returns `None` for a spelling outside its
/// grammar, such as `COMMIT AND CHAIN`. The refusal must catch those too, so
/// it reads the first word alone.
const TRANSACTION_CONTROL_HEADS: &[&str] = &[
    "BEGIN",
    "START",
    "COMMIT",
    "END",
    "ROLLBACK",
    "ABORT",
    "SAVEPOINT",
    "RELEASE",
];

/// Every first word of a session command.
const SESSION_COMMAND_HEADS: &[&str] = &["SET", "SHOW", "RESET"];

/// First words of DDL. They catch a DDL statement that
/// `nodedb_sql::ddl_ast::parse` does not claim.
const DDL_HEADS: &[&str] = &["CREATE", "ALTER", "DROP", "GRANT", "REVOKE"];

/// The solo kind of `statement`, or `None` for DML and queries.
///
/// The first word is read after whitespace and comments. A statement the
/// typed DDL parser claims is DDL, whatever its first word. Any other
/// statement `router::claims` takes is [`SoloKind::Routed`].
pub(super) fn solo_kind(statement: &str) -> Option<SoloKind> {
    let head = first_sql_word(statement)
        .map(str::to_ascii_uppercase)
        .unwrap_or_default();
    let head = head.as_str();
    if TRANSACTION_CONTROL_HEADS.contains(&head) {
        return Some(SoloKind::TransactionControl);
    }
    if SESSION_COMMAND_HEADS.contains(&head) {
        return Some(SoloKind::SessionCommand);
    }
    if DDL_HEADS.contains(&head) || nodedb_sql::ddl_ast::parse(statement).is_some() {
        return Some(SoloKind::Ddl);
    }
    if router::claims(statement) {
        return Some(SoloKind::Routed);
    }
    None
}

/// Refuse a request of several statements that holds a solo statement.
///
/// A request of one statement always passes. The error names the first solo
/// statement and tells the client to send it as its own request.
pub(super) fn refuse_solo_statements(statements: &[String]) -> crate::Result<()> {
    if statements.len() < 2 {
        return Ok(());
    }
    for (index, statement) in statements.iter().enumerate() {
        let Some(kind) = solo_kind(statement) else {
            continue;
        };
        let head = first_sql_word(statement).unwrap_or_default();
        return Err(crate::Error::FeatureNotSupported {
            detail: format!(
                "statement {} of {} ({head}) is {}. {} Send this statement as its own request.",
                index + 1,
                statements.len(),
                kind.describe(),
                kind.rule(),
            ),
        });
    }
    Ok(())
}
