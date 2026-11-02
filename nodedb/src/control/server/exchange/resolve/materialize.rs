// SPDX-License-Identifier: BUSL-1.1

//! Pass 1 of plan resolution: provider materialization.
//!
//! [`materialize_providers`] walks the plan tree and fills the rows of every
//! `QueryOp::ProviderScan { provider: Some(name), rows: [] }`. A continuous
//! aggregate's rows come from the core that owns its root source. Every
//! other name is a catalog table.
//!
//! The filled scan keeps its relation name. The task is authorized again
//! after resolution, and that check must demand Read on the relation the
//! rows came from, as the check before resolution did. Without the name, the
//! scan reads as a constant SELECT, which demands tenant-wide Read, so a user
//! granted Read on only that relation is refused. The Data Plane reads only
//! the rows.
//!
//! This pass happens per-request, post-cache, so identity-scoped catalog rows
//! never enter the plan cache.

use nodedb_physical::physical_plan::{ExchangeOp, PhysicalPlan, QueryOp};
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::catalog;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::encode_binary_rows;

/// Walk `plan` and fill the rows of every `ProviderScan{provider: Some(name)}`,
/// keeping `name`.
///
/// The walk is structural: it recurses into `HashJoin` inputs and
/// `LateralTopK`/`LateralLoop` outer plans, plus `Exchange` children so that
/// catalog providers nested inside Exchange children are also filled before the
/// Exchange itself is resolved.
///
/// Collection relations list the collections of `database_id`, the database
/// the read runs in.
pub(super) async fn materialize_providers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    plan: PhysicalPlan,
) -> crate::Result<PhysicalPlan> {
    match plan {
        PhysicalPlan::Query(QueryOp::ProviderScan {
            provider: Some(name),
            rows: _,
            sources,
            filters,
            projection,
            computed_columns,
            window_functions,
            sort_keys,
            limit,
            offset,
            distinct,
            system_time,
        }) => {
            let rows = match super::cagg_rows::continuous_aggregate_rows(
                state,
                identity,
                database_id,
                &name,
                system_time,
            )
            .await?
            {
                Some(rows) => rows,
                None => catalog::catalog_rows(&name, state, identity, database_id).await?,
            };
            let encoded = encode_binary_rows(&rows);
            Ok(PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: Some(name),
                rows: encoded,
                sources,
                filters,
                projection,
                computed_columns,
                window_functions,
                sort_keys,
                limit,
                offset,
                distinct,
                system_time,
            }))
        }

        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp { child, mode })) => {
            let child =
                Box::pin(materialize_providers(state, identity, database_id, *child)).await?;
            Ok(PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
                child: Box::new(child),
                mode,
            })))
        }

        // Aggregate over a sub-plan (catalog): recurse so the nested
        // `ProviderScan{provider: Some(name)}` gets its identity-scoped rows
        // filled per-request before the aggregate runs.
        PhysicalPlan::Query(QueryOp::Aggregate {
            collection,
            input: Some(input),
            group_by,
            aggregates,
            filters,
            system_time,
            having,
            limit,
            sub_group_by,
            sub_aggregates,
            grouping_sets,
            sort_keys,
            group_key_aliases,
        }) => {
            let input =
                Box::pin(materialize_providers(state, identity, database_id, *input)).await?;
            Ok(PhysicalPlan::Query(QueryOp::Aggregate {
                collection,
                input: Some(Box::new(input)),
                group_by,
                aggregates,
                filters,
                system_time,
                having,
                limit,
                sub_group_by,
                sub_aggregates,
                grouping_sets,
                sort_keys,
                group_key_aliases,
            }))
        }

        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection,
            right_collection,
            left_alias,
            right_alias,
            on,
            join_type,
            limit,
            projection,
            computed_projection,
            join_filters,
            post_filters,
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
            left_rls_filters,
            right_rls_filters,
            left_scan_filters,
            right_scan_filters,
        }) => {
            let left_input = match left_input {
                Some(p) => Some(Box::new(
                    Box::pin(materialize_providers(state, identity, database_id, *p)).await?,
                )),
                None => None,
            };
            let right_input = match right_input {
                Some(p) => Some(Box::new(
                    Box::pin(materialize_providers(state, identity, database_id, *p)).await?,
                )),
                None => None,
            };
            let left_bitmap = match left_bitmap {
                Some(p) => Some(Box::new(
                    Box::pin(materialize_providers(state, identity, database_id, *p)).await?,
                )),
                None => None,
            };
            let right_bitmap = match right_bitmap {
                Some(p) => Some(Box::new(
                    Box::pin(materialize_providers(state, identity, database_id, *p)).await?,
                )),
                None => None,
            };
            Ok(PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection,
                right_collection,
                left_alias,
                right_alias,
                on,
                join_type,
                limit,
                projection,
                computed_projection,
                join_filters,
                post_filters,
                left_input,
                right_input,
                left_bitmap,
                right_bitmap,
                left_rls_filters,
                right_rls_filters,
                left_scan_filters,
                right_scan_filters,
            }))
        }

        PhysicalPlan::Query(QueryOp::LateralTopK {
            outer_plan,
            outer_alias,
            inner_collection,
            inner_filters,
            inner_order_by,
            inner_limit,
            correlation_keys,
            lateral_alias,
            projection,
            left_join,
            inner_system_time,
        }) => {
            let outer_plan = Box::pin(materialize_providers(
                state,
                identity,
                database_id,
                *outer_plan,
            ))
            .await?;
            Ok(PhysicalPlan::Query(QueryOp::LateralTopK {
                outer_plan: Box::new(outer_plan),
                outer_alias,
                inner_collection,
                inner_filters,
                inner_order_by,
                inner_limit,
                correlation_keys,
                lateral_alias,
                projection,
                left_join,
                inner_system_time,
            }))
        }

        PhysicalPlan::Query(QueryOp::LateralLoop {
            outer_plan,
            outer_alias,
            inner_collection,
            inner_filters,
            correlation_predicates,
            lateral_alias,
            projection,
            left_join,
            outer_row_cap,
            inner_system_time,
        }) => {
            let outer_plan = Box::pin(materialize_providers(
                state,
                identity,
                database_id,
                *outer_plan,
            ))
            .await?;
            Ok(PhysicalPlan::Query(QueryOp::LateralLoop {
                outer_plan: Box::new(outer_plan),
                outer_alias,
                inner_collection,
                inner_filters,
                correlation_predicates,
                lateral_alias,
                projection,
                left_join,
                outer_row_cap,
                inner_system_time,
            }))
        }

        // PostProcess: recurse into the materialized child so any catalog
        // providers nested in the subquery body are filled before resolution.
        PhysicalPlan::Query(QueryOp::PostProcess {
            input,
            filters,
            projection,
            computed_columns,
            window_functions,
            sort_keys,
            limit,
            offset,
            distinct,
        }) => {
            let input =
                Box::pin(materialize_providers(state, identity, database_id, *input)).await?;
            Ok(PhysicalPlan::Query(QueryOp::PostProcess {
                input: Box::new(input),
                filters,
                projection,
                computed_columns,
                window_functions,
                sort_keys,
                limit,
                offset,
                distinct,
            }))
        }

        // SetOp: recurse into every branch so nested catalog providers are
        // filled before the set-op resolver materializes the branches.
        PhysicalPlan::Query(QueryOp::SetOp { inputs, op }) => {
            let mut filled = Vec::with_capacity(inputs.len());
            for input in inputs {
                filled.push(
                    Box::pin(materialize_providers(state, identity, database_id, input)).await?,
                );
            }
            Ok(PhysicalPlan::Query(QueryOp::SetOp { inputs: filled, op }))
        }

        // All other variants: no catalog providers can be nested here —
        // pass through unchanged.
        other => Ok(other),
    }
}

/// `task` with the rows of every provider relation it reads filled for
/// `identity`.
///
/// A transport calls this before it sends a task through the gateway. The
/// node that runs a forwarded task has no identity to fill the rows with,
/// so a provider scan that leaves here empty answers no rows there.
pub(crate) async fn materialize_task_providers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    mut task: PhysicalTask,
) -> crate::Result<PhysicalTask> {
    task.plan = materialize_providers(state, identity, task.database_id, task.plan).await?;
    Ok(task)
}
