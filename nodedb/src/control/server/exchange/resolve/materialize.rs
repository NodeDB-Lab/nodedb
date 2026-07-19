// SPDX-License-Identifier: BUSL-1.1

//! Pass 1 of plan resolution: catalog provider materialization.
//!
//! [`materialize_providers`] walks the plan tree and replaces every
//! `QueryOp::ProviderScan { provider: Some(name), rows: [] }` with a
//! fully-populated `ProviderScan { provider: None, rows: <encoded> }`.
//!
//! This pass happens per-request, post-cache, so identity-scoped catalog rows
//! never enter the plan cache.

use nodedb_physical::physical_plan::{ExchangeOp, PhysicalPlan, QueryOp};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::catalog;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::encode_binary_rows;

/// Walk `plan` and replace every `ProviderScan{provider: Some(name), rows: []}`
/// with `ProviderScan{provider: None, rows: <encoded>}`.
///
/// The walk is structural: it recurses into `HashJoin` inputs and
/// `LateralTopK`/`LateralLoop` outer plans, plus `Exchange` children so that
/// catalog providers nested inside Exchange children are also filled before the
/// Exchange itself is resolved.
pub(super) async fn materialize_providers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    plan: PhysicalPlan,
) -> crate::Result<PhysicalPlan> {
    match plan {
        PhysicalPlan::Query(QueryOp::ProviderScan {
            provider: Some(name),
            rows: _,
            filters,
            projection,
            sort_keys,
            limit,
            offset,
            distinct,
        }) => {
            let rows = catalog::catalog_rows(&name, state, identity).await?;
            let encoded = encode_binary_rows(&rows);
            Ok(PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: encoded,
                filters,
                projection,
                sort_keys,
                limit,
                offset,
                distinct,
            }))
        }

        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp { child, mode })) => {
            let child = Box::pin(materialize_providers(state, identity, *child)).await?;
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
            having,
            limit,
            sub_group_by,
            sub_aggregates,
            grouping_sets,
            sort_keys,
        }) => {
            let input = Box::pin(materialize_providers(state, identity, *input)).await?;
            Ok(PhysicalPlan::Query(QueryOp::Aggregate {
                collection,
                input: Some(Box::new(input)),
                group_by,
                aggregates,
                filters,
                having,
                limit,
                sub_group_by,
                sub_aggregates,
                grouping_sets,
                sort_keys,
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
            post_group_by,
            post_aggregates,
            projection,
            computed_projection,
            join_filters,
            post_filters,
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
        }) => {
            let left_input = match left_input {
                Some(p) => Some(Box::new(
                    Box::pin(materialize_providers(state, identity, *p)).await?,
                )),
                None => None,
            };
            let right_input = match right_input {
                Some(p) => Some(Box::new(
                    Box::pin(materialize_providers(state, identity, *p)).await?,
                )),
                None => None,
            };
            let left_bitmap = match left_bitmap {
                Some(p) => Some(Box::new(
                    Box::pin(materialize_providers(state, identity, *p)).await?,
                )),
                None => None,
            };
            let right_bitmap = match right_bitmap {
                Some(p) => Some(Box::new(
                    Box::pin(materialize_providers(state, identity, *p)).await?,
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
                post_group_by,
                post_aggregates,
                projection,
                computed_projection,
                join_filters,
                post_filters,
                left_input,
                right_input,
                left_bitmap,
                right_bitmap,
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
        }) => {
            let outer_plan = Box::pin(materialize_providers(state, identity, *outer_plan)).await?;
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
        }) => {
            let outer_plan = Box::pin(materialize_providers(state, identity, *outer_plan)).await?;
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
            }))
        }

        // All other variants: no catalog providers can be nested here —
        // pass through unchanged.
        other => Ok(other),
    }
}
