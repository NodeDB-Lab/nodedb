// SPDX-License-Identifier: BUSL-1.1

//! One-shot read of a query source.
//!
//! The source plan runs through the same resolution a SELECT takes: catalog
//! and continuous-aggregate rows are materialized for the statement's
//! identity, Exchange nodes gather, and the remaining plan dispatches to the
//! owner of its collection. The Data Plane bounds the result by its scan byte
//! budget and fails loudly past it. The rows are then shaped by the same
//! materialized SELECT shaper every protocol uses.

use nodedb_physical::physical_plan::{QueryOp, QuerySource};

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::server::exchange::{ReadScope, Resolved, resolve_and_materialize};
use crate::control::server::response_shape::compose::{ShapeOutcome, shape_response_materialized};
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::types::describe_plan;
use crate::control::server::shared::response_payload::payload_or_typed_error;
use crate::types::TraceId;

use super::context::SourceReadContext;
use super::shape::RowShaper;

/// Read every row of `source`, shaped by its column map.
pub(crate) async fn read_query_rows(
    ctx: &SourceReadContext<'_>,
    source: &QuerySource,
) -> crate::Result<Vec<Vec<u8>>> {
    let response = run_source_plan(ctx, (*source.plan).clone()).await?;
    // A refused read keeps its Data-Plane code: an exhausted scan budget
    // reaches the client as the budget error.
    let payload = payload_or_typed_error(response)?;
    let shaped = shape_response_materialized(MaterializedShapeRequest {
        payload: &payload,
        plan: &source.plan,
        plan_kind: describe_plan(&source.plan),
        projection: None,
        state: ctx.state,
        database_id: ctx.database_id,
        tenant_id: ctx.tenant_id,
        redaction: None,
        sequences: None,
    })
    .await?;
    let ShapeOutcome::Rows(shaped) = shaped else {
        return Err(crate::Error::Internal {
            detail: format!(
                "source plan for '{}' answers with no rows",
                source.plan.collection().unwrap_or("<query>")
            ),
        });
    };
    let shaper = RowShaper::decode(&source.column_map)?;
    let crs = crate::control::crs_context::crs_context_for(ctx.state);
    shaped
        .rows
        .into_iter()
        .map(|row| {
            shaper.shape_value(
                &nodedb_types::Value::Object(row.into_iter().collect()),
                &crs,
            )
        })
        .collect()
}

/// Resolve and dispatch the source plan, returning one merged response.
async fn run_source_plan(
    ctx: &SourceReadContext<'_>,
    plan: PhysicalPlan,
) -> crate::Result<Response> {
    let scope = ReadScope {
        database_id: ctx.database_id,
        tenant_id: ctx.tenant_id,
        trace_id: TraceId::ZERO,
        txn_id: ctx.txn_id,
        linearizable: true,
    };
    match resolve_and_materialize(ctx.state, ctx.identity, plan, scope).await? {
        Resolved::Gathered(response, _shard_watermarks, _captures) => Ok(response),
        Resolved::Stream(stream) => {
            crate::control::server::exchange::gather::stream_to_response(stream).await
        }
        Resolved::Plan(plan) => {
            let routing = routing_collection(&plan);
            let vshard_id =
                nodedb_types::CollectionKey::from_qualified_str(ctx.database_id, routing)?.vshard();
            crate::control::server::dispatch_utils::dispatch_to_data_plane_with_txn(
                ctx.state,
                ctx.tenant_id,
                ctx.database_id,
                vshard_id,
                *plan,
                TraceId::ZERO,
                ctx.txn_id,
            )
            .await
        }
    }
}

/// The collection a resolved source plan routes by. A materialized provider
/// scan carries its rows and runs on the coordinator, as the converter
/// routes it.
fn routing_collection(plan: &PhysicalPlan) -> &str {
    match plan {
        PhysicalPlan::Query(QueryOp::ProviderScan { .. }) => "",
        other => other.collection().unwrap_or(""),
    }
}
