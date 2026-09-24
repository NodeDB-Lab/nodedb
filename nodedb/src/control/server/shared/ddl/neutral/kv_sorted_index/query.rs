// SPDX-License-Identifier: BUSL-1.1

//! The sorted-index read functions: `RANK`, `TOPK`, `RANGE`, `SORTED_COUNT`.
//!
//! Each names an index and returns keys, ranks, or counts drawn from the
//! collection it was built over, so each resolves that collection and gates on
//! it before a plan is built (see [`super::gate`]). The native sorted-index
//! read opcodes run through [`run_read`] too, so both protocols gate, route and
//! see the caller's transaction the same way.

use crate::bridge::envelope::Response;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::{PhysicalPlan, SortedIndexRead};

use super::super::super::result::{DdlError, DdlResult};
use super::dispatch::{SortedIndexTarget, dispatch_read, respond_json, respond_rows};
use super::gate::gate_read;
use super::parse::{ddl_err, parse_function_args, parse_score_arg, unquote};
use super::txn_read::{ReadScope, plan_read};

/// What a read delivers instead of row bodies, for the refusal message.
fn what(read: &SortedIndexRead) -> &'static str {
    match read {
        SortedIndexRead::Rank { .. } => {
            "RANK(), which returns a position in the sorted index rather than rows"
        }
        SortedIndexRead::TopK { .. } => {
            "TOPK(), which returns the sorted index's ranked keys rather than rows"
        }
        SortedIndexRead::Range { .. } => {
            "RANGE(), which returns the sorted index's ranked keys rather than rows"
        }
        SortedIndexRead::Count => {
            "SORTED_COUNT(), which returns a count over the sorted index rather than rows"
        }
        SortedIndexRead::Score { .. } => {
            "a sorted-index score read, which returns a sort key rather than rows"
        }
    }
}

/// Gate, plan and dispatch one sorted-index read.
///
/// Gated on the index's owning collection, routed to the core that holds its
/// rows, and run in the caller's transaction when one is open. Returns the
/// plan that ran and the Data Plane reply.
pub(crate) async fn run_read(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    txn_ctx: &DmlTxnCtx<'_>,
    index_name: &str,
    read: SortedIndexRead,
) -> Result<(PhysicalPlan, Response), DdlError> {
    let collection = gate_read(state, identity, database_id, index_name, what(&read))?;
    let sorted = plan_read(
        &ReadScope {
            txn_ctx,
            tenant_id: identity.tenant_id,
            database_id,
            collection: &collection,
            index_name,
        },
        read,
    );
    let plan = sorted.plan.clone();
    let response = dispatch_read(
        state,
        &SortedIndexTarget {
            tenant_id: identity.tenant_id,
            database_id,
            collection: &collection,
        },
        sorted,
    )
    .await?;
    Ok((plan, response))
}

/// Handle `SELECT RANK(index_name, 'key_value')`
pub async fn select_rank(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql)?;
    if args.len() < 2 {
        return Err(ddl_err(
            "42601",
            "RANK requires 2 arguments: (index_name, key_value)",
        ));
    }

    // A string literal is data: the index name resolves exactly as written,
    // with no case folding.
    let index_name = unquote(&args[0]);
    let primary_key = unquote(&args[1]).into_bytes();

    let read = SortedIndexRead::Rank { primary_key };
    let (_, response) = run_read(state, identity, database_id, txn_ctx, &index_name, read).await?;
    Ok(respond_json(&response, "rank"))
}

/// Handle `SELECT * FROM TOPK(index_name, k)` or `SELECT TOPK(index_name, k)`
pub async fn select_topk(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql)?;
    if args.len() < 2 {
        return Err(ddl_err(
            "42601",
            "TOPK requires 2 arguments: (index_name, k)",
        ));
    }

    // A string literal is data: the index name resolves exactly as written,
    // with no case folding.
    let index_name = unquote(&args[0]);
    let k: u32 = args[1].trim().parse().map_err(|_| {
        ddl_err(
            "42601",
            format!("TOPK: k must be a positive integer, got '{}'", args[1]),
        )
    })?;

    let read = SortedIndexRead::TopK { k };
    let (_, response) = run_read(state, identity, database_id, txn_ctx, &index_name, read).await?;
    respond_rows(&response)
}

/// Handle `SELECT * FROM RANGE(index_name, score_min, score_max)`
pub async fn select_range(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql)?;
    if args.len() < 3 {
        return Err(ddl_err(
            "42601",
            "RANGE requires 3 arguments: (index_name, score_min, score_max)",
        ));
    }

    // A string literal is data: the index name resolves exactly as written,
    // with no case folding.
    let index_name = unquote(&args[0]);
    let read = SortedIndexRead::Range {
        score_min: parse_score_arg(&args[1]),
        score_max: parse_score_arg(&args[2]),
    };
    let (_, response) = run_read(state, identity, database_id, txn_ctx, &index_name, read).await?;
    respond_rows(&response)
}

/// Handle `SELECT SORTED_COUNT(index_name)`
pub async fn select_sorted_count(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql)?;
    if args.is_empty() {
        return Err(ddl_err(
            "42601",
            "SORTED_COUNT requires 1 argument: (index_name)",
        ));
    }

    // A string literal is data: the index name resolves exactly as written,
    // with no case folding.
    let index_name = unquote(&args[0]);
    let (_, response) = run_read(
        state,
        identity,
        database_id,
        txn_ctx,
        &index_name,
        SortedIndexRead::Count,
    )
    .await?;
    Ok(respond_json(&response, "sorted_count"))
}
