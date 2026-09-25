// SPDX-License-Identifier: BUSL-1.1

//! Shared in-transaction dispatch path plus the SQL-text argument-parsing and
//! response-shaping helpers reused across the KV DDL families in this
//! directory (`handlers` here, and the sibling `kv_sorted_index`,
//! `weighted_pick`, `rate_gate`, `transfer` modules).

use std::sync::Arc;

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::Status;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::super::result::{DdlError, DdlResult};
use super::super::read_gate::CollectionReadGate;

/// Dispatch a KvOp through the protocol-neutral in-transaction staging gate
/// and return the JSON response as a single text-column row keyed by the
/// lower-cased function name.
///
/// Outside a transaction block the gate answers `Autocommit`, and the op takes
/// the durable route every planned autocommit write takes: proposed through
/// Raft in cluster mode, otherwise the write funnel with `AppendHere`. Either
/// way a WAL record reproduces the value the op computed, and a replica
/// applies it.
///
/// Inside a transaction, `KvOp::Incr` / `IncrFloat` / `Cas` / `GetSet` /
/// `Transfer` / `TransferItem` are staged into the per-transaction overlay
/// (`is_stageable_write`). This function reads the computed value back from
/// `StagedWriteOutcome::payload`, so a `SELECT KV_INCR(...)` inside
/// `BEGIN..COMMIT` returns the value the staged overlay now holds, and a
/// following `SELECT KV_INCR(...)` on the same key chains off it.
///
/// `collections` names every collection the op touches, in the caller's own
/// words rather than read back out of the plan: `TRANSFER_ITEM` touches two.
/// Each is authorized here before the op is routed anywhere.
///
/// Reused by the sibling `transfer.rs` module for `TRANSFER` /
/// `TRANSFER_ITEM`.
pub(crate) async fn dispatch_and_respond(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    vshard: VShardId,
    mut plan: PhysicalPlan,
    func_name: &str,
    collections: &[&str],
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    use crate::control::server::shared::session::staging_gate::{
        InTxnRoute, StagingGateError, route_in_tx_write,
    };

    let tenant_id = identity.tenant_id;
    let database_id = DatabaseId::DEFAULT;

    // Every caller here names its collections in the SQL text and reaches the
    // Data Plane through a hand-built `KvOp`. The op reports the value it
    // replaced or computed, so it is a read as much as a write and needs both
    // grants — and a cross-collection move needs them on each side, hence the
    // slice.
    let gate = CollectionReadGate::for_request(state, identity, database_id);
    for collection in collections {
        gate.authorize(collection)?;
        gate.authorize_permission(collection, Permission::Write)?;
    }

    // Row-level security is resolved by the same injection pass the
    // planner-driven path runs, against the very same op. These functions
    // build the identical `KvOp`s a planned statement builds. A local refusal
    // here while the planner enforced, or the reverse, gives the same
    // operation two answers that depend on the syntax that reached it. The pass compiles the write policy into the op's own gate
    // slot for the Data Plane to decide the image against, injects the read
    // filter where the reply is a row body, and refuses outright where the
    // reply is a value computed from a row the policy hides.
    gate.inject_rls(&mut plan)?;

    let task = PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };

    let routed = route_in_tx_write(
        state,
        txn_ctx.sessions,
        txn_ctx.session_id,
        task,
        |staged| {
            crate::control::server::dispatch_utils::dispatch_to_data_plane_with_txn(
                state,
                staged.tenant_id,
                staged.database_id,
                staged.vshard_id,
                staged.plan,
                TraceId::ZERO,
                staged.txn_id,
            )
        },
    )
    .await;

    let payload = match routed {
        Ok(InTxnRoute::Autocommit(task)) => dispatch_autocommit(state, identity, *task).await?,
        Ok(InTxnRoute::Staged(outcome)) => outcome.payload,
        // Every `KvOp` this module builds is a write, and a stageable one once
        // in a transaction (`is_stageable_write`). A read route has no
        // durable apply, and a buffered route has no value to answer with, so
        // either one is a classification break, never an answer.
        Ok(InTxnRoute::Read(_)) => {
            return Err(ddl_err(
                "XX000",
                format!("{func_name}: the staging gate classified this write as a read"),
            ));
        }
        Ok(InTxnRoute::Buffered) => {
            return Err(ddl_err(
                "XX000",
                format!(
                    "{func_name}: the staging gate buffered this write, so it has no value to \
                     return at the statement"
                ),
            ));
        }
        Err(StagingGateError::Dispatch(e)) => return Err(ddl_err("XX000", e.to_string())),
        Err(StagingGateError::Rejected { code }) => return Err(data_plane_error(code)),
    };

    let payload_text = crate::data::executor::response_codec::decode_payload_to_json(&payload);
    let col_name = func_name.to_lowercase();
    Ok(vec![single_text_col(&col_name, payload_text)])
}

/// Apply one autocommit op on the durable route and return its payload.
///
/// The task passes the same clone-write gate and authorization every
/// transport runs before a write dispatches.
async fn dispatch_autocommit(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    task: PhysicalTask,
) -> Result<Vec<u8>, DdlError> {
    use crate::control::server::shared::clone_write::{
        CloneCheckedOutcome, InterceptAndAuthorizeParams, intercept_and_authorize,
    };

    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    let outcome = intercept_and_authorize(InterceptAndAuthorizeParams {
        state,
        task,
        identity,
        tenant_id: identity.tenant_id,
        permissions: &state.permissions,
        roles: &state.roles,
        emitter: &emitter,
    })
    .await
    .map_err(|e| error_to_ddl(&e))?;
    let response = match outcome {
        CloneCheckedOutcome::Handled(resp) => resp,
        CloneCheckedOutcome::Proceed(checked) => {
            crate::control::server::dispatch_utils::dispatch_authorized_durable_write(
                state,
                checked,
                TraceId::ZERO,
            )
            .await
            .map_err(|e| error_to_ddl(&e))?
        }
    };
    // A refused write comes back as `Ok(Response)` carrying `Status::Error`:
    // the funnel reports the dispatch as successful and puts the verdict in the
    // response. Its payload is empty, so an unchecked forward answers
    // `SELECT KV_INCR(...)` with one blank column. Every terminal outcome these
    // functions can produce arrives this way on the local route — a policy
    // refusal, a type mismatch, an overflow, an insufficient balance, a
    // missing key — so the status decides, never the payload's emptiness.
    if response.status == Status::Error {
        return Err(data_plane_error(response.error_code.map(|code| *code)));
    }
    Ok(response.payload.as_ref().to_vec())
}

/// Map a dispatch error to the client-facing error. A Data-Plane verdict
/// arrives here as `Error::DataPlane` on the replicated route, and it renders
/// the same way the local route's error status does.
fn error_to_ddl(error: &crate::Error) -> DdlError {
    match error {
        crate::Error::DataPlane(code) => data_plane_error(Some(code.clone())),
        other => {
            let (_, sqlstate, message) =
                crate::control::server::pgwire::types::error_to_sqlstate(other);
            ddl_err(sqlstate, message)
        }
    }
}

/// Translate a terminal Data-Plane verdict into the client-facing error.
///
/// Shared by both routes a refusal can arrive on — the staging gate's
/// `Rejected`, and an `Ok(Response)` whose status is `Error` — so the same
/// verdict never reaches the client as two different messages depending on
/// whether the statement ran inside a transaction.
///
/// `None` means the Data Plane reported a failure without a code, which is a
/// bug in whatever produced it; it is surfaced as an internal error rather than
/// silently downgraded to success.
fn data_plane_error(code: Option<crate::bridge::envelope::ErrorCode>) -> DdlError {
    let Some(code) = code else {
        return ddl_err("XX000", "unknown data plane error");
    };
    let (_, sqlstate, message) =
        crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(&code);
    // The public error carries the code a client classifies by and the
    // details naming the collection, which the SQLSTATE alone cannot give.
    let public = nodedb_types::NodeDbError::from(crate::Error::DataPlane(code));
    DdlError::from_public(sqlstate, message, &public)
}

/// Build a single-text-column row set carrying `text` under `col`.
pub(crate) fn single_text_col(col: &str, text: String) -> DdlResult {
    let mut row = Map::new();
    row.insert(col.to_string(), JsonValue::String(text));
    DdlResult::Rows(ShapedRows::text_rows(vec![col.to_string()], vec![row]))
}

/// Parse function arguments from `SELECT FUNC_NAME(arg1, arg2, ...)`.
///
/// Handles quoted strings with commas inside them.
pub(crate) fn parse_function_args(sql: &str, _func_name: &str) -> Result<Vec<String>, DdlError> {
    let start = sql
        .find('(')
        .ok_or_else(|| ddl_err("42601", "expected '(' in function call"))?;
    let end = sql
        .rfind(')')
        .ok_or_else(|| ddl_err("42601", "expected ')' in function call"))?;
    if start >= end {
        return Ok(Vec::new());
    }

    let inner = &sql[start + 1..end];
    Ok(split_args(inner))
}

/// Split comma-separated arguments, respecting single-quoted strings.
///
/// Handles SQL-standard escaped quotes: `''` inside a quoted string becomes `'`.
/// Example: `'O''Reilly'` → `O'Reilly`.
pub(crate) fn split_args(s: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut chars = s.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' if !in_quote => {
                in_quote = true;
                current.push(ch);
            }
            '\'' if in_quote => {
                // Check if next char is also ' (escaped quote).
                if chars.peek() == Some(&'\'') {
                    chars.next(); // Consume the second '.
                    current.push('\''); // Keep one ' in the output.
                    current.push('\'');
                } else {
                    in_quote = false;
                    current.push(ch);
                }
            }
            ',' if !in_quote => {
                args.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        args.push(trimmed);
    }
    args
}

/// Remove surrounding single quotes from a string argument.
pub(crate) fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

/// Parse an i64 from a string argument.
pub(crate) fn parse_i64(s: &str, func_name: &str) -> Result<i64, DdlError> {
    s.trim().parse().map_err(|_| {
        ddl_err(
            "42601",
            format!("{func_name}: delta must be an integer, got '{}'", s.trim()),
        )
    })
}

/// Parse optional `TTL => seconds` from remaining args.
///
/// Supports: `TTL => 86400` or just a bare number as the 4th arg.
pub(crate) fn parse_optional_ttl(args: &[String]) -> Result<u64, DdlError> {
    if args.is_empty() {
        return Ok(0);
    }

    // Check for `TTL => value` pattern.
    for (i, arg) in args.iter().enumerate() {
        let upper = arg.trim().to_uppercase();
        if upper.starts_with("TTL") {
            // Formats: "TTL => 86400" or split across args: "TTL", "=>", "86400"
            if let Some(val_str) = upper
                .strip_prefix("TTL")
                .map(|r| r.trim_start_matches("=>").trim_start_matches('=').trim())
                && !val_str.is_empty()
            {
                return parse_ttl_seconds(val_str);
            }
            // Look at next arg(s) for the value.
            let remaining: Vec<&str> = args[i + 1..].iter().map(|s| s.trim()).collect();
            for r in &remaining {
                let cleaned = r.trim_start_matches("=>").trim_start_matches('=').trim();
                if !cleaned.is_empty() {
                    return parse_ttl_seconds(cleaned);
                }
            }
        }
    }

    Ok(0)
}

/// Parse TTL seconds → milliseconds.
fn parse_ttl_seconds(s: &str) -> Result<u64, DdlError> {
    let secs: u64 = s.parse().map_err(|_| {
        ddl_err(
            "42601",
            format!("TTL must be a positive integer (seconds), got '{s}'"),
        )
    })?;
    Ok(secs * 1000)
}

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
pub(crate) fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}
