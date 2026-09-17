// SPDX-License-Identifier: BUSL-1.1

//! Shared in-transaction dispatch path plus the SQL-text argument-parsing and
//! response-shaping helpers reused across the KV DDL families in this
//! directory (`handlers` here, and the sibling `kv_sorted_index`,
//! `weighted_pick`, `rate_gate`, `transfer` modules).

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::Status;
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
/// Outside a transaction block (or for the read half of the gate, which
/// never applies here since every `KvOp` this module builds is a write),
/// `route_in_tx_write` dispatches immediately -- byte-identical to the
/// pre-staging behavior. Inside a transaction, `KvOp::Incr` / `IncrFloat` /
/// `Cas` / `GetSet` are staged into the per-transaction overlay
/// (`is_stageable_write`) and this function reads the computed value back
/// from `StagedWriteOutcome::payload` (forwarded verbatim by the staging
/// gate for `StagedTagKind::RawPayload`), so a `SELECT KV_INCR(...)` inside
/// `BEGIN..COMMIT` returns the same value the staged overlay now holds, and
/// a following `SELECT KV_INCR(...)` on the same key in the same
/// transaction chains off it.
///
/// `collections` names every collection the op touches, in the caller's own
/// words rather than read back out of the plan: these `KvOp`s carry no
/// collection the plan-classification helpers report, and `TRANSFER_ITEM`
/// touches two. Each is authorized here before the op is routed anywhere.
///
/// Reused by the sibling `transfer.rs` module for the identical
/// in-transaction routing for `TRANSFER` / `TRANSFER_ITEM` instead of the
/// direct `dispatch_to_data_plane` call it used before those two `KvOp`s
/// became stageable.
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
    // Data Plane through a hand-built `KvOp`, which carries no identity and is
    // never authorized downstream. The op reports the value it replaced or
    // computed, so it is a read as much as a write and needs both grants — and
    // a cross-collection move needs them on each side, hence the slice.
    let gate = CollectionReadGate::for_request(state, identity, database_id);
    for collection in collections {
        gate.authorize(collection)?;
        gate.authorize_permission(collection, Permission::Write)?;
    }

    // Row-level security is resolved by the same injection pass the
    // planner-driven path runs, against the very same op. That is the point of
    // routing it through here rather than restating a verdict locally: these
    // functions build the identical `KvOp`s a planned statement builds, so a
    // local refusal here while the planner enforced — or the reverse — would
    // give the same operation two different answers depending on which syntax
    // reached it. The pass compiles the write policy into the op's own gate
    // slot for the Data Plane to decide the image against, injects the read
    // filter where the reply is a row body, and still refuses outright where
    // the reply is a value computed from a row the policy hides.
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
        Ok(InTxnRoute::Read(task)) => {
            let task = *task;
            match crate::control::server::dispatch_utils::dispatch_to_data_plane_with_txn(
                state,
                task.tenant_id,
                task.database_id,
                task.vshard_id,
                task.plan,
                TraceId::ZERO,
                task.txn_id,
            )
            .await
            {
                // A refused write comes back as `Ok(Response)` carrying
                // `Status::Error` — `submit_write` reports the dispatch itself
                // as having succeeded and puts the verdict inside the response.
                // Its payload is empty, so forwarding it unchecked would answer
                // `SELECT KV_INCR(...)` with one blank column and let the caller
                // read a refusal as a completed write. Every terminal outcome
                // these functions can produce arrives this way — a policy
                // refusal, a type mismatch, an overflow, an insufficient
                // balance, a missing key — so the status is what decides,
                // never the payload's emptiness.
                Ok(resp) if resp.status == Status::Error => {
                    return Err(data_plane_error(resp.error_code.map(|code| *code)));
                }
                Ok(resp) => resp.payload.as_ref().to_vec(),
                Err(e) => return Err(ddl_err("XX000", e.to_string())),
            }
        }
        // Every `KvOp` this module builds is stageable once in a
        // transaction (`is_stageable_write`), so `Buffered` never occurs;
        // handled defensively with an empty payload rather than a panic.
        Ok(InTxnRoute::Buffered) => Vec::new(),
        Ok(InTxnRoute::Staged(outcome)) => outcome.payload,
        Err(StagingGateError::Dispatch(e)) => return Err(ddl_err("XX000", e.to_string())),
        Err(StagingGateError::Rejected { code }) => return Err(data_plane_error(code)),
    };

    let payload_text = crate::data::executor::response_codec::decode_payload_to_json(&payload);
    let col_name = func_name.to_lowercase();
    Ok(vec![single_text_col(&col_name, payload_text)])
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
    let (_, sqlstate, message) = match code {
        Some(code) => crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(&code),
        None => ("ERROR", "XX000", "unknown data plane error".to_owned()),
    };
    ddl_err(sqlstate, message)
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
