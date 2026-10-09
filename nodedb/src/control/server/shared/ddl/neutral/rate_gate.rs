// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral rate gate / cooldown SQL functions.
//!
//! `SELECT RATE_CHECK(gate_name, key, max_count, window_secs)`
//!   — Atomically increments a counter for `(gate, key)`.
//!   — If counter > max_count → returns error with retry_after_ms.
//!   — Counter stored as KV key `_rate:{gate}:{key}` with TTL = window_secs.
//!   — Fixed window: TTL is set on first call only, not reset on subsequent calls.
//!
//! `SELECT RATE_REMAINING(gate_name, key, max_count, window_secs)`
//!   — Read-only: returns remaining budget and time until reset.
//!
//! `SELECT RATE_RESET(gate_name, key)`
//!   — Deletes the counter key (admin cooldown clear).

use crate::bridge::envelope::{ErrorCode, PhysicalPlan};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::response_payload::payload_or_typed_error;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};
use nodedb_physical::physical_plan::KvOp;

use super::super::result::{DdlError, DdlResult};
use super::kv_atomic::single_text_col;

/// Internal collection used for rate gate counters.
const RATE_COLLECTION: &str = "_system_rate_gates";

/// Handle `SELECT RATE_CHECK(gate_name, key, max_count, window_secs)`
pub async fn rate_check(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = super::kv_atomic::parse_function_args(sql, "RATE_CHECK")?;
    if args.len() < 4 {
        return Err(ddl_err(
            "42601",
            "RATE_CHECK requires 4 arguments: (gate_name, key, max_count, window_secs)",
        ));
    }

    let gate_name = unquote(&args[0]);
    let key = unquote(&args[1]);
    let max_count: i64 = parse_i64(&args[2], "RATE_CHECK", "max_count")?;
    let window_secs: u64 = parse_u64(&args[3], "RATE_CHECK", "window_secs")?;

    if max_count <= 0 {
        return Err(ddl_err("42601", "RATE_CHECK: max_count must be positive"));
    }
    if window_secs == 0 {
        return Err(ddl_err("42601", "RATE_CHECK: window_secs must be positive"));
    }

    let rate_key = format!("_rate:{gate_name}:{key}");
    let tenant_id = identity.tenant_id;
    let vshard =
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, RATE_COLLECTION).vshard();
    let ttl_ms = window_secs * 1000;

    // Fixed-window semantics: TTL is set ONLY on the first call (new key).
    // Subsequent calls within the window increment without resetting TTL.
    // To achieve this: check if key exists first, then INCR with ttl_ms=0
    // if it does, or ttl_ms=window if it doesn't.
    let key_exists = {
        let check = PhysicalPlan::Kv(KvOp::GetTtl {
            collection: nodedb_types::QualifiedCollection::new(
                DatabaseId::DEFAULT,
                RATE_COLLECTION,
            ),
            key: rate_key.as_bytes().to_vec(),
        });
        let result = crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state,
            tenant_id,
            crate::types::DatabaseId::DEFAULT,
            vshard,
            check,
            TraceId::ZERO,
        )
        .await;
        ttl_from("RATE_CHECK", result)?.is_some()
    };

    let actual_ttl = if key_exists { 0 } else { ttl_ms };

    let surrogate = crate::control::server::surrogate_exchange::assign_surrogate_routed(
        state,
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, RATE_COLLECTION),
        tenant_id,
        rate_key.as_bytes(),
        crate::types::TraceId::ZERO,
    )
    .await
    .map_err(|e| DdlError::from_error_in_context("RATE_CHECK", &e))?;
    let plan = PhysicalPlan::Kv(KvOp::Incr {
        collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, RATE_COLLECTION),
        key: rate_key.as_bytes().to_vec(),
        delta: 1,
        ttl_ms: actual_ttl,
        surrogate,
        // The rate-gate collection is internal bookkeeping, not user rows. It
        // is never user-created and can never carry an RLS policy, so this
        // dispatch bypasses the injection pass by design rather than pending
        // a decision that will never come.
        rls_write_check: nodedb_types::RlsWriteCheck::system_internal_collection(),
        // The rate-gate collection declares no columns: a counter is decimal
        // text, which `rate_remaining` reads back.
        shape: nodedb_physical::physical_plan::KvCounterShape::Raw,
    });

    let payload = counter_write_payload(
        "RATE_CHECK",
        dispatch_counter_write(state, tenant_id, vshard, plan).await,
    )?;
    let payload_text = crate::data::executor::response_codec::decode_payload_to_json(&payload);
    let current: i64 = sonic_rs::from_str::<serde_json::Value>(&payload_text)
        .ok()
        .and_then(|v| v.get("value")?.as_i64())
        .ok_or(DdlError::internal(format!(
            "RATE_CHECK: counter '{rate_key}' increment answered no integer value"
        )))?;

    if current > max_count {
        // Read TTL to compute retry_after_ms.
        let ttl_remaining = read_ttl_ms("RATE_CHECK", state, tenant_id, vshard, &rate_key).await?;
        Err(ddl_err(
            "53300",
            format!(
                "rate limit exceeded for {gate_name}:{key}, retry after {ttl_remaining}ms (current={current}, max={max_count})"
            ),
        ))
    } else {
        let result = serde_json::json!({
            "allowed": true,
            "current": current,
            "max_count": max_count,
            "remaining": max_count - current,
        });
        Ok(vec![single_text_col("rate_check", result.to_string())])
    }
}

/// Handle `SELECT RATE_REMAINING(gate_name, key, max_count, window_secs)`
pub async fn rate_remaining(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = super::kv_atomic::parse_function_args(sql, "RATE_REMAINING")?;
    if args.len() < 4 {
        return Err(ddl_err(
            "42601",
            "RATE_REMAINING requires 4 arguments: (gate_name, key, max_count, window_secs)",
        ));
    }

    let gate_name = unquote(&args[0]);
    let key = unquote(&args[1]);
    let max_count: i64 = parse_i64(&args[2], "RATE_REMAINING", "max_count")?;
    let _window_secs: u64 = parse_u64(&args[3], "RATE_REMAINING", "window_secs")?;

    let rate_key = format!("_rate:{gate_name}:{key}");
    let tenant_id = identity.tenant_id;
    let vshard =
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, RATE_COLLECTION).vshard();

    // Read current counter value (non-destructive).
    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, RATE_COLLECTION),
        key: rate_key.as_bytes().to_vec(),
        rls_filters: Vec::new(),
        surrogate_ceiling: None,
    });

    let result = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        crate::types::DatabaseId::DEFAULT,
        vshard,
        plan,
        TraceId::ZERO,
    )
    .await;
    let current = counter_from(&rate_key, result)?;

    let ttl_remaining = if current > 0 {
        read_ttl_ms("RATE_REMAINING", state, tenant_id, vshard, &rate_key).await?
    } else {
        0
    };

    let remaining = (max_count - current).max(0);
    let result = serde_json::json!({
        "remaining": remaining,
        "current": current,
        "max_count": max_count,
        "resets_in_ms": ttl_remaining,
    });
    Ok(vec![single_text_col("rate_remaining", result.to_string())])
}

/// Handle `SELECT RATE_RESET(gate_name, key)`
pub async fn rate_reset(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = super::kv_atomic::parse_function_args(sql, "RATE_RESET")?;
    if args.len() < 2 {
        return Err(ddl_err(
            "42601",
            "RATE_RESET requires 2 arguments: (gate_name, key)",
        ));
    }

    let gate_name = unquote(&args[0]);
    let key = unquote(&args[1]);

    let rate_key = format!("_rate:{gate_name}:{key}");
    let tenant_id = identity.tenant_id;
    let vshard =
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, RATE_COLLECTION).vshard();

    let plan = PhysicalPlan::Kv(KvOp::Delete {
        collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, RATE_COLLECTION),
        keys: vec![rate_key.as_bytes().to_vec()],
        // Internal bookkeeping collection — see `rate_gate`'s INCR above.
        rls_write_check: nodedb_types::RlsWriteCheck::system_internal_collection(),
        returning: None,
        rls_filters: Vec::new(),
        provenance: None,
    });

    // A refusal means the counter is still there, so the reset did not happen.
    counter_write_payload(
        "RATE_RESET",
        dispatch_counter_write(state, tenant_id, vshard, plan).await,
    )?;
    let result = serde_json::json!({
        "gate": gate_name,
        "key": key,
        "reset": true,
    });
    Ok(vec![single_text_col("rate_reset", result.to_string())])
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Apply a write to the rate-gate counter collection on the durable route:
/// Raft in cluster mode, else the write funnel's `AppendHere`. A counter
/// written any other way has no WAL record, so a crash resets the gate and a
/// replica never counts the call.
async fn dispatch_counter_write(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    vshard: VShardId,
    plan: PhysicalPlan,
) -> crate::Result<crate::bridge::envelope::Response> {
    crate::control::server::dispatch_utils::dispatch_durable_autocommit_write(
        state,
        crate::control::server::dispatch_utils::AutocommitWrite {
            tenant_id,
            database_id: DatabaseId::DEFAULT,
            vshard_id: vshard,
            plan,
            trace_id: TraceId::ZERO,
            event_source: crate::event::EventSource::User,
            txn_id: None,
        },
    )
    .await
}

/// The payload of a counter write, or its error with `context` before the
/// message. A refusal arrives as an error status inside an `Ok` response. A
/// coded refusal keeps its SQLSTATE and code. Only a refusal with no code is
/// `XX000`.
fn counter_write_payload(
    context: &str,
    result: crate::Result<crate::bridge::envelope::Response>,
) -> Result<Vec<u8>, DdlError> {
    result
        .and_then(payload_or_typed_error)
        .map_err(|e| DdlError::from_error_in_context(context, &e))
}

/// The `ttl_ms` a `GetTtl` read reports for a key that does not exist.
const TTL_ABSENT: i64 = -2;

/// The payload of a counter read, or `None` when the key is absent. A
/// `NotFound` verdict is a result, not an error. Every other refusal or
/// dispatch error keeps its SQLSTATE and code, with `context` before the
/// message.
fn read_payload(
    context: &str,
    result: crate::Result<crate::bridge::envelope::Response>,
) -> Result<Option<Vec<u8>>, DdlError> {
    match result.and_then(payload_or_typed_error) {
        Ok(payload) => Ok(Some(payload)),
        Err(crate::Error::DataPlane(ErrorCode::NotFound)) => Ok(None),
        Err(e) => Err(DdlError::from_error_in_context(context, &e)),
    }
}

/// The TTL a `GetTtl` read reports, or `None` when the key is absent.
fn ttl_from(
    context: &str,
    result: crate::Result<crate::bridge::envelope::Response>,
) -> Result<Option<i64>, DdlError> {
    let Some(payload) = read_payload(context, result)? else {
        return Ok(None);
    };
    let text = crate::data::executor::response_codec::decode_payload_to_json(&payload);
    let ttl_ms = sonic_rs::from_str::<serde_json::Value>(&text)
        .ok()
        .and_then(|v| v.get("ttl_ms")?.as_i64())
        .ok_or(DdlError::internal(format!(
            "{context}: TTL read answered no integer ttl_ms: {text}"
        )))?;
    Ok((ttl_ms != TTL_ABSENT).then_some(ttl_ms))
}

/// The counter a `Get` read reports. An absent key has no usage, so it
/// reads as `0`.
fn counter_from(
    rate_key: &str,
    result: crate::Result<crate::bridge::envelope::Response>,
) -> Result<i64, DdlError> {
    match read_payload("RATE_REMAINING", result)? {
        None => Ok(0),
        Some(payload) if payload.is_empty() => Ok(0),
        // `KV_INCR` stores the counter as a raw body: its decimal text.
        Some(payload) => std::str::from_utf8(&payload)
            .ok()
            .and_then(|text| text.parse::<i64>().ok())
            .ok_or(ddl_err(
                "22P02",
                format!(
                    "RATE_REMAINING: counter '{rate_key}' does not hold decimal text; \
                     reset the gate with RATE_RESET"
                ),
            )),
    }
}

/// Read TTL remaining for a KV key (in milliseconds). An absent key, or a
/// key with no expiry, reads as `0`. `context` names the calling function.
async fn read_ttl_ms(
    context: &str,
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    vshard: VShardId,
    key: &str,
) -> Result<u64, DdlError> {
    let plan = PhysicalPlan::Kv(KvOp::GetTtl {
        collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, RATE_COLLECTION),
        key: key.as_bytes().to_vec(),
    });

    let result = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        crate::types::DatabaseId::DEFAULT,
        vshard,
        plan,
        TraceId::ZERO,
    )
    .await;
    remaining_ttl_ms(context, result)
}

/// The milliseconds a `GetTtl` read leaves on a key. An absent key, or a key
/// with no expiry (`-1`), reads as `0`.
fn remaining_ttl_ms(
    context: &str,
    result: crate::Result<crate::bridge::envelope::Response>,
) -> Result<u64, DdlError> {
    Ok(ttl_from(context, result)?.map_or(0, |ttl| u64::try_from(ttl).unwrap_or(0)))
}

fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

fn parse_i64(s: &str, func: &str, param: &str) -> Result<i64, DdlError> {
    s.trim().parse().map_err(|_| {
        ddl_err(
            "42601",
            format!("{func}: {param} must be an integer, got '{}'", s.trim()),
        )
    })
}

fn parse_u64(s: &str, func: &str, param: &str) -> Result<u64, DdlError> {
    s.trim().parse().map_err(|_| {
        ddl_err(
            "42601",
            format!(
                "{func}: {param} must be a positive integer, got '{}'",
                s.trim()
            ),
        )
    })
}

fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

#[cfg(test)]
mod tests {
    use nodedb_types::error::sqlstate;

    use super::*;
    use crate::bridge::envelope::{Payload, Response, Status};
    use crate::types::{Lsn, RequestId};

    fn refusal(code: Option<ErrorCode>) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: code.map(Box::new),
            stage_vote: None,
            read_version_lsn: Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    /// A refused counter write keeps its SQLSTATE and code, with the function
    /// name before the message.
    #[test]
    fn a_coded_refusal_keeps_its_sqlstate() {
        let refused = refusal(Some(ErrorCode::Unsupported {
            detail: "not on this engine".into(),
        }));
        let err = counter_write_payload("RATE_CHECK", Ok(refused))
            .expect_err("a refused counter write fails the call");
        assert_eq!(err.sqlstate, sqlstate::FEATURE_NOT_SUPPORTED, "{err:?}");
        assert_eq!(err.code, nodedb_types::error::ErrorCode::SQL_NOT_ENABLED);
        assert!(err.message.starts_with("RATE_CHECK: "), "{}", err.message);
    }

    /// A dispatch error keeps its own class too.
    #[test]
    fn a_dispatch_error_keeps_its_sqlstate() {
        let deadline = crate::Error::DeadlineExceeded {
            request_id: RequestId::new(1),
        };
        let err = counter_write_payload("RATE_RESET", Err(deadline))
            .expect_err("a failed dispatch fails the call");
        assert_eq!(err.sqlstate, sqlstate::QUERY_CANCELED.0, "{err:?}");
    }

    /// A refusal with no code has no class of its own.
    #[test]
    fn a_refusal_with_no_code_is_internal() {
        let err = counter_write_payload("RATE_RESET", Ok(refusal(None)))
            .expect_err("a refused counter write fails the call");
        assert_eq!(err.sqlstate, sqlstate::INTERNAL_ERROR, "{err:?}");
    }

    fn answer(payload: Vec<u8>) -> Response {
        Response {
            status: Status::Ok,
            payload: Payload::from_vec(payload),
            error_code: None,
            ..refusal(None)
        }
    }

    /// The msgpack map `{"ttl_ms": ttl}` a `GetTtl` read answers with. `ttl`
    /// fits a msgpack fixint.
    fn ttl_payload(ttl: i8) -> Vec<u8> {
        let mut bytes = vec![0x81, 0xa6];
        bytes.extend_from_slice(b"ttl_ms");
        bytes.push(ttl.to_ne_bytes()[0]);
        bytes
    }

    fn deadline() -> crate::Error {
        crate::Error::DeadlineExceeded {
            request_id: RequestId::new(1),
        }
    }

    fn not_found() -> Response {
        refusal(Some(ErrorCode::NotFound))
    }

    /// The existence check propagates a dispatch error and a coded refusal
    /// with their own class, instead of reading them as "no key".
    #[test]
    fn the_existence_check_propagates_errors() {
        let err = ttl_from("RATE_CHECK", Err(deadline())).expect_err("a failed read fails");
        assert_eq!(err.sqlstate, sqlstate::QUERY_CANCELED.0, "{err:?}");
        assert!(err.message.starts_with("RATE_CHECK: "), "{}", err.message);

        let refused = refusal(Some(ErrorCode::Unsupported {
            detail: "not on this engine".into(),
        }));
        let err = ttl_from("RATE_CHECK", Ok(refused)).expect_err("a refused read fails");
        assert_eq!(err.sqlstate, sqlstate::FEATURE_NOT_SUPPORTED, "{err:?}");
    }

    /// An absent key reads as absent, whether the read answers `-2` or a
    /// `NotFound` verdict. A live key reads as present.
    #[test]
    fn the_existence_check_reads_absence_as_a_result() {
        assert_eq!(
            ttl_from("RATE_CHECK", Ok(answer(ttl_payload(-2)))).expect("read succeeds"),
            None
        );
        assert_eq!(
            ttl_from("RATE_CHECK", Ok(not_found())).expect("read succeeds"),
            None
        );
        assert_eq!(
            ttl_from("RATE_CHECK", Ok(answer(ttl_payload(30)))).expect("read succeeds"),
            Some(30)
        );
        assert_eq!(
            ttl_from("RATE_CHECK", Ok(answer(ttl_payload(-1)))).expect("read succeeds"),
            Some(-1)
        );
    }

    /// A TTL read that answers no `ttl_ms` is an internal error, never a guess.
    #[test]
    fn a_ttl_read_with_no_ttl_is_internal() {
        let err = ttl_from("RATE_CHECK", Ok(answer(Vec::new()))).expect_err("no ttl_ms fails");
        assert_eq!(err.sqlstate, sqlstate::INTERNAL_ERROR, "{err:?}");
    }

    /// The counter read propagates a dispatch error instead of reading it as
    /// no usage.
    #[test]
    fn the_counter_read_propagates_errors() {
        let err = counter_from("_rate:g:k", Err(deadline())).expect_err("a failed read fails");
        assert_eq!(err.sqlstate, sqlstate::QUERY_CANCELED.0, "{err:?}");
        assert!(
            err.message.starts_with("RATE_REMAINING: "),
            "{}",
            err.message
        );
    }

    /// An absent counter reads as zero usage. A stored counter reads as its
    /// decimal value.
    #[test]
    fn the_counter_read_reads_absence_as_zero() {
        assert_eq!(
            counter_from("_rate:g:k", Ok(answer(Vec::new()))).expect("read succeeds"),
            0
        );
        assert_eq!(
            counter_from("_rate:g:k", Ok(not_found())).expect("read succeeds"),
            0
        );
        assert_eq!(
            counter_from("_rate:g:k", Ok(answer(b"7".to_vec()))).expect("read succeeds"),
            7
        );
    }

    /// A counter that holds text other than a decimal is a stored value the
    /// read cannot parse: `22P02`, never an internal error.
    #[test]
    fn a_non_decimal_counter_is_invalid_text() {
        let err = counter_from("_rate:g:k", Ok(answer(b"abc".to_vec())))
            .expect_err("a non-decimal counter fails the read");
        assert_eq!(
            err.sqlstate,
            sqlstate::INVALID_TEXT_REPRESENTATION,
            "{err:?}"
        );
        assert_eq!(err.code, nodedb_types::error::ErrorCode::DATA_EXCEPTION);
    }

    /// The TTL read behind `retry after` propagates a dispatch error instead
    /// of reading it as no time left.
    #[test]
    fn the_ttl_read_propagates_errors() {
        let err =
            remaining_ttl_ms("RATE_REMAINING", Err(deadline())).expect_err("a failed read fails");
        assert_eq!(err.sqlstate, sqlstate::QUERY_CANCELED.0, "{err:?}");
    }

    /// An absent key and a key with no expiry leave no time. A live key
    /// leaves its TTL.
    #[test]
    fn the_ttl_read_reads_absence_as_zero() {
        assert_eq!(
            remaining_ttl_ms("RATE_REMAINING", Ok(not_found())).expect("read succeeds"),
            0
        );
        for (ttl, expected) in [(-2, 0), (-1, 0), (30, 30)] {
            let remaining = remaining_ttl_ms("RATE_REMAINING", Ok(answer(ttl_payload(ttl))))
                .expect("read succeeds");
            assert_eq!(remaining, expected, "ttl_ms {ttl}");
        }
    }
}
