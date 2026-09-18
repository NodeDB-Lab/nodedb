// SPDX-License-Identifier: BUSL-1.1

//! RESP handlers for Redis-compatible sorted set commands.
//!
//! ZADD, ZREM, ZRANK, ZREVRANK, ZRANGE, ZRANGEBYSCORE, ZCARD, ZSCORE, ZINCRBY.
//!
//! These commands operate on a sorted index (created via CREATE SORTED INDEX).
//! The sorted index name is the RESP session's current collection (SELECT db).

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::KvOp;
use nodedb_types::QualifiedCollection;

use super::codec::RespValue;
use super::command::RespCommand;
use super::handler::{dispatch_kv, dispatch_kv_write};
use super::payload::{payload_field_i64, payload_json};
use super::redaction::resp_redaction;
use super::session::RespSession;
use crate::control::server::response_shape::redaction::redact_envelope_row;

/// ZADD key score member [score member ...]
///
/// Adds members with scores to a sorted index. The "key" is the sorted index name.
/// Each score/member pair is inserted by writing to the underlying KV collection
/// (which triggers sorted index auto-maintenance).
///
/// For RESP compatibility, ZADD dispatches a KV PUT with the score embedded
/// in the value, which auto-updates the sorted index.
pub(super) async fn handle_zadd(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    // ZADD needs at least: key score member
    if cmd.argc() < 3 || !cmd.argc().is_multiple_of(2) {
        return RespValue::err("ERR wrong number of arguments for 'zadd' command");
    }

    // In RESP mode, the sorted index name = session.collection.
    // The args are: score1 member1 [score2 member2 ...]
    let index_name = session.collection.clone();
    let mut added = 0i64;

    let mut i = 0;
    while i + 1 < cmd.argc() {
        let score_str = match cmd.arg_str(i) {
            Some(s) => s,
            None => return RespValue::err("ERR value is not a valid float"),
        };
        let score: f64 = match score_str.parse() {
            Ok(v) => v,
            Err(_) => return RespValue::err("ERR value is not a valid float"),
        };
        let member = cmd.args[i + 1].clone();

        // Write to the underlying KV collection as a MessagePack document
        // containing the score and member. The sorted index auto-maintenance
        // in KvEngine::put will update the order-statistic tree.
        let value = nodedb_types::json_to_msgpack(&serde_json::json!({
            "score": score,
            "member": String::from_utf8_lossy(&member),
        }))
        .unwrap_or_default();

        let surrogate = match state.surrogate_assigner.assign(
            crate::types::DatabaseId::DEFAULT,
            session.tenant_id,
            &index_name,
            &member,
        ) {
            Ok(s) => s,
            Err(e) => return RespValue::from_error(&e),
        };
        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(crate::types::DatabaseId::DEFAULT, &index_name),
            key: member,
            value,
            ttl_ms: 0,
            surrogate,
            returning: None,
            rls_filters: Vec::new(),
        });

        match dispatch_kv_write(state, session, plan).await {
            Ok(_) => added += 1,
            Err(e) => return RespValue::from_error(&e),
        }

        i += 2;
    }

    RespValue::integer(added)
}

/// ZREM key member [member ...]
pub(super) async fn handle_zrem(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 1 {
        return RespValue::err("ERR wrong number of arguments for 'zrem' command");
    }

    let keys: Vec<Vec<u8>> = cmd.args.clone();
    let plan = PhysicalPlan::Kv(KvOp::Delete {
        collection: QualifiedCollection::new(
            crate::types::DatabaseId::DEFAULT,
            &session.collection,
        ),
        keys,
        // Filled by the RLS injection pass `dispatch_kv_write` runs.
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        // RESP has no RETURNING clause.
        returning: None,
        rls_filters: Vec::new(),
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) => {
            let count = payload_field_i64(&resp.payload, "deleted").unwrap_or(0);
            RespValue::integer(count)
        }
        Err(e) => RespValue::from_error(&e),
    }
}

/// ZRANK key member — returns 0-based rank (Redis convention).
pub(super) async fn handle_zrank(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    let Some(member) = cmd.arg(0) else {
        return RespValue::err("ERR wrong number of arguments for 'zrank' command");
    };

    let plan = PhysicalPlan::Kv(KvOp::SortedIndexRank {
        index_name: session.collection.clone(),
        primary_key: member.to_vec(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let rank = payload_field_i64(&resp.payload, "rank");
            match rank {
                Some(r) if r > 0 => RespValue::integer(r - 1), // Convert to 0-based.
                _ => RespValue::nil(),
            }
        }
        Ok(_) => RespValue::nil(),
        Err(e) => RespValue::from_error(&e),
    }
}

/// ZRANGE key start stop — returns members in rank range [start, stop] (0-based).
pub(super) async fn handle_zrange(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    if cmd.argc() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'zrange' command");
    }

    // Resolved once for this command, before the dispatch whose rows it covers.
    let redaction = resp_redaction(state, session);

    let start = cmd.arg_i64(0).unwrap_or(0);
    let stop = cmd.arg_i64(1).unwrap_or(-1);

    // Fetch enough entries to cover the range.
    // ZRANGE uses 0-based inclusive indices; negative means from end.
    // We'll fetch top_k with k = stop + 1 (or all if stop is negative).
    let k = if stop < 0 {
        u32::MAX
    } else {
        (stop + 1) as u32
    };

    let plan = PhysicalPlan::Kv(KvOp::SortedIndexTopK {
        index_name: session.collection.clone(),
        k,
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let mut rows: Vec<serde_json::Value> = match payload_json(&resp.payload) {
                serde_json::Value::Array(rows) => rows,
                _ => Vec::new(),
            };
            // Each row is `{rank, key}`: `rank` is index metadata, `key` is the
            // member's primary key — the same column a KV scan lists, so the
            // same rule covers it here.
            for row in &mut rows {
                redact_envelope_row(redaction.as_ref(), &state.redaction, row);
            }
            let total = rows.len() as i64;

            let actual_start = if start < 0 {
                (total + start).max(0) as usize
            } else {
                start as usize
            };
            let actual_stop = if stop < 0 {
                (total + stop).max(0) as usize
            } else {
                (stop as usize).min(rows.len().saturating_sub(1))
            };

            if actual_start > actual_stop || actual_start >= rows.len() {
                return RespValue::array(vec![]);
            }

            let items: Vec<RespValue> = rows[actual_start..=actual_stop]
                .iter()
                .filter_map(|row| {
                    row.get("key")
                        .and_then(|v| v.as_str())
                        .map(|k| RespValue::bulk(k.as_bytes().to_vec()))
                })
                .collect();
            RespValue::array(items)
        }
        Ok(_) => RespValue::array(vec![]),
        Err(e) => RespValue::from_error(&e),
    }
}

/// ZCARD key — returns cardinality.
pub(super) async fn handle_zcard(session: &RespSession, state: &SharedState) -> RespValue {
    let plan = PhysicalPlan::Kv(KvOp::SortedIndexCount {
        index_name: session.collection.clone(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let count = payload_field_i64(&resp.payload, "count").unwrap_or(0);
            RespValue::integer(count)
        }
        Ok(_) => RespValue::integer(0),
        Err(e) => RespValue::from_error(&e),
    }
}

/// ZSCORE key member — returns score as bulk string.
pub(super) async fn handle_zscore(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    let Some(member) = cmd.arg(0) else {
        return RespValue::err("ERR wrong number of arguments for 'zscore' command");
    };

    // Resolved once for this command, before the dispatch whose value it covers.
    let redaction = resp_redaction(state, session);

    let plan = PhysicalPlan::Kv(KvOp::SortedIndexScore {
        index_name: session.collection.clone(),
        primary_key: member.to_vec(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            // The payload names its one cell `score`, which is also the field
            // ZADD stores the ordering value under, so a rule on `score`
            // covers this read the same way it covers a SELECT of that column.
            let mut json = payload_json(&resp.payload);
            redact_envelope_row(redaction.as_ref(), &state.redaction, &mut json);
            if let Some(serde_json::Value::String(score)) = json.get("score") {
                if score == "null" {
                    return RespValue::nil();
                }
                return RespValue::bulk(score.as_bytes().to_vec());
            }
            RespValue::nil()
        }
        Ok(_) => RespValue::nil(),
        Err(e) => RespValue::from_error(&e),
    }
}
