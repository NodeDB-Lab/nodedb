//! RESP handlers for Redis-compatible sorted set commands.
//!
//! ZADD, ZREM, ZRANK, ZREVRANK, ZRANGE, ZRANGEBYSCORE, ZCARD, ZSCORE, ZINCRBY.
//!
//! These commands operate on a sorted index (created via CREATE SORTED INDEX).
//! The sorted index name is the RESP session's current collection (SELECT db).

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::bridge::physical_plan::KvOp;
use crate::control::state::SharedState;

use super::codec::RespValue;
use super::command::RespCommand;
use super::handler::{dispatch_kv, dispatch_kv_write, parse_json_field_i64};
use super::session::RespSession;

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
        let value = rmp_serde::to_vec(&serde_json::json!({
            "score": score,
            "member": String::from_utf8_lossy(&member),
        }))
        .unwrap_or_default();

        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: index_name.clone(),
            key: member,
            value,
            ttl_ms: 0,
        });

        match dispatch_kv_write(state, session, plan).await {
            Ok(_) => added += 1,
            Err(e) => return RespValue::err(format!("ERR {e}")),
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
        collection: session.collection.clone(),
        keys,
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) => {
            let count = parse_json_field_i64(&resp.payload, "deleted").unwrap_or(0);
            RespValue::integer(count)
        }
        Err(e) => RespValue::err(format!("ERR {e}")),
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
            let rank = parse_json_field_i64(&resp.payload, "rank");
            match rank {
                Some(r) if r > 0 => RespValue::integer(r - 1), // Convert to 0-based.
                _ => RespValue::nil(),
            }
        }
        Ok(_) => RespValue::nil(),
        Err(e) => RespValue::err(format!("ERR {e}")),
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
            let rows: Vec<serde_json::Value> =
                serde_json::from_slice(&resp.payload).unwrap_or_default();
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
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}

/// ZCARD key — returns cardinality.
pub(super) async fn handle_zcard(session: &RespSession, state: &SharedState) -> RespValue {
    let plan = PhysicalPlan::Kv(KvOp::SortedIndexCount {
        index_name: session.collection.clone(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let count = parse_json_field_i64(&resp.payload, "count").unwrap_or(0);
            RespValue::integer(count)
        }
        Ok(_) => RespValue::integer(0),
        Err(e) => RespValue::err(format!("ERR {e}")),
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

    let plan = PhysicalPlan::Kv(KvOp::SortedIndexScore {
        index_name: session.collection.clone(),
        primary_key: member.to_vec(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let payload_text =
                crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&payload_text)
                && let Some(serde_json::Value::String(score)) = json.get("score")
            {
                if score == "null" {
                    return RespValue::nil();
                }
                return RespValue::bulk(score.as_bytes().to_vec());
            }
            RespValue::nil()
        }
        Ok(_) => RespValue::nil(),
        Err(e) => RespValue::err(format!("ERR {e}")),
    }
}
