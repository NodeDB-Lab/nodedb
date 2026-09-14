// SPDX-License-Identifier: BUSL-1.1

//! RESP command handlers: translate Redis commands into KvOp dispatches.

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::credential::store::{AuthRejection, PasswordVerification};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::KvOp;
use nodedb_types::{DatabaseId, QualifiedCollection};

use super::codec::RespValue;
use super::command::RespCommand;
// Re-export for sub-handlers that import via `super::handler::dispatch_kv` etc.
pub(super) use super::gateway_dispatch::{dispatch_kv, dispatch_kv_write};
use super::payload::{payload_field_i64, redacted_scan_keys, scan_keys};
use super::redaction::resp_redaction;
use super::session::RespSession;

/// Execute a RESP command and return the response.
pub async fn execute(
    cmd: &RespCommand,
    session: &mut RespSession,
    state: &SharedState,
) -> RespValue {
    match cmd.name.as_str() {
        "PING" => handle_ping(cmd),
        "ECHO" => handle_echo(cmd),
        "SELECT" => handle_select(cmd, session),
        "DBSIZE" => handle_dbsize(session, state).await,
        "GET" => super::handler_kv::handle_get(cmd, session, state).await,
        "SET" => super::handler_kv::handle_set(cmd, session, state).await,
        "DEL" => super::handler_kv::handle_del(cmd, session, state).await,
        "EXISTS" => super::handler_kv::handle_exists(cmd, session, state).await,
        "MGET" => super::handler_kv::handle_mget(cmd, session, state).await,
        "MSET" => super::handler_kv::handle_mset(cmd, session, state).await,
        "INCR" => super::handler_kv::handle_incr(cmd, session, state, 1).await,
        "DECR" => super::handler_kv::handle_incr(cmd, session, state, -1).await,
        "INCRBY" => super::handler_kv::handle_incrby(cmd, session, state).await,
        "DECRBY" => super::handler_kv::handle_decrby(cmd, session, state).await,
        "INCRBYFLOAT" => super::handler_kv::handle_incrbyfloat(cmd, session, state).await,
        "GETSET" => super::handler_kv::handle_getset(cmd, session, state).await,
        "ZADD" => super::handler_sorted::handle_zadd(cmd, session, state).await,
        "ZREM" => super::handler_sorted::handle_zrem(cmd, session, state).await,
        "ZRANK" => super::handler_sorted::handle_zrank(cmd, session, state).await,
        "ZRANGE" => super::handler_sorted::handle_zrange(cmd, session, state).await,
        "ZCARD" => super::handler_sorted::handle_zcard(session, state).await,
        "ZSCORE" => super::handler_sorted::handle_zscore(cmd, session, state).await,
        "EXPIRE" => handle_expire(cmd, session, state, false).await,
        "PEXPIRE" => handle_expire(cmd, session, state, true).await,
        "TTL" => handle_ttl(cmd, session, state, false).await,
        "PTTL" => handle_ttl(cmd, session, state, true).await,
        "PERSIST" => handle_persist(cmd, session, state).await,
        "SCAN" => handle_scan(cmd, session, state).await,
        "KEYS" => handle_keys(cmd, session, state).await,
        "HGET" => super::handler_hash::handle_hget(cmd, session, state).await,
        "HMGET" => super::handler_hash::handle_hmget(cmd, session, state).await,
        "HSET" => super::handler_hash::handle_hset(cmd, session, state).await,
        "FLUSHDB" => super::handler_hash::handle_flushdb(session, state).await,
        "AUTH" => handle_auth(cmd, session, state),
        "PUBLISH" => super::handler_pubsub::handle_publish(cmd, session, state).await,
        "INFO" => handle_info(cmd, session, state).await,
        "COMMAND" => RespValue::ok(), // Stub: redis-cli sends COMMAND on connect.
        "QUIT" => RespValue::ok(),
        _ => RespValue::err(format!("ERR unknown command '{}'", cmd.name)),
    }
}

// ---------------------------------------------------------------------------
// Simple commands
// ---------------------------------------------------------------------------

fn handle_ping(cmd: &RespCommand) -> RespValue {
    match cmd.arg(0) {
        Some(msg) => RespValue::bulk(msg.to_vec()),
        None => RespValue::SimpleString("PONG".into()),
    }
}

fn handle_echo(cmd: &RespCommand) -> RespValue {
    match cmd.arg(0) {
        Some(msg) => RespValue::bulk(msg.to_vec()),
        None => RespValue::err("ERR wrong number of arguments for 'echo' command"),
    }
}

fn handle_select(cmd: &RespCommand, session: &mut RespSession) -> RespValue {
    match cmd.arg_str(0) {
        Some(name) if is_internal_collection(name) => {
            RespValue::err("NOPERM the internal catalog collection cannot be selected")
        }
        Some(name) => {
            session.collection = name.to_string();
            RespValue::ok()
        }
        None => RespValue::err("ERR wrong number of arguments for 'select' command"),
    }
}

/// Whether `name` addresses server-internal catalog storage.
///
/// Authorization refuses these collections at dispatch, but SELECT is where the
/// client names one, and refusing it there both reports the mistake at its
/// source and keeps the session from carrying an internal collection in its
/// state at all.
fn is_internal_collection(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name == "_system" || name.starts_with("_system.")
}

/// AUTH [username] password
///
/// Redis supports two forms:
/// - `AUTH password` — authenticates with default username "nodedb"
/// - `AUTH username password` — authenticates with explicit username
///
/// On success, updates `session.tenant_id` from the authenticated identity.
fn handle_auth(cmd: &RespCommand, session: &mut RespSession, state: &SharedState) -> RespValue {
    // A missing or non-UTF-8 argument is a malformed command, not an empty
    // credential: falling back to "" would send a blank password down the
    // verification path and charge the caller a lockout strike for it.
    let (username, password) = match cmd.argc() {
        1 => match cmd.arg_str(0) {
            Some(password) => ("nodedb", password),
            None => return RespValue::err("ERR invalid password for 'auth' command"),
        },
        2 => match (cmd.arg_str(0), cmd.arg_str(1)) {
            (Some(username), Some(password)) => (username, password),
            _ => return RespValue::err("ERR invalid arguments for 'auth' command"),
        },
        _ => return RespValue::err("ERR wrong number of arguments for 'auth' command"),
    };

    // Validate credentials using the same path as native/pgwire auth.
    state.credentials.check_lockout(username).ok();

    match state
        .credentials
        .verify_password_with_status(username, password)
    {
        PasswordVerification::Verified(_) => {}
        PasswordVerification::Rejected(reason) => {
            // Only a genuine credential failure counts toward the lockout
            // counter. A policy rejection (expired / must-change password,
            // inactive account) or an internal error must not.
            if reason == AuthRejection::BadCredential {
                let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
                state
                    .credentials
                    .record_login_failure(username, None, &emitter);
            }
            state.auth_metrics.record_auth_failure("resp_password");
            return RespValue::err("WRONGPASS invalid username-password pair");
        }
    }

    state.credentials.record_login_success(username);

    // Resolve identity to get tenant_id.
    match state.credentials.to_identity(
        username,
        crate::control::security::identity::AuthMethod::CleartextPassword,
    ) {
        Some(identity) => {
            // The TLS policy runs before the identity is bound to the session:
            // a refused connection must leave the session unauthenticated, so
            // every data command keeps failing closed.
            if let Err(e) = crate::control::server::session_auth::check_transport_security(
                state,
                &identity,
                session.transport,
                &session.peer_addr,
            ) {
                return RespValue::err(format!("NOPERM {e}"));
            }
            session.tenant_id = identity.tenant_id;
            session.identity = Some(identity);
            state.auth_metrics.record_auth_success("resp_password");
            RespValue::ok()
        }
        None => RespValue::err("ERR user not found after authentication"),
    }
}

// ---------------------------------------------------------------------------
// TTL commands
// ---------------------------------------------------------------------------

async fn handle_expire(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
    is_pexpire: bool,
) -> RespValue {
    if cmd.argc() < 2 {
        let name = if is_pexpire { "pexpire" } else { "expire" };
        return RespValue::err(format!(
            "ERR wrong number of arguments for '{name}' command"
        ));
    }

    let key = cmd.args[0].clone();
    let ttl_ms = match cmd.arg_i64(1) {
        Some(v) if v > 0 => {
            if is_pexpire {
                v as u64
            } else {
                (v as u64) * 1000
            }
        }
        _ => return RespValue::err("ERR value is not an integer or out of range"),
    };

    let plan = PhysicalPlan::Kv(KvOp::Expire {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
        key,
        ttl_ms,
        // Filled by the RLS injection pass `dispatch_kv_write` runs.
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => RespValue::integer(1),
        Ok(_) => RespValue::integer(0),
        Err(e) => RespValue::from_error(&e),
    }
}

async fn handle_ttl(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
    is_pttl: bool,
) -> RespValue {
    let Some(key) = cmd.arg(0) else {
        let name = if is_pttl { "pttl" } else { "ttl" };
        return RespValue::err(format!(
            "ERR wrong number of arguments for '{name}' command"
        ));
    };

    let plan = PhysicalPlan::Kv(KvOp::GetTtl {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
        key: key.to_vec(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            let ttl_ms = payload_field_i64(&resp.payload, "ttl_ms").unwrap_or(-2);
            if ttl_ms < 0 {
                // -1 (no TTL) or -2 (not found) — same for both TTL and PTTL.
                RespValue::integer(ttl_ms)
            } else if is_pttl {
                RespValue::integer(ttl_ms)
            } else {
                // TTL returns seconds, round up to avoid reporting 0 for sub-second TTLs.
                RespValue::integer((ttl_ms + 999) / 1000)
            }
        }
        Ok(_) => RespValue::integer(-2),
        Err(e) => RespValue::from_error(&e),
    }
}

async fn handle_persist(
    cmd: &RespCommand,
    session: &RespSession,
    state: &SharedState,
) -> RespValue {
    let Some(key) = cmd.arg(0) else {
        return RespValue::err("ERR wrong number of arguments for 'persist' command");
    };

    let plan = PhysicalPlan::Kv(KvOp::Persist {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
        key: key.to_vec(),
        // Filled by the RLS injection pass `dispatch_kv_write` runs.
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
    });

    match dispatch_kv_write(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => RespValue::integer(1),
        Ok(_) => RespValue::integer(0),
        Err(e) => RespValue::from_error(&e),
    }
}

// ---------------------------------------------------------------------------
// SCAN / KEYS
// ---------------------------------------------------------------------------

async fn handle_scan(cmd: &RespCommand, session: &RespSession, state: &SharedState) -> RespValue {
    // Resolved once for this command, before the dispatch whose rows it covers.
    let redaction = resp_redaction(state, session);
    let cursor_str = cmd.arg_str(0).unwrap_or("0");
    let cursor = if cursor_str == "0" {
        Vec::new()
    } else {
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, cursor_str)
            .unwrap_or_default()
    };

    // Parse MATCH, COUNT, and FILTER options.
    let mut match_pattern: Option<String> = None;
    let mut count: usize = 10;
    let mut filter_bytes: Vec<u8> = Vec::new();
    let mut i = 1;
    while i < cmd.argc() {
        match cmd.arg_str(i).map(|s| s.to_uppercase()) {
            Some(ref flag) if flag == "MATCH" => {
                match_pattern = cmd.arg_str(i + 1).map(|s| s.to_string());
                i += 2;
            }
            Some(ref flag) if flag == "COUNT" => {
                count = cmd.arg_i64(i + 1).unwrap_or(10) as usize;
                i += 2;
            }
            // NodeDB extension: SCAN 0 FILTER <field> = <value>
            Some(ref flag) if flag == "FILTER" && i + 4 <= cmd.argc() => {
                // Parse simple "field = value" predicate (needs 4 args: FILTER field = value).
                let field = cmd.arg_str(i + 1).unwrap_or("");
                let _op = cmd.arg_str(i + 2).unwrap_or(""); // "=" expected
                let value = cmd.arg_str(i + 3).unwrap_or("");
                let scan_filter = serde_json::json!([{
                    "field": field,
                    "op": "eq",
                    "value": value,
                }]);
                match nodedb_types::json_to_msgpack(&scan_filter) {
                    Ok(bytes) => filter_bytes = bytes,
                    Err(_) => {
                        return RespValue::err("ERR filter serialization failed");
                    }
                }
                i += 4;
            }
            _ => {
                i += 1;
            }
        }
    }

    let plan = PhysicalPlan::Kv(KvOp::Scan {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
        cursor,
        count,
        filters: filter_bytes,
        match_pattern,
        sort_keys: Vec::new(),
        surrogate_ceiling: None,
        projection: Vec::new(),
        computed_columns: Vec::new(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            match redacted_scan_keys(&resp.payload, redaction.as_ref(), &state.redaction) {
                // Cursor "0" signals scan complete (no pagination in this path).
                Some(keys) => RespValue::array(vec![
                    RespValue::bulk_str("0"),
                    RespValue::array(keys.into_iter().map(RespValue::bulk).collect()),
                ]),
                None => {
                    tracing::warn!("RESP SCAN: failed to decode KV scan payload");
                    RespValue::err("ERR scan result could not be decoded")
                }
            }
        }
        Ok(_) => RespValue::array(vec![RespValue::bulk_str("0"), RespValue::array(vec![])]),
        Err(e) => RespValue::from_error(&e),
    }
}

async fn handle_keys(cmd: &RespCommand, session: &RespSession, state: &SharedState) -> RespValue {
    // Resolved once for this command, before the dispatch whose rows it covers.
    let redaction = resp_redaction(state, session);
    let pattern = cmd.arg_str(0).unwrap_or("*");

    let plan = PhysicalPlan::Kv(KvOp::Scan {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
        cursor: Vec::new(),
        count: 100_000,
        filters: Vec::new(),
        match_pattern: Some(pattern.to_string()),
        sort_keys: Vec::new(),
        surrogate_ceiling: None,
        projection: Vec::new(),
        computed_columns: Vec::new(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => {
            match redacted_scan_keys(&resp.payload, redaction.as_ref(), &state.redaction) {
                Some(keys) => RespValue::array(keys.into_iter().map(RespValue::bulk).collect()),
                None => {
                    tracing::warn!("RESP KEYS: failed to decode KV scan payload");
                    RespValue::err("ERR keys result could not be decoded")
                }
            }
        }
        Ok(_) => RespValue::array(vec![]),
        Err(e) => RespValue::from_error(&e),
    }
}

// ---------------------------------------------------------------------------
// Info / stats
// ---------------------------------------------------------------------------

async fn handle_dbsize(session: &RespSession, state: &SharedState) -> RespValue {
    let plan = PhysicalPlan::Kv(KvOp::Scan {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, &session.collection),
        cursor: Vec::new(),
        count: 0,
        filters: Vec::new(),
        match_pattern: None,
        sort_keys: Vec::new(),
        surrogate_ceiling: None,
        projection: Vec::new(),
        computed_columns: Vec::new(),
    });

    match dispatch_kv(state, session, plan).await {
        Ok(resp) if resp.status == Status::Ok => match scan_keys(&resp.payload) {
            Some(keys) => RespValue::integer(keys.len() as i64),
            None => {
                tracing::warn!("RESP DBSIZE: failed to decode KV scan payload");
                RespValue::err("ERR dbsize result could not be decoded")
            }
        },
        Ok(_) => RespValue::integer(0),
        Err(e) => RespValue::from_error(&e),
    }
}

async fn handle_info(_cmd: &RespCommand, session: &RespSession, _state: &SharedState) -> RespValue {
    let info = format!(
        "# Server\r\nnodedb_version:{}\r\n\r\n# Keyspace\r\ndb:{}\r\n",
        crate::version::VERSION,
        session.collection
    );
    RespValue::bulk(info.into_bytes())
}
